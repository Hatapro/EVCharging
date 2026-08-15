"""
Uso:
python EV_Central.py --kafka 192.168.18.148:9092 --tcp-port 5000 --web-port 5001 --db-api 192.168.18.148:7100 --weather 192.168.18.148:8000
"""

import argparse
import json
import socket
import threading
import time
import os
import base64
import hashlib
import requests
import signal
import sys
import traceback

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from io import BytesIO
from datetime import datetime
from confluent_kafka import Consumer, Producer
from colorama import Fore, Style, init as colorama_init
from flask import Flask, render_template, jsonify, request, send_file
from flask_socketio import SocketIO, emit
from protocol import recibir_mensaje, enviar_mensaje

colorama_init(autoreset=True)

DISCONNECT_TIMEOUT = 3.0


# Utilidades de tiempo y logging
def now_ts():
    return time.time()


def now_iso():
    return datetime.now().isoformat() + "Z"


def log(tag, msg, color=Fore.CYAN):
    print(f"{color}[{tag}] {msg}{Style.RESET_ALL}")


# Helpers para comunicación con EV_DB_API
db_api_url = None
weather_service_url = None


# Función de comunicación GET
def db_get(path):
    try:
        url = f"{db_api_url}{path}"
        resp = requests.get(url, timeout=5)

        return resp.status_code, resp.json() if resp.content else {}

    except Exception as e:
        log("DB-API", f"Error GET {path}: {e}", Fore.RED)

        return 500, {}


# Función de comunicación POST
def db_post(path, payload):
    try:
        url = f"{db_api_url}{path}"
        resp = requests.post(url, json=payload, timeout=5)

        return resp.status_code, resp.json() if resp.content else {}

    except Exception as e:
        log("DB-API", f"Error POST {path}: {e}", Fore.RED)

        return 500, {}


# Función para consultar temperatura a EV_W
def get_temperature(ubicacion, force_refresh=False):
    if not weather_service_url:
        log("WEATHER", "weather_service no configurado", Fore.RED)

        return None

    try:
        url = f"{weather_service_url}/api/weather/temperature?location={ubicacion}"

        if force_refresh:
            url += "&force_refresh=true"

        log("WEATHER", f"Consultando temperatura: {url}", Fore.CYAN)
        resp = requests.get(url, timeout=3)
        log("WEATHER", f"Respuesta {resp.status_code}: {resp.text[:200]}", Fore.CYAN)

        if resp.status_code == 200:
            temp = resp.json().get("temperature", 0)
            log(
                "WEATHER",
                f"Temperatura obtenida: {temp}°C para {ubicacion}",
                Fore.GREEN,
            )

            return temp

        else:
            log(
                "WEATHER",
                f"Error consultando temperatura para {ubicacion}: {resp.status_code}",
                Fore.YELLOW,
            )

            return None

    except Exception as e:
        log("WEATHER", f"Error consultando temperatura: {e}", Fore.RED)
        traceback.print_exc()

        return None


# Flask y SocketIO setup
app = Flask(__name__)
app.config["SECRET_KEY"] = "ev-charging-secret-2025"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# Variables globales
kafka_bootstrap = None
tcp_port = None
web_port = None
cp_registry = {}
registry_lock = threading.Lock()
stop_event = threading.Event()
producer = None


# Ruta principal
@app.route("/")
def index():
    return render_template("central.html", kafka=kafka_bootstrap, port=tcp_port)


# Conexión de cliente WebSocket
@socketio.on("connect")
def on_connect():
    log("WEB", "Cliente conectado", Fore.GREEN)

    with registry_lock:
        emit("cp_registry", {"cps": cp_registry})


# Desconexión de cliente WebSocket
@socketio.on("disconnect")
def on_disconnect():
    log("WEB", "Desconectado", Fore.YELLOW)


# API para recibir alertas de clima desde EV_W
@app.route("/api/internal/weather_alert", methods=["POST"])
def api_weather_alert():
    data = request.get_json() or {}
    location = data.get("location", "Unknown")
    alert_type = data.get("alert_type", "weather_alert")
    severity = data.get("severity", "medium")
    details = data.get("details", "")
    cp_id = data.get("cp_id", "Unknown")
    timestamp = data.get("timestamp", now_iso())
    log(
        "WEATHER-ALERT",
        f"{alert_type} en {location} (CP: {cp_id}, severity: {severity}): {details}",
        Fore.YELLOW,
    )

    db_post(
        "/api/db/audit",
        {
            "timestamp": timestamp,
            "source_type": "Weather",
            "source_id": cp_id,
            "source_ip": request.remote_addr,
            "action": "WEATHER_ALERT",
            "details": json.dumps({"details": details}),
        },
    )

    try:
        socketio.emit(
            "weather_alert",
            {
                "location": location,
                "alert_type": alert_type,
                "severity": severity,
                "details": details,
                "cp_id": cp_id,
                "timestamp": timestamp,
            },
        )

    except Exception as e:
        log("WEBSOCKET", f"Error enviando alerta por WebSocket: {e}", Fore.YELLOW)

    return jsonify({"status": "ok"}), 200


# API para recibir cambios de estado del CP desde EV_W
@app.route("/api/engine/cp_state_change", methods=["POST"])
def api_engine_state_change():
    data = request.get_json() or {}
    cp_id = data.get("cp_id", "Unknown")
    new_state = data.get("new_state", "activo")
    reason = data.get("reason", "weather")
    log(
        "ENGINE-STATE",
        f"CP {cp_id} cambiará a '{new_state}' (razón: {reason})",
        Fore.YELLOW,
    )

    with registry_lock:
        if cp_id in cp_registry:
            cp_registry[cp_id]["estado"] = new_state

            if reason == "weather" and new_state == "averiado":
                cp_registry[cp_id]["weather_blocked"] = True
                log(
                    "ENGINE-STATE",
                    f"{cp_id} estado=averiado, weather_blocked=True en Central",
                    Fore.GREEN,
                )

            elif reason == "weather" and new_state == "activo":
                cp_registry[cp_id]["weather_blocked"] = False
                log(
                    "ENGINE-STATE",
                    f"{cp_id} estado=activo, weather_blocked=False en Central",
                    Fore.GREEN,
                )

            socketio.emit(
                "cp_updated",
                {
                    "cp_id": cp_id,
                    "estado": new_state,
                    "weather_blocked": cp_registry[cp_id].get("weather_blocked", False),
                },
            )

    try:
        resp = requests.get(f"{db_api_url}/api/db/cp/{cp_id}/info", timeout=5)

        if resp.status_code != 200:
            log("ENGINE-STATE", f"No encontrado CP {cp_id} en BD", Fore.RED)
            return jsonify({"status": "error", "message": "CP not found"}), 404

        cp_info = resp.json()
        engine_ip = cp_info.get("engine_ip")
        engine_web_port = cp_info.get("engine_port", 5011)

        if not engine_ip:
            log("ENGINE-STATE", f"Engine IP no configurado para CP {cp_id}", Fore.RED)
            return jsonify(
                {"status": "error", "message": "Engine IP not configured"}
            ), 500

        resp = requests.post(
            f"http://{engine_ip}:{engine_web_port}/api/set_state",
            json={"new_state": new_state, "reason": reason},
            timeout=5,
        )

        if resp.status_code == 200:
            log(
                "ENGINE-STATE",
                f"CP {cp_id} estado cambiado a '{new_state}' en Engine",
                Fore.GREEN,
            )

        else:
            log(
                "ENGINE-STATE",
                f"Error actualizando Engine: {resp.status_code}",
                Fore.YELLOW,
            )

    except Exception as e:
        log("ENGINE-STATE", f"Error conectando con Engine: {e}", Fore.YELLOW)

    return jsonify({"status": "ok"}), 200


# Comandos desde WebSocket
@socketio.on("command")
def on_command(data):
    cmd = data.get("cmd")
    cp_id = data.get("cp_id")

    try:
        source_ip = request.remote_addr

    except Exception:
        source_ip = "Kafka"
    log("CMD", f"{cmd} → {cp_id}", Fore.YELLOW)

    with registry_lock:
        if cp_id in cp_registry:
            if cmd == "pause":
                cp_registry[cp_id]["estado"] = "Out of order"

                db_post(
                    "/api/db/audit",
                    {
                        "timestamp": now_iso(),
                        "source_type": "Central",
                        "source_id": cp_id,
                        "source_ip": source_ip,
                        "action": "CP_OUT_OF_ORDER",
                        "details": json.dumps({"reason": "manual_pause"}),
                    },
                )

            elif cmd == "resume":
                ubicacion = cp_registry[cp_id].get("ubicacion", "N/A")
                temp_ok = False
                temp_value = 0

                try:
                    temp_value = get_temperature(ubicacion)

                    if temp_value is not None:
                        if temp_value >= 0:
                            temp_ok = True
                            log(
                                "CMD",
                                f"{cp_id} temperatura OK: {temp_value}°C >= 0°C",
                                Fore.GREEN,
                            )

                        else:
                            log(
                                "CMD",
                                f"{cp_id} reanudado como 'averiado' - Temperatura {temp_value}°C < 0°C",
                                Fore.YELLOW,
                            )

                    else:
                        log(
                            "CMD",
                            f"{cp_id} - Error consultando temperatura",
                            Fore.YELLOW,
                        )

                except Exception as e:
                    log(
                        "CMD",
                        f"{cp_id} - Error consultando temperatura: {e}",
                        Fore.YELLOW,
                    )

                if not temp_ok:
                    cp_registry[cp_id]["estado"] = "averiado"
                    cp_registry[cp_id]["weather_blocked"] = True

                    db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "averiado"})

                    engine_ip = cp_registry[cp_id].get("engine_ip")
                    engine_port = cp_registry[cp_id].get("engine_port")

                    if engine_ip and engine_port:
                        try:
                            engine_url = (
                                f"http://{engine_ip}:{engine_port}/api/set_state"
                            )
                            resp = requests.post(
                                engine_url,
                                json={"new_state": "averiado", "reason": "weather"},
                                timeout=3,
                            )

                            if resp.status_code == 200:
                                log(
                                    "ENGINE-STATE",
                                    f"{cp_id} bloqueado por clima en Engine",
                                    Fore.RED,
                                )

                            else:
                                log(
                                    "ENGINE-STATE",
                                    f"{cp_id} respuesta {resp.status_code}: {resp.text}",
                                    Fore.YELLOW,
                                )

                        except Exception as e:
                            log(
                                "ENGINE-STATE",
                                f"Error al notificar Engine de {cp_id}: {e}",
                                Fore.YELLOW,
                            )

                    socketio.emit(
                        "cp_updated",
                        {
                            "cp_id": cp_id,
                            "estado": "averiado",
                            "ubicacion": cp_registry[cp_id].get("ubicacion"),
                            "weather_blocked": True,
                            "connected": True,
                        },
                    )

                    socketio.emit(
                        "error",
                        {"message": f"{cp_id}: reanudado como 'averiado' por clima"},
                    )
                    return

                cp_registry[cp_id]["estado"] = "activo"
                cp_registry[cp_id]["weather_blocked"] = False

                db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "activo"})

                db_post(
                    "/api/db/audit",
                    {
                        "timestamp": now_iso(),
                        "source_type": "Central",
                        "source_id": cp_id,
                        "source_ip": source_ip,
                        "action": "CP_RESUMED",
                        "details": json.dumps(
                            {"reason": "manual_resume", "temperatura": temp_value}
                        ),
                    },
                )

                socketio.emit(
                    "cp_updated",
                    {
                        "cp_id": cp_id,
                        "estado": "activo",
                        "ubicacion": cp_registry[cp_id].get("ubicacion"),
                        "weather_blocked": False,
                        "connected": True,
                    },
                )

                cmd_data = {
                    "cp_id": cp_id,
                    "command": "resume_service",
                    "ts": now_iso(),
                }
                producer.produce("CP_COMMAND", json.dumps(cmd_data).encode("utf-8"))
                producer.flush()
                log("CMD", f"{cp_id} reanudado a 'activo'", Fore.GREEN)

                return

            else:
                return


# Comando para pausar todos los CPs
@socketio.on("pause_all")
def on_pause_all():
    try:
        source_ip = request.remote_addr

    except Exception:
        source_ip = "Kafka"
    log("CMD", "Pausando TODOS los CPs", Fore.YELLOW)

    with registry_lock:
        paused_cps = []

        for cp_id in cp_registry:
            if cp_registry[cp_id]["connected"]:
                if cp_registry[cp_id].get("weather_blocked", False):
                    log(
                        "CMD",
                        f"Ignorado pause_all para {cp_id} (weather_blocked)",
                        Fore.YELLOW,
                    )
                    continue

                cp_registry[cp_id]["estado"] = "Out of order"

                db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "Out of order"})

                paused_cps.append(cp_id)

                db_post(
                    "/api/db/audit",
                    {
                        "timestamp": now_iso(),
                        "source_type": "Central",
                        "source_id": cp_id,
                        "source_ip": source_ip,
                        "action": "CP_OUT_OF_ORDER",
                        "details": json.dumps({"reason": "pause_all_command"}),
                    },
                )

                cmd_data = {
                    "cp_id": cp_id,
                    "command": "Out of order",
                    "ts": now_iso(),
                }

                producer.produce("CP_COMMAND", json.dumps(cmd_data).encode("utf-8"))
        producer.flush()
        log("CMD", f"{len(paused_cps)} CPs pausados: {paused_cps}", Fore.GREEN)
        socketio.emit("all_cps_paused", {"cps": paused_cps, "count": len(paused_cps)})


# Comando para reanudar todos los CPs
@socketio.on("resume_all")
def on_resume_all():
    try:
        source_ip = request.remote_addr

    except Exception:
        source_ip = "Kafka"
    log("CMD", "Reanudando TODOS los CPs", Fore.YELLOW)

    with registry_lock:
        resumed_cps = []
        not_resumed = []

        for cp_id in cp_registry:
            if cp_registry[cp_id]["connected"]:
                if cp_registry[cp_id].get("weather_blocked", False):
                    not_resumed.append(f"{cp_id} (bloqueado clima)")
                    log(
                        "CMD",
                        f"Ignorado resume_all para {cp_id} (weather_blocked)",
                        Fore.YELLOW,
                    )
                    continue

                ubicacion = cp_registry[cp_id].get("ubicacion", "N/A")

                temp_ok = False
                temp_value = 0

                try:
                    temp = get_temperature(ubicacion)

                    if temp is not None:
                        if temp >= 0:
                            temp_ok = True

                        else:
                            temp_ok = False
                            temp_value = temp

                    else:
                        log("CMD", f"{cp_id} - Error consultando clima", Fore.YELLOW)
                except Exception as e:
                    log(
                        "CMD",
                        f"{cp_id} - Error de conexión con clima: {e}",
                        Fore.YELLOW,
                    )
                    temp_ok = False

                if not temp_ok:
                    cp_registry[cp_id]["estado"] = "averiado"
                    cp_registry[cp_id]["weather_blocked"] = True

                    db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "averiado"})

                    not_resumed.append(
                        f"{cp_id} (T={temp_value if 'temp_value' in locals() else 'N/A'}°C)"
                    )
                    engine_ip = cp_registry[cp_id].get("engine_ip")
                    engine_port = cp_registry[cp_id].get("engine_port")

                    if engine_ip and engine_port:
                        try:
                            engine_url = (
                                f"http://{engine_ip}:{engine_port}/api/set_state"
                            )
                            resp = requests.post(
                                engine_url,
                                json={"new_state": "averiado", "reason": "weather"},
                                timeout=3,
                            )

                            if resp.status_code == 200:
                                log(
                                    "ENGINE-STATE",
                                    f"{cp_id} bloqueado por clima en Engine",
                                    Fore.RED,
                                )

                            else:
                                log(
                                    "ENGINE-STATE",
                                    f"{cp_id} respuesta {resp.status_code}: {resp.text}",
                                    Fore.YELLOW,
                                )

                        except Exception as e:
                            log(
                                "ENGINE-STATE",
                                f"Error al notificar Engine de {cp_id}: {e}",
                                Fore.YELLOW,
                            )
                    continue

                cp_registry[cp_id]["estado"] = "activo"
                cp_registry[cp_id]["weather_blocked"] = False

                db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "activo"})

                resumed_cps.append(cp_id)

                db_post(
                    "/api/db/audit",
                    {
                        "timestamp": now_iso(),
                        "source_type": "Central",
                        "source_id": cp_id,
                        "source_ip": source_ip,
                        "action": "CP_RESUMED",
                        "details": json.dumps(
                            {"reason": "resume_all_command", "temperatura": temp}
                        ),
                    },
                )

                cmd_data = {
                    "cp_id": cp_id,
                    "command": "resume_service",
                    "ts": now_iso(),
                }
                producer.produce("CP_COMMAND", json.dumps(cmd_data).encode("utf-8"))

        producer.flush()

        if resumed_cps:
            log("CMD", f"{len(resumed_cps)} CPs reanudados: {resumed_cps}", Fore.GREEN)
            socketio.emit(
                "all_cps_resumed", {"cps": resumed_cps, "count": len(resumed_cps)}
            )

        if not_resumed:
            log(
                "CMD",
                f"{len(not_resumed)} CPs NO reanudados por clima: {not_resumed}",
                Fore.YELLOW,
            )
            socketio.emit(
                "partial_resume", {"resumed": resumed_cps, "not_resumed": not_resumed}
            )


# API para obtener info del CP
@app.route("/api/cp/<cp_id>/info", methods=["GET"])
def get_cp_info(cp_id):
    try:
        status_u, ubic_body = db_get(f"/api/db/cp/ubicacion/{cp_id}")
        status_p, price_body = db_get(f"/api/db/cp/price/{cp_id}")

        if status_u != 200 or status_p != 200:
            return jsonify({"error": "CP not found"}), 404

        ubicacion = ubic_body.get("ubicacion", "ubicacion-EV_Central2")
        precio_kwh = price_body.get("precio_kwh", 0.30)

        with registry_lock:
            estado = cp_registry.get(cp_id, {}).get("estado", "desconectado")

        return jsonify(
            {
                "cp_id": cp_id,
                "ubicacion": ubicacion,
                "precio_kwh": precio_kwh,
                "estado": estado,
            }
        )

    except Exception as e:
        log("API", f"Error: {e}", Fore.RED)
        return jsonify({"error": str(e)}), 500


# API para actualizar ubicación del CP
@app.route("/api/cp/<cp_id>/location", methods=["POST"])
def update_cp_location(cp_id):
    data = request.get_json() or {}
    new_location = data.get("location", "").strip()

    if not new_location:
        return jsonify({"status": "error", "message": "location requerida"}), 400

    status, body = db_post(
        "/api/db/cp/ubicacion", {"cp_id": cp_id, "ubicacion": new_location}
    )

    if status != 200:
        log("CP", f"Error guardando ubicación en BD: {body}", Fore.RED)
        return jsonify({"status": "error", "message": "Error guardando ubicación"}), 500

    log("CP", f"Ubicación de {cp_id} actualizada a {new_location}", Fore.GREEN)
    return jsonify(
        {"status": "ok", "message": f"Ubicación actualizada a {new_location}"}
    ), 200


# API para recibir notificación de cambio de ubicación desde Engine
@app.route("/api/engine/location_change", methods=["POST"])
def api_engine_location_change():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    new_location = data.get("ubicacion")

    if not cp_id or not new_location:
        return jsonify(
            {"status": "error", "message": "cp_id y ubicacion requeridos"}
        ), 400

    log("LOCATION", f"{cp_id} cambio de ubicación a {new_location}", Fore.CYAN)

    with registry_lock:
        if cp_id in cp_registry:
            cp_registry[cp_id]["ubicacion"] = new_location

    def resolve_engine_endpoint(cp_id):
        engine_ip = None
        engine_port = None

        try:
            resp = requests.get(f"{db_api_url}/api/db/cp/{cp_id}/info", timeout=5)

            if resp.status_code == 200:
                data = resp.json()
                engine_ip = data.get("engine_ip")
                engine_port = data.get("engine_port")

        except Exception as e:
            log("ENGINE-CMD", f"Error leyendo Engine de BD: {e}", Fore.YELLOW)

        if not engine_ip:
            with registry_lock:
                cp_data = cp_registry.get(cp_id, {})
                engine_ip = cp_data.get("engine_ip")
                engine_port = cp_data.get("engine_port")

        if engine_port is None:
            engine_port = 5011

        return engine_ip, engine_port

    engine_ip, engine_port = resolve_engine_endpoint(cp_id)

    if not engine_ip:
        log(
            "ENGINE-CMD",
            f"Engine IP no disponible para {cp_id}; no se puede aplicar cambio de estado",
            Fore.RED,
        )
        return jsonify({"status": "error", "message": "Engine IP no configurada"}), 500

    engine_url = f"http://{engine_ip}:{engine_port}/api/set_state"
    temp = get_temperature(new_location, force_refresh=True)

    if temp is None:
        log(
            "WEATHER",
            f"No se pudo consultar temperatura, bloqueando {cp_id} por seguridad",
            Fore.RED,
        )

        try:
            requests.post(
                engine_url,
                json={"new_state": "averiado", "reason": "weather"},
                timeout=3,
            )

        except Exception as e:
            log("ENGINE-CMD", f"Error bloqueando {cp_id}: {e}", Fore.RED)

        return jsonify(
            {
                "status": "ok",
                "message": "CP bloqueado por seguridad (clima no disponible)",
            }
        ), 200

    if temp < 0:
        log(
            "WEATHER", f"Bloqueando {cp_id} - {new_location} T={temp}°C < 0°C", Fore.RED
        )

        try:
            requests.post(
                engine_url,
                json={"new_state": "averiado", "reason": "weather"},
                timeout=3,
            )

        except Exception as e:
            log("ENGINE-CMD", f"Error bloqueando {cp_id}: {e}", Fore.RED)

    else:
        log(
            "WEATHER",
            f"Desbloqueando {cp_id} - {new_location} T={temp}°C >= 0°C",
            Fore.GREEN,
        )

        try:
            requests.post(
                engine_url, json={"new_state": "activo", "reason": "weather"}, timeout=3
            )

        except Exception as e:
            log("ENGINE-CMD", f"Error desbloqueando {cp_id}: {e}", Fore.RED)

    return jsonify({"status": "ok"}), 200


# API para obtener clima del CP desde EV_W
@app.route("/api/cp/<cp_id>/weather", methods=["GET"])
def get_cp_weather(cp_id):
    try:
        status_u, ubic_body = db_get(f"/api/db/cp/ubicacion/{cp_id}")

        if status_u != 200:
            return jsonify({"status": "error", "message": "CP not found"}), 404

        ubicacion = ubic_body.get("ubicacion", "Desconocida")
        ev_w_host = os.environ.get("EV_W_HOST", "localhost")
        ev_w_port = os.environ.get("EV_W_PORT", "8000")
        weather_url = (
            f"http://{ev_w_host}:{ev_w_port}/api/weather?ubicacion={ubicacion}"
        )

        try:
            resp = requests.get(weather_url, timeout=5)

            if resp.status_code == 200:
                weather_data = resp.json()

                return jsonify(
                    {
                        "status": "ok",
                        "cp_id": cp_id,
                        "ubicacion": ubicacion,
                        "weather": weather_data,
                    }
                ), 200

            else:
                return jsonify(
                    {"status": "error", "message": "EV_W unavailable", "cp_id": cp_id}
                ), 503

        except Exception as e:
            log("WEATHER", f"Error consultando EV_W: {e}", Fore.YELLOW)
            return jsonify(
                {"status": "offline", "message": "EV_W not responding", "cp_id": cp_id}
            ), 503

    except Exception as e:
        log("API", f"Error: {e}", Fore.RED)
        return jsonify({"error": str(e)}), 500


# Sincronizar y devolver todos los CPs
@app.route("/api/cps", methods=["GET"])
def api_get_cps():
    with registry_lock:
        sync_cps_from_db()
        cps_copy = {}

        for cid, data in cp_registry.items():
            data_copy = dict(data)

            if "weather_blocked" not in data_copy:
                data_copy["weather_blocked"] = False
            cps_copy[cid] = data_copy

        return jsonify({"cps": cps_copy})


# Función para sincronizar CPs desde BD
def sync_cps_from_db():
    for cp_id in list(cp_registry.keys()):
        try:
            status_u, ubic_body = db_get(f"/api/db/cp/ubicacion/{cp_id}")
            status_p, price_body = db_get(f"/api/db/cp/price/{cp_id}")

            if status_u == 200:
                ubicacion_bd = ubic_body.get("ubicacion")

                if ubicacion_bd and ubicacion_bd != cp_registry[cp_id].get("ubicacion"):
                    cp_registry[cp_id]["ubicacion"] = ubicacion_bd
                    log(
                        "SYNC",
                        f"{cp_id} ubicación actualizada desde BD: {ubicacion_bd}",
                        Fore.CYAN,
                    )

            if status_p == 200:
                precio_bd = price_body.get("precio_kwh")

                if precio_bd and precio_bd != cp_registry[cp_id].get("precio_kwh"):
                    cp_registry[cp_id]["precio_kwh"] = precio_bd

        except Exception as e:
            log("SYNC", f"Error sincronizando {cp_id}: {e}", Fore.YELLOW)


# API para enviar comandos al CP
@app.route("/api/command", methods=["POST"])
def api_command():
    data = request.get_json()
    cmd = data.get("cmd")
    cp_id = data.get("cp_id")

    try:
        source_ip = request.remote_addr

    except Exception:
        source_ip = "Kafka"
    log("API-CMD", f"{cmd} → {cp_id}", Fore.YELLOW)

    with registry_lock:
        if cp_id not in cp_registry:
            return jsonify({"status": "error", "message": "CP not found"}), 404

        if cmd == "pause":
            cp_registry[cp_id]["estado"] = "Out of order"

            db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "Out of order"})

            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "Central",
                    "source_id": cp_id,
                    "source_ip": source_ip,
                    "action": "CP_OUT_OF_ORDER",
                    "details": json.dumps({"reason": "manual_pause_api"}),
                },
            )

            cmd_data = {"cp_id": cp_id, "command": "Out of order", "ts": now_iso()}
            producer.produce("CP_COMMAND", json.dumps(cmd_data).encode("utf-8"))
            producer.flush()
            log("API-CMD", f"{cp_id} pausado", Fore.YELLOW)

            return jsonify({"status": "ok"})

        elif cmd == "resume":
            ubicacion = cp_registry[cp_id].get("ubicacion", "N/A")
            temp_ok = False
            temp_value = 0

            try:
                temp_value = get_temperature(ubicacion)

                if temp_value is not None:
                    if temp_value >= 0:
                        temp_ok = True
                        log(
                            "API-CMD",
                            f"{cp_id} temperatura OK: {temp_value}°C >= 0°C",
                            Fore.GREEN,
                        )

                    else:
                        log(
                            "API-CMD",
                            f"{cp_id} reanudado como 'averiado' - Temperatura {temp_value}°C < 0°C",
                            Fore.YELLOW,
                        )

                else:
                    log(
                        "API-CMD",
                        f"{cp_id} - Error consultando temperatura",
                        Fore.YELLOW,
                    )

            except Exception as e:
                log(
                    "API-CMD",
                    f"{cp_id} - Error consultando temperatura: {e}",
                    Fore.YELLOW,
                )

            if not temp_ok:
                cp_registry[cp_id]["estado"] = "averiado"
                cp_registry[cp_id]["weather_blocked"] = True

                db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "averiado"})

                engine_ip = cp_registry[cp_id].get("engine_ip")
                engine_port = cp_registry[cp_id].get("engine_port")

                if engine_ip and engine_port:
                    try:
                        engine_url = f"http://{engine_ip}:{engine_port}/api/set_state"
                        resp = requests.post(
                            engine_url,
                            json={"new_state": "averiado", "reason": "weather"},
                            timeout=3,
                        )

                        if resp.status_code == 200:
                            log(
                                "ENGINE-STATE",
                                f" {cp_id} bloqueado por clima en Engine",
                                Fore.RED,
                            )

                        else:
                            log(
                                "ENGINE-STATE",
                                f"{cp_id} respuesta {resp.status_code}: {resp.text}",
                                Fore.YELLOW,
                            )

                    except Exception as e:
                        log(
                            "ENGINE-STATE",
                            f"Error al notificar Engine de {cp_id}: {e}",
                            Fore.YELLOW,
                        )

                socketio.emit(
                    "cp_updated",
                    {
                        "cp_id": cp_id,
                        "estado": "averiado",
                        "ubicacion": cp_registry[cp_id].get("ubicacion"),
                        "weather_blocked": True,
                        "connected": True,
                    },
                )
                log("API-CMD", f"{cp_id} reanudado a 'averiado' por clima", Fore.YELLOW)

                return jsonify(
                    {"status": "ok", "message": "Reanudado como averiado por clima"}
                ), 200

            cp_registry[cp_id]["estado"] = "activo"
            cp_registry[cp_id]["weather_blocked"] = False

            db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "activo"})

            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "Central",
                    "source_id": cp_id,
                    "source_ip": source_ip,
                    "action": "CP_RESUMED",
                    "details": json.dumps(
                        {"reason": "manual_resume_api", "temperatura": temp_value}
                    ),
                },
            )

            socketio.emit(
                "cp_updated",
                {
                    "cp_id": cp_id,
                    "estado": "activo",
                    "ubicacion": cp_registry[cp_id].get("ubicacion"),
                    "weather_blocked": False,
                    "connected": True,
                },
            )

            cmd_data = {"cp_id": cp_id, "command": "resume_service", "ts": now_iso()}
            producer.produce("CP_COMMAND", json.dumps(cmd_data).encode("utf-8"))
            producer.flush()
            log("API-CMD", f"{cp_id} reanudado a 'activo'", Fore.GREEN)

            return jsonify({"status": "ok"})

        elif cmd == "deactivate_cp_credentials":
            db_post("/api/db/cp/credentials/deactivate", {"cp_id": cp_id})

            cp_registry[cp_id]["connected"] = False
            cp_registry[cp_id]["estado"] = "desconectado"

            db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "desconectado"})

            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "CP",
                    "source_id": cp_id,
                    "source_ip": source_ip,
                    "action": "CP_DEACTIVATED",
                    "details": json.dumps({"reason": "credentials_deactivated"}),
                },
            )

            socketio.emit(
                "cp_updated",
                {"cp_id": cp_id, "estado": "desconectado", "connected": False},
            )
            log("API-CMD", f"{cp_id} desactivado (credenciales inactivas)", Fore.RED)

        elif cmd == "altered_cp_clave":
            db_post("/api/db/cp/credentials/deactivate", {"cp_id": cp_id})

            cp_registry[cp_id]["connected"] = False
            cp_registry[cp_id]["estado"] = "desconectado"

            db_post("/api/db/cp/state", {"cp_id": cp_id, "estado": "desconectado"})

            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "CP",
                    "source_id": cp_id,
                    "source_ip": source_ip,
                    "action": "CP_DISCONNECTED_KEY_ALTERED",
                    "details": json.dumps({"reason": "Clave alterada"}),
                },
            )

            socketio.emit(
                "cp_updated",
                {"cp_id": cp_id, "estado": "desconectado", "connected": False},
            )
            log("API-CMD", f"{cp_id} desactivado (clave alterada)", Fore.RED)

        return jsonify({"status": "ok"})

    return jsonify({"status": "error", "message": "Unknown command"}), 400


# API para obtener credenciales de CPs
@app.route("/api/cp_credentials", methods=["GET"])
def api_get_cp_credentials():
    status, body = db_get("/api/db/credentials/all")

    if status == 200 and body.get("status") == "ok":
        return jsonify({"credentials": body["data"]})

    return jsonify({"credentials": []}), 200


# API para obtener eventos de auditoría
@app.route("/api/audit", methods=["GET"])
def api_get_audit():
    status, body = db_get("/api/db/audit/list?limit=10")

    if status == 200 and body.get("status") == "ok":
        return jsonify({"events": body["data"]})

    return jsonify({"events": []}), 200


# API para exportar eventos de auditoría a PDF
@app.route("/api/audit/export", methods=["GET"])
def api_export_audit():
    status, body = db_get("/api/db/audit/list?limit=20")

    if status != 200 or body.get("status") != "ok":
        return jsonify({"error": "No se pudieron obtener los eventos"}), 500
    events = body.get("data", [])
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        rightMargin=1 * cm,
        leftMargin=1 * cm,
        topMargin=1 * cm,
        bottomMargin=1 * cm,
    )
    elements = []
    styles = getSampleStyleSheet()
    title = Paragraph("<b>Últimos 20 Eventos de Auditoría</b>", styles["Title"])
    elements.append(title)
    elements.append(Spacer(1, 0.5 * cm))
    data = [["Hora", "Origen", "IP", "Acción", "Detalles"]]

    for ev in events:
        timestamp = ev.get("timestamp", "")

        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            formatted_time = dt.strftime("%d/%m/%Y, %H:%M:%S")

        except Exception:
            formatted_time = timestamp
        origen = f"{ev.get('source_type', '')} ({ev.get('source_id', '-')})"
        ip = ev.get("source_ip", "")
        action = ev.get("action", "")
        details = ev.get("details", "")

        try:
            obj = json.loads(details)
            details_str = ", ".join([f"{k}={v}" for k, v in obj.items()])

            if len(details_str) > 60:
                details_str = details_str[:57] + "..."

        except Exception:
            details_str = details[:60] if details else ""

        data.append([formatted_time, origen, ip, action, details_str])

    table = Table(data, colWidths=[4 * cm, 4.5 * cm, 3.5 * cm, 4.5 * cm, 8 * cm])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#667eea")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    elements.append(table)
    doc.build(elements)
    buffer.seek(0)

    return send_file(
        buffer,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"auditoria_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
    )


# API proxy para obtener clima desde EV_W
@app.route("/api/weather_proxy", methods=["GET"])
def api_weather_proxy():
    ubicacion = request.args.get("ubicacion", "Madrid")

    if not weather_service_url:
        return jsonify(
            {
                "error": "Servicio de clima no configurado",
                "temp": None,
                "temperature": None,
            }
        ), 503

    try:
        weather_url = f"{weather_service_url}/api/weather?ubicacion={ubicacion}"
        response = requests.get(weather_url, timeout=5)

        if response.status_code == 200:
            data = response.json()

            if "temperature" not in data and "temp" in data:
                data["temperature"] = data["temp"]

            return jsonify(data), 200

        else:
            return jsonify(
                {"error": "Error obteniendo clima", "temp": None, "temperature": None}
            ), response.status_code

    except Exception as e:
        log("WEATHER-PROXY", f"Error: {e}", Fore.RED)

        return jsonify({"error": str(e), "temp": None, "temperature": None}), 500


# Hilo del servidor TCP para autenticación de CPs
def tcp_server_thread():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", tcp_port))
    s.listen(5)
    s.settimeout(1.0)
    log("TCP", f"Puerto {tcp_port} [PROTOCOLO ESTÁNDAR]\n", Fore.MAGENTA)

    while not stop_event.is_set():
        try:
            conn, addr = s.accept()
            threading.Thread(target=handle_auth, args=(conn, addr), daemon=True).start()
        except socket.timeout:
            continue
        except Exception as e:
            if not stop_event.is_set():
                log("TCP", f"Error: {e}", Fore.RED)
    s.close()


# Hilo para manejar autenticación de CPs
def handle_auth(conn, addr):
    try:
        msg = recibir_mensaje(conn, timeout=5.0)
        if not msg:
            log("AUTH", f"Error recibiendo mensaje desde {addr}", Fore.RED)
            return

        if msg.get("type") != "auth_request":
            log("AUTH", f"Mensaje no válido desde {addr}: {msg}", Fore.RED)
            return

        cp_id = msg.get("cp_id")
        username = msg.get("username")
        password = msg.get("password") or ""
        ubicacion = msg.get("ubicacion", "ubicacion-EV_Central1")
        status, body = db_get(f"/api/db/cp/credentials/{cp_id}")

        if status != 200 or body.get("status") != "ok":
            creds = None
        else:
            creds = body["data"]

        if not creds or creds["username"] != username or not creds["activo"]:
            log("AUTH", f"Credenciales inválidas para {cp_id}", Fore.RED)
            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "CP",
                    "source_id": cp_id,
                    "source_ip": addr[0],
                    "action": "AUTH_FAIL",
                    "details": f"username={username}",
                },
            )
            response = {
                "type": "auth_response",
                "status": "NACK",
                "cp_id": cp_id,
                "message": "Credenciales inválidas",
            }
            enviar_mensaje(conn, response)
            return

        password_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
        if password_hash != creds["password_hash"]:
            log("AUTH", f"Password incorrecto para {cp_id}", Fore.RED)
            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "CP",
                    "source_id": cp_id,
                    "source_ip": addr[0],
                    "action": "AUTH_FAIL",
                    "details": "password_mismatch",
                },
            )
            response = {
                "type": "auth_response",
                "status": "NACK",
                "cp_id": cp_id,
                "message": "Credenciales inválidas",
            }
            enviar_mensaje(conn, response)
            return

        status_p, price_body = db_get(f"/api/db/cp/price/{cp_id}")
        precio_guardado = price_body.get("precio_kwh") if status_p == 200 else None
        status_u, ubic_body = db_get(f"/api/db/cp/ubicacion/{cp_id}")
        ubicacion_guardada = ubic_body.get("ubicacion") if status_u == 200 else None

        if precio_guardado is None:
            db_post("/api/db/cp/register", {"cp_id": cp_id})
            status_p2, body2 = db_get(f"/api/db/cp/price/{cp_id}")
            precio = body2.get("precio_kwh", 0.30) if status_p2 == 200 else 0.30
        else:
            precio = precio_guardado

        raw_key = os.urandom(32)
        key_b64 = base64.b64encode(raw_key).decode("ascii")
        db_post("/api/db/cp/key", {"cp_id": cp_id, "symmetric_key": key_b64})
        ubic_final = ubicacion_guardada or ubicacion

        with registry_lock:
            if cp_id not in cp_registry:
                cp_registry[cp_id] = {
                    "cp_id": cp_id,
                    "ubicacion": ubic_final,
                    "estado": "activo",
                    "connected": True,
                    "precio_kwh": precio,
                    "last_seen": now_ts(),
                }
            else:
                cp_registry[cp_id]["ubicacion"] = ubic_final
                cp_registry[cp_id]["connected"] = True
                cp_registry[cp_id]["last_seen"] = now_ts()

        response = {
            "type": "auth_response",
            "status": "ACK",
            "cp_id": cp_id,
            "message": "OK",
            "precio_kwh": precio,
            "symmetric_key": key_b64,
        }

        if enviar_mensaje(conn, response):
            log("AUTH", f"{cp_id} autenticado con ubicación: {ubic_final}", Fore.GREEN)
            socketio.emit(
                "cp_updated",
                {
                    "cp_id": cp_id,
                    "estado": "activo",
                    "ubicacion": ubic_final,
                    "precio_kwh": precio,
                    "connected": True,
                },
            )
            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "CP",
                    "source_id": cp_id,
                    "source_ip": addr[0],
                    "action": "AUTH_OK",
                    "details": f"username={username}",
                },
            )
        else:
            log("AUTH", f"Se pudo enviar respuesta a {cp_id}", Fore.GREEN)

    except Exception as e:
        log("AUTH", f"Error: {e}", Fore.RED)
    finally:
        conn.close()


# Hilos de escucha CP_STATUS de Kafka
def kafka_cp_status_listener():
    c = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "central_cp_status",
            "auto.offset.reset": "latest",
        }
    )
    c.subscribe(["CP_STATUS"])
    log("KAFKA", "Escuchando CP_STATUS", Fore.YELLOW)

    while not stop_event.is_set():
        msg = c.poll(0.5)
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                cp_id = data.get("cp_id")
                nuevo_estado = data.get("estado", "estado-desconocido")

                log("KAFKA", f"CP_STATUS: {cp_id} → {nuevo_estado}", Fore.CYAN)

                engine_ip = data.get("engine_ip")
                engine_port = data.get("engine_port")
                if engine_ip and engine_port:
                    db_post(
                        "/api/db/cp/engine",
                        {
                            "cp_id": cp_id,
                            "engine_ip": engine_ip,
                            "engine_port": engine_port,
                        },
                    )

                with registry_lock:
                    if cp_id not in cp_registry:
                        log(
                            "KAFKA",
                            f"CP {cp_id} envía estado pero NO está autenticado aún, ignorando...",
                            Fore.YELLOW,
                        )
                    else:
                        r = cp_registry[cp_id]
                        estado_anterior = r.get("estado")

                        if engine_ip:
                            r["engine_ip"] = engine_ip
                        if engine_port:
                            r["engine_port"] = engine_port

                        cp_ip = r.get("engine_ip", "unknown")

                        if "weather_blocked" not in r:
                            r["weather_blocked"] = False

                        if (
                            r.get("weather_blocked", False)
                            and r.get("estado") == "averiado"
                        ):
                            if nuevo_estado != "averiado":
                                log(
                                    "KAFKA",
                                    f"{cp_id} permanece en 'averiado' (weather_blocked=True), ignorando CP_STATUS={nuevo_estado}",
                                    Fore.YELLOW,
                                )
                                nuevo_estado = "averiado"

                        if (
                            nuevo_estado == "suministrando"
                            or r.get("estado") == "suministrando"
                        ):
                            r["estado"] = nuevo_estado
                        elif r.get("estado") != "Out of order":
                            r["estado"] = nuevo_estado
                        r["connected"] = True
                        r["last_seen"] = now_ts()

                        if (
                            estado_anterior != nuevo_estado
                            and nuevo_estado
                            in ["averiado", "activo", "disponible", "reseteo"]
                            and cp_ip != "unknown"
                            and estado_anterior is not None
                        ):
                            db_post(
                                "/api/db/audit",
                                {
                                    "timestamp": now_iso(),
                                    "source_type": "CP",
                                    "source_id": cp_id,
                                    "source_ip": cp_ip,
                                    "action": "CP_STATE_CHANGE",
                                    "details": json.dumps(
                                        {
                                            "estado_anterior": estado_anterior,
                                            "estado_nuevo": nuevo_estado,
                                        }
                                    ),
                                },
                            )

                        if cp_registry[cp_id].get("estado") != "suministrando":
                            if cp_registry[cp_id].get("estado") != "Out of order":
                                db_post(
                                    "/api/db/cp/state",
                                    {"cp_id": cp_id, "estado": nuevo_estado},
                                )

                        socketio.emit("cp_updated", data)
                        log(
                            "WEBSOCKET",
                            f"cp_updated → {cp_id} ({nuevo_estado})",
                            Fore.GREEN,
                        )

            except Exception as e:
                log("KAFKA", f"Error: {e}", Fore.RED)
    c.close()


# Hilo para escuchar SUPPLY_DIRECT_START iniciados por CPs
def kafka_direct_supply_listener():
    c = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "central_direct_supply",
            "auto.offset.reset": "latest",
        }
    )
    c.subscribe(["SUPPLY_DIRECT_START"])
    log("KAFKA", "Escuchando SUPPLY_DIRECT_START", Fore.YELLOW)

    while not stop_event.is_set():
        msg = c.poll(0.5)
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                cp_id = data.get("cp_id")
                driver_id = data.get("driver_id")
                kwh_requested = data.get("kwh_requested", 10.0)

                log(
                    "DIRECT-SUPPLY",
                    f"CP {cp_id} inicia suministro directo con {driver_id} ({kwh_requested} kWh)",
                    Fore.MAGENTA,
                )

                cp_ip = "unknown"
                with registry_lock:
                    if cp_id in cp_registry:
                        cp_ip = cp_registry[cp_id].get("engine_ip", "unknown")
                if cp_ip != "unknown":
                    db_post(
                        "/api/db/audit",
                        {
                            "timestamp": now_iso(),
                            "source_type": "CP",
                            "source_id": cp_id,
                            "source_ip": cp_ip,
                            "action": "SUPPLY_DIRECT_START",
                            "details": json.dumps(
                                {"driver_id": driver_id, "kwh_requested": kwh_requested}
                            ),
                        },
                    )

                precio = (
                    cp_registry.get(cp_id, {}).get("precio_kwh")
                    if cp_registry
                    else None
                )
                if precio is None:
                    status_p, price_body = db_get(f"/api/db/cp/price/{cp_id}")
                    precio = (
                        price_body.get("precio_kwh", 0.30) if status_p == 200 else 0.30
                    )

                auth_response = {
                    "driver_id": driver_id,
                    "cp_id": cp_id,
                    "estado": "autorizado",
                    "kwh_requested": kwh_requested,
                    "precio_kwh": precio,
                    "ts": now_iso(),
                }

                producer.produce("SUPPLY_RESPONSE", json.dumps(auth_response).encode())
                producer.flush()

                log(
                    "DIRECT-AUTH",
                    f"Autorización automática enviada a {driver_id}",
                    Fore.GREEN,
                )

                socketio.emit(
                    "supply_alert",
                    {
                        "type": "direct_start",
                        "cp_id": cp_id,
                        "driver_id": driver_id,
                        "kwh": kwh_requested,
                    },
                )

            except Exception as e:
                log("KAFKA", f"Error DIRECT_SUPPLY: {e}", Fore.RED)
    c.close()


# Hilo para escuchar SUPPLY_REQUEST de Drivers
def kafka_supply_request_listener():
    c = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "central_supply_requests",
            "auto.offset.reset": "latest",
        }
    )
    c.subscribe(["SUPPLY_REQUEST"])
    log("KAFKA", "Escuchando SUPPLY_REQUEST", Fore.YELLOW)

    while not stop_event.is_set():
        msg = c.poll(0.5)
        if msg and not msg.error():
            try:
                request_data = json.loads(msg.value().decode("utf-8"))
                driver_id = request_data.get("driver_id")
                preferred_cp_id = request_data.get("preferred_cp_id")
                kwh_requested = request_data.get("kWh", 10.0)

                log(
                    "SUPPLY-REQ",
                    f"Driver {driver_id} solicita {kwh_requested} kWh en {preferred_cp_id}",
                    Fore.MAGENTA,
                )

                cp_ip = "Kafka"
                with registry_lock:
                    if preferred_cp_id in cp_registry:
                        cp_ip = cp_registry[preferred_cp_id].get("engine_ip", "Kafka")
                db_post(
                    "/api/db/audit",
                    {
                        "timestamp": now_iso(),
                        "source_type": "Driver",
                        "source_id": driver_id,
                        "source_ip": cp_ip,
                        "action": "SUPPLY_REQUESTED",
                        "details": json.dumps(
                            {"cp_id": preferred_cp_id, "kwh_requested": kwh_requested}
                        ),
                    },
                )

                with registry_lock:
                    cp = cp_registry.get(preferred_cp_id)

                    if not cp:
                        response = {
                            "driver_id": driver_id,
                            "cp_id": preferred_cp_id,
                            "success": False,
                            "reason": "CP no encontrado",
                            "ts": now_iso(),
                        }
                        producer.produce(
                            "SUPPLY_RESPONSE", json.dumps(response).encode()
                        )
                        producer.flush()
                        log("SUPPLY-REJ", f"CP {preferred_cp_id} no existe", Fore.RED)
                        socketio.emit(
                            "supply_alert",
                            {
                                "type": "rechazado",
                                "driver_id": driver_id,
                                "reason": "CP no encontrado",
                            },
                        )
                        continue

                    if cp["estado"] not in ["activo", "disponible"]:
                        response = {
                            "driver_id": driver_id,
                            "cp_id": preferred_cp_id,
                            "success": False,
                            "reason": f"CP en estado {cp['estado']}",
                            "ts": now_iso(),
                        }
                        producer.produce(
                            "SUPPLY_RESPONSE", json.dumps(response).encode()
                        )
                        producer.flush()
                        log(
                            "SUPPLY-REJ",
                            f"CP {preferred_cp_id} no disponible ({cp['estado']})",
                            Fore.RED,
                        )

                        cp_ip = cp.get("engine_ip", "Kafka")
                        db_post(
                            "/api/db/audit",
                            {
                                "timestamp": now_iso(),
                                "source_type": "CP",
                                "source_id": preferred_cp_id,
                                "source_ip": cp_ip,
                                "action": "SUPPLY_REJECTED",
                                "details": json.dumps(
                                    {
                                        "driver_id": driver_id,
                                        "reason": f"CP en estado {cp['estado']}",
                                    }
                                ),
                            },
                        )

                        socketio.emit(
                            "supply_alert",
                            {
                                "type": "rechazado",
                                "driver_id": driver_id,
                                "reason": f"CP en estado {cp['estado']}",
                            },
                        )
                        continue

                    precio = cp.get("precio_kwh")
                    if precio is None:
                        status_p, price_body = db_get(
                            f"/api/db/cp/price/{preferred_cp_id}"
                        )
                        precio = (
                            price_body.get("precio_kwh", 0.30)
                            if status_p == 200
                            else 0.30
                        )

                    supply_command = {
                        "cp_id": preferred_cp_id,
                        "command": "start_supply",
                        "driver_id": driver_id,
                        "kwh_requested": kwh_requested,
                        "precio_kwh": precio,
                        "ts": now_iso(),
                    }

                    producer.produce("CP_COMMAND", json.dumps(supply_command).encode())
                    producer.flush()

                    auth_response = {
                        "driver_id": driver_id,
                        "cp_id": preferred_cp_id,
                        "estado": "autorizado",
                        "kwh_requested": kwh_requested,
                        "precio_kwh": precio,
                        "ts": now_iso(),
                    }

                    producer.produce(
                        "SUPPLY_RESPONSE", json.dumps(auth_response).encode()
                    )
                    producer.flush()

                    log(
                        "SUPPLY-AUTH",
                        f"Autorizado: {driver_id} → {preferred_cp_id} ({kwh_requested} kWh)",
                        Fore.GREEN,
                    )

                    socketio.emit(
                        "supply_alert",
                        {
                            "type": "autorizado",
                            "driver_id": driver_id,
                            "cp_id": preferred_cp_id,
                            "kwh": kwh_requested,
                        },
                    )

            except Exception as e:
                log("KAFKA", f"Error SUPPLY_REQUEST: {e}", Fore.RED)
                cp_ip = cp.get("engine_ip", "Kafka")
                db_post(
                    "/api/db/audit",
                    {
                        "timestamp": now_iso(),
                        "source_type": "Central",
                        "source_id": "kafka_supply_request",
                        "source_ip": cp_ip,
                        "action": "ERROR_SUPPLY_REQUEST",
                        "details": json.dumps({"error": str(e)}),
                    },
                )
    c.close()


# Hilo para escuchar SUPPLY_PROGRESS de CPs
def kafka_supply_progress_listener():
    c = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "central_supply_progress",
            "auto.offset.reset": "latest",
        }
    )
    c.subscribe(["SUPPLY_PROGRESS"])
    log("KAFKA", "Escuchando SUPPLY_PROGRESS", Fore.CYAN)

    while not stop_event.is_set():
        msg = c.poll(0.5)
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                cp_id = data.get("cp_id")
                driver_id = data.get("driver_id")
                energia = data.get("energia_actual", 0)
                importe = data.get("importe_actual", 0)
                porcentaje = data.get("porcentaje", 0)

                log(
                    "SUPPLY-PROG",
                    f"CP {cp_id}: {energia} kWh, {importe}€ ({porcentaje}%)",
                    Fore.GREEN,
                )

                with registry_lock:
                    if cp_id in cp_registry:
                        if not cp_registry[cp_id].get("driver_actual"):
                            cp_ip = cp_registry[cp_id].get("engine_ip", "unknown")
                            if cp_ip != "unknown":
                                db_post(
                                    "/api/db/audit",
                                    {
                                        "timestamp": now_iso(),
                                        "source_type": "CP",
                                        "source_id": cp_id,
                                        "source_ip": cp_ip,
                                        "action": "SUPPLY_STARTED",
                                        "details": json.dumps(
                                            {
                                                "driver_id": driver_id,
                                                "energia_kwh": energia,
                                            }
                                        ),
                                    },
                                )

                with registry_lock:
                    if cp_id in cp_registry:
                        cp_registry[cp_id]["energia_actual"] = energia
                        cp_registry[cp_id]["importe_actual"] = importe
                        cp_registry[cp_id]["porcentaje"] = porcentaje
                        cp_registry[cp_id]["driver_actual"] = driver_id

            except Exception as e:
                log("KAFKA", f"Error SUPPLY_PROGRESS: {e}", Fore.RED)
    c.close()


# Hilo para escuchar SUPPLY_COMPLETED de CPs
def kafka_supply_completed_listener():
    c = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "central_supply_completed",
            "auto.offset.reset": "latest",
        }
    )
    c.subscribe(["SUPPLY_COMPLETED"])
    log("KAFKA", "Escuchando SUPPLY_COMPLETED", Fore.YELLOW)

    while not stop_event.is_set():
        msg = c.poll(0.5)
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                cp_id = data.get("cp_id")
                driver_id = data.get("driver_id")
                delivered_kwh = data.get("delivered_kwh", 0)
                importe_eur = data.get("importe_eur", 0)

                log(
                    "SUPPLY-DONE",
                    f"Completado: {cp_id} → {driver_id} ({delivered_kwh} kWh, {importe_eur}€)",
                    Fore.GREEN,
                )

                db_post(
                    "/api/db/supply",
                    {
                        "cp_id": cp_id,
                        "driver_id": driver_id,
                        "energia_kwh": delivered_kwh,
                        "importe_eur": importe_eur,
                        "estado": "completado",
                    },
                )

                cp_ip = "Kafka"
                with registry_lock:
                    if cp_id in cp_registry:
                        cp_ip = cp_registry[cp_id].get("engine_ip", "Kafka")
                db_post(
                    "/api/db/audit",
                    {
                        "timestamp": now_iso(),
                        "source_type": "CP",
                        "source_id": cp_id,
                        "source_ip": cp_ip,
                        "action": "SUPPLY_COMPLETED",
                        "details": json.dumps(
                            {
                                "driver_id": driver_id,
                                "delivered_kwh": delivered_kwh,
                                "importe_eur": importe_eur,
                            }
                        ),
                    },
                )

                with registry_lock:
                    if cp_id in cp_registry:
                        cp_registry[cp_id]["estado"] = "activo"
                        cp_registry[cp_id]["energia_actual"] = 0
                        cp_registry[cp_id]["importe_actual"] = 0
                        cp_registry[cp_id]["porcentaje"] = 0
                        cp_registry[cp_id]["driver_actual"] = None

                socketio.emit("cp_updated", {"cp_id": cp_id, "estado": "activo"})

                response = {
                    "driver_id": driver_id,
                    "cp_id": cp_id,
                    "success": True,
                    "delivered_kwh": delivered_kwh,
                    "importe_eur": importe_eur,
                    "ts": now_iso(),
                }

                producer.produce("SUPPLY_RESPONSE", json.dumps(response).encode())
                producer.flush()

                socketio.emit(
                    "supply_alert",
                    {
                        "type": "completado",
                        "driver_id": driver_id,
                        "cp_id": cp_id,
                        "kwh": delivered_kwh,
                        "importe": importe_eur,
                    },
                )

            except Exception as e:
                log("KAFKA", f"Error SUPPLY_COMPLETED: {e}", Fore.RED)
                db_post(
                    "/api/db/audit",
                    {
                        "timestamp": now_iso(),
                        "source_type": "Central",
                        "source_id": "kafka_supply_completed",
                        "source_ip": "Kafka",
                        "action": "ERROR_SUPPLY_COMPLETED",
                        "details": json.dumps({"error": str(e)}),
                    },
                )
    c.close()


# Hilo para escuchar SUPPLY_RESPONSE (rechazos desde CP)
def kafka_supply_response_listener():
    c = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "central_supply_response",
            "auto.offset.reset": "latest",
        }
    )
    c.subscribe(["SUPPLY_RESPONSE"])
    log("KAFKA", "Escuchando SUPPLY_RESPONSE", Fore.YELLOW)

    while not stop_event.is_set():
        msg = c.poll(0.5)
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode("utf-8"))
                success = data.get("success", True)
                cp_id = data.get("cp_id")
                driver_id = data.get("driver_id")
                reason = data.get("reason", "")

                if not success and cp_id and driver_id:
                    log(
                        "SUPPLY-RESPONSE",
                        f"Rechazo: {cp_id} → {driver_id} ({reason})",
                        Fore.YELLOW,
                    )

                    cp_ip = "unknown"
                    with registry_lock:
                        if cp_id in cp_registry:
                            cp_ip = cp_registry[cp_id].get("engine_ip", "unknown")

                    if cp_ip != "unknown":
                        db_post(
                            "/api/db/audit",
                            {
                                "timestamp": now_iso(),
                                "source_type": "CP",
                                "source_id": cp_id,
                                "source_ip": cp_ip,
                                "action": "SUPPLY_REJECTED",
                                "details": json.dumps(
                                    {"driver_id": driver_id, "reason": reason}
                                ),
                            },
                        )
            except Exception as e:
                log("KAFKA", f"Error SUPPLY_RESPONSE: {e}", Fore.RED)
    c.close()


# Publicador periódico del estado completo de CPs (incluyendo weather_blocked)
def kafka_cp_registry_publisher():
    log("KAFKA", "Publicando CP_REGISTRY periódicamente", Fore.CYAN)

    while not stop_event.is_set():
        try:
            with registry_lock:
                for cp_id, cp_data in cp_registry.items():
                    estado = cp_data.get("estado")
                    msg = {
                        "cp_id": cp_id,
                        "estado": estado,
                        "ubicacion": cp_data.get("ubicacion"),
                        "engine_ok": cp_data.get("connected", False),
                        "monitor_ok": cp_data.get("connected", False),
                        "weather_blocked": cp_data.get("weather_blocked", False),
                        "ts": now_iso(),
                    }
                    producer.produce("CP_REGISTRY", json.dumps(msg).encode())

            producer.flush()
            time.sleep(3)

        except Exception as e:
            log("KAFKA", f"Error publicando CP_REGISTRY: {e}", Fore.RED)
            time.sleep(1)


# Hilo para verificar desconexiones por timeout
def timeout_checker():
    while not stop_event.is_set():
        time.sleep(DISCONNECT_TIMEOUT)
        with registry_lock:
            now = now_ts()
            for cp_id, cp_data in cp_registry.items():
                if (
                    cp_data.get("connected")
                    and (now - cp_data.get("last_seen", now)) > DISCONNECT_TIMEOUT
                ):
                    cp_data["connected"] = False
                    cp_data["estado"] = "desconectado"

                    db_post("/api/db/cp/credentials/deactivate", {"cp_id": cp_id})

                    cp_ip = cp_data.get("engine_ip", "unknown")
                    if cp_ip != "unknown":
                        db_post(
                            "/api/db/audit",
                            {
                                "timestamp": now_iso(),
                                "source_type": "CP",
                                "source_id": cp_id,
                                "source_ip": cp_ip,
                                "action": "CP_DISCONNECTED",
                                "details": json.dumps({"reason": "timeout"}),
                            },
                        )

                    socketio.emit(
                        "cp_updated",
                        {"cp_id": cp_id, "estado": "desconectado", "connected": False},
                    )
            tnow = now_ts()
            for cp, r in list(cp_registry.items()):
                if (
                    r.get("connected")
                    and (tnow - r.get("last_seen", 0)) > DISCONNECT_TIMEOUT
                ):
                    r["connected"] = False
                    r["estado"] = "desconectado"
                    db_post("/api/db/cp/state", {"cp_id": cp, "estado": "desconectado"})
                    socketio.emit("cp_updated", {"cp_id": cp, "estado": "desconectado"})

        status, body = db_get("/api/db/credentials/all")
        if status == 200 and body.get("status") == "ok":
            all_creds = body.get("data", [])
            inactive_cp_ids = [
                c["cp_id"] for c in all_creds if not c.get("activo", True)
            ]

            with registry_lock:
                for cp_id in inactive_cp_ids:
                    if cp_id in cp_registry:
                        cp_data = cp_registry[cp_id]
                        if cp_data.get("estado") != "desconectado":
                            last_seen = cp_data.get("last_seen", 0)
                            time_since_last = now_ts() - last_seen

                            if cp_data.get("connected") and time_since_last < 10:
                                log(
                                    "AUTH-CHECK",
                                    f"CP {cp_id} con credenciales inactivas PERO publicando estado - IGNORANDO desconexión",
                                    Fore.CYAN,
                                )
                                continue

                            cp_data["connected"] = False
                            cp_data["estado"] = "desconectado"
                            log(
                                "AUTH-CHECK",
                                f"CP {cp_id} desconectado (credenciales inactivas)",
                                Fore.YELLOW,
                            )

                            db_post(
                                "/api/db/audit",
                                {
                                    "timestamp": now_iso(),
                                    "source_type": "CP",
                                    "source_id": cp_id,
                                    "source_ip": cp_data.get("engine_ip", "unknown"),
                                    "action": "CP_DEACTIVATED",
                                    "details": json.dumps(
                                        {"reason": "inactive_credentials"}
                                    ),
                                },
                            )

                            socketio.emit(
                                "cp_updated",
                                {
                                    "cp_id": cp_id,
                                    "estado": "desconectado",
                                    "connected": False,
                                },
                            )


# Análisis de argumentos
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka", required=True)
    p.add_argument("--tcp-port", type=int, default=5000)
    p.add_argument("--web-port", type=int, default=5001)
    p.add_argument("--db-api", required=True, help="host:7100")
    p.add_argument("--weather", type=str)
    return p.parse_args()


def cleanup_on_exit():
    with registry_lock:
        for cp_id in list(cp_registry.keys()):
            cp_registry[cp_id]["connected"] = False
            cp_registry[cp_id]["estado"] = "desconectado"

            db_post("/api/db/cp/disconnect", {"cp_id": cp_id})


def signal_handler(sig, frame):
    cleanup_on_exit()
    stop_event.set()
    sys.exit(0)


# Programa principal y arranque de hilos
if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    args = parse_args()
    kafka_bootstrap = args.kafka
    tcp_port = args.tcp_port
    web_port = args.web_port

    db_host_port = args.db_api.strip().rstrip("/")
    db_api_url = f"http://{db_host_port}"

    weather_url = args.weather.strip().rstrip("/")
    if not weather_url.startswith("http://") and not weather_url.startswith("https://"):
        weather_service_url = f"http://{weather_url}"
    else:
        weather_service_url = weather_url

    log("DB", f"Usando DB_API en {db_api_url}", Fore.GREEN)
    log("WEATHER", f"Usando servicio de clima en {weather_service_url}", Fore.GREEN)

    producer = Producer({"bootstrap.servers": kafka_bootstrap})

    status, body = db_get("/api/db/cp/all")
    if status == 200 and body.get("status") == "ok":
        cps = body["data"]
    else:
        cps = {}
    log("CENTRAL", f"CPs cargados desde BD: {len(cps)}", Fore.GREEN)
    cp_registry = cps.copy()

    threading.Thread(target=tcp_server_thread, daemon=True).start()
    threading.Thread(target=kafka_cp_status_listener, daemon=True).start()
    threading.Thread(target=kafka_supply_request_listener, daemon=True).start()
    threading.Thread(target=kafka_supply_response_listener, daemon=True).start()
    threading.Thread(target=kafka_supply_progress_listener, daemon=True).start()
    threading.Thread(target=kafka_supply_completed_listener, daemon=True).start()
    threading.Thread(target=kafka_direct_supply_listener, daemon=True).start()
    threading.Thread(target=kafka_cp_registry_publisher, daemon=True).start()
    threading.Thread(target=timeout_checker, daemon=True).start()

    log("CENTRAL", f"Web puerto {web_port}", Fore.GREEN)

    try:
        socketio.run(
            app, host="0.0.0.0", port=web_port, debug=False, use_reloader=False
        )
    except KeyboardInterrupt:
        log("CENTRAL", "Stop", Fore.YELLOW)
        cleanup_on_exit()
        stop_event.set()
    finally:
        log("CENTRAL", "Apagado completado", Fore.GREEN)
