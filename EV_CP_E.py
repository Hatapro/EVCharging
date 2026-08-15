"""
Uso:
python EV_CP_E.py --cp-id CP-001 --kafka 192.168.18.148:9092 --ubicacion "Alicante" --precio-kwh 0.35 --engine-port 6000 --web-port 5011 --monitor-port 5101 --central-web-port 5001 --engine-ip 192.168.18.148
"""

import argparse
import json
import os
import socket
import threading
import time
from datetime import datetime
from confluent_kafka import Consumer, Producer
from flask import Flask, jsonify, render_template, request
from colorama import Fore, Style, init as colorama_init
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.units import inch
import requests
from protocol import enviar_mensaje_simple, recibir_mensaje_simple

colorama_init(autoreset=True)


# Utilidades de tiempo y logging
def now_iso():
    return datetime.now().isoformat() + "Z"


def log(tag, msg, color=Fore.CYAN):
    print(f"{color}[{tag}] {msg}{Style.RESET_ALL}")


# Inicialización de Flask
app = Flask(__name__)
app.config["SECRET_KEY"] = "ev-cp-engine-2025"

# Configuración inicial
cp_id = None
bootstrap = None
ubicacion = None
engine_port = None
web_port = None
monitor_port = None
engine_ip = None
central_base_url = None
PRECIO_KWH = 0.30
KW_POR_SEGUNDO = 0.01
MULTIPLICADOR_VELOCIDAD = 10
INTERVALO_MS = 20
cp_state = {
    "cp_id": None,
    "ubicacion": "Desconocida",
    "estado": "desconectado",
    "monitor_ok": False,
    "engine_ok": True,
    "connected": False,
    "suministrando": False,
    "energia_kwh": 0.0,
    "energia_objetivo": 0.0,
    "importe_eur": 0.0,
    "driver_id": None,
    "alerta_solicitud": False,
    "alerta_solicitud_driver": None,
    "alerta_desconectar": False,
    "kwh_totales": 0.0,
    "importe_total": 0.0,
    "weather_blocked": False,
}
drivers_disponibles = {}
drivers_lock = threading.Lock()
state_lock = threading.Lock()
stop_event = threading.Event()
kafka_queue = []
kafka_lock = threading.Lock()
producer = None


# Ruta principal
@app.route("/")
def index():
    return render_template("cp.html", cp_id=cp_id, ubicacion=ubicacion)


# API para obtener el estado del CP
@app.route("/api/cp_state", methods=["GET"])
def api_get_state():
    with state_lock:
        return jsonify(cp_state)


# API para actualizar la ubicación del CP
@app.route("/api/cp_state/location", methods=["POST"])
def api_update_location():
    global ubicacion
    data = request.get_json() or {}
    new_location = data.get("ubicacion", "").strip()

    if not new_location:
        return jsonify({"status": "error", "message": "ubicacion requerida"}), 400

    ubicacion = new_location
    with state_lock:
        cp_state["ubicacion"] = new_location

    log("ENGINE", f"Ubicación actualizada a: {new_location}", Fore.GREEN)

    try:
        if not central_base_url:
            log(
                "ENGINE",
                "Central no configurada, no se notifica cambio de ubicación",
                Fore.YELLOW,
            )
        else:
            requests.post(
                f"{central_base_url}/api/engine/location_change",
                json={"cp_id": cp_id, "ubicacion": new_location},
                timeout=3,
            )
    except Exception as e:
        log(
            "ENGINE",
            f"No se pudo notificar a Central del cambio de ubicación: {e}",
            Fore.YELLOW,
        )

    return jsonify(
        {"status": "ok", "message": f"Ubicación actualizada a {new_location}"}
    ), 200


# API para cambiar el estado del CP por motivos climáticos
@app.route("/api/set_state", methods=["POST"])
def api_set_state():
    data = request.get_json() or {}
    new_state = data.get("new_state", "activo").lower()
    reason = data.get("reason", "unknown")

    if new_state not in ["averiado", "activo", "desconectado"]:
        return jsonify({"status": "error", "message": "Estado no válido"}), 400

    need_interrupt = False
    with state_lock:
        old_state = cp_state.get("estado", "desconectado")

        if reason == "credentials_deactivated":
            cp_state["estado"] = "desconectado"
            cp_state["weather_blocked"] = False
            cp_state["connected"] = False
            if cp_state.get("suministrando"):
                need_interrupt = True
            log("ENGINE-STATE", "CP DESCONECTADO - credenciales desactivadas", Fore.RED)

        elif new_state == "averiado" and reason == "weather":
            cp_state["weather_blocked"] = True
            cp_state["estado"] = "averiado"
            if cp_state.get("suministrando"):
                need_interrupt = True
            log("ENGINE-STATE", "CP BLOQUEADO por clima (T < 0°C)", Fore.RED)

        elif new_state == "activo":
            cp_state["weather_blocked"] = False
            if cp_state["monitor_ok"]:
                cp_state["estado"] = "activo"
            else:
                cp_state["estado"] = "desconectado"
            log("ENGINE-STATE", "CP DESBLOQUEADO - temperatura normal", Fore.GREEN)
        else:
            cp_state["estado"] = new_state

    if need_interrupt:
        interrupt_supply("weather")

    with kafka_lock:
        kafka_queue.append(
            {
                "estado": cp_state["estado"],
                "monitor_ok": cp_state["monitor_ok"],
                "engine_ok": cp_state["engine_ok"],
            }
        )

    log(
        "ENGINE-STATE",
        f"Estado cambiado de '{old_state}' a '{cp_state['estado']}' (razón: {reason})",
        Fore.YELLOW,
    )

    return jsonify(
        {"status": "ok", "message": f"Estado actualizado a {cp_state['estado']}"}
    ), 200


# API para obtener la lista de drivers disponibles
@app.route("/api/drivers", methods=["GET"])
def api_get_drivers():
    with drivers_lock:
        return jsonify({"drivers": drivers_disponibles})


# API para realizar acciones en el CP
@app.route("/api/action", methods=["POST"])
def api_action():
    data = request.get_json()
    action = data.get("action")
    log("API", f"Acción: {action}", Fore.YELLOW)

    try:
        with state_lock:
            if cp_state["estado"] == "Out of order":
                log("API", "CP bloqueado", Fore.RED)
                return jsonify({"status": "error", "msg": "Out of order"}), 403

            if cp_state["estado"] == "desconectado":
                log("API", "CP desconectado - no se permiten acciones", Fore.RED)
                return jsonify(
                    {
                        "status": "error",
                        "msg": "CP desconectado - no se permiten acciones",
                    }
                ), 403

            if cp_state.get("weather_blocked", False):
                log("API", "CP bloqueado por clima (T < 0°C)", Fore.RED)
                return jsonify(
                    {
                        "status": "error",
                        "msg": "Bloqueado por clima - temperatura bajo 0°C",
                    }
                ), 403

            if action == "averiar":
                if cp_state["suministrando"]:
                    interrupt_supply("averia")
                cp_state["estado"] = "averiado"
                cp_state["engine_ok"] = False
                estado_actual = "averiado"
                log("API", "AVERIADO", Fore.RED)

            elif action == "resetear":
                if cp_state.get("weather_blocked", False):
                    log(
                        "API",
                        "No se puede resetear - bloqueado por clima (T < 0°C)",
                        Fore.YELLOW,
                    )
                    return jsonify(
                        {
                            "status": "error",
                            "msg": "No se puede resetear - bloqueado por clima",
                        }
                    ), 403

                cp_state["estado"] = (
                    "activo" if cp_state["monitor_ok"] else "desconectado"
                )
                cp_state["engine_ok"] = True
                estado_actual = cp_state["estado"]
                log("API", "RESETEADO", Fore.GREEN)

            elif action == "desconectar":
                if cp_state["suministrando"]:
                    interrupt_supply("Out of order")
                cp_state["estado"] = "Out of order"
                cp_state["engine_ok"] = False
                estado_actual = "Out of order"
                log("API", "Out of order", Fore.YELLOW)

            elif action == "aceptar_solicitud":
                if cp_state["alerta_solicitud"]:
                    threading.Thread(target=start_supply_accepted, daemon=True).start()
                return jsonify({"status": "ok", "aceptado": True})

            elif action == "denegar_solicitud":
                if cp_state["alerta_solicitud"]:
                    driver = cp_state.get("alerta_solicitud_driver")
                    cp_state["alerta_solicitud"] = False
                    cp_state["alerta_solicitud_driver"] = None
                    cp_state["driver_id"] = None
                    cp_state["energia_objetivo"] = 0.0
                    cp_state["estado"] = "activo"
                    response = {
                        "driver_id": driver,
                        "cp_id": cp_id,
                        "success": False,
                        "reason": "Solicitud denegada por operario",
                        "ts": now_iso(),
                    }
                    producer.produce("SUPPLY_RESPONSE", json.dumps(response).encode())
                    producer.flush()
                    log("SUPPLY-REJ", f"Solicitud denegada: {driver}", Fore.RED)

                    with kafka_lock:
                        kafka_queue.append(
                            {
                                "estado": "activo",
                                "monitor_ok": cp_state["monitor_ok"],
                                "engine_ok": cp_state["engine_ok"],
                            }
                        )
                return jsonify({"status": "ok", "denegado": True})

            elif action == "terminar_suministro":
                if cp_state["suministrando"]:
                    driver = cp_state.get("driver_id")
                    kwh_entregados = cp_state.get("energia_kwh", 0)
                    importe = kwh_entregados * PRECIO_KWH
                    cp_state["kwh_totales"] += kwh_entregados
                    cp_state["importe_total"] += importe
                    cp_state["suministrando"] = False
                    cp_state["estado"] = "activo"
                    cp_state["alerta_desconectar"] = False
                    cp_state["driver_id"] = None
                    cp_state["energia_kwh"] = 0
                    cp_state["energia_objetivo"] = 0
                    cp_state["importe_eur"] = 0
                    completion = {
                        "cp_id": cp_id,
                        "driver_id": driver,
                        "delivered_kwh": round(kwh_entregados, 4),
                        "importe_eur": round(importe, 3),
                        "ts": now_iso(),
                    }
                    producer.produce(
                        "SUPPLY_COMPLETED", json.dumps(completion).encode()
                    )
                    ticket_data = {
                        "cp_id": cp_id,
                        "delivered_kwh": round(kwh_entregados, 3),
                        "importe_eur": round(importe, 3),
                        "ts": now_iso(),
                        "terminar_suministro": True,
                    }
                    ticket_path = generate_ticket_pdf(driver, ticket_data)
                    log("PDF", f"Ticket guardado en: {ticket_path}", Fore.GREEN)
                    producer.flush()
                    log(
                        "SUPPLY",
                        f"TERMINADO MANUALMENTE: {kwh_entregados} kWh",
                        Fore.GREEN,
                    )

                    with kafka_lock:
                        kafka_queue.append(
                            {
                                "estado": "activo",
                                "monitor_ok": cp_state["monitor_ok"],
                                "engine_ok": cp_state["engine_ok"],
                            }
                        )
                return jsonify({"status": "ok", "terminado": True})

            elif action == "iniciar_suministro_directo":
                driver_id = data.get("driver_id")
                kwh_solicitado = float(data.get("kwh", 10.0))

                if not driver_id:
                    return jsonify(
                        {"status": "error", "msg": "Driver ID requerido"}
                    ), 400

                if cp_state["suministrando"]:
                    return jsonify(
                        {"status": "error", "msg": "Ya hay un suministro en progreso"}
                    ), 400

                if cp_state.get("weather_blocked", False):
                    log(
                        "DIRECT-START",
                        "Suministro bloqueado - temperatura bajo 0°C",
                        Fore.RED,
                    )
                    return jsonify(
                        {
                            "status": "error",
                            "msg": "CP bloqueado por clima - temperatura bajo 0°C",
                        }
                    ), 403

                if cp_state["estado"] not in ["activo", "disponible"]:
                    return jsonify(
                        {
                            "status": "error",
                            "msg": f"CP no disponible: {cp_state['estado']}",
                        }
                    ), 400
                direct_start = {
                    "type": "direct_start",
                    "cp_id": cp_id,
                    "driver_id": driver_id,
                    "kwh_requested": kwh_solicitado,
                    "ts": now_iso(),
                }
                producer.produce(
                    "SUPPLY_DIRECT_START", json.dumps(direct_start).encode()
                )
                producer.flush()
                log(
                    "DIRECT-START",
                    f"Iniciando suministro directo: {driver_id} → {kwh_solicitado} kWh",
                    Fore.MAGENTA,
                )
                cp_state["driver_id"] = driver_id
                cp_state["energia_objetivo"] = kwh_solicitado
                cp_state["suministrando"] = True
                cp_state["estado"] = "suministrando"
                cp_state["energia_kwh"] = 0.0
                cp_state["importe_eur"] = 0.0

                with kafka_lock:
                    kafka_queue.append(
                        {
                            "estado": "suministrando",
                            "monitor_ok": cp_state["monitor_ok"],
                            "engine_ok": cp_state["engine_ok"],
                        }
                    )
                threading.Thread(
                    target=simulate_supply_realtime,
                    args=(driver_id, kwh_solicitado),
                    daemon=True,
                ).start()
                return jsonify({"status": "ok", "iniciado": True})

            else:
                return jsonify({"status": "error"}), 400

            if action not in [
                "aceptar_solicitud",
                "denegar_solicitud",
                "terminar_suministro",
                "iniciar_suministro_directo",
            ]:
                with kafka_lock:
                    kafka_queue.append(
                        {
                            "estado": estado_actual,
                            "monitor_ok": cp_state["monitor_ok"],
                            "engine_ok": cp_state["engine_ok"],
                        }
                    )
        return jsonify({"status": "ok"})

    except Exception as e:
        log("API", f"Error: {e}", Fore.RED)
        return jsonify({"status": "error", "msg": str(e)}), 500


# API para descartar alertas
@app.route("/api/dismiss_alert", methods=["POST"])
def api_dismiss_alert():
    data = request.get_json()
    alert_type = data.get("type")

    with state_lock:
        if alert_type == "solicitud":
            cp_state["alerta_solicitud"] = False
            cp_state["alerta_solicitud_driver"] = None

        elif alert_type == "desconectar":
            cp_state["alerta_desconectar"] = False
    return jsonify({"status": "ok"})


# API para alterar credenciales
@app.route("/api/credentials/delete", methods=["POST"])
def api_credentials_delete():
    try:
        resp = requests.post(
            f"http://localhost:{monitor_port}/api/credentials/delete", timeout=5
        )
        return jsonify(resp.json()), resp.status_code
    except Exception as e:
        log(
            "ENGINE", f"Error al llamar al Monitor para alterar la clave: {e}", Fore.RED
        )
        return jsonify({"status": "error", "message": str(e)}), 500


# Obtener datos desde Central
def get_data_from_central(cp_id, kafka_address, central_web_port):
    try:
        central_ip = kafka_address.split(":")[0]
        central_url = f"http://{central_ip}:{central_web_port}"
        endpoint = f"{central_url}/api/cp/{cp_id}/info"
        log("CENTRAL-SYNC", f"Consultando: {endpoint}", Fore.CYAN)
        response = requests.get(endpoint, timeout=5)

        if response.status_code == 200:
            data = response.json()
            log(
                "CENTRAL-SYNC",
                f"Datos recibidos de Central: {data.get('ubicacion')}, {data.get('precio_kwh')}€/kWh",
                Fore.GREEN,
            )
            return data

        else:
            log("CENTRAL-SYNC", f"Código {response.status_code}", Fore.RED)
            return None

    except requests.exceptions.ConnectionError:
        log("CENTRAL-SYNC", f"Central no disponible en {endpoint}", Fore.RED)
        return None

    except Exception as e:
        log("CENTRAL-SYNC", f"Error: {e}", Fore.RED)
        return None


# Interrumpir suministro
def interrupt_supply(razon):
    driver = cp_state.get("driver_id")
    kwh_entregados = cp_state.get("energia_kwh", 0)
    importe = kwh_entregados * PRECIO_KWH
    log("SUPPLY", f"INTERRUMPIDO: {razon}", Fore.RED)
    cp_state["kwh_totales"] += kwh_entregados
    cp_state["importe_total"] += importe
    interruption = {
        "driver_id": driver,
        "cp_id": cp_id,
        "success": False,
        "interrupted": True,
        "reason": razon,
        "delivered_kwh": kwh_entregados,
        "importe_eur": importe,
        "ts": now_iso(),
    }
    ticket_data = {
        "cp_id": cp_id,
        "delivered_kwh": round(kwh_entregados, 3),
        "importe_eur": round(importe, 3),
        "ts": now_iso(),
        "interrupted": True,
        "reason": razon,
    }
    ticket_path = generate_ticket_pdf(driver, ticket_data)
    log("PDF", f"Ticket de interrupción guardado en: {ticket_path}", Fore.GREEN)
    producer.produce("SUPPLY_RESPONSE", json.dumps(interruption).encode())
    producer.flush()

    if razon in ["averia", "averia_engine_muerto", "weather"]:
        estado_final = "averiado"
    elif razon == "Out of order":
        estado_final = "Out of order"
    else:
        estado_final = "activo"
    cp_state["suministrando"] = False
    cp_state["estado"] = estado_final
    cp_state["driver_id"] = None
    cp_state["energia_kwh"] = 0
    cp_state["energia_objetivo"] = 0
    cp_state["importe_eur"] = 0
    cp_state["alerta_solicitud"] = False
    cp_state["alerta_solicitud_driver"] = None
    cp_state["alerta_desconectar"] = False

    with kafka_lock:
        kafka_queue.append(
            {
                "estado": estado_final,
                "monitor_ok": cp_state["monitor_ok"],
                "engine_ok": cp_state["engine_ok"],
            }
        )


# Escuchar DRIVER_STATUS desde Kafka
def kafka_driver_status_listener():
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"cp_driver_status_{cp_id}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["DRIVER_STATUS"])
    log("KAFKA-DRIVER", "Escuchando DRIVER_STATUS", Fore.MAGENTA)

    while not stop_event.is_set():
        try:
            msg = consumer.poll(0.5)

            if msg and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))
                driver_id = data.get("driver_id")
                estado = data.get("estado", "desconocido")

                with drivers_lock:
                    if estado == "libre":
                        drivers_disponibles[driver_id] = {
                            "driver_id": driver_id,
                            "estado": estado,
                            "ts": data.get("ts"),
                        }
                        log("DRIVER-LIST", f"{driver_id} disponible", Fore.GREEN)

                    else:
                        if driver_id in drivers_disponibles:
                            del drivers_disponibles[driver_id]
                            log(
                                "DRIVER-LIST",
                                f"{driver_id} no disponible ({estado})",
                                Fore.YELLOW,
                            )

        except Exception as e:
            log("KAFKA-DRIVER", f"Error: {e}", Fore.RED)
    consumer.close()


# Servidor TCP para el Engine
def tcp_engine_thread():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", engine_port))
    s.listen(5)
    s.settimeout(1.0)
    log("ENGINE-TCP", f"Puerto {engine_port} [PROTOCOLO ESTÁNDAR]", Fore.MAGENTA)

    while not stop_event.is_set():
        try:
            conn, addr = s.accept()
            log("ENGINE-TCP", f"Monitor: {addr}", Fore.GREEN)
            threading.Thread(
                target=handle_engine_client, args=(conn, addr), daemon=True
            ).start()

        except socket.timeout:
            continue

        except Exception as e:
            if not stop_event.is_set():
                log("ENGINE-TCP", f"Error: {e}", Fore.RED)
    s.close()


# Manejo de conexión del Engine con el Monitor
def handle_engine_client(conn, addr):
    try:
        with state_lock:
            cp_state["monitor_ok"] = True
            cp_state["connected"] = True

            if cp_state["engine_ok"] and cp_state["estado"] != "Out of order":
                cp_state["estado"] = "activo"
            estado_actual = cp_state["estado"]
            monitor_actual = True
            engine_actual = cp_state["engine_ok"]

        with kafka_lock:
            kafka_queue.append(
                {
                    "estado": estado_actual,
                    "monitor_ok": monitor_actual,
                    "engine_ok": engine_actual,
                }
            )
        log("ENGINE-TCP", "Monitor conectado [PROTOCOLO OK]", Fore.GREEN)

        while not stop_event.is_set():
            try:
                msg = recibir_mensaje_simple(conn, timeout=5.0)

                if not msg:
                    break

                if msg.get("type") == "health_check":
                    with state_lock:
                        engine_status = "OK" if cp_state["engine_ok"] else "KO"
                    response = {
                        "type": "health_ack",
                        "status": engine_status,
                        "ts": now_iso(),
                    }

                    if not enviar_mensaje_simple(conn, response):
                        break

            except Exception as e:
                log("ENGINE-TCP", f"Error: {e}", Fore.RED)
                break

    except Exception as e:
        log("ENGINE-TCP", f"Error: {e}", Fore.RED)

    finally:
        try:
            conn.close()

        except Exception:
            pass

        with state_lock:
            cp_state["monitor_ok"] = False
            cp_state["connected"] = False

            if cp_state["estado"] != "Out of order" and not cp_state["suministrando"]:
                cp_state["estado"] = "desconectado"

        with kafka_lock:
            kafka_queue.append(
                {
                    "estado": cp_state["estado"],
                    "monitor_ok": False,
                    "engine_ok": cp_state["engine_ok"],
                }
            )
        log("ENGINE-TCP", "Monitor desconectado", Fore.YELLOW)


# Escuchar CP_COMMAND desde Kafka
def kafka_command_listener():
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"cp_engine_{cp_id}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["CP_COMMAND"])
    log("KAFKA-CMD", f"Escuchando para {cp_id}", Fore.MAGENTA)

    while not stop_event.is_set():
        try:
            msg = consumer.poll(0.5)

            if msg and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))

                if data.get("cp_id") != cp_id:
                    continue
                cmd = data.get("command")

                with state_lock:
                    if cmd == "Out of order":
                        if cp_state.get("weather_blocked", False):
                            log(
                                "KAFKA-CMD",
                                "Ignorado Out of order por clima (T < 0°C)",
                                Fore.YELLOW,
                            )
                            continue

                        if cp_state["suministrando"]:
                            interrupt_supply("Out of order")

                        else:
                            cp_state["estado"] = "Out of order"
                            cp_state["engine_ok"] = False

                        with kafka_lock:
                            kafka_queue.append(
                                {
                                    "estado": "Out of order",
                                    "monitor_ok": cp_state["monitor_ok"],
                                    "engine_ok": False,
                                }
                            )
                        log("KAFKA-CMD", "Out of order", Fore.YELLOW)

                    elif cmd == "resume_service":
                        cp_state["estado"] = "activo"
                        cp_state["engine_ok"] = True

                        with kafka_lock:
                            kafka_queue.append(
                                {
                                    "estado": "activo",
                                    "monitor_ok": cp_state["monitor_ok"],
                                    "engine_ok": True,
                                }
                            )
                        log("KAFKA-CMD", "resume_service", Fore.GREEN)

                    elif cmd == "start_supply":
                        if (
                            cp_state["estado"] == "activo"
                            and not cp_state["suministrando"]
                        ):
                            driver_id = data.get("driver_id")
                            kwh_req = data.get("kwh_requested", 10.0)
                            cp_state["driver_id"] = driver_id
                            cp_state["energia_objetivo"] = kwh_req
                            cp_state["alerta_solicitud"] = True
                            cp_state["alerta_solicitud_driver"] = driver_id
                            log(
                                "KAFKA-CMD",
                                f"{driver_id} solicita {kwh_req} kWh",
                                Fore.MAGENTA,
                            )

        except json.JSONDecodeError:
            continue

        except Exception as e:
            log("KAFKA-CMD", f"Error: {e}", Fore.RED)
    consumer.close()


# Escuchar CP_STATUS desde Kafka
def kafka_cp_status_listener():
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"cp_engine_listener_{cp_id}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["CP_STATUS"])

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))

            if data.get("cp_id") != cp_id:
                continue
            estado_recibido = data.get("estado", "desconectado")
            engine_ok_kafka = data.get("engine_ok", False)

            with state_lock:
                old_state = cp_state["estado"]

                if cp_state.get("weather_blocked", False):
                    if estado_recibido != "averiado":
                        log(
                            "KAFKA-STATUS",
                            "Cambio ignorado - bloqueado por clima (T < 0°C)",
                            Fore.YELLOW,
                        )
                        continue

                cp_state["estado"] = estado_recibido

                if (
                    old_state == "suministrando"
                    and estado_recibido == "averiado"
                    and not engine_ok_kafka
                    and cp_state["suministrando"]
                ):
                    log(
                        "KAFKA-STATUS",
                        "ENGINE MUERTO - INTERRUMPIENDO SUMINISTRO",
                        Fore.RED,
                    )
                    interrupt_supply("averia_engine_muerto")
                    cp_state["estado"] = "averiado"
                    cp_state["engine_ok"] = False
                log(
                    "KAFKA-STATUS",
                    f"Estado actualizado: {old_state} → {estado_recibido}",
                    Fore.CYAN,
                )

        except Exception as e:
            log("KAFKA-STATUS", f"Error: {e}", Fore.RED)


# Iniciar suministro tras aceptación del operario
def start_supply_accepted():
    with state_lock:
        if not cp_state["alerta_solicitud"]:
            return

        if cp_state.get("weather_blocked", False):
            log("SUPPLY", "Suministro bloqueado - temperatura bajo 0°C", Fore.RED)
            cp_state["alerta_solicitud"] = False
            cp_state["alerta_solicitud_driver"] = None
            return

        driver_id = cp_state.get("alerta_solicitud_driver")
        kwh_req = cp_state.get("energia_objetivo", 0)
        cp_state["alerta_solicitud"] = False
        cp_state["alerta_solicitud_driver"] = None
        cp_state["suministrando"] = True
        cp_state["estado"] = "suministrando"
        cp_state["energia_kwh"] = 0.0
        cp_state["importe_eur"] = 0.0
        log("SUPPLY", f"ACEPTADO - INICIANDO: {driver_id} → {kwh_req} kWh", Fore.GREEN)

        with kafka_lock:
            kafka_queue.append(
                {
                    "estado": "suministrando",
                    "monitor_ok": cp_state["monitor_ok"],
                    "engine_ok": cp_state["engine_ok"],
                }
            )
    simulate_supply_realtime(driver_id, kwh_req)


# Simulación de suministro en tiempo real
def simulate_supply_realtime(driver_id, kwh_objetivo):
    try:
        incremento = (KW_POR_SEGUNDO * MULTIPLICADOR_VELOCIDAD) / 10
        ticks_totales = int((kwh_objetivo / incremento) + 1)
        tiempo_estimado = (ticks_totales * INTERVALO_MS) / 1000
        log(
            "SUPPLY",
            f"VELOCIDAD LUZ: {kwh_objetivo} kWh en ~{tiempo_estimado:.2f}s",
            Fore.LIGHTGREEN_EX,
        )

        for tick in range(ticks_totales):
            if stop_event.is_set():
                break

            with state_lock:
                if not cp_state["suministrando"]:
                    return
                cp_state["energia_kwh"] += incremento

                if cp_state["energia_kwh"] > kwh_objetivo:
                    cp_state["energia_kwh"] = kwh_objetivo
                cp_state["importe_eur"] = cp_state["energia_kwh"] * PRECIO_KWH
                cp_state["porcentaje"] = (cp_state["energia_kwh"] / kwh_objetivo) * 100
                progreso = {
                    "cp_id": cp_id,
                    "driver_id": driver_id,
                    "estado": "suministrando",
                    "energia_actual": round(cp_state["energia_kwh"], 3),
                    "energia_objetivo": kwh_objetivo,
                    "importe_actual": round(cp_state["importe_eur"], 3),
                    "porcentaje": round(
                        (cp_state["energia_kwh"] / kwh_objetivo) * 100, 1
                    ),
                    "ts": now_iso(),
                }
            producer.produce("SUPPLY_PROGRESS", json.dumps(progreso).encode())
            producer.flush()
            time.sleep(INTERVALO_MS / 1000)

        with state_lock:
            kwh_final = kwh_objetivo
            importe_final = kwh_objetivo * PRECIO_KWH
            cp_state["kwh_totales"] += kwh_final
            cp_state["importe_total"] += importe_final
            cp_state["suministrando"] = False
            cp_state["estado"] = "activo"
            cp_state["alerta_desconectar"] = True

            with kafka_lock:
                kafka_queue.append(
                    {
                        "estado": "activo",
                        "monitor_ok": cp_state["monitor_ok"],
                        "engine_ok": cp_state["engine_ok"],
                    }
                )
            completion = {
                "cp_id": cp_id,
                "driver_id": driver_id,
                "delivered_kwh": round(kwh_final, 3),
                "importe_eur": round(importe_final, 3),
                "ts": now_iso(),
            }
            ticket_data = {
                "cp_id": cp_id,
                "delivered_kwh": round(kwh_final, 3),
                "importe_eur": round(importe_final, 3),
                "ts": now_iso(),
            }
            ticket_path = generate_ticket_pdf(driver_id, ticket_data)
            log("PDF", f"Ticket guardado en: {ticket_path}", Fore.GREEN)
            producer.produce("SUPPLY_COMPLETED", json.dumps(completion).encode())
            producer.flush()
            log(
                "SUPPLY-DONE",
                f"COMPLETADO: {kwh_final:.3f} kWh, {importe_final:.3f}€",
                Fore.GREEN,
            )
            cp_state["energia_kwh"] = 0
            cp_state["energia_objetivo"] = 0
            cp_state["importe_eur"] = 0
            cp_state["driver_id"] = None
            cp_state["suministrando"] = False
        time.sleep(2)

    except Exception as e:
        log("SUPPLY", f"Error: {e}", Fore.RED)

        with state_lock:
            cp_state["suministrando"] = False
            cp_state["estado"] = "activo"


# Publicador Kafka para actualizar estado del CP
def kafka_publisher_thread():
    while not stop_event.is_set():
        try:
            with kafka_lock:
                if kafka_queue:
                    msg_data = kafka_queue.pop(0)

                else:
                    msg_data = None

            if msg_data:
                data = {
                    "cp_id": cp_id,
                    "ubicacion": ubicacion,
                    "estado": msg_data["estado"],
                    "suministrando": msg_data["estado"] == "suministrando",
                    "monitor_ok": msg_data["monitor_ok"],
                    "engine_ok": msg_data["engine_ok"],
                    "weather_blocked": cp_state.get("weather_blocked", False),
                    "engine_ip": engine_ip,
                    "engine_port": web_port,
                    "ts": now_iso(),
                }
                producer.produce("CP_STATUS", json.dumps(data).encode("utf-8"))
                producer.flush()
                log("KAFKA-PUBLISH", f"{msg_data['estado']}", Fore.CYAN)

            else:
                time.sleep(0.1)

        except Exception as e:
            log("KAFKA-PUBLISH", f"Error: {e}", Fore.RED)
            time.sleep(1)


# Generar ticket en PDF
def generate_ticket_pdf(driver_id, data):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        tickets_dir = os.path.join(base_dir, "tickets")
        os.makedirs(tickets_dir, exist_ok=True)
        timestamp = data.get("ts", now_iso()).replace(":", "-").replace("Z", "")
        filename = f"{driver_id}_{timestamp}.pdf"
        filepath = os.path.join(tickets_dir, filename)
        doc = SimpleDocTemplate(filepath, pagesize=letter)
        elements = []
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "CustomTitle",
            parent=styles["Heading1"],
            fontSize=18,
            textColor=colors.HexColor("#1a7f1f"),
            spaceAfter=20,
            alignment=1,
        )
        title = Paragraph("📋 TICKET DE SUMINISTRO", title_style)
        elements.append(title)
        elements.append(Spacer(1, 0.2 * inch))
        cp_id_val = data.get("cp_id", "N/A")
        delivered = data.get("delivered_kwh", 0)
        importe = data.get("importe_eur", 0)

        if data.get("interrupted"):
            estado = f"❌ INTERRUMPIDO - {data.get('reason', 'N/A')}"

        elif data.get("terminar_suministro"):
            estado = "✅ COMPLETADO (MANUAL)"

        else:
            estado = "✅ COMPLETADO"
        table_data = [
            ["CAMPO", "VALOR"],
            ["Driver ID", driver_id],
            ["CP ID", cp_id_val],
            ["Energía (kWh)", f"{delivered:.3f}"],
            ["Importe (€)", f"{importe:.2f}"],
            ["Fecha/Hora", f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"],
            ["Estado", estado],
        ]
        table = Table(table_data, colWidths=[2 * inch, 2.5 * inch])
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a7f1f")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 12),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                    (
                        "ROWBACKGROUNDS",
                        (0, 1),
                        (-1, -1),
                        [colors.white, colors.lightgrey],
                    ),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 10),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        elements.append(table)
        elements.append(Spacer(1, 0.3 * inch))
        footer_style = ParagraphStyle(
            "Footer",
            parent=styles["Normal"],
            fontSize=8,
            textColor=colors.grey,
            alignment=1,
            spaceAfter=0,
        )
        footer = Paragraph(
            f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} | Sistema EV-CP v1.0 | Ticket automático",
            footer_style,
        )
        elements.append(footer)
        doc.build(elements)
        log("PDF", f"✅ Ticket: {filename}", Fore.GREEN)
        return filepath

    except Exception as e:
        log("PDF", f"Error: {e}", Fore.RED)
        return None


# Análisis de argumentos
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--cp-id", required=True)
    p.add_argument("--kafka", required=True)
    p.add_argument("--ubicacion", default="Desconocido")
    p.add_argument("--precio-kwh", type=float, default=0.30)
    p.add_argument("--engine-port", type=int, default=6000)
    p.add_argument("--web-port", type=int, default=5011)
    p.add_argument("--monitor-port", type=int, default=5101)
    p.add_argument("--central-web-port", type=int, default=5001)
    p.add_argument("--engine-ip", default=socket.gethostbyname(socket.gethostname()))
    return p.parse_args()


# Programa principal y arranque de hilos
if __name__ == "__main__":
    args = parse_args()
    cp_id = args.cp_id
    bootstrap = args.kafka
    engine_port = args.engine_port
    web_port = args.web_port
    monitor_port = args.monitor_port
    engine_ip = args.engine_ip
    central_ip = args.kafka.split(":")[0]
    central_base_url = f"http://{central_ip}:{args.central_web_port}"
    central_data = get_data_from_central(cp_id, args.kafka, args.central_web_port)

    if central_data:
        ubicacion = central_data.get("ubicacion", args.ubicacion)
        if (
            ubicacion == "ubicacion-EV_DB_API"
            or not ubicacion
            or ubicacion == "Desconocida"
        ):
            ubicacion = args.ubicacion
        PRECIO_KWH = central_data.get("precio_kwh", args.precio_kwh)
        if PRECIO_KWH == 0.3:
            PRECIO_KWH = args.precio_kwh
        log("CP-INIT", f"De Central: {ubicacion}, {PRECIO_KWH}€/kWh", Fore.GREEN)

    else:
        ubicacion = args.ubicacion
        PRECIO_KWH = args.precio_kwh if args.precio_kwh else 0.30
        log("CP-INIT", f"Fallback: {ubicacion}, {PRECIO_KWH}€/kWh", Fore.YELLOW)

    if (
        not ubicacion
        or ubicacion == "Desconocida"
        or ubicacion.startswith("ubicacion-")
    ):
        ubicacion = args.ubicacion or "Desconocida"

    cp_state["ubicacion"] = ubicacion
    cp_state["cp_id"] = cp_id
    producer = Producer({"bootstrap.servers": bootstrap})
    threading.Thread(target=tcp_engine_thread, daemon=True).start()
    threading.Thread(target=kafka_command_listener, daemon=True).start()
    threading.Thread(target=kafka_publisher_thread, daemon=True).start()
    threading.Thread(target=kafka_cp_status_listener, daemon=True).start()
    threading.Thread(target=kafka_driver_status_listener, daemon=True).start()
    log("CP-ENGINE", f"CP {cp_id} iniciando", Fore.CYAN)
    log("CP-ENGINE", f"Engine TCP: {engine_port}", Fore.CYAN)
    log("CP-ENGINE", f"Web: {web_port}", Fore.CYAN)
    log("CP-ENGINE", "Protocolo: ESTÁNDAR <STX><DATA><ETX><LRC>", Fore.GREEN)

    try:
        app.run(host="0.0.0.0", port=web_port, debug=False, use_reloader=False)

    except KeyboardInterrupt:
        log("CP-ENGINE", "Stop", Fore.YELLOW)
        stop_event.set()
