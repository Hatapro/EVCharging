"""
Uso:
python EV_Driver.py --driver-id DRIVER-001 --kafka 192.168.18.148:9092 --web-port 5051 --services-file servicios.txt
"""

import argparse
import json
import os
import threading
import time
from datetime import datetime
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from confluent_kafka import Consumer, Producer
from colorama import init as colorama_init, Fore, Style

colorama_init(autoreset=True)


# Utilidades de tiempo y logging
def now():
    return datetime.now().isoformat() + "Z"


def log(tag, msg, color=Fore.CYAN):
    print(f"{color}[{tag}] {msg}{Style.RESET_ALL}")


# Flask y SocketIO setup
app = Flask(__name__)
app.config["SECRET_KEY"] = "ev-driver-secret-2025"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# Estado global
driver_id = None
bootstrap = None
services_file = None
services_queue = []
driver_state = {
    "driver_id": None,
    "estado": "libre",
    "cp_actual": None,
    "kwh_solicitado": 0.0,
    "kwh_entregado": 0.0,
    "importe_eur": 0.0,
    "kwh_totales": 0.0,
    "importe_total": 0.0,
    "servicios_completados": 0,
    "porcentaje_suministro": 0,
    "central_disponible": True,
    "ultimo_heartbeat_central": 0,
}
cp_states = {}
cp_lock = threading.Lock()
state_lock = threading.Lock()
stop_event = threading.Event()
producer = None
first_service_started = False
pending_requests = {}


# Rutas Flask
@app.route("/")
def index():
    return render_template("driver.html", driver_id=driver_id)


@app.route("/api/driver_state", methods=["GET"])
def api_get_state():
    with state_lock:
        return jsonify(driver_state)


@app.route("/api/cps", methods=["GET"])
def api_get_cps():
    with cp_lock:
        return jsonify({"cps": cp_states})


@socketio.on("connect")
def on_connect():
    log("WEB", "Driver conectado", Fore.GREEN)

    with state_lock:
        state_copy = driver_state.copy()

    with cp_lock:
        cps_copy = cp_states.copy()
    emit("driver_state", state_copy)
    emit("cp_states", cps_copy)
    log("WEB", "Enviado datos", Fore.GREEN)


@socketio.on("select_cp")
def on_select_cp(data):
    cp_id = data.get("cp_id")
    kwh = float(data.get("kwh", 10.0))

    with state_lock:
        if not driver_state["central_disponible"]:
            log("DRIVER", "Central no disponible - rechazando solicitud", Fore.RED)
            emit(
                "alert",
                {
                    "type": "error",
                    "title": "CENTRAL NO DISPONIBLE",
                    "message": "La Central ha caído. No se pueden procesar solicitudes normales.\n\nLos CPs pueden iniciar suministros directos.",
                },
            )
            return
    log("DRIVER", f"Solicita {kwh} kWh en {cp_id}", Fore.MAGENTA)
    request_msg = {
        "driver_id": driver_id,
        "preferred_cp_id": cp_id,
        "kWh": kwh,
        "ts": now(),
    }
    request_id = f"{driver_id}_{cp_id}_{int(time.time())}"
    with state_lock:
        pending_requests[request_id] = {
            "cp_id": cp_id,
            "kwh": kwh,
            "timestamp": time.time(),
            "timeout": 10,
        }
    producer.produce("SUPPLY_REQUEST", json.dumps(request_msg).encode())
    producer.flush()

    with state_lock:
        driver_state["estado"] = "esperando"
        driver_state["cp_actual"] = cp_id
        driver_state["kwh_solicitado"] = kwh
        driver_state["porcentaje_suministro"] = 0
    socketio.emit("driver_state", driver_state)
    log("DRIVER", f"Solicitud enviada a {cp_id}", Fore.GREEN)


# Carga servicios desde archivo
def load_services():
    global services_queue

    if services_file and os.path.exists(services_file):
        try:
            with open(services_file, "r") as f:
                for line in f:
                    line = line.strip()

                    if line:
                        parts = line.split(",")

                        if len(parts) >= 2:
                            services_queue.append(
                                {
                                    "cp_id": parts[0].strip(),
                                    "kwh": float(parts[1].strip()),
                                }
                            )
            log("SERVICES", f"Cargados {len(services_queue)} servicios", Fore.GREEN)

        except Exception as e:
            log("SERVICES", f"Error: {e}", Fore.RED)


# Procesa el siguiente servicio en la cola
def process_next_service():
    global services_queue

    if services_queue:
        with state_lock:
            if not driver_state["central_disponible"]:
                log(
                    "SERVICES",
                    "Central caída - pausando servicios automáticos",
                    Fore.YELLOW,
                )
                return
        time.sleep(4)
        servicio = services_queue.pop(0)
        log(
            "SERVICES",
            f"Procesando: {servicio['cp_id']} ({servicio['kwh']} kWh)",
            Fore.CYAN,
        )

        with state_lock:
            driver_state["estado"] = "esperando"
            driver_state["cp_actual"] = servicio["cp_id"]
            driver_state["kwh_solicitado"] = servicio["kwh"]
            driver_state["porcentaje_suministro"] = 0
        socketio.emit("driver_state", driver_state)
        request_msg = {
            "driver_id": driver_id,
            "preferred_cp_id": servicio["cp_id"],
            "kWh": servicio["kwh"],
            "ts": now(),
        }
        producer.produce("SUPPLY_REQUEST", json.dumps(request_msg).encode())
        producer.flush()
        log("DRIVER", f"Solicitud automática: {servicio['cp_id']}", Fore.GREEN)


# Inicia el primer servicio
def start_first_service():
    global first_service_started
    time.sleep(2)

    with state_lock:
        if first_service_started or driver_state["estado"] != "libre":
            return
        first_service_started = True

    if services_queue:
        log("AUTO-START", "Iniciando primer servicio automáticamente", Fore.GREEN)
        process_next_service()

    else:
        log("AUTO-START", "No hay servicios en cola", Fore.YELLOW)


# NUEVO: Monitor de timeout de solicitudes
def request_timeout_monitor():
    while not stop_event.is_set():
        time.sleep(2)
        current_time = time.time()
        expired_requests = []

        with state_lock:
            for req_id, req_data in list(pending_requests.items()):
                if current_time - req_data["timestamp"] > req_data["timeout"]:
                    expired_requests.append(req_id)
                    log("TIMEOUT", f"Solicitud expirada: {req_data['cp_id']}", Fore.RED)
                    driver_state["central_disponible"] = False

                    if driver_state["estado"] == "esperando":
                        driver_state["estado"] = "libre"
                        driver_state["cp_actual"] = None
                        driver_state["kwh_solicitado"] = 0
                    socketio.emit("driver_state", driver_state)
                    socketio.emit(
                        "alert",
                        {
                            "type": "timeout",
                            "title": "TIMEOUT - CENTRAL NO RESPONDE",
                            "message": f"No se recibió respuesta de Central para {req_data['cp_id']}.\n\nLa Central puede haber caído.\n\nLos CPs pueden iniciar suministros directos.",
                        },
                    )

            for req_id in expired_requests:
                del pending_requests[req_id]


# Monitor de disponibilidad de Central
def central_availability_monitor():
    check_interval = 5
    timeout_threshold = 5

    while not stop_event.is_set():
        time.sleep(check_interval)
        current_time = time.time()

        with state_lock:
            time_since_last = current_time - driver_state["ultimo_heartbeat_central"]

            if time_since_last > timeout_threshold:
                if driver_state["central_disponible"]:
                    driver_state["central_disponible"] = False
                    log("MONITOR", "Central no responde - marcada como caída", Fore.RED)
                    socketio.emit("central_status", {"disponible": False})

            else:
                if not driver_state["central_disponible"]:
                    driver_state["central_disponible"] = True
                    log("MONITOR", "Central recuperada", Fore.GREEN)
                    socketio.emit("central_status", {"disponible": True})


# Listeners CP_REGISTRY de Kafka (estado completo desde Central)
def kafka_cp_registry_listener():
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"driver_cp_registry_{driver_id}_{int(time.time())}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["CP_REGISTRY"])
    log("KAFKA", "Escuchando CP_REGISTRY", Fore.YELLOW)

    while not stop_event.is_set():
        try:
            msg = consumer.poll(0.5)

            if msg and not msg.error():
                with state_lock:
                    driver_state["ultimo_heartbeat_central"] = time.time()

                data = json.loads(msg.value().decode("utf-8"))
                cp_id = data.get("cp_id")
                estado = data.get("estado", "desconocido")
                ubicacion = data.get("ubicacion")
                weather_blocked = data.get("weather_blocked", False)

                with cp_lock:
                    if estado in ["activo", "disponible"] and not weather_blocked:
                        if cp_id in cp_states:
                            cp_states[cp_id]["estado"] = estado
                            cp_states[cp_id]["engine_ok"] = data.get("engine_ok", False)
                            cp_states[cp_id]["monitor_ok"] = data.get(
                                "monitor_ok", False
                            )
                            cp_states[cp_id]["weather_blocked"] = weather_blocked

                            if ubicacion and ubicacion != "N/A":
                                cp_states[cp_id]["ubicacion"] = ubicacion
                            log(
                                "CP-LIST",
                                f"{cp_id} actualizado: {estado} (weather_blocked={weather_blocked})",
                                Fore.GREEN,
                            )

                        else:
                            cp_states[cp_id] = {
                                "cp_id": cp_id,
                                "estado": estado,
                                "ubicacion": ubicacion,
                                "engine_ok": data.get("engine_ok", False),
                                "monitor_ok": data.get("monitor_ok", False),
                                "weather_blocked": weather_blocked,
                            }
                            log(
                                "CP-LIST",
                                f"{cp_id} disponible ({ubicacion}, weather_blocked={weather_blocked})",
                                Fore.GREEN,
                            )

                    else:
                        if cp_id in cp_states:
                            del cp_states[cp_id]
                            if weather_blocked:
                                log(
                                    "CP-LIST",
                                    f"{cp_id} bloqueado por clima ({ubicacion})",
                                    Fore.YELLOW,
                                )
                            else:
                                log(
                                    "CP-LIST",
                                    f"{cp_id} no disponible ({estado})",
                                    Fore.YELLOW,
                                )
                    socketio.emit("cp_states", cp_states)

        except Exception as e:
            log("KAFKA", f"Error: {e}", Fore.RED)
    consumer.close()


# Listener para SUPPLY_DIRECT_START
def kafka_direct_supply_listener():
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"driver_direct_{driver_id}_{int(time.time())}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["SUPPLY_DIRECT_START"])
    log("KAFKA", "Escuchando SUPPLY_DIRECT_START", Fore.MAGENTA)

    while not stop_event.is_set():
        try:
            msg = consumer.poll(0.5)

            if msg and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))

                if data.get("driver_id") != driver_id:
                    continue
                cp_id = data.get("cp_id")
                kwh_requested = data.get("kwh_requested", 10.0)
                log(
                    "DIRECT-START",
                    f"CP {cp_id} inició suministro directo: {kwh_requested} kWh",
                    Fore.MAGENTA,
                )

                with state_lock:
                    driver_state["estado"] = "suministrando"
                    driver_state["cp_actual"] = cp_id
                    driver_state["kwh_solicitado"] = kwh_requested
                    driver_state["kwh_entregado"] = 0.0
                    driver_state["importe_eur"] = 0.0
                    driver_state["porcentaje_suministro"] = 0
                socketio.emit("driver_state", driver_state)
                socketio.emit(
                    "alert",
                    {
                        "type": "direct_start",
                        "title": "SUMINISTRO INICIADO",
                        "message": f"El CP {cp_id} ha iniciado un suministro de {kwh_requested} kWh",
                    },
                )

        except Exception as e:
            log("KAFKA", f"Error DIRECT_START: {e}", Fore.RED)
    consumer.close()


# Listener para SUPPLY_RESPONSE
def kafka_response_listener():
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"driver_resp_{driver_id}_{int(time.time())}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["SUPPLY_RESPONSE"])
    log("KAFKA", "Escuchando SUPPLY_RESPONSE", Fore.MAGENTA)

    while not stop_event.is_set():
        try:
            msg = consumer.poll(0.5)

            if msg and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))

                if data.get("driver_id") != driver_id:
                    continue

                with state_lock:
                    cp_id = data.get("cp_id")

                    for req_id in list(pending_requests.keys()):
                        if pending_requests[req_id]["cp_id"] == cp_id:
                            del pending_requests[req_id]
                            break

                if data.get("interrupted"):
                    reason = data.get("reason", "desconocido")
                    delivered = data.get("delivered_kwh", 0)
                    importe = data.get("importe_eur", 0)
                    log("SUPPLY-RESP", f"INTERRUPCIÓN: {reason}", Fore.RED)

                    with state_lock:
                        driver_state["estado"] = "libre"
                        driver_state["cp_actual"] = None
                        driver_state["kwh_entregado"] = delivered
                        driver_state["importe_eur"] = importe
                        driver_state["importe_total"] += importe
                        driver_state["porcentaje_suministro"] = 0
                        driver_state["servicios_completados"] += 1
                        driver_state["kwh_solicitado"] = 0
                    socketio.emit("driver_state", driver_state)
                    socketio.emit(
                        "alert",
                        {
                            "type": "interrupcion",
                            "title": f"INTERRUPCIÓN: {reason}",
                            "message": f"Suministro interrumpido: {reason}\nEnergía: {delivered:.2f} kWh\nImporte: {importe:.2f}€",
                            "reason": reason,
                            "data": {
                                "delivered_kwh": delivered,
                                "importe_eur": importe,
                            },
                        },
                    )

                    if services_queue:
                        threading.Thread(
                            target=process_next_service, daemon=True
                        ).start()

                elif data.get("success") == False:  # noqa: E712
                    log(
                        "SUPPLY-RESP",
                        f"Rechazado: {data.get('reason', 'N/A')}",
                        Fore.RED,
                    )

                    with state_lock:
                        driver_state["estado"] = "libre"
                        driver_state["cp_actual"] = None
                        driver_state["porcentaje_suministro"] = 0
                    socketio.emit("driver_state", driver_state)
                    socketio.emit(
                        "alert",
                        {
                            "type": "rechazado",
                            "title": "Solicitud Rechazada",
                            "message": data.get("reason", "CP no disponible"),
                        },
                    )

                    if services_queue:
                        threading.Thread(
                            target=process_next_service, daemon=True
                        ).start()

                elif data.get("estado") == "autorizado":
                    log("SUPPLY-RESP", "Autorizado", Fore.GREEN)

                    with state_lock:
                        driver_state["estado"] = "suministrando"
                        driver_state["kwh_solicitado"] = data.get("kwh_requested", 0)
                        driver_state["importe_eur"] = 0
                        driver_state["kwh_entregado"] = 0
                        driver_state["porcentaje_suministro"] = 0
                    socketio.emit("driver_state", driver_state)

                elif data.get("success") == True:  # noqa: E712
                    delivered = data.get("delivered_kwh", 0)
                    importe = data.get("importe_eur", 0)
                    log(
                        "SUPPLY-RESP",
                        f"COMPLETADO: {delivered} kWh, {importe}€",
                        Fore.GREEN,
                    )

                    with state_lock:
                        driver_state["kwh_entregado"] = delivered
                        driver_state["kwh_totales"] += delivered
                        driver_state["importe_eur"] = importe
                        driver_state["importe_total"] += importe
                        driver_state["servicios_completados"] += 1
                        driver_state["porcentaje_suministro"] = 100
                    socketio.emit("driver_state", driver_state)
                    socketio.emit(
                        "alert",
                        {
                            "type": "ticket",
                            "title": "SUMINISTRO COMPLETADO",
                            "message": f"Energía: {delivered} kWh\nImporte: {importe}€",
                            "data": data,
                        },
                    )
                    time.sleep(2)

                    with state_lock:
                        driver_state["estado"] = "libre"
                        driver_state["cp_actual"] = None
                        driver_state["kwh_solicitado"] = 0
                        driver_state["kwh_entregado"] = 0
                        driver_state["importe_eur"] = 0
                        driver_state["porcentaje_suministro"] = 0
                    socketio.emit("driver_state", driver_state)

                    if services_queue:
                        log(
                            "SERVICES",
                            f"Servicios restantes: {len(services_queue)}",
                            Fore.CYAN,
                        )
                        threading.Thread(
                            target=process_next_service, daemon=True
                        ).start()

                    else:
                        log("SERVICES", "Todos los servicios completados", Fore.GREEN)

        except Exception as e:
            log("KAFKA", f"Error: {e}", Fore.RED)
    consumer.close()


# Listener para SUPPLY_PROGRESS
def kafka_progress_listener():
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"driver_prog_{driver_id}_{int(time.time())}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe(["SUPPLY_PROGRESS"])
    log("KAFKA", "Escuchando SUPPLY_PROGRESS", Fore.CYAN)

    while not stop_event.is_set():
        try:
            msg = consumer.poll(0.5)

            if msg and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))

                if data.get("driver_id") != driver_id:
                    continue
                energia_actual = data.get("energia_actual", 0)
                importe_actual = data.get("importe_actual", 0)
                porcentaje = data.get("porcentaje", 0)

                with state_lock:
                    driver_state["kwh_entregado"] = energia_actual
                    driver_state["importe_eur"] = importe_actual
                    driver_state["porcentaje_suministro"] = porcentaje
                socketio.emit(
                    "supply_progress",
                    {
                        "energia": energia_actual,
                        "importe": importe_actual,
                        "porcentaje": porcentaje,
                    },
                )

        except Exception as e:
            log("KAFKA", f"Error: {e}", Fore.RED)
    consumer.close()


# Publicador de DRIVER_STATUS
def kafka_driver_status_publisher():
    log("KAFKA", "Publicando estado de driver", Fore.CYAN)

    while not stop_event.is_set():
        try:
            with state_lock:
                estado_actual = driver_state.get("estado", "libre")
            status_msg = {"driver_id": driver_id, "estado": estado_actual, "ts": now()}
            producer.produce("DRIVER_STATUS", json.dumps(status_msg).encode())
            producer.flush()
            time.sleep(2)

        except Exception as e:
            log("KAFKA", f"Error publicando estado: {e}", Fore.RED)
            time.sleep(1)


# Análisis de argumentos
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--driver-id", required=True)
    p.add_argument("--kafka", required=True)
    p.add_argument("--web-port", type=int, default=5051)
    p.add_argument("--services-file", default=None)
    return p.parse_args()


# Programa principal y arranque de hilos
if __name__ == "__main__":
    args = parse_args()
    driver_id = args.driver_id
    bootstrap = args.kafka
    services_file = args.services_file

    driver_state["driver_id"] = driver_id
    driver_state["ultimo_heartbeat_central"] = time.time()
    producer = Producer({"bootstrap.servers": bootstrap})
    load_services()
    threading.Thread(target=kafka_cp_registry_listener, daemon=True).start()
    threading.Thread(target=kafka_response_listener, daemon=True).start()
    threading.Thread(target=kafka_progress_listener, daemon=True).start()
    threading.Thread(target=kafka_direct_supply_listener, daemon=True).start()
    threading.Thread(target=kafka_driver_status_publisher, daemon=True).start()
    threading.Thread(target=request_timeout_monitor, daemon=True).start()
    threading.Thread(target=central_availability_monitor, daemon=True).start()

    if services_file and services_queue:
        threading.Thread(target=start_first_service, daemon=True).start()

    log("DRIVER", f"Driver {driver_id} iniciando", Fore.CYAN)
    log("DRIVER", f"Kafka: {bootstrap}", Fore.CYAN)
    log("DRIVER", f"Web: {args.web_port}", Fore.CYAN)

    if services_queue:
        log("DRIVER", f"Servicios: {len(services_queue)}", Fore.GREEN)

    try:
        socketio.run(
            app, host="0.0.0.0", port=args.web_port, debug=False, use_reloader=False
        )

    except KeyboardInterrupt:
        log("DRIVER", "Stop", Fore.YELLOW)
        stop_event.set()
