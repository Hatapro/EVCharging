"""
Uso:
python EV_CP_M.py --kafka 192.168.18.148:9092 --cp-id CP-001 --engine 192.168.18.148:6000 --central 192.168.18.148:5000 --registry 192.168.18.148:7000 --web-port 5101 --engine-web-port 5011 --central-web-port 5001
"""

import json
import time
import socket
import threading
import argparse
import requests
import os
import hashlib
import signal
import base64
from cryptography.fernet import Fernet
from confluent_kafka import Producer, Consumer
from datetime import datetime
from colorama import init as colorama_init, Fore, Style
from protocol import enviar_mensaje_simple, recibir_mensaje_simple
from flask import Flask, jsonify, render_template, request
from werkzeug.serving import make_server

colorama_init(autoreset=True)


# Clave de encriptación simétrica para guardar contraseñas y tokens
def get_encryption_key():
    try:
        system_id = f"{socket.gethostname()}:{os.getenv('USERNAME')}"
        key_material = hashlib.sha256(system_id.encode()).digest()
        key = base64.urlsafe_b64encode(key_material)
        return key
    except Exception as e:
        log("CRYPTO", f"Error generando clave: {e}", Fore.RED)
        return None


FERNET_KEY = get_encryption_key()


# Utilidades de tiempo, logging y manejo de archivos
def now():
    return datetime.now().isoformat() + "Z"


def log(tag, msg, color=Fore.CYAN):
    print(f"{color}[{tag}] {msg}{Style.RESET_ALL}")


def password_file_path(cp_id):
    return f"claves/{cp_id}.secret"


def token_file_path(cp_id):
    return f"claves/{cp_id}.token"


def hash_password(raw_password: str) -> str:
    return hashlib.sha256(raw_password.encode("utf-8")).hexdigest()


def load_saved_password_hash(cp_id):
    path = password_file_path(cp_id)
    if not os.path.exists(path):
        return None
    try:
        if FERNET_KEY:
            with open(path, "rb") as f:
                encrypted_data = f.read()
            cipher = Fernet(FERNET_KEY)
            decrypted = cipher.decrypt(encrypted_data).decode("utf-8")
            return decrypted.strip()
        else:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
    except Exception as e:
        log("PWD", f"Error leyendo/desencriptando hash: {e}", Fore.RED)
        return None


def save_password_hash(cp_id, password_hash):
    path = password_file_path(cp_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        if FERNET_KEY:
            cipher = Fernet(FERNET_KEY)
            encrypted = cipher.encrypt(password_hash.encode())
            with open(path, "wb") as f:
                f.write(encrypted)
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(password_hash)
        log("PWD", f"Contraseña guardada de forma encriptada en {path}", Fore.GREEN)
        return True
    except Exception as e:
        log("PWD", f"Error guardando hash: {e}", Fore.RED)
        return False


def load_saved_token(cp_id):
    path = token_file_path(cp_id)
    if not os.path.exists(path):
        return None
    try:
        if FERNET_KEY:
            with open(path, "rb") as f:
                encrypted_data = f.read()
            cipher = Fernet(FERNET_KEY)
            decrypted = cipher.decrypt(encrypted_data).decode("utf-8")
            return decrypted.strip()
        else:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
    except Exception:
        return None


def save_token(cp_id, token):
    path = token_file_path(cp_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        if FERNET_KEY:
            cipher = Fernet(FERNET_KEY)
            encrypted = cipher.encrypt(token.encode())
            with open(path, "wb") as f:
                f.write(encrypted)
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(token)
        log("TOKEN", f"Token guardado de forma encriptada en {path}", Fore.GREEN)
        return True
    except Exception as e:
        log("TOKEN", f"Error guardando token: {e}", Fore.RED)
        return False


app = Flask(__name__)


class ServerThread(threading.Thread):
    def __init__(self, app, host, port):
        super().__init__(daemon=True)
        self.server = make_server(host, port, app)

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


monitor_instance = None
WEB_PORT = None
shutdown_requested = threading.Event()


# Ruta principal
@app.route("/")
def monitor_index():
    return render_template("monitor.html", cp_id=monitor_instance.cp_id)


# API para obtener el estado del monitor
@app.route("/api/monitor_state", methods=["GET"])
def api_monitor_state():
    with monitor_instance.lock:
        estado_publicado = (
            "desconectado"
            if not monitor_instance.authenticated
            else monitor_instance.cp_state.get("estado", "desconectado")
        )
        data = {
            "cp_id": monitor_instance.cp_id,
            "estado": estado_publicado,
            "estado_raw": monitor_instance.cp_state.get("estado"),
            "monitor_ok": monitor_instance.cp_state.get("monitor_ok", True),
            "engine_ok": monitor_instance.cp_state.get("engine_ok", True),
            "ubicacion": monitor_instance.cp_state.get("ubicacion"),
            "ts": now(),
            "authenticated": monitor_instance.authenticated,
        }
        return jsonify(data)


# API para cambiar la ubicación del CP
@app.route("/api/monitor/location", methods=["POST"])
def api_set_location():
    data = request.get_json() or {}
    new_location = data.get("location", "").strip()

    if not new_location:
        return jsonify({"status": "error", "message": "location requerida"}), 400

    with monitor_instance.lock:
        old_location = monitor_instance.cp_state.get("ubicacion")
        monitor_instance.cp_state["ubicacion"] = new_location

    log(
        "MONITOR", f"Ubicación cambiada de {old_location} a {new_location}", Fore.YELLOW
    )

    if old_location and old_location != new_location:
        monitor_instance.unregister_from_ev_w(old_location)

    monitor_instance.register_in_ev_w(new_location)

    if monitor_instance.bearer_token and monitor_instance.registry_host_port:
        try:
            headers = {"Authorization": f"Bearer {monitor_instance.bearer_token}"}
            resp = requests.post(
                f"http://{monitor_instance.registry_host_port}/api/registry/location",
                json={"ubicacion": new_location},
                headers=headers,
                timeout=5,
            )
            if resp.status_code == 200:
                log("MONITOR", "Ubicación actualizada en Registry", Fore.GREEN)
            else:
                log(
                    "MONITOR",
                    f"Error actualizando ubicación en Registry: {resp.status_code}",
                    Fore.YELLOW,
                )
        except Exception as e:
            log("MONITOR", f"Error notificando a Registry: {e}", Fore.YELLOW)

    try:
        resp = requests.post(
            f"http://{monitor_instance.central_ip}:{monitor_instance.central_web_port}/api/cp/{monitor_instance.cp_id}/location",
            json={"location": new_location},
            timeout=5,
        )
        if resp.status_code == 200:
            log(
                "MONITOR",
                f"Ubicación guardada en Central BD para {monitor_instance.cp_id}",
                Fore.GREEN,
            )
        else:
            log(
                "MONITOR",
                f"Error guardando ubicación en Central: {resp.status_code}",
                Fore.RED,
            )
    except Exception as e:
        log("MONITOR", f"Error notificando a Central: {e}", Fore.YELLOW)

    try:
        engine_web_port = monitor_instance.engine_web_port
        resp = requests.post(
            f"http://{monitor_instance.engine_ip}:{engine_web_port}/api/cp_state/location",
            json={"ubicacion": new_location},
            timeout=5,
        )
        if resp.status_code == 200:
            log(
                "MONITOR",
                f"Engine actualizado con ubicación {new_location}",
                Fore.GREEN,
            )
        else:
            log(
                "MONITOR", f"Error actualizando Engine: {resp.status_code}", Fore.YELLOW
            )
    except Exception as e:
        log("MONITOR", f"Error conectando con Engine: {e}", Fore.YELLOW)

    return jsonify(
        {"status": "ok", "message": f"Ubicación actualizada a {new_location}"}
    ), 200


# Conexión y registro en Registry
@app.route("/api/registry/register", methods=["POST"])
def api_registry_register():
    if not monitor_instance.registry_password:
        return jsonify(
            {"status": "error", "msg": "Configura la contraseña primero"}
        ), 400

    ok_reg = monitor_instance.register_in_registry()
    if not ok_reg:
        return jsonify({"status": "error", "msg": "Error en Registry"}), 500

    ok_auth = monitor_instance.authenticate_with_central(
        monitor_instance.central_ip, monitor_instance.central_port
    )
    if not ok_auth:
        return jsonify(
            {"status": "error", "msg": "Error autenticando con Central"}
        ), 500

    try:
        resp = requests.get(
            f"http://{monitor_instance.central_ip}:{monitor_instance.central_web_port}/api/cp/{monitor_instance.cp_id}/info",
            timeout=5,
        )
        if resp.status_code == 200:
            info = resp.json()
            ubic_real = info.get("ubicacion")
            if ubic_real:
                with monitor_instance.lock:
                    monitor_instance.cp_state["ubicacion"] = ubic_real
    except Exception as e:
        log("MON", f"Error obteniendo ubicacion real desde Central: {e}", Fore.RED)

    engine_web_port = monitor_instance.engine_web_port
    log(
        "MON",
        "Sincronizando estado desde Engine después de autenticación...",
        Fore.CYAN,
    )
    try:
        resp = requests.get(
            f"http://{monitor_instance.engine_ip}:{engine_web_port}/api/cp_state",
            timeout=3,
        )
        if resp.status_code == 200:
            engine_state = resp.json()
            with monitor_instance.lock:
                monitor_instance.cp_state["estado"] = engine_state.get(
                    "estado", "desconectado"
                )
                monitor_instance.cp_state["engine_ok"] = engine_state.get(
                    "engine_ok", True
                )
            log(
                "MON",
                f"Estado sincronizado desde Engine: {engine_state.get('estado')}",
                Fore.GREEN,
            )
    except Exception as e:
        log("MON", f"Error sincronizando estado desde Engine: {e}", Fore.YELLOW)

    with monitor_instance.lock:
        monitor_instance.authenticated = True

    return jsonify({"status": "ok"}), 200


# API para configurar la contraseña del registro
@app.route("/api/registry/set_password", methods=["POST"])
def api_set_registry_password():
    data = request.get_json() or {}
    pwd = data.get("password")
    if not pwd:
        return jsonify({"status": "error", "msg": "Password requerida"}), 400

    pwd_hash = hash_password(pwd)

    if monitor_instance.saved_password_hash is None:
        if not save_password_hash(monitor_instance.cp_id, pwd_hash):
            return jsonify(
                {"status": "error", "msg": "No se pudo guardar la contraseña"}
            ), 500
        monitor_instance.saved_password_hash = pwd_hash
        monitor_instance.registry_password = pwd
        return jsonify({"status": "ok", "msg": "Contraseña registrada por primera vez"})

    if pwd_hash != monitor_instance.saved_password_hash:
        return jsonify(
            {"status": "error", "msg": "Contraseña incorrecta para este CP"}
        ), 403

    monitor_instance.registry_password = pwd
    return jsonify({"status": "ok", "msg": "Contraseña correcta"})


# Desconexión del registro
@app.route("/api/registry/unregister", methods=["POST"])
def api_registry_unregister():
    cp_id = monitor_instance.cp_id
    central_ip = monitor_instance.central_ip
    central_web_port = monitor_instance.central_web_port
    engine_ip = monitor_instance.engine_ip
    engine_web_port = monitor_instance.engine_web_port

    with monitor_instance.lock:
        monitor_instance.authenticated = False
        monitor_instance.registry_password = None

    try:
        resp = requests.post(
            f"http://{engine_ip}:{engine_web_port}/api/set_state",
            json={"new_state": "desconectado", "reason": "credentials_deactivated"},
            timeout=3,
        )
        log("ENGINE", f"Engine notificado para desconectar {cp_id}", Fore.CYAN)
    except Exception as e:
        log("ENGINE", f"Error notificando Engine: {e}", Fore.YELLOW)

    try:
        central_url = f"http://{central_ip}:{central_web_port}"
        resp = requests.post(
            f"{central_url}/api/command",
            json={"cmd": "deactivate_cp_credentials", "cp_id": cp_id},
            timeout=5,
        )
        log(
            "REGISTRY",
            f"Solicitud de desactivación enviada a Central para {cp_id}",
            Fore.CYAN,
        )
    except Exception as e:
        log("REGISTRY", f"Error notificando a Central: {e}", Fore.YELLOW)

    if monitor_instance.bearer_token and monitor_instance.registry_host_port:
        if len(monitor_instance.bearer_token) > 10:
            try:
                log("REGISTRY", "Enviando unregister con token válido", Fore.CYAN)
                headers = {"Authorization": f"Bearer {monitor_instance.bearer_token}"}
                resp = requests.delete(
                    f"http://{monitor_instance.registry_host_port}/api/registry/unregister/{cp_id}",
                    headers=headers,
                    timeout=5,
                )
                if resp.status_code == 200:
                    log("REGISTRY", f"CP {cp_id} unregistered en Registry", Fore.GREEN)
                elif resp.status_code == 401:
                    log(
                        "REGISTRY",
                        "Token ya no es válido en Registry (esperado en segundo logout)",
                        Fore.YELLOW,
                    )
                else:
                    log(
                        "REGISTRY",
                        f"Error en unregister de Registry: {resp.status_code}",
                        Fore.YELLOW,
                    )
            except Exception as e:
                log(
                    "REGISTRY",
                    f"Error llamando a Registry unregister: {e} (ignorado)",
                    Fore.YELLOW,
                )
        else:
            log(
                "REGISTRY",
                "Token inválido o corrupto, no se enviará unregister",
                Fore.YELLOW,
            )
    else:
        log(
            "REGISTRY",
            "Sin Bearer Token para unregister (ya eliminado o no autenticado)",
            Fore.YELLOW,
        )

    log("REGISTRY", "CP dado de baja (logout)", Fore.YELLOW)
    return jsonify({"status": "ok", "msg": "Desconectado del registro"}), 200


# API para eliminar las credenciales del CP
@app.route("/api/credentials/delete", methods=["POST"])
def api_credentials_delete():
    cp_id = monitor_instance.cp_id
    engine_ip = monitor_instance.engine_ip
    engine_web_port = monitor_instance.engine_web_port

    token = load_saved_token(cp_id)
    if not token:
        return jsonify({"status": "error", "message": "No hay token guardado"}), 401

    try:
        registry_url = f"http://{monitor_instance.registry_host_port}"
        resp = requests.delete(
            f"{registry_url}/api/registry/credentials/delete",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        if resp.status_code != 200:
            return jsonify(
                {
                    "status": "error",
                    "message": f"Registry respondió: {resp.status_code}",
                }
            ), resp.status_code
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    with monitor_instance.lock:
        monitor_instance.authenticated = False
        monitor_instance.registry_password = None
        monitor_instance.bearer_token = None

    token_path = token_file_path(cp_id)
    secret_path = password_file_path(cp_id)

    if os.path.exists(token_path):
        os.remove(token_path)

    if os.path.exists(secret_path):
        os.remove(secret_path)

    try:
        central_url = (
            f"http://{monitor_instance.central_ip}:{monitor_instance.central_web_port}"
        )
        resp = requests.post(
            f"{central_url}/api/command",
            json={"cmd": "altered_cp_clave", "cp_id": cp_id},
            timeout=5,
        )
        log("CENTRAL", f"Central notificada: clave alterada para {cp_id}", Fore.CYAN)
    except Exception as e:
        log("CENTRAL", f"Error notificando a Central: {e}", Fore.YELLOW)

    try:
        requests.post(
            f"http://{engine_ip}:{engine_web_port}/api/set_state",
            json={"new_state": "desconectado", "reason": "clave_alterada"},
            timeout=3,
        )
        log("ENGINE", "Engine notificado: clave alterada", Fore.CYAN)
    except Exception as e:
        log("ENGINE", f"Error notificando Engine: {e}", Fore.YELLOW)

    return jsonify(
        {"status": "ok", "message": "Clave alterada. Debes volver a registrarte."}
    ), 200


# Clase principal del Monitor del CP
class Monitor:
    def __init__(
        self,
        bootstrap,
        cp_id,
        engine_ip,
        engine_port,
        central_ip,
        central_port,
        central_web_port,
        engine_web_port,
    ):
        self.bootstrap = bootstrap
        self.cp_id = cp_id
        self.engine_ip = engine_ip
        self.engine_port = engine_port
        self.central_ip = central_ip
        self.central_port = central_port
        self.central_web_port = central_web_port
        self.engine_web_port = engine_web_port
        self.registry_host_port = None
        self.registry_credentials = None
        self.symmetric_key = None
        self.registry_password = None
        self.saved_password_hash = load_saved_password_hash(cp_id)
        self.bearer_token = load_saved_token(cp_id)
        self.authenticated = False

        self.engine_sock = None
        self.connected_engine = False
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.producer = Producer({"bootstrap.servers": bootstrap})
        self.consumer_kafka = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"monitor-{cp_id}-{int(time.time())}",
                "auto.offset.reset": "latest",
                "session.timeout.ms": 6000,
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 1000,
            }
        )
        self.consumer_kafka.subscribe(["CP_STATUS"])
        self.consumer_command = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"monitor-cmd-{cp_id}-{int(time.time())}",
                "auto.offset.reset": "latest",
                "session.timeout.ms": 6000,
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 1000,
            }
        )
        self.consumer_command.subscribe(["CP_COMMAND"])
        self.cp_state = {
            "cp_id": cp_id,
            "estado": "desconectado",
            "monitor_ok": True,
            "engine_ok": False,
            "ubicacion": None,
        }

    # Sincronizar ubicación desde el Engine
    def sync_from_engine(self, retries=3):
        for attempt in range(retries):
            try:
                resp = requests.get(
                    f"http://{self.engine_ip}:{self.engine_web_port}/api/cp_state",
                    timeout=3,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    with self.lock:
                        ubicacion_del_engine = data.get("ubicacion")
                        if (
                            ubicacion_del_engine
                            and ubicacion_del_engine != "Desconocida"
                        ):
                            self.cp_state["ubicacion"] = ubicacion_del_engine
                            log(
                                "MON",
                                f"Ubicación sincronizada del Engine: {ubicacion_del_engine}",
                                Fore.GREEN,
                            )
                            return True
                        else:
                            log(
                                "MON",
                                f"Ubicación del Engine inválida o desconocida (intento {attempt + 1}/{retries})",
                                Fore.YELLOW,
                            )
                            if attempt < retries - 1:
                                time.sleep(0.5)
                                continue
                            return False
            except Exception as e:
                log(
                    "MON",
                    f"Error leyendo cp_state del Engine (intento {attempt + 1}/{retries}): {e}",
                    Fore.YELLOW,
                )
                if attempt < retries - 1:
                    time.sleep(0.5)
                    continue
                return False
        return False

    # Registro en el Registry
    def register_in_registry(self):
        if not self.registry_host_port:
            log("REGISTRY", "No se ha configurado --registry", Fore.RED)
            return False

        if not self.registry_password:
            if self.saved_password_hash:
                log(
                    "REGISTRY",
                    "Contraseña ya configurada (cargada del archivo)",
                    Fore.YELLOW,
                )
            else:
                log("REGISTRY", "No hay contraseña configurada aún", Fore.RED)
                return False

        log("REGISTRY", "Sincronizando ubicación del Engine...", Fore.CYAN)
        sync_ok = self.sync_from_engine(retries=3)

        with self.lock:
            ubic = self.cp_state.get("ubicacion")
            if not ubic or ubic == "Desconocida":
                ubic = "Desconocida"
                if not sync_ok:
                    log(
                        "REGISTRY",
                        "No se pudo sincronizar la ubicación del Engine, usando 'Desconocida'",
                        Fore.YELLOW,
                    )

        url = f"http://{self.registry_host_port}/api/registry/register"

        payload = {
            "cp_id": self.cp_id,
            "ubicacion": ubic,
            "password": self.registry_password,
        }

        try:
            log("REGISTRY", f"Registrando CP en {url} con ubicación: {ubic}", Fore.CYAN)
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code != 200:
                return False
            data = resp.json()
            if data.get("status") != "ok":
                return False

            self.registry_credentials = {
                "username": data.get("username"),
                "password": data.get("password"),
            }

            token = data.get("token")
            if token:
                if save_token(self.cp_id, token):
                    self.bearer_token = token
                    log("REGISTRY", "Token guardado exitosamente", Fore.GREEN)
                else:
                    log("REGISTRY", "Error guardando token", Fore.RED)

            log(
                "REGISTRY", f"CP registrado. Usuario={data.get('username')}", Fore.GREEN
            )

            self.register_in_ev_w(ubic)

            return True
        except Exception as e:
            log("REGISTRY", f"Error conectando con Registry: {e}", Fore.RED)
            return False

    # Registro en EV_W para monitoreo meteorológico
    def register_in_ev_w(self, location):
        if not self.central_ip or not self.central_web_port:
            log("EV_W", "No configurado central para EV_W", Fore.YELLOW)
            return

        try:
            url = f"http://{socket.gethostbyname(socket.gethostname())}:8000/api/weather/monitor"
            payload = {"location": location, "cp_id": self.cp_id}

            log(
                "EV_W",
                f"Registrando ubicación {location} en EV_W para monitoreo...",
                Fore.CYAN,
            )
            resp = requests.post(url, json=payload, timeout=5)

            if resp.status_code == 200:
                log(
                    "EV_W",
                    f"Ubicación {location} registrada en EV_W para monitoreo",
                    Fore.GREEN,
                )
            else:
                log(
                    "EV_W",
                    f"Error registrando en EV_W: {resp.status_code}",
                    Fore.YELLOW,
                )
        except Exception as e:
            log("EV_W", f"Error conectando con EV_W: {e}", Fore.YELLOW)

    # Desregistro de EV_W
    def unregister_from_ev_w(self, location):
        if not self.central_ip or not self.central_web_port:
            return

        try:
            url = f"http://{socket.gethostbyname(socket.gethostname())}:8000/api/weather/unmonitor"
            payload = {"location": location, "cp_id": self.cp_id}

            log("EV_W", f"Desregistrando ubicación {location} de EV_W...", Fore.CYAN)
            resp = requests.post(url, json=payload, timeout=5)

            if resp.status_code == 200:
                log("EV_W", f"Ubicación {location} desregistrada de EV_W", Fore.GREEN)
        except Exception as e:
            log("EV_W", f"Error desregistrando de EV_W: {e}", Fore.YELLOW)

    # Autenticación con Central
    def authenticate_with_central(self, central_ip, central_port):
        if not self.registry_credentials:
            return False

        username = self.registry_credentials["username"]
        password = self.registry_password

        if not password:
            log("AUTH", "No hay contraseña válida configurada", Fore.RED)
            return False

        auth_msg = {
            "type": "auth_request",
            "cp_id": self.cp_id,
            "username": username,
            "password": password,
            "ts": now(),
        }

        try:
            log("AUTH", f"Conectando a Central {central_ip}:{central_port}", Fore.CYAN)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((central_ip, central_port))

            if not enviar_mensaje_simple(sock, auth_msg):
                log("AUTH", "No se pudo enviar auth_request", Fore.RED)
                sock.close()
                return False

            resp = recibir_mensaje_simple(sock, timeout=5.0)
            sock.close()
            if not resp:
                log("AUTH", "Sin respuesta de Central", Fore.RED)
                return False

            if resp.get("status") != "ACK":
                log("AUTH", f"Autenticación RECHAZADA: {resp.get('message')}", Fore.RED)
                return False

            key_b64 = resp.get("symmetric_key")
            if not key_b64:
                log("AUTH", "Central no devolvió clave simétrica", Fore.RED)
                return False

            self.symmetric_key = key_b64
            log("AUTH", "Autenticación OK. Clave simétrica recibida.", Fore.GREEN)
            return True

        except Exception as e:
            log("AUTH", f"Error autenticando con Central: {e}", Fore.RED)
            return False

    # Chequeo de salud del Engine
    def health_check(self):
        log("HEALTH", "Usando PROTOCOLO ESTÁNDAR <STX><DATA><ETX><LRC>", Fore.CYAN)

        while not self.stop_event.is_set():
            try:
                if not self.connected_engine:
                    self.connect_engine()

                if self.connected_engine:
                    health_msg = {"type": "health_check", "ts": now()}

                    if enviar_mensaje_simple(self.engine_sock, health_msg):
                        try:
                            response = recibir_mensaje_simple(
                                self.engine_sock, timeout=2.0
                            )

                            if response and response.get("type") == "health_ack":
                                engine_ok = response.get("status") == "OK"

                                with self.lock:
                                    self.cp_state["engine_ok"] = engine_ok
                                    if self.cp_state["estado"] not in [
                                        "suministrando",
                                        "Out of order",
                                    ]:
                                        if self.cp_state["monitor_ok"] and engine_ok:
                                            self.cp_state["estado"] = "activo"
                                            log(
                                                "HEALTH",
                                                "Monitor + Engine OK → activo",
                                                Fore.GREEN,
                                            )

                            else:
                                with self.lock:
                                    self.cp_state["engine_ok"] = False

                                    if self.cp_state["estado"] == "suministrando":
                                        log(
                                            "HEALTH",
                                            "ENGINE MUERTO DURANTE SUMINISTRO",
                                            Fore.RED,
                                        )
                                        self.cp_state["estado"] = "averiado"
                                        self.report_engine_failure()

                                    elif self.cp_state["estado"] not in [
                                        "Out of order"
                                    ]:
                                        self.cp_state["estado"] = "averiado"
                                self.connected_engine = False

                        except Exception as e:
                            log("HEALTH", f"Error recibiendo respuesta: {e}", Fore.RED)
                            self.connected_engine = False

                            with self.lock:
                                self.cp_state["engine_ok"] = False

                                if self.cp_state["estado"] == "suministrando":
                                    log(
                                        "HEALTH",
                                        "ENGINE MUERTO DURANTE SUMINISTRO",
                                        Fore.RED,
                                    )
                                    self.cp_state["estado"] = "averiado"
                                    self.report_engine_failure()

                                elif self.cp_state["estado"] not in ["Out of order"]:
                                    self.cp_state["estado"] = "averiado"

                    else:
                        self.connected_engine = False

                        with self.lock:
                            self.cp_state["engine_ok"] = False

                            if self.cp_state["estado"] == "suministrando":
                                log(
                                    "HEALTH",
                                    "ENGINE MUERTO DURANTE SUMINISTRO",
                                    Fore.RED,
                                )
                                self.cp_state["estado"] = "averiado"
                                self.report_engine_failure()

                            elif self.cp_state["estado"] not in ["Out of order"]:
                                self.cp_state["estado"] = "averiado"
                time.sleep(1)

            except Exception as e:
                log("HEALTH", f"Error general: {e}", Fore.RED)
                time.sleep(1)

    # Reportar fallo de engine a Central
    def report_engine_failure(self):
        status_msg = {
            "cp_id": self.cp_id,
            "estado": "averiado",
            "monitor_ok": True,
            "engine_ok": False,
            "ts": now(),
            "reason": "engine_muerto",
        }
        self.producer.produce("CP_STATUS", json.dumps(status_msg).encode())
        self.producer.flush()
        log("MONITOR", "Reportado a Central: AVERIADO", Fore.RED)

    # Conexión con el Engine
    def connect_engine(self):
        if self.connected_engine:
            return
        retry_count = 0
        max_retries_before_log = 3
        log(
            "CONNECT",
            "Intentando conectar con Engine usando PROTOCOLO ESTÁNDAR",
            Fore.YELLOW,
        )

        while not self.stop_event.is_set() and not self.connected_engine:
            try:
                retry_count += 1

                if retry_count % max_retries_before_log == 0:
                    log(
                        "CONNECT",
                        f"Intentando conectar con Engine ({retry_count} intentos)",
                        Fore.YELLOW,
                    )
                self.engine_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.engine_sock.settimeout(3.0)
                self.engine_sock.connect((self.engine_ip, self.engine_port))

                with self.lock:
                    self.connected_engine = True
                    self.cp_state["engine_ok"] = True

                    if self.cp_state["estado"] == "averiado":
                        self.cp_state["estado"] = "activo"
                        log("CONNECT", "ENGINE RECUPERADO", Fore.GREEN)
                log(
                    "CONNECT",
                    f"Conectado con Engine en {self.engine_ip}:{self.engine_port} [PROTOCOLO OK]",
                    Fore.GREEN,
                )
                return

            except (socket.timeout, socket.error, ConnectionRefusedError):
                self.connected_engine = False
                time.sleep(2)

    # Escucha CP_STATUS desde Kafka
    def kafka_listener(self):
        log("KAFKA", f"Escuchando CP_STATUS para {self.cp_id}", Fore.MAGENTA)

        while not self.stop_event.is_set():
            try:
                msg = self.consumer_kafka.poll(0.5)

                if msg and not msg.error():
                    data = json.loads(msg.value().decode("utf-8"))

                    if data.get("cp_id") != self.cp_id:
                        continue

                    if data.get("command"):
                        continue
                    estado_recibido = data.get("estado", "desconectado")
                    engine_ok_kafka = data.get("engine_ok", False)

                    with self.lock:
                        self.cp_state["estado"] = estado_recibido
                        self.cp_state["engine_ok"] = engine_ok_kafka
                    log("KAFKA", f"CP dice: {estado_recibido}", Fore.CYAN)

            except json.JSONDecodeError:
                continue

            except Exception as e:
                log("KAFKA", f"Error: {e}", Fore.RED)

    # Escucha CP_COMMAND desde Kafka
    def kafka_command_listener(self):
        log("COMMAND", f"Escuchando CP_COMMAND para {self.cp_id}", Fore.MAGENTA)

        while not self.stop_event.is_set():
            try:
                msg = self.consumer_command.poll(0.5)

                if msg and not msg.error():
                    data = json.loads(msg.value().decode("utf-8"))

                    if data.get("cp_id") != self.cp_id:
                        continue
                    cmd = data.get("command")

                    if cmd == "resume_service":
                        with self.lock:
                            self.cp_state["estado"] = "activo"
                        log("COMMAND", "REANUDADO por Central", Fore.GREEN)

                    elif cmd == "Out of order":
                        with self.lock:
                            self.cp_state["estado"] = "Out of order"
                        log("COMMAND", "PAUSADO por Central", Fore.YELLOW)

            except json.JSONDecodeError:
                continue

            except Exception as e:
                log("COMMAND", f"Error: {e}", Fore.RED)

    # Publicador CP_STATUS a Kafka
    def kafka_publisher(self):
        log("PUBLISHER", "Publicando estado cada 3 segundos", Fore.MAGENTA)
        engine_web_port = monitor_instance.engine_web_port
        while not self.stop_event.is_set():
            try:
                if not self.authenticated:
                    try:
                        resp = requests.get(
                            f"http://{self.engine_ip}:{engine_web_port}/api/cp_state",
                            timeout=2,
                        )
                        if resp.status_code == 200:
                            engine_state = resp.json()
                            with self.lock:
                                self.cp_state["estado"] = engine_state.get(
                                    "estado", "desconectado"
                                )
                                self.cp_state["engine_ok"] = engine_state.get(
                                    "engine_ok", True
                                )
                    except Exception:
                        pass

                with self.lock:
                    estado_actual = (
                        "desconectado"
                        if not self.authenticated
                        else self.cp_state["estado"]
                    )
                    engine_ok = self.cp_state["engine_ok"]
                    monitor_ok = self.cp_state["monitor_ok"]
                data = {
                    "cp_id": self.cp_id,
                    "estado": estado_actual,
                    "monitor_ok": monitor_ok,
                    "engine_ok": engine_ok,
                    "ts": now(),
                }
                self.producer.produce("CP_STATUS", json.dumps(data).encode("utf-8"))
                self.producer.flush()
                log("PUBLISHER", f"Publicado: {estado_actual}", Fore.GREEN)
                time.sleep(3)

            except Exception as e:
                log("PUBLISHER", f"Error: {e}", Fore.RED)
                time.sleep(1)

    # Servidor TCP para la Central
    def tcp_server(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("0.0.0.0", self.central_port))
            server.listen(1)
            log(
                "TCP-SERVER",
                f"Escuchando en puerto {self.central_port} [PROTOCOLO ESTÁNDAR]",
                Fore.MAGENTA,
            )

            while not self.stop_event.is_set():
                try:
                    server.settimeout(1.0)
                    conn, addr = server.accept()
                    log("TCP-SERVER", f"Central conectada: {addr}", Fore.GREEN)

                    while not self.stop_event.is_set():
                        try:
                            data = recibir_mensaje_simple(conn, timeout=5.0)

                            if not data:
                                break

                            with self.lock:
                                response = {
                                    "cp_id": self.cp_id,
                                    "estado": self.cp_state["estado"],
                                    "monitor_ok": self.cp_state["monitor_ok"],
                                    "engine_ok": self.cp_state["engine_ok"],
                                    "ts": now(),
                                }

                            if not enviar_mensaje_simple(conn, response):
                                break

                        except Exception:
                            break
                    conn.close()

                except socket.timeout:
                    continue

                except Exception as e:
                    log("TCP-SERVER", f"Error: {e}", Fore.RED)
            server.close()

        except Exception as e:
            log("TCP-SERVER", f"Error servidor: {e}", Fore.RED)

    # Método principal para iniciar los threads
    def run(self):
        log("MONITOR", f"CP Monitor {self.cp_id} iniciando", Fore.CYAN)
        log("MONITOR", f"Engine: {self.engine_ip}:{self.engine_port}", Fore.CYAN)
        log("MONITOR", f"Kafka: {self.bootstrap}", Fore.CYAN)
        log("MONITOR", f"TCP Port: {self.central_port}", Fore.CYAN)
        log("MONITOR", "Protocolo: ESTÁNDAR <STX><DATA><ETX><LRC>", Fore.GREEN)

        if self.registry_credentials and self.registry_password:
            self.authenticate_with_central(
                central_ip=self.central_ip, central_port=self.central_port
            )

        threading.Thread(target=self.health_check, daemon=True).start()
        threading.Thread(target=self.tcp_server, daemon=True).start()
        kafka_thread = threading.Thread(target=self.kafka_listener, daemon=False)
        kafka_thread.start()
        command_thread = threading.Thread(
            target=self.kafka_command_listener, daemon=False
        )
        command_thread.start()
        publisher_thread = threading.Thread(target=self.kafka_publisher, daemon=False)
        publisher_thread.start()

        try:
            while not self.stop_event.is_set():
                time.sleep(1)

        except KeyboardInterrupt:
            log("MONITOR", "Deteniendo", Fore.YELLOW)
            self.stop_event.set()
        kafka_thread.join(timeout=5)
        command_thread.join(timeout=5)
        publisher_thread.join(timeout=5)
        log("MONITOR", "Detenido", Fore.YELLOW)


# Análisis de argumentos
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka", required=True)
    p.add_argument("--cp-id", required=True)
    p.add_argument("--engine", required=True)
    p.add_argument("--central", required=True)
    p.add_argument("--registry", required=False)
    p.add_argument("--ev-w", default="192.168.18.148:8000")
    p.add_argument("--web-port", type=int, default=5101)
    p.add_argument("--engine-web-port", type=int, default=5011)
    p.add_argument("--central-web-port", type=int, default=5001)
    return p.parse_args()


# Programa principal
if __name__ == "__main__":
    args = parse_args()
    engine_ip, engine_port = args.engine.split(":")
    engine_port = int(engine_port)

    central_ip, central_port = args.central.split(":")
    central_port = int(central_port)

    monitor_instance, WEB_PORT
    WEB_PORT = args.web_port
    ENGINE_WEB_PORT = args.engine_web_port

    CENTRAL_WEB_PORT = args.central_web_port

    monitor_instance = Monitor(
        bootstrap=args.kafka,
        cp_id=args.cp_id,
        engine_ip=engine_ip,
        engine_port=engine_port,
        central_ip=central_ip,
        central_port=central_port,
        central_web_port=CENTRAL_WEB_PORT,
        engine_web_port=ENGINE_WEB_PORT,
    )
    monitor_instance.registry_host_port = args.registry

    if monitor_instance.saved_password_hash:
        log(
            "MONITOR",
            f"Contraseña ya configurada (cargada desde {args.cp_id}.secret)",
            Fore.GREEN,
        )
    else:
        log(
            "MONITOR",
            "Contraseña no configurada aún - Regístrate desde la web",
            Fore.YELLOW,
        )

    monitor_thread = threading.Thread(target=monitor_instance.run, daemon=False)
    monitor_thread.start()

    flask_server = ServerThread(app, "0.0.0.0", WEB_PORT)
    flask_server.start()

    def handle_sigint(signum, frame):
        if shutdown_requested.is_set():
            return
        shutdown_requested.set()
        log("MONITOR", "Deteniendo...", Fore.YELLOW)
        monitor_instance.stop_event.set()
        flask_server.shutdown()

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    try:
        while not shutdown_requested.is_set():
            time.sleep(0.2)
    finally:
        shutdown_requested.set()
        monitor_instance.stop_event.set()
        flask_server.shutdown()
        flask_server.join(timeout=3)
        monitor_thread.join(timeout=6)
