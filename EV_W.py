"""
Uso:
python EV_W.py --secret-file EV_W.secret --port 8000 --central-host 192.168.18.148 --central-port 5001
"""

import argparse
import requests
import threading
import time
from datetime import datetime
from flask import Flask, jsonify, request
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)

# Configuración global
openweather_key = None
central_host = None
central_port = None
port = 8000

# Cache de clima
weather_cache = {}
CACHE_TTL = 1800

# Control de estado de temperatura para alertas
temperature_state = {}
monitored_locations = {}
monitored_locations_lock = threading.Lock()

# Estado global
api_calls_count = 0
api_calls_lock = threading.Lock()

# Flask setup
app = Flask(__name__)
app.config["SECRET_KEY"] = "ev-weather-secret-2025"


def now_iso():
    return datetime.now().isoformat() + "Z"


def log(tag, msg, color=Fore.CYAN):
    print(f"{color}[{tag}] {msg}{Style.RESET_ALL}")


# Pillar temperatura desde OpenWeather
def get_weather_from_openweather(location: str) -> dict:
    global api_calls_count

    if not openweather_key:
        log("WEATHER", "API key no configurada", Fore.YELLOW)
        return {
            "status": "offline",
            "error": "No API key",
            "temp": 15.0,
            "humidity": 50,
            "rain_probability": 0,
            "description": "Datos no disponibles",
            "condition": "unknown",
        }

    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": location,
            "appid": openweather_key,
            "units": "metric",
            "lang": "es",
        }

        log("WEATHER", f"Consultando OpenWeather para: {location}", Fore.CYAN)
        resp = requests.get(url, params=params, timeout=5)

        with api_calls_lock:
            api_calls_count += 1

        if resp.status_code != 200:
            log(
                "WEATHER",
                f"Error OpenWeather {resp.status_code}: {resp.text}",
                Fore.YELLOW,
            )
            return {
                "status": "error",
                "error": f"API error {resp.status_code}",
                "temp": 15.0,
                "humidity": 50,
                "rain_probability": 0,
                "description": "Datos no disponibles",
                "condition": "unknown",
            }

        data = resp.json()

        temp = data.get("main", {}).get("temp", 15.0)
        humidity = data.get("main", {}).get("humidity", 50)
        description = data.get("weather", [{}])[0].get("description", "Desconocido")
        condition = data.get("weather", [{}])[0].get("main", "unknown").lower()

        rain_probability = 0
        if "rain" in data and "1h" in data["rain"]:
            rain_probability = min(100, data["rain"]["1h"] * 10)
        elif "rain" in condition or "drizzle" in condition:
            rain_probability = 70
        elif "cloud" in condition:
            rain_probability = 30

        result = {
            "status": "ok",
            "temp": round(temp, 1),
            "temperature": round(temp, 1),
            "humidity": humidity,
            "rain_probability": int(rain_probability),
            "precipitation": int(rain_probability),
            "description": description.capitalize(),
            "condition": condition,
            "timestamp": now_iso(),
        }

        log(
            "WEATHER",
            f"OpenWeather OK: {temp}°C, {humidity}% humedad, lluvia {rain_probability}%",
            Fore.GREEN,
        )
        return result

    except requests.exceptions.Timeout:
        log("WEATHER", "Timeout consultando OpenWeather", Fore.YELLOW)
        return {
            "status": "timeout",
            "error": "Timeout",
            "temp": 15.0,
            "humidity": 50,
            "rain_probability": 0,
            "description": "Datos no disponibles",
            "condition": "unknown",
        }
    except Exception as e:
        log("WEATHER", f"Error consultando OpenWeather: {e}", Fore.YELLOW)
        return {
            "status": "offline",
            "error": str(e),
            "temp": 15.0,
            "humidity": 50,
            "rain_probability": 0,
            "description": "Datos no disponibles",
            "condition": "unknown",
        }


# Obtener clima con caché
def get_cached_or_fresh_weather(location: str, force_refresh: bool = False) -> dict:
    global weather_cache

    if not force_refresh and location in weather_cache:
        cached = weather_cache[location]
        age = time.time() - cached["cached_at"]
        if age < CACHE_TTL:
            log("WEATHER", f"Cache hit para {location} (edad: {int(age)}s)", Fore.CYAN)
            return cached["data"]

    if force_refresh:
        log("WEATHER", f"Actualizando {location} (force_refresh=true)", Fore.YELLOW)
    data = get_weather_from_openweather(location)
    weather_cache[location] = {"data": data, "cached_at": time.time()}

    return data


# Notificar alerta a Central
def notify_central_alert(
    location: str, alert_type: str, severity: str, details: str, cp_id: str = None
):
    if not central_host:
        log("ALERT", "Central host no configurado", Fore.RED)
        return

    try:
        url = f"http://{central_host}:{central_port}/api/internal/weather_alert"
        payload = {
            "location": location,
            "alert_type": alert_type,
            "severity": severity,
            "details": details,
            "cp_id": cp_id,
            "timestamp": now_iso(),
        }

        log(
            "ALERT",
            f"→ Central ({central_host}:{central_port}): {alert_type} | {location} | CP {cp_id}",
            Fore.CYAN,
        )
        resp = requests.post(url, json=payload, timeout=5)

        if resp.status_code == 200:
            log("ALERT", f"{alert_type} registrada en BD", Fore.GREEN)
        else:
            log("ALERT", f"Central respondió {resp.status_code}", Fore.RED)

    except Exception as e:
        log("ALERT", f"Error conectando con Central: {e}", Fore.RED)


# Notificar cambio de estado al Engine
def notify_engine_state_change(cp_id: str, new_state: str):
    if not central_host:
        return

    try:
        url = f"http://{central_host}:{central_port}/api/engine/cp_state_change"
        payload = {"cp_id": cp_id, "new_state": new_state, "reason": "weather"}

        resp = requests.post(url, json=payload, timeout=5)
        if resp.status_code == 200:
            log(
                "ENGINE",
                f"Estado del CP {cp_id} cambió a '{new_state}' en Engine",
                Fore.GREEN,
            )
    except Exception as e:
        log("ENGINE", f"Error notificando Engine: {e}", Fore.YELLOW)


# Retorna clima actual de una ubicación
@app.route("/api/weather", methods=["GET"])
def api_get_weather():
    global temperature_state
    location = request.args.get("ubicacion", "Madrid")
    if not location or location.strip() == "":
        return jsonify({"status": "error", "message": "ubicacion requerida"}), 400

    weather_data = get_cached_or_fresh_weather(location)
    temp = weather_data.get("temp", 15.0)

    if location not in temperature_state:
        temperature_state[location] = {
            "temp_is_below_zero": temp < 0,
            "last_notified_state": "unknown",
        }

    state = temperature_state[location]
    temp_below_zero = temp < 0

    if temp_below_zero and not state["temp_is_below_zero"]:
        notify_central_alert(
            location=location,
            alert_type="freezing_risk",
            severity="high",
            details=f"Temperatura ha caído bajo 0°C: {temp}°C - RIESGO DE CONGELACIÓN",
        )
        state["last_notified_state"] = "freezing"

    elif (
        not temp_below_zero
        and state["temp_is_below_zero"]
        and state["last_notified_state"] == "freezing"
    ):
        notify_central_alert(
            location=location,
            alert_type="freezing_cleared",
            severity="low",
            details=f"Temperatura vuelve a la normalidad: {temp}°C - RIESGO DESAPARECIDO",
        )
        state["last_notified_state"] = "normal"

    state["temp_is_below_zero"] = temp_below_zero

    return jsonify(weather_data), 200


# Estado del servicio
@app.route("/api/weather/status", methods=["GET"])
def api_weather_status():
    with api_calls_lock:
        calls = api_calls_count

    return jsonify(
        {
            "status": "ok",
            "service": "EV_W (Weather Control Office)",
            "api_calls_total": calls,
            "cache_entries": len(weather_cache),
            "last_update": now_iso(),
            "openweather_configured": openweather_key is not None,
        }
    ), 200


# Health check
@app.route("/api/weather/health", methods=["GET"])
def api_weather_health():
    return jsonify({"status": "ok", "healthy": True}), 200


# Limpiar caché
@app.route("/api/weather/cache/clear", methods=["POST"])
def api_clear_cache():
    global weather_cache
    weather_cache.clear()
    log("CACHE", "Caché limpiado", Fore.GREEN)
    return jsonify({"status": "ok", "message": "Cache cleared"}), 200


# Obtener temperatura actual de una ubicación
@app.route("/api/weather/temperature", methods=["GET"])
def api_weather_temperature():
    location = request.args.get("location", "").strip()
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"

    if not location:
        return jsonify({"status": "error", "message": "location requerida"}), 400

    try:
        weather_data = get_cached_or_fresh_weather(
            location, force_refresh=force_refresh
        )
        if weather_data.get("status") == "error":
            return jsonify(
                {
                    "status": "error",
                    "message": weather_data.get("message", "No se pudo obtener clima"),
                }
            ), 500

        return jsonify(
            {
                "status": "ok",
                "location": location,
                "temperature": weather_data.get("temperature"),
                "humidity": weather_data.get("humidity"),
                "precipitation": weather_data.get("precipitation"),
                "description": weather_data.get("description"),
                "force_refresh": force_refresh,
            }
        ), 200
    except Exception as e:
        log("WEATHER", f"Error consultando temperatura: {e}", Fore.RED)
        return jsonify({"status": "error", "message": str(e)}), 500


# Monitorear ubicación para alertas de temperatura
@app.route("/api/weather/monitor", methods=["POST"])
def api_monitor_location():
    global monitored_locations, temperature_state
    data = request.get_json() or {}
    location = data.get("location", "").strip()
    cp_id = data.get("cp_id", "Unknown").strip()

    if not location:
        return jsonify({"status": "error", "message": "location requerida"}), 400

    with monitored_locations_lock:
        monitored_locations[location] = {
            "cp_id": cp_id,
            "connected": True,
            "registered_at": now_iso(),
        }

    weather_data = get_cached_or_fresh_weather(location)
    temp = weather_data.get("temp", 15.0)

    if location not in temperature_state:
        temperature_state[location] = {
            "temp_is_below_zero": temp < 0,
            "last_notified_state": "unknown",
            "cp_id": cp_id,
            "connected": False,
            "is_first_check": True,
        }
    else:
        temperature_state[location]["cp_id"] = cp_id
        temperature_state[location]["connected"] = False
        temperature_state[location]["is_first_check"] = True

    log(
        "MONITOR",
        f"Comenzando monitoreo de temperatura en: {location} (CP: {cp_id})",
        Fore.GREEN,
    )

    try:
        check_temperature_state(location)
    except Exception as e:
        log("MONITOR", f"Error en check_temperature_state: {e}", Fore.RED)
        import traceback

        traceback.print_exc()

    return jsonify(
        {"status": "ok", "message": f"Monitoreando {location}", "temp": temp}
    ), 200


# Dejar de monitorear ubicación
@app.route("/api/weather/unmonitor", methods=["POST"])
def api_unmonitor_location():
    global monitored_locations
    data = request.get_json() or {}
    location = data.get("location", "").strip()
    cp_id = data.get("cp_id", "Unknown")

    if not location:
        return jsonify({"status": "error", "message": "location requerida"}), 400

    with monitored_locations_lock:
        if location in monitored_locations:
            del monitored_locations[location]

    log(
        "MONITOR",
        f"Detenido monitoreo de temperatura en: {location} (CP: {cp_id})",
        Fore.YELLOW,
    )
    return jsonify(
        {"status": "ok", "message": f"Monitoreo detenido para {location}"}
    ), 200


# Verificar estado de temperatura y enviar alertas
def check_temperature_state(location: str):
    global temperature_state
    weather_data = get_cached_or_fresh_weather(location)
    temp = weather_data.get("temp", 15.0)

    with monitored_locations_lock:
        cp_info = monitored_locations.get(location)
        if not cp_info:
            return
        cp_id = cp_info.get("cp_id")
        connected = cp_info.get("connected", False)

    if location not in temperature_state:
        temperature_state[location] = {
            "temp_is_below_zero": temp < 0,
            "last_notified_state": "unknown",
            "cp_id": cp_id,
            "connected": connected,
            "is_first_check": True,
        }

    state = temperature_state[location]
    temp_below_zero = temp < 0

    if "connected" not in state:
        state["connected"] = connected

    if state.get("is_first_check", False):
        state["is_first_check"] = False
        if connected and temp_below_zero:
            log(
                "MONITOR",
                f"¡¡ALERTA!! CP {cp_id} conectado con T < 0°C en {location}: {temp}°C",
                Fore.RED,
            )
            notify_central_alert(
                location=location,
                alert_type="freezing_risk",
                severity="high",
                details=f"CP {cp_id} conectado bajo 0°C: {temp}°C - RIESGO DE CONGELACIÓN",
                cp_id=cp_id,
            )
            notify_engine_state_change(cp_id, "averiado")
            state["last_notified_state"] = "freezing"
        elif connected and not temp_below_zero:
            log(
                "MONITOR",
                f"CP {cp_id} conectado en clima normal {location}: {temp}°C",
                Fore.GREEN,
            )
            notify_central_alert(
                location=location,
                alert_type="connection_ok",
                severity="low",
                details=f"CP {cp_id} conectado en clima normal: {temp}°C - SIN RIESGO DE CONGELACIÓN",
                cp_id=cp_id,
            )
            notify_engine_state_change(cp_id, "activo")
            state["last_notified_state"] = "normal"

    elif connected and not state.get("connected", False) and temp_below_zero:
        log(
            "MONITOR",
            f"¡¡ALERTA!! CP {cp_id} conectado con T < 0°C en {location}: {temp}°C",
            Fore.RED,
        )
        notify_central_alert(
            location=location,
            alert_type="freezing_risk",
            severity="high",
            details=f"CP {cp_id} conectado bajo 0°C: {temp}°C - RIESGO DE CONGELACIÓN",
            cp_id=cp_id,
        )
        notify_engine_state_change(cp_id, "averiado")
        state["last_notified_state"] = "freezing"

    elif connected and not state.get("connected", False) and not temp_below_zero:
        log(
            "MONITOR",
            f"CP {cp_id} conectado en clima normal {location}: {temp}°C",
            Fore.GREEN,
        )
        notify_central_alert(
            location=location,
            alert_type="connection_ok",
            severity="low",
            details=f"CP {cp_id} conectado en clima normal: {temp}°C - SIN RIESGO DE CONGELACIÓN",
            cp_id=cp_id,
        )
        notify_engine_state_change(cp_id, "activo")
        state["last_notified_state"] = "normal"

    elif (
        temp_below_zero
        and not state.get("temp_is_below_zero", False)
        and state.get("connected", False)
        and state.get("last_notified_state") != "freezing"
    ):
        log(
            "MONITOR",
            f"¡¡ALERTA!! Temperatura bajó de 0°C en {location}: {temp}°C",
            Fore.RED,
        )
        notify_central_alert(
            location=location,
            alert_type="freezing_risk",
            severity="high",
            details=f"Temperatura ha caído bajo 0°C: {temp}°C - RIESGO DE CONGELACIÓN",
            cp_id=cp_id,
        )
        notify_engine_state_change(cp_id, "averiado")
        state["last_notified_state"] = "freezing"

    elif (
        not temp_below_zero
        and state.get("temp_is_below_zero", False)
        and state.get("last_notified_state") == "freezing"
    ):
        log(
            "MONITOR",
            f"Temperatura volvió a la normalidad en {location}: {temp}°C",
            Fore.GREEN,
        )
        notify_central_alert(
            location=location,
            alert_type="freezing_cleared",
            severity="low",
            details=f"Temperatura vuelve a la normalidad: {temp}°C - RIESGO DESAPARECIDO",
            cp_id=cp_id,
        )
        notify_engine_state_change(cp_id, "activo")
        state["last_notified_state"] = "normal"

    state["temp_is_below_zero"] = temp_below_zero
    state["connected"] = connected
    state["is_first_check"] = False


# Monitoreo continuo de ubicaciones registradas
def continuous_monitoring():
    while True:
        try:
            time.sleep(10)

            with monitored_locations_lock:
                locations_to_check = list(monitored_locations.keys())

            if locations_to_check:
                for location in locations_to_check:
                    check_temperature_state(location)

        except Exception as e:
            log("MONITOR", f"Error en monitoreo continuo: {e}", Fore.RED)


# Limpieza periódica de caché
def periodic_cleanup():
    while True:
        try:
            time.sleep(300)

            now = time.time()
            expired = [
                loc
                for loc, data in weather_cache.items()
                if now - data["cached_at"] > CACHE_TTL
            ]

            for loc in expired:
                del weather_cache[loc]

            if expired:
                log("CACHE", f"Limpiadas {len(expired)} entradas expiradas", Fore.CYAN)

        except Exception as e:
            log("CLEANUP", f"Error en limpieza: {e}", Fore.RED)


# Cargar API key desde archivo
def load_api_key(secret_file):
    try:
        with open(secret_file, "r") as f:
            key = f.read().strip()
            if not key:
                log("ERROR", f"Archivo {secret_file} existe pero está vacío", Fore.RED)
                return None
            return key
    except FileNotFoundError:
        log("ERROR", f"Archivo {secret_file} no encontrado", Fore.RED)
        return None
    except Exception as e:
        log("ERROR", f"Error leyendo {secret_file}: {e}", Fore.RED)
        return None


# Parsear argumentos de línea de comandos
def parse_args():
    p = argparse.ArgumentParser(description="EV_W - Weather Control Office")
    p.add_argument("--secret-file", required=True)
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--central-host", default="192.168.18.148")
    p.add_argument("--central-port", type=int, default=5001)
    return p.parse_args()


# Main
if __name__ == "__main__":
    args = parse_args()

    openweather_key = load_api_key(args.secret_file)
    if not openweather_key:
        print(
            f"{Fore.RED}[FATAL] No se pudo cargar la API key. Verifica {args.secret_file}{Style.RESET_ALL}"
        )
        exit(1)

    central_host = args.central_host
    central_port = args.central_port
    port = args.port

    cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
    cleanup_thread.start()

    monitor_thread = threading.Thread(target=continuous_monitoring, daemon=True)
    monitor_thread.start()

    log("EV_W", "=== Weather Control Office ===", Fore.CYAN)
    log("EV_W", f"OpenWeather API: cargada desde {args.secret_file}", Fore.GREEN)
    log(
        "EV_W",
        f"Central: {central_host}:{central_port}",
        Fore.GREEN if central_host else Fore.YELLOW,
    )
    log("EV_W", f"Puerto: {port}", Fore.GREEN)
    log("EV_W", f"Cache TTL: {CACHE_TTL}s", Fore.CYAN)
    log("EV_W", "Condición alerta: Temperatura < 0°C", Fore.CYAN)
    log(
        "EV_W",
        "Notificará también cuando la temperatura vuelva a ser >= 0°C",
        Fore.CYAN,
    )

    try:
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        log("EV_W", "Deteniendo...", Fore.YELLOW)
