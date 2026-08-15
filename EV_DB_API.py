"""
Uso:
python EV_DB_API.py --db evcharging.db --port 7100
"""

import argparse
import sqlite3
import time
from datetime import datetime

from flask import Flask, request, jsonify
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)

# Utilidades de tiempo y logging
app = Flask(__name__)
db_path = None


def log(tag, msg, color=Fore.CYAN):
    print(f"{color}[{tag}] {msg}{Style.RESET_ALL}")


def now_ts():
    return time.time()


def now_iso():
    return datetime.now().isoformat() + "Z"


# Inicialización BD
def init_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS charging_points (
        cp_id TEXT PRIMARY KEY,
        ubicacion TEXT NOT NULL,
        precio_kwh REAL NOT NULL DEFAULT 0.30,
        potencia_kw REAL NOT NULL DEFAULT 7.0,
        estado TEXT DEFAULT 'desconectado',
        fecha_registro TEXT,
        last_seen REAL,
        engine_ip TEXT,
        engine_port INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS suministros (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cp_id TEXT,
        driver_id TEXT,
        energia_kwh REAL,
        importe_eur REAL,
        fecha_inicio TEXT,
        fecha_fin TEXT,
        estado TEXT,
        FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cp_credentials (
        cp_id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        token TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        last_used TEXT,
        expires_at TEXT,
        activo INTEGER DEFAULT 1
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cp_keys (
        cp_id TEXT PRIMARY KEY,
        symmetric_key TEXT NOT NULL,
        issued_at TEXT,
        revoked INTEGER DEFAULT 0
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        source_type TEXT,
        source_id TEXT,
        source_ip TEXT,
        action TEXT,
        details TEXT
    )
    """)

    conn.commit()
    conn.close()
    log("DB", f"BD inicializada: {db_path}", Fore.GREEN)


# Métodos de la BD
# Registro y gestión de CPs
def register_cp(cp_id, ubicacion="Desconocida", precio=0.30, potencia=7.0):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT ubicacion, precio_kwh, potencia_kw FROM charging_points WHERE cp_id=?",
            (cp_id,),
        )
        row = cursor.fetchone()

        if row:
            cursor.execute(
                "UPDATE charging_points SET last_seen=? WHERE cp_id=?",
                (now_ts(), cp_id),
            )

        else:
            cursor.execute(
                """INSERT INTO charging_points
                   (cp_id, ubicacion, precio_kwh, potencia_kw, fecha_registro, last_seen, estado)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    cp_id,
                    ubicacion,
                    precio,
                    potencia,
                    now_iso(),
                    now_ts(),
                    "desconectado",
                ),
            )
        conn.commit()

        return True

    except Exception as e:
        log("DB", f"Error registrando CP {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


# Gestión de credenciales y tokens
def create_or_update_cp_credentials(
    cp_id, username, password_hash, token=None, expires_at=None
):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO cp_credentials (cp_id, username, password_hash, token, expires_at, activo)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(cp_id) DO UPDATE SET
                username=excluded.username,
                password_hash=excluded.password_hash,
                token=excluded.token,
                expires_at=excluded.expires_at,
                activo=1
        """,
            (cp_id, username, password_hash, token, expires_at),
        )
        conn.commit()

        return True

    except Exception as e:
        log("DB", f"Error guardando credenciales de {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


# Añadir evento de auditoría
def insert_audit_event(timestamp, source_type, source_id, source_ip, action, details):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO audit_log (timestamp, source_type, source_id, source_ip, action, details)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (timestamp, source_type, source_id, source_ip, action, details),
        )
        conn.commit()

        return True

    except Exception as e:
        log("DB", f"Error insertando auditoría: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


# Obtener credenciales de un CP
def get_cp_credentials(cp_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT username, password_hash, activo, token
            FROM cp_credentials
            WHERE cp_id=?
        """,
            (cp_id,),
        )
        row = cursor.fetchone()

        if not row:
            return None
        username, password_hash, activo, token = row
        return {
            "cp_id": cp_id,
            "username": username,
            "password_hash": password_hash,
            "activo": bool(activo),
            "token": token,
        }

    except Exception as e:
        log("DB", f"Error leyendo credenciales de {cp_id}: {e}", Fore.RED)

        return None

    finally:
        conn.close()


# Actualizar estado de un CP
def update_cp_state(cp_id, estado, last_seen=None):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "UPDATE charging_points SET estado=?, last_seen=? WHERE cp_id=?",
            (estado, last_seen or now_ts(), cp_id),
        )
        conn.commit()

        return True

    except Exception as e:
        log("DB", f"Error actualizando estado de {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


# Actualizar ubicación de un CP
def update_cp_ubicacion(cp_id, ubicacion):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "UPDATE charging_points SET ubicacion=? WHERE cp_id=?",
            (ubicacion, cp_id),
        )
        conn.commit()
        log("DB", f"Ubicación de {cp_id} actualizada a {ubicacion}", Fore.GREEN)

        return True

    except Exception as e:
        log("DB", f"Error actualizando ubicación de {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


# Actualizar engine_ip y engine_port de un CP
def update_cp_engine(cp_id, engine_ip, engine_port):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "UPDATE charging_points SET engine_ip=?, engine_port=? WHERE cp_id=?",
            (engine_ip, engine_port, cp_id),
        )
        conn.commit()
        log(
            "DB",
            f"Engine de {cp_id} actualizado a {engine_ip}:{engine_port}",
            Fore.GREEN,
        )

        return True

    except Exception as e:
        log("DB", f"Error actualizando engine de {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


# Obtener todos los CPs
def get_all_cps():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT cp_id, ubicacion, precio_kwh, estado, last_seen FROM charging_points"
        )
        rows = cursor.fetchall()
        cps = {}

        for cp_id, ubic, precio, estado, last in rows:
            cps[cp_id] = {
                "cp_id": cp_id,
                "ubicacion": ubic,
                "precio_kwh": precio,
                "estado": estado or "desconectado",
                "last_seen": last or 0,
                "connected": False,
            }

        return cps

    finally:
        conn.close()


# Obtener precio_kWh de un CP
def get_cp_price(cp_id, default=0.30):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT precio_kwh FROM charging_points WHERE cp_id=?", (cp_id,))
        row = cursor.fetchone()

        if row and row[0] is not None:
            return row[0]

        return default

    finally:
        conn.close()


# Obtener ubicación de un CP
def get_cp_ubicacion(cp_id, default="ubicacion-EV_DB_API"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT ubicacion FROM charging_points WHERE cp_id=?", (cp_id,))
        row = cursor.fetchone()

        if row and row[0] is not None:
            return row[0]

        return default

    finally:
        conn.close()


# Validar Bearer Token
def validate_token(token):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT cp_id, activo, expires_at
            FROM cp_credentials
            WHERE token=?
        """,
            (token,),
        )
        row = cursor.fetchone()

        if not row:
            return None

        cp_id, activo, expires_at = row

        if not activo:
            return None

        if expires_at:
            exp_dt = datetime.fromisoformat(expires_at)
            now = datetime.now()

            if now > exp_dt:
                log("DB", f"Token expirado para {cp_id}", Fore.YELLOW)

                return None
        cursor.execute(
            """
            UPDATE cp_credentials
            SET last_used = ?
            WHERE cp_id = ?
        """,
            (now_iso(), cp_id),
        )
        conn.commit()

        return cp_id

    except Exception as e:
        log("DB", f"Error validando token: {e}", Fore.RED)

        return None

    finally:
        conn.close()


# Obtener toda la información de un CP
def get_cp_info(cp_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("PRAGMA table_info(charging_points)")
        columns = [col[1] for col in cursor.fetchall()]

        if "engine_ip" in columns and "engine_port" in columns:
            cursor.execute(
                """
                SELECT cp_id, ubicacion, precio_kwh, potencia_kw, estado, 
                       last_seen, engine_ip, engine_port
                FROM charging_points WHERE cp_id=?
            """,
                (cp_id,),
            )
            row = cursor.fetchone()

            if row:
                return {
                    "cp_id": row[0],
                    "ubicacion": row[1],
                    "precio_kwh": row[2],
                    "potencia_kw": row[3],
                    "estado": row[4],
                    "last_seen": row[5],
                    "engine_ip": row[6],
                    "engine_port": row[7],
                }
        else:
            cursor.execute(
                """
                SELECT cp_id, ubicacion, precio_kwh, potencia_kw, estado, last_seen
                FROM charging_points WHERE cp_id=?
            """,
                (cp_id,),
            )
            row = cursor.fetchone()

            if row:
                return {
                    "cp_id": row[0],
                    "ubicacion": row[1],
                    "precio_kwh": row[2],
                    "potencia_kw": row[3],
                    "estado": row[4],
                    "last_seen": row[5],
                }

        return None

    finally:
        conn.close()


# Guardar suministro
def save_supply(cp_id, driver_id, energia_kwh, importe_eur, estado):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO suministros(cp_id, driver_id, energia_kwh, importe_eur, fecha_fin, estado) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (cp_id, driver_id, energia_kwh, importe_eur, now_iso(), estado),
        )
        conn.commit()

        return True

    except Exception as e:
        log("DB", f"Error guardando suministro: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


# Desactivar credenciales de un CP
def deactivate_cp_credentials(cp_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "UPDATE cp_credentials SET activo=0 WHERE cp_id=?",
            (cp_id,),
        )
        conn.commit()

        return cursor.rowcount > 0

    except Exception as e:
        log("DB", f"Error desactivando credenciales de {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


def store_cp_key(cp_id, symmetric_key, issued_at=None):
    if issued_at is None:
        issued_at = now_iso()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO cp_keys (cp_id, symmetric_key, issued_at, revoked)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(cp_id) DO UPDATE SET
                symmetric_key=excluded.symmetric_key,
                issued_at=excluded.issued_at,
                revoked=0
        """,
            (cp_id, symmetric_key, issued_at),
        )
        conn.commit()

        return True

    except Exception as e:
        log("DB", f"Error guardando clave de {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return False

    finally:
        conn.close()


# API REST
# Registro de un nuevo CP
@app.route("/api/db/cp/register", methods=["POST"])
def api_db_cp_register():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    ubicacion = data.get("ubicacion", "Desconocida")
    precio = data.get("precio_kwh", 0.30)

    if not cp_id:
        return jsonify({"status": "error", "message": "cp_id requerido"}), 400

    if not register_cp(cp_id, ubicacion, precio):
        return jsonify({"status": "error", "message": "DB error"}), 500

    return jsonify({"status": "ok"}), 200


# Guardar o actualizar credenciales de un CP
@app.route("/api/db/cp/credentials", methods=["POST"])
def api_db_cp_credentials():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    username = data.get("username")
    password_hash = data.get("password_hash")
    token = data.get("token")

    if not cp_id or not username or not password_hash:
        return jsonify({"status": "error", "message": "Missing fields"}), 400

    if not create_or_update_cp_credentials(cp_id, username, password_hash, token):
        return jsonify({"status": "error", "message": "DB error"}), 500

    return jsonify({"status": "ok"}), 200


# Validar token de un CP
@app.route("/api/db/token/validate", methods=["POST"])
def api_db_token_validate():
    data = request.get_json() or {}
    token = data.get("token")

    if not token:
        return jsonify({"status": "error", "message": "Missing token"}), 400
    cp_id = validate_token(token)

    if not cp_id:
        return jsonify(
            {"status": "invalid", "message": "Token not found or inactive"}
        ), 401

    return jsonify({"status": "valid", "cp_id": cp_id}), 200


# Añadir evento de auditoría
@app.route("/api/db/audit", methods=["POST"])
def api_db_audit():
    data = request.get_json() or {}
    ts = data.get("timestamp", now_iso())
    ok = insert_audit_event(
        ts,
        data.get("source_type", "UNKNOWN"),
        data.get("source_id", ""),
        data.get("source_ip", request.remote_addr),
        data.get("action", ""),
        data.get("details", ""),
    )

    if not ok:
        return jsonify({"status": "error", "message": "DB error"}), 500

    return jsonify({"status": "ok"}), 200


# Obtener credenciales de un CP
@app.route("/api/db/cp/credentials/<cp_id>", methods=["GET"])
def api_db_get_cp_credentials(cp_id):
    creds = get_cp_credentials(cp_id)

    if not creds:
        return jsonify({"status": "not_found"}), 404

    return jsonify({"status": "ok", "data": creds}), 200


# Actualizar estado de un CP
@app.route("/api/db/cp/state", methods=["POST"])
def api_db_cp_state():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    estado = data.get("estado")

    if not cp_id or not estado:
        return jsonify({"status": "error", "message": "Missing fields"}), 400

    if not update_cp_state(cp_id, estado):
        return jsonify({"status": "error", "message": "DB error"}), 500

    return jsonify({"status": "ok"}), 200


# Actualizar ubicación de un CP
@app.route("/api/db/cp/update_location", methods=["POST"])
def api_db_cp_update_location():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    ubicacion = data.get("ubicacion")

    if not cp_id or not ubicacion:
        return jsonify({"status": "error", "message": "Missing fields"}), 400
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "UPDATE charging_points SET ubicacion=? WHERE cp_id=?", (ubicacion, cp_id)
        )
        conn.commit()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        log("DB", f"Error actualizando ubicación de {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        conn.close()


# Obtener todos los CPs
@app.route("/api/db/cp/all", methods=["GET"])
def api_db_cp_all():
    cps = get_all_cps()

    return jsonify({"status": "ok", "data": cps}), 200


# Desconectar un CP
@app.route("/api/db/cp/disconnect", methods=["POST"])
def api_db_cp_disconnect():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")

    if not cp_id:
        return jsonify({"status": "error", "message": "Missing cp_id"}), 400
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "UPDATE charging_points SET estado='desconectado' WHERE cp_id=?", (cp_id,)
        )
        conn.commit()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        log("DB", f"Error desconectando {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        conn.close()


# Obtener precio_kWh de un CP
@app.route("/api/db/cp/price/<cp_id>", methods=["GET"])
def api_db_cp_price(cp_id):
    precio = get_cp_price(cp_id, 0.30)

    return jsonify({"status": "ok", "precio_kwh": precio}), 200


# Obtener ubicación de un CP
@app.route("/api/db/cp/ubicacion/<cp_id>", methods=["GET"])
def api_db_cp_ubicacion(cp_id):
    ubic = get_cp_ubicacion(cp_id)

    return jsonify({"status": "ok", "ubicacion": ubic}), 200


# Actualizar ubicación de un CP
@app.route("/api/db/cp/ubicacion", methods=["POST"])
def api_db_cp_ubicacion_post():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    ubicacion = data.get("ubicacion")

    if not cp_id or not ubicacion:
        return jsonify(
            {"status": "error", "message": "Missing cp_id or ubicacion"}
        ), 400

    if not update_cp_ubicacion(cp_id, ubicacion):
        return jsonify({"status": "error", "message": "DB error"}), 500

    return jsonify(
        {"status": "ok", "message": f"Ubicación actualizada a {ubicacion}"}
    ), 200


# Obtener toda la información de un CP
@app.route("/api/db/cp/<cp_id>/info", methods=["GET"])
def api_db_cp_info(cp_id):
    cp_info = get_cp_info(cp_id)

    if not cp_info:
        return jsonify({"status": "error", "message": "CP not found"}), 404

    return jsonify(cp_info), 200


# Actualizar engine_ip y engine_port de un CP
@app.route("/api/db/cp/engine", methods=["POST"])
def api_db_cp_engine():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    engine_ip = data.get("engine_ip")
    engine_port = data.get("engine_port", 6000)

    if not cp_id or not engine_ip:
        return jsonify(
            {"status": "error", "message": "Missing cp_id or engine_ip"}
        ), 400

    if not update_cp_engine(cp_id, engine_ip, engine_port):
        return jsonify({"status": "error", "message": "DB error"}), 500

    return jsonify(
        {"status": "ok", "message": f"Engine actualizado a {engine_ip}:{engine_port}"}
    ), 200


# Guardar suministro
@app.route("/api/db/supply", methods=["POST"])
def api_db_supply():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    driver_id = data.get("driver_id")
    energia = data.get("energia_kwh", 0.0)
    importe = data.get("importe_eur", 0.0)
    estado = data.get("estado", "completado")

    if not cp_id or not driver_id:
        return jsonify({"status": "error", "message": "Missing fields"}), 400

    if not save_supply(cp_id, driver_id, energia, importe, estado):
        return jsonify({"status": "error", "message": "DB error"}), 500

    return jsonify({"status": "ok"}), 200


# Desactivar credenciales de un CP
@app.route("/api/db/cp/credentials/deactivate", methods=["POST"])
def api_db_cp_credentials_deactivate():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")

    if not cp_id:
        return jsonify({"status": "error", "message": "cp_id requerido"}), 400

    if not deactivate_cp_credentials(cp_id):
        return jsonify({"status": "error", "message": "No rows updated"}), 404

    return jsonify({"status": "ok"}), 200


# Altera clave de un CP
@app.route("/api/db/cp/credentials/delete", methods=["POST"])
def api_db_cp_credentials_delete():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")

    if not cp_id:
        return jsonify({"status": "error", "message": "cp_id requerido"}), 400
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM cp_credentials WHERE cp_id = ?", (cp_id,))
        rows_deleted = cursor.rowcount
        conn.commit()

        if rows_deleted == 0:
            return jsonify(
                {"status": "error", "message": "No se encontró el cp_id"}
            ), 404

        return jsonify({"status": "ok", "message": f"Clave de {cp_id} alteradas"}), 200

    except Exception as e:
        log("DB", f"Error al alterar clave de {cp_id}: {e}", Fore.RED)
        conn.rollback()

        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        conn.close()


# Guardar clave simétrica de un CP
@app.route("/api/db/cp/key", methods=["POST"])
def api_db_cp_key():
    data = request.get_json() or {}
    cp_id = data.get("cp_id")
    symmetric_key = data.get("symmetric_key")

    if not cp_id or not symmetric_key:
        return jsonify({"status": "error", "message": "Missing fields"}), 400

    if not store_cp_key(cp_id, symmetric_key, now_iso()):
        return jsonify({"status": "error", "message": "DB error"}), 500

    return jsonify({"status": "ok"}), 200


# Obtener todas las credenciales de CPs
@app.route("/api/db/credentials/all", methods=["GET"])
def api_db_credentials_all():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT cp_id, username, activo, created_at FROM cp_credentials")
        rows = cursor.fetchall()
        data = []

        for cp_id, username, activo, created_at in rows:
            data.append(
                {
                    "cp_id": cp_id,
                    "username": username,
                    "activo": bool(activo),
                    "created_at": created_at,
                }
            )

        return jsonify({"status": "ok", "data": data}), 200

    finally:
        conn.close()


# Listar eventos de auditoría
@app.route("/api/db/audit/list", methods=["GET"])
def api_db_audit_list():
    limit = request.args.get("limit", 50, type=int)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT timestamp, source_type, source_id, source_ip, action, details
            FROM audit_log
            ORDER BY id DESC
            LIMIT ?
        """,
            (limit,),
        )
        rows = cursor.fetchall()
        events = []

        for ts, src_type, src_id, src_ip, action, details in rows:
            events.append(
                {
                    "timestamp": ts,
                    "source_type": src_type,
                    "source_id": src_id,
                    "source_ip": src_ip,
                    "action": action,
                    "details": details,
                }
            )

        return jsonify({"status": "ok", "data": events}), 200

    finally:
        conn.close()


# Main
def main():
    global db_path
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Ruta a evcharging.db")
    parser.add_argument("--port", type=int, default=7100)
    args = parser.parse_args()
    db_path = args.db

    init_db()
    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()
