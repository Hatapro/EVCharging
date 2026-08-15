"""
Uso:
python EV_Registry.py --db-api 192.168.18.148:7100 --port 7000
"""

import argparse
import hashlib
import json
import secrets
from datetime import datetime
from functools import wraps

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

db_api_url = None


# Utilidades de tiempo y logging
def now_iso():
    return datetime.now().isoformat() + "Z"


def hash_password(raw_password: str) -> str:
    return hashlib.sha256(raw_password.encode("utf-8")).hexdigest()


# Función POST a la DB
def db_post(path, payload):
    url = f"{db_api_url}{path}"
    try:
        resp = requests.post(url, json=payload, timeout=5)
        try:
            return resp.status_code, resp.json() if resp.content else {}
        except Exception as e:
            print(f"[REGISTRY] Error parseando JSON de {path}: {e}")
            return resp.status_code, {"error": str(e), "response_text": resp.text}
    except Exception as e:
        print(f"[REGISTRY] Error en POST a {path}: {e}")
        return 500, {"error": str(e)}


# Función GET a la DB
def db_get(path):
    url = f"{db_api_url}{path}"
    try:
        resp = requests.get(url, timeout=5)
        try:
            return resp.status_code, resp.json() if resp.content else {}
        except Exception as e:
            print(f"[REGISTRY] Error parseando JSON de {path}: {e}")
            return resp.status_code, {"error": str(e), "response_text": resp.text}
    except Exception as e:
        print(f"[REGISTRY] Error en GET a {path}: {e}")
        return 500, {"error": str(e)}


# Decorador de validación de Bearer Token
def require_bearer_token(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            cp_id = kwargs.get("cp_id") or kwargs.get("validated_cp_id")
            if not cp_id:
                try:
                    data = request.get_json() or {}
                    cp_id = data.get("cp_id")
                except Exception:
                    pass
            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "Registry",
                    "source_id": cp_id or "UNKNOWN",
                    "source_ip": request.remote_addr,
                    "action": "INVALID_TOKEN",
                    "details": json.dumps(
                        {"error": "Missing or malformed Authorization header"}
                    ),
                },
            )
            return jsonify({"error": "Missing or invalid token"}), 401

        token = auth_header.split(" ")[1]

        status, response = db_post("/api/db/token/validate", {"token": token})
        if status != 200 or response.get("status") != "valid":
            cp_id_attempt = response.get("cp_id") if response else None
            if not cp_id_attempt:
                cp_id_attempt = kwargs.get("cp_id") or kwargs.get("validated_cp_id")
            if not cp_id_attempt:
                try:
                    data = request.get_json() or {}
                    cp_id_attempt = data.get("cp_id")
                except Exception:
                    pass
            db_post(
                "/api/db/audit",
                {
                    "timestamp": now_iso(),
                    "source_type": "Registry",
                    "source_id": cp_id_attempt or "UNKNOWN",
                    "source_ip": request.remote_addr,
                    "action": "INVALID_TOKEN",
                    "details": json.dumps({"error": "Invalid or inactive token"}),
                },
            )
            return jsonify({"error": "Invalid token"}), 401

        cp_id = response.get("cp_id")
        db_post(
            "/api/db/audit",
            {
                "timestamp": now_iso(),
                "source_type": "Registry",
                "source_id": cp_id,
                "source_ip": request.remote_addr,
                "action": "TOKEN_VALIDATED",
                "details": json.dumps({"endpoint": request.endpoint}),
            },
        )

        kwargs["validated_cp_id"] = cp_id
        return f(*args, **kwargs)

    return decorated


# Endpoint de registro de CP
@app.route("/api/registry/register", methods=["POST"])
def api_register_cp():
    try:
        data = request.get_json() or {}
        cp_id = data.get("cp_id")
        ubic_param = data.get("ubicacion", "Desconocida")
        raw_password = data.get("password")

        if not cp_id or not raw_password:
            return jsonify({"status": "error", "message": "Missing fields"}), 400

        if len(raw_password) < 1:
            return jsonify(
                {
                    "status": "error",
                    "message": "Password must have at least 1 character",
                }
            ), 400

        username = cp_id
        password_hash = hash_password(raw_password)
        token = secrets.token_urlsafe(32)
        status_cp, _ = db_post(
            "/api/db/cp/register",
            {"cp_id": cp_id, "ubicacion": ubic_param, "precio_kwh": 0.30},
        )
        if status_cp != 200:
            return jsonify({"status": "error", "message": "DB CP register error"}), 500

        status_cred, response_cred = db_post(
            "/api/db/cp/credentials",
            {
                "cp_id": cp_id,
                "username": username,
                "password_hash": password_hash,
                "token": token,
            },
        )
        if status_cred != 200:
            print(
                f"[REGISTRY ERROR] DB credentials error: {status_cred} - {response_cred}"
            )
            return jsonify(
                {
                    "status": "error",
                    "message": "DB credentials error",
                    "details": str(response_cred),
                }
            ), 500

        db_post(
            "/api/db/audit",
            {
                "timestamp": now_iso(),
                "source_type": "Registry",
                "source_id": cp_id,
                "source_ip": request.remote_addr,
                "action": "CP_REGISTERED",
                "details": json.dumps({"ubicacion": ubic_param}),
            },
        )

        return jsonify(
            {
                "status": "ok",
                "cp_id": cp_id,
                "username": username,
                "password": raw_password,
                "token": token,
            }
        ), 200
    except Exception as e:
        print(f"[REGISTRY EXCEPTION] {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# Endpoint de desregistro de CP
@app.route("/api/registry/unregister/<cp_id>", methods=["DELETE"])
@require_bearer_token
def api_unregister_cp(cp_id, validated_cp_id=None):
    if validated_cp_id != cp_id:
        return jsonify({"error": "Token does not match CP_ID"}), 403

    db_post(
        "/api/db/cp/credentials/deactivate",
        {"cp_id": cp_id},
    )

    db_post(
        "/api/db/audit",
        {
            "timestamp": now_iso(),
            "source_type": "Registry",
            "source_id": cp_id,
            "source_ip": request.remote_addr,
            "action": "CP_UNREGISTERED",
            "details": "",
        },
    )
    return jsonify({"status": "ok"}), 200


# Endpoint para actualizar ubicación de un CP
@app.route("/api/registry/location", methods=["POST"])
@require_bearer_token
def api_update_location(validated_cp_id=None):
    data = request.get_json() or {}
    new_location = data.get("ubicacion")

    if not new_location:
        return jsonify({"error": "Missing ubicacion field"}), 400

    status, _ = db_post(
        "/api/db/cp/update_location",
        {"cp_id": validated_cp_id, "ubicacion": new_location},
    )

    if status != 200:
        return jsonify({"status": "error", "message": "DB error"}), 500

    db_post(
        "/api/db/audit",
        {
            "timestamp": now_iso(),
            "source_type": "Registry",
            "source_id": validated_cp_id,
            "source_ip": request.remote_addr,
            "action": "CP_LOCATION_UPDATED",
            "details": json.dumps({"new_location": new_location}),
        },
    )

    return jsonify({"status": "ok", "ubicacion": new_location}), 200


# Endpoint para alterar las credenciales de un CP
@app.route("/api/registry/credentials/delete", methods=["DELETE"])
@require_bearer_token
def api_delete_credentials(validated_cp_id=None):
    status, response = db_post(
        "/api/db/cp/credentials/delete", {"cp_id": validated_cp_id}
    )

    if status == 404:
        return jsonify({"status": "error", "message": "CP no encontrado"}), 404
    if status != 200:
        return jsonify({"status": "error", "message": "DB error"}), 500

    db_post(
        "/api/db/audit",
        {
            "timestamp": now_iso(),
            "source_type": "Registry",
            "source_id": validated_cp_id,
            "source_ip": request.remote_addr,
            "action": "CP_KEY_ALTERED",
            "details": json.dumps({"message": "Clave alterada completamente"}),
        },
    )

    return jsonify({"status": "ok", "message": "Clave alterada"}), 200


def main():
    global db_api_url
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-api", required=True, help="192.168.18.148:7100")
    parser.add_argument("--port", type=int, default=7000)
    args = parser.parse_args()

    host_port = args.db_api.strip().rstrip("/")
    db_api_url = f"http://{host_port}"

    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()
