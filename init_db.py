"""
Uso:
python init_db.py evcharging.db
"""

import sqlite3
import sys
from datetime import datetime


# Inicializa la BD con CPs de ejemplo
def init_database(db_path):
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
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        token TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        last_used TEXT,
        expires_at TEXT,
        activo INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cp_keys (
        cp_id TEXT PRIMARY KEY,
        symmetric_key TEXT NOT NULL,
        issued_at TEXT,
        revoked INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_id TEXT,
        source_ip TEXT,
        action TEXT NOT NULL,
        details TEXT
    )
    """)

    cps_ejemplo = [
        ("CP-001", "Ottawa", 0.35, 9.0),
        ("CP-002", "Madrid", 0.28, 11.0),
        ("CP-003", "Valencia", 0.32, 5.0),
        ("CP-004", "Sevilla", 0.29, 22.0),
        ("CP-005", "Bilbao", 0.31, 20.0),
    ]

    fecha_registro = datetime.now().isoformat() + "Z"

    for cp_id, ubicacion, precio, potencia in cps_ejemplo:
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO charging_points 
                (cp_id, ubicacion, precio_kwh, potencia_kw, estado, fecha_registro)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (cp_id, ubicacion, precio, potencia, "desconectado", fecha_registro),
            )
            print(f"Registrado {cp_id} - {ubicacion}")
        except Exception as e:
            print(f"Error registrando {cp_id}: {e}")

    conn.commit()
    conn.close()

    print(f"\nBase de datos '{db_path}' inicializada correctamente")
    print(f"{len(cps_ejemplo)} puntos de carga registrados")


if __name__ == "__main__":
    db_name = sys.argv[1] if len(sys.argv) > 1 else "evcharging.db"
    init_database(db_name)
