"""
seed_users.py — Populate Heritage Procurement Portal users from plant_hpc_mapping.xlsx

Run once (or --reset to recreate all):
    python seed_users.py
    python seed_users.py --reset
"""

import sys, sqlite3, os, pandas as pd
from werkzeug.security import generate_password_hash

SQLITE_PATH  = r"C:\AnalyticsPortal\portal.db"
MAPPING_FILE = r"plant_hpc_mapping.xlsx"   # put this file next to seed_users.py
PASSWORD     = "Heritage@123"

def get_db():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_users_table(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT UNIQUE NOT NULL,
            full_name     TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL CHECK(role IN ('admin','cxo','zh','rh','plant')),
            scope_zone    TEXT DEFAULT '',
            scope_region  TEXT DEFAULT '',
            scope_plant   TEXT DEFAULT '',
            is_active     INTEGER DEFAULT 1,
            last_login    TEXT,
            created_at    TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()

def upsert(conn, username, full_name, role, scope_zone='', scope_region='', scope_plant='', reset=False):
    ph = generate_password_hash(PASSWORD)
    existing = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
    if existing:
        if reset:
            conn.execute("""
                UPDATE users SET full_name=?,password_hash=?,role=?,
                scope_zone=?,scope_region=?,scope_plant=?,is_active=1
                WHERE username=?
            """, (full_name, ph, role, scope_zone, scope_region, scope_plant, username))
            print(f"  UPDATED  {role:6} | {username}")
        else:
            print(f"  SKIP     {role:6} | {username} (already exists)")
    else:
        conn.execute("""
            INSERT INTO users(username,full_name,password_hash,role,scope_zone,scope_region,scope_plant)
            VALUES(?,?,?,?,?,?,?)
        """, (username, full_name, ph, role, scope_zone, scope_region, scope_plant))
        print(f"  CREATED  {role:6} | {username}")

def main():
    reset = '--reset' in sys.argv
    if reset:
        print("\n⚠  --reset mode: existing users will be overwritten\n")

    if not os.path.exists(MAPPING_FILE):
        print(f"ERROR: {MAPPING_FILE} not found. Place the file next to seed_users.py")
        sys.exit(1)

    df = pd.read_excel(MAPPING_FILE)
    conn = get_db()
    ensure_users_table(conn)

    print("\n── ADMIN & CXO ─────────────────────────────────────────────")
    upsert(conn, 'admin@heritagefoods.in',       'Administrator',              'admin',  reset=reset)
    upsert(conn, 'cxo@heritagefoods.in',         'Chief Procurement Officer',  'cxo',    reset=reset)

    print("\n── ZONE HEADS (ZH) ─────────────────────────────────────────")
    zones = sorted(df['zone'].unique())
    for zone in zones:
        # username: ZONE-1, ZONE-2 etc (no spaces)
        uname = zone.replace(' ', '').replace('-', '').lower()  # zone1, zone2 etc
        uname = f"zh.{uname}@heritagefoods.in"
        upsert(conn, uname, f"Zone Head — {zone}", 'zh', scope_zone=zone, reset=reset)

    print("\n── REGION HEADS (RH) ───────────────────────────────────────")
    region_zone = df.groupby('region')['zone'].first().to_dict()
    for region, zone in sorted(region_zone.items()):
        uname = f"rh.{region.lower().replace(' ', '.').replace('-', '')}@heritagefoods.in"
        upsert(conn, uname, f"Region Head — {region}", 'rh',
               scope_zone=zone, scope_region=region, reset=reset)

    print("\n── PLANT INCHARGES ─────────────────────────────────────────")
    plant_info = df.groupby('plant_code')[['region','zone']].first().to_dict('index')
    for plant_code, info in sorted(plant_info.items()):
        # username = plant code (e.g. "1200"), password = Heritage@123
        uname = str(plant_code)
        upsert(conn, uname, f"Plant Incharge — {plant_code}",
               'plant', scope_zone=info['zone'], scope_region=info['region'],
               scope_plant=str(plant_code), reset=reset)

    conn.commit()
    conn.close()

    # Print summary
    conn2 = get_db()
    counts = conn2.execute("""
        SELECT role, COUNT(*) as cnt FROM users WHERE is_active=1 GROUP BY role
    """).fetchall()
    conn2.close()

    print("\n──────────────────────────────────────────────────────────")
    print("  SEEDING COMPLETE")
    for row in counts:
        print(f"  {row['role']:8} → {row['cnt']:4} users")
    print(f"\n  Password for all: Heritage@123")
    print(f"  Login usernames:")
    print(f"    admin/cxo → email (e.g. cxo@heritagefoods.in)")
    print(f"    ZH        → zh.zone1@heritagefoods.in etc.")
    print(f"    RH        → rh.tirupati@heritagefoods.in etc.")
    print(f"    Plant     → plant_code (e.g. 1200, 1201 ...)")
    print("──────────────────────────────────────────────────────────\n")

if __name__ == '__main__':
    main()