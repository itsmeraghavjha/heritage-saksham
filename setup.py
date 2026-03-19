"""
Heritage Procurement Portal v2 — First-Time Setup
Run this ONCE before starting the portal for the first time.

Creates:
  - users table in portal.db
  - Default admin user (username: admin, password: Heritage@2024)
  - Sample users for each role (optional)

Usage:
  python setup.py
  python setup.py --admin-password YourSecurePassword
"""
import sqlite3
import argparse
from werkzeug.security import generate_password_hash

SQLITE_PATH = r"C:\AnalyticsPortal\portal.db"


def create_users_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT    UNIQUE NOT NULL,
            password_hash TEXT    NOT NULL,
            full_name     TEXT    NOT NULL,
            role          TEXT    NOT NULL CHECK(role IN ('admin','cxo','zh','rh','plant')),
            zone          TEXT    DEFAULT '',
            region        TEXT    DEFAULT '',
            plant_code    TEXT    DEFAULT '',
            is_active     INTEGER DEFAULT 1,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_login    DATETIME
        )
    """)
    conn.commit()
    print("Users table ready.")


def create_user(conn, username, password, full_name, role,
                zone="", region="", plant_code=""):
    try:
        conn.execute("""
            INSERT INTO users (username, password_hash, full_name, role,
                               zone, region, plant_code, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """, (username, generate_password_hash(password),
               full_name, role, zone, region, plant_code))
        conn.commit()
        print(f"  Created: {username} ({role})"
              + (f" | zone={zone}" if zone else "")
              + (f" | region={region}" if region else "")
              + (f" | plant={plant_code}" if plant_code else ""))
        return True
    except sqlite3.IntegrityError:
        print(f"  Skipped: {username} already exists")
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin-password", default="Heritage@2024",
                        help="Password for admin user")
    parser.add_argument("--create-samples", action="store_true",
                        help="Also create sample users for each role")
    args = parser.parse_args()

    conn = sqlite3.connect(SQLITE_PATH)
    create_users_table(conn)

    print("\nCreating users...")
    create_user(conn, "admin", args.admin_password,
                "System Administrator", "admin")

    if args.create_samples:
        # CXO — sees all data
        create_user(conn, "cxo_user", "Heritage@2024",
                    "Chief Executive Officer", "cxo")

        # Zone Heads — each sees only their zone
        # Update zone names to match your actual zones
        for zone in ["ZONE-1", "ZONE-2", "ZONE-3", "ZONE-4"]:
            uname = f"zh_{zone.lower().replace('-','')}"
            create_user(conn, uname, "Heritage@2024",
                        f"Zone Head {zone}", "zh", zone=zone)

        # Sample RH — update region to match your actual regions
        create_user(conn, "rh_vijayawada", "Heritage@2024",
                    "Regional Head Vijayawada", "rh", region="VIJAYAWADA")
        create_user(conn, "rh_nashik", "Heritage@2024",
                    "Regional Head Nashik", "rh", region="NASHIK")

        # Sample Plant Incharge
        create_user(conn, "plant_1226", "Heritage@2024",
                    "Plant Incharge 1226", "plant", plant_code="1226")

    conn.close()

    print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Setup complete!

Default admin credentials:
  Username: admin
  Password: {args.admin_password}

IMPORTANT: Change the admin password immediately after first login.
You can do this via Admin → User Management.

Start the portal:
  python app.py
  Then open: http://localhost:5001
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")


if __name__ == "__main__":
    main()