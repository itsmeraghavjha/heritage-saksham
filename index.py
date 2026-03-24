import sqlite3

SQLITE_PATH = r"C:\AnalyticsPortal\portal.db"
conn = sqlite3.connect(SQLITE_PATH)

rows = conn.execute("""
    SELECT
        zone,
        ROUND(SUM(CASE WHEN DATE(proc_date) BETWEEN '2026-02-01' AND '2026-02-28'
                  THEN qty_ltr ELSE 0 END) / 28.0, 0) AS mtd,
        ROUND(SUM(CASE WHEN DATE(proc_date) BETWEEN '2025-02-01' AND '2025-02-28'
                  THEN qty_ltr ELSE 0 END) / 28.0, 0) AS lymtd
    FROM proc_daily_hpc
    WHERE zone IS NOT NULL
    GROUP BY zone
    ORDER BY mtd DESC
""").fetchall()

print(f"{'Zone':<15} {'MTD':>12} {'LYMTD':>12} {'YoY%':>8}")
print("-" * 50)
for r in rows:
    yoy = ((r[1]-r[2])/r[2]*100) if r[2] else 0
    print(f"{r[0]:<15} {int(r[1]):>12,} {int(r[2]):>12,} {yoy:>7.1f}%")

conn.close()