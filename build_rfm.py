"""
build_rfm.py  —  Run this ONCE after pipeline.py --snapshot-only
Builds proc_farmer_rfm and proc_hpc_farmer_health tables.

Usage:
    python build_rfm.py

No dependencies beyond sqlite3 (stdlib). Reads the same portal.db.
"""

import sqlite3, sys, logging
from datetime import date

# ── CONFIG — change if your DB path is different ──────────────────────────────
SQLITE_PATH = r"C:\AnalyticsPortal\portal.db"

# ── LOGGING (utf-8 safe on Windows) ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(stream=open(sys.stdout.fileno(), mode='w',
                                          encoding='utf-8', closefd=False)),
    ]
)
log = logging.getLogger(__name__)

def get_db():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_tables(conn):
    # Check if proc_farmer_rfm has plant_code column — if not, drop and recreate
    cols = [r[1] for r in conn.execute("PRAGMA table_info(proc_farmer_rfm)").fetchall()]
    if cols and 'plant_code' not in cols:
        log.info("proc_farmer_rfm missing plant_code column — recreating table")
        conn.execute("DROP TABLE IF EXISTS proc_farmer_rfm")
        conn.commit()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS proc_hpc_farmer_health (
            snapshot_date TEXT, hpc_plant_key TEXT,
            total_farmers INTEGER, consistent INTEGER, irregular INTEGER,
            consistency_pct REAL, churned INTEGER, acquired INTEGER,
            net_change INTEGER, attrition_pct REAL, segment TEXT
        );
        CREATE TABLE IF NOT EXISTS proc_farmer_rfm (
            snapshot_date TEXT, farmer_code TEXT, farmer_name TEXT,
            hpc_plant_key TEXT, region TEXT, zone TEXT, plant_code TEXT,
            milk_type TEXT, r_score INTEGER, f_score INTEGER, m_score INTEGER,
            rfm_score INTEGER, tier TEXT,
            total_qty_ltr REAL, total_payout REAL,
            delivery_days INTEGER, avg_fat REAL, lpd REAL
        );
    """)
    conn.commit()

def build_farmer_health(conn, snap_date, curr_yr, curr_mth, prev_yr, prev_mth):
    log.info(f"Building farmer health for {snap_date} ...")

    conn.execute("DELETE FROM proc_hpc_farmer_health WHERE snapshot_date=?", (snap_date,))

    conn.execute(f"""
        INSERT INTO proc_hpc_farmer_health
        WITH
        curr AS (
            SELECT hpc_plant_key, farmer_code, delivery_days
            FROM proc_monthly_farmer
            WHERE yr={curr_yr} AND mth={curr_mth} AND delivery_days > 0
        ),
        prev AS (
            SELECT hpc_plant_key, farmer_code
            FROM proc_monthly_farmer
            WHERE yr={prev_yr} AND mth={prev_mth} AND delivery_days > 0
        ),
        all_pairs AS (
            SELECT c.hpc_plant_key, c.farmer_code, c.delivery_days,
                   CASE WHEN p.farmer_code IS NOT NULL THEN 1 ELSE 0 END AS in_prev
            FROM curr c
            LEFT JOIN prev p
                ON c.hpc_plant_key = p.hpc_plant_key
               AND c.farmer_code   = p.farmer_code
            UNION ALL
            SELECT p.hpc_plant_key, p.farmer_code, 0 AS delivery_days, 1 AS in_prev
            FROM prev p
            WHERE NOT EXISTS (
                SELECT 1 FROM curr c
                WHERE c.hpc_plant_key = p.hpc_plant_key
                  AND c.farmer_code   = p.farmer_code
            )
        ),
        stats AS (
            SELECT
                hpc_plant_key,
                SUM(CASE WHEN delivery_days > 0 THEN 1 ELSE 0 END)          AS total_farmers,
                SUM(CASE WHEN delivery_days >= 20 THEN 1 ELSE 0 END)         AS consistent,
                SUM(CASE WHEN delivery_days > 0 AND delivery_days < 10 THEN 1 ELSE 0 END) AS irregular,
                SUM(CASE WHEN delivery_days = 0 AND in_prev = 1 THEN 1 ELSE 0 END) AS churned,
                SUM(CASE WHEN delivery_days > 0 AND in_prev = 0 THEN 1 ELSE 0 END) AS acquired,
                SUM(in_prev)                                                  AS prev_total
            FROM all_pairs
            GROUP BY hpc_plant_key
        )
        SELECT
            '{snap_date}', s.hpc_plant_key,
            s.total_farmers, s.consistent, s.irregular,
            ROUND(CASE WHEN s.total_farmers > 0 THEN s.consistent*100.0/s.total_farmers ELSE 0 END, 1),
            s.churned, s.acquired,
            s.acquired - s.churned,
            ROUND(CASE WHEN s.prev_total > 0 THEN s.churned*100.0/s.prev_total ELSE 0 END, 1),
            CASE
                WHEN s.acquired > s.churned THEN 'Growing'
                WHEN s.prev_total > 0 AND s.churned*100.0/s.prev_total > 15 THEN 'Churning'
                WHEN s.total_farmers > 0 AND s.consistent*100.0/s.total_farmers >= 60 THEN 'Stable'
                ELSE 'At Risk'
            END
        FROM stats s
    """)
    conn.commit()

    n = conn.execute(
        "SELECT COUNT(*) FROM proc_hpc_farmer_health WHERE snapshot_date=?", (snap_date,)
    ).fetchone()[0]
    log.info(f"  Farmer health done: {n:,} HPCs")

def build_farmer_rfm(conn, snap_date, curr_yr, curr_mth, prev_yr, prev_mth):
    log.info(f"Building farmer RFM for {snap_date} ...")

    # Diagnostic counts
    curr_active = conn.execute(
        f"SELECT COUNT(*) FROM proc_monthly_farmer WHERE yr={curr_yr} AND mth={curr_mth} AND delivery_days>0"
    ).fetchone()[0]
    dormant_count = conn.execute(f"""
        SELECT COUNT(*) FROM proc_monthly_farmer pm
        WHERE pm.yr={prev_yr} AND pm.mth={prev_mth} AND pm.delivery_days > 0
          AND NOT EXISTS (
            SELECT 1 FROM proc_monthly_farmer c2
            WHERE c2.farmer_code   = pm.farmer_code
              AND c2.hpc_plant_key = pm.hpc_plant_key
              AND c2.yr={curr_yr}  AND c2.mth={curr_mth}
              AND c2.delivery_days > 0
          )
    """).fetchone()[0]
    log.info(f"  Current month active farmers : {curr_active:,}")
    log.info(f"  Dormant candidates           : {dormant_count:,}")

    # Compute F/M thresholds
    th = conn.execute(f"""
        WITH curr AS (
            SELECT delivery_days, total_net_price AS payout
            FROM proc_monthly_farmer
            WHERE yr={curr_yr} AND mth={curr_mth} AND delivery_days > 0
        ),
        f_s AS (SELECT delivery_days, ROW_NUMBER() OVER (ORDER BY delivery_days) AS rn, COUNT(*) OVER () AS t FROM curr),
        m_s AS (SELECT payout, ROW_NUMBER() OVER (ORDER BY payout) AS rn, COUNT(*) OVER () AS t FROM curr)
        SELECT
            (SELECT delivery_days FROM f_s WHERE rn=MAX(1,t/3)   LIMIT 1) AS f_low,
            (SELECT delivery_days FROM f_s WHERE rn=MAX(1,t*2/3) LIMIT 1) AS f_high,
            (SELECT payout        FROM m_s WHERE rn=MAX(1,t/3)   LIMIT 1) AS m_low,
            (SELECT payout        FROM m_s WHERE rn=MAX(1,t*2/3) LIMIT 1) AS m_high
        FROM curr LIMIT 1
    """).fetchone()

    f_low  = th[0] if th and th[0] else 10
    f_high = th[1] if th and th[1] else 20
    m_low  = th[2] if th and th[2] else 1000
    m_high = th[3] if th and th[3] else 5000
    log.info(f"  Thresholds: F=[{f_low},{f_high}d]  M=[Rs.{m_low:.0f}, Rs.{m_high:.0f}]")

    conn.execute("DELETE FROM proc_farmer_rfm WHERE snapshot_date=?", (snap_date,))

    conn.execute(f"""
        INSERT INTO proc_farmer_rfm
        WITH
        curr AS (
            SELECT mf.farmer_code, mf.farmer_name, mf.hpc_plant_key,
                   mf.region, mf.zone,
                   CAST(s.plant_code AS TEXT) AS plant_code,
                   mf.milk_type,
                   mf.delivery_days,
                   mf.total_qty_ltr,
                   mf.total_net_price  AS total_payout,
                   mf.avg_fat,
                   mf.lpd,
                   3 AS r_score
            FROM proc_monthly_farmer mf
            JOIN proc_period_snapshot s
              ON mf.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date  = '{snap_date}'
            WHERE mf.yr={curr_yr} AND mf.mth={curr_mth} AND mf.delivery_days > 0
        ),
        dormant AS (
            SELECT pm.farmer_code, pm.farmer_name, pm.hpc_plant_key,
                   s.region, s.zone,
                   CAST(s.plant_code AS TEXT) AS plant_code,
                   pm.milk_type,
                   0   AS delivery_days,
                   0   AS total_qty_ltr,
                   0   AS total_payout,
                   pm.avg_fat,
                   0.0 AS lpd,
                   2   AS r_score
            FROM proc_monthly_farmer pm
            JOIN proc_period_snapshot s
              ON pm.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date  = '{snap_date}'
            WHERE pm.yr={prev_yr} AND pm.mth={prev_mth} AND pm.delivery_days > 0
              AND NOT EXISTS (
                SELECT 1 FROM proc_monthly_farmer c2
                WHERE c2.farmer_code   = pm.farmer_code
                  AND c2.hpc_plant_key = pm.hpc_plant_key
                  AND c2.yr={curr_yr}  AND c2.mth={curr_mth}
                  AND c2.delivery_days > 0
              )
        ),
        all_f AS (
            SELECT *,
                CASE WHEN delivery_days >= {f_high} THEN 3
                     WHEN delivery_days >= {f_low}  THEN 2
                     ELSE 1 END AS f_score,
                CASE WHEN total_payout >= {m_high} THEN 3
                     WHEN total_payout >= {m_low}  THEN 2
                     ELSE 1 END AS m_score
            FROM curr
            UNION ALL
            SELECT *, 1 AS f_score, 1 AS m_score
            FROM dormant
        )
        SELECT
            '{snap_date}',
            farmer_code, farmer_name, hpc_plant_key, region, zone, plant_code, milk_type,
            r_score, f_score, m_score,
            r_score + f_score + m_score AS rfm_score,
            CASE
                WHEN r_score=3 AND f_score=3 AND m_score>=2 THEN 'Champion'
                WHEN r_score=3 AND f_score>=2               THEN 'Loyal'
                WHEN r_score=3                               THEN 'At Risk'
                WHEN r_score=2                               THEN 'Dormant'
                ELSE                                              'Churned'
            END AS tier,
            total_qty_ltr, total_payout, delivery_days, avg_fat, lpd
        FROM all_f
    """)
    conn.commit()

    # Print tier breakdown
    rows = conn.execute(f"""
        SELECT tier, COUNT(*) AS cnt
        FROM proc_farmer_rfm WHERE snapshot_date='{snap_date}'
        GROUP BY tier ORDER BY cnt DESC
    """).fetchall()
    log.info("  RFM tier breakdown:")
    for r in rows:
        log.info(f"    {r['tier']:12} : {r['cnt']:>8,}")

def main():
    conn = get_db()
    ensure_tables(conn)

    snap_date = conn.execute(
        "SELECT MAX(snapshot_date) FROM proc_period_snapshot"
    ).fetchone()[0]

    if not snap_date:
        log.error("No snapshot found. Run pipeline.py --snapshot-only first.")
        sys.exit(1)

    log.info(f"Using snapshot date: {snap_date}")

    sd = date.fromisoformat(snap_date)
    curr_yr, curr_mth = sd.year, sd.month
    prev_yr, prev_mth = (curr_yr-1, 12) if curr_mth == 1 else (curr_yr, curr_mth-1)

    log.info(f"Current period : {curr_yr}-{curr_mth:02d}")
    log.info(f"Previous period: {prev_yr}-{prev_mth:02d}")

    build_farmer_health(conn, snap_date, curr_yr, curr_mth, prev_yr, prev_mth)
    build_farmer_rfm(conn, snap_date, curr_yr, curr_mth, prev_yr, prev_mth)

    conn.close()
    log.info("Done. Restart Flask to pick up new data.")

if __name__ == "__main__":
    main()