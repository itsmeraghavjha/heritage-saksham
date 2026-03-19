"""
Rebuild proc_quality_incidents only — with correct thresholds.
Change FAT_THRESHOLD and SNF_THRESHOLD to your actual values first.
Run: python fix_quality_incidents.py
"""
import sqlite3, duckdb, logging
from datetime import date
from pathlib import Path

PARQUET_DIR     = Path(r"C:\AnalyticsPortal\parquet")
SQLITE_PATH     = r"C:\AnalyticsPortal\portal.db"

# ← Change these to Heritage's actual rejection thresholds
FAT_THRESHOLD   = 3.5
SNF_THRESHOLD   = 8.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

conn = sqlite3.connect(SQLITE_PATH)
conn.execute("PRAGMA journal_mode=WAL")

files = sorted(PARQUET_DIR.glob("proc_*.parquet"))
ym_set = set()
for f in files:
    try:
        d = date.fromisoformat(f.stem.replace("proc_", ""))
        ym_set.add((d.year, d.month))
    except ValueError:
        continue

months = sorted(ym_set)
log.info(f"Rebuilding quality_incidents for {len(months)} months "
         f"(FAT<{FAT_THRESHOLD} OR SNF<{SNF_THRESHOLD})")

duck = duckdb.connect()
duck.execute("SET memory_limit='6GB'")
duck.execute("SET threads=2")

conn.execute("DELETE FROM proc_quality_incidents")
conn.commit()
log.info("Cleared existing proc_quality_incidents")

for yr, mth in months:
    prefix    = f"proc_{yr}-{mth:02d}-"
    mth_files = sorted(PARQUET_DIR.glob(f"{prefix}*.parquet"))
    if not mth_files:
        continue

    ym_str    = f"{yr}-{mth:02d}"
    file_list = ", ".join(f"'{f}'" for f in mth_files)

    duck.execute(f"CREATE OR REPLACE VIEW month_raw AS SELECT * FROM read_parquet([{file_list}])")

    df = duck.execute(f"""
        SELECT
            proc_date,
            hpc_plant_key,
            hpc_code,
            plant_code,
            farmer_code,
            region,
            zone,
            fat,
            snf,
            qty_ltr,
            dumping_amt,
            CASE
                WHEN fat < {FAT_THRESHOLD} AND snf < {SNF_THRESHOLD} THEN 'LOW_FAT_SNF'
                WHEN fat < {FAT_THRESHOLD} THEN 'LOW_FAT'
                WHEN snf < {SNF_THRESHOLD} THEN 'LOW_SNF'
                ELSE 'OTHER'
            END AS incident_type
        FROM month_raw
        WHERE fat < {FAT_THRESHOLD} OR snf < {SNF_THRESHOLD}
    """).df()

    if not df.empty:
        df.to_sql("proc_quality_incidents", conn, if_exists="append", index=False)
        conn.commit()
        log.info(f"  {ym_str}: {len(df):,} incidents")
    else:
        log.info(f"  {ym_str}: 0 incidents")

duck.close()
conn.close()
log.info("Done — restart Flask to rebuild cache")