"""
Heritage Dairy — Procurement Pipeline v2
Clean rewrite with all learnings from v1.

All fixes baked in:
  - hpc_plant_key = plant_code + '_' + hpc_code   (HPC codes repeat across plants)
  - Farmer identity = [Vendor] column              (not [Farmer Code], which is 1,2,3 per HPC)
  - Shift = '1' / '2'                              (not 'M'/'E')
  - quality_incidents = fat<3.5 OR snf<8.0         (dumping_amt is always NULL)
  - DATE() on every proc_date comparison           (stored as '2024-01-01 00:00:00')
  - Monthly processing to avoid OOM               (year-at-once crashed on 8GB RAM)
  - Snapshot uses MAX(DATE(proc_date)) not today   (data updates on pay cycles, not daily)

CLI:
  python pipeline_v2.py --nightly                  # normal nightly run
  python pipeline_v2.py --backfill-from 2024-01    # first time setup
  python pipeline_v2.py --rebuild-from 2025-06     # rebuild from a month onwards
  python pipeline_v2.py --snapshot-only            # just rebuild snapshot
  python pipeline_v2.py --retention-only           # just apply retention + purge
"""

import os, sys, logging, calendar, sqlite3, argparse
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
import duckdb
import pyodbc

# ── CONFIG ────────────────────────────────────────────────────────────────────
PARQUET_DIR  = Path(r"C:\AnalyticsPortal\parquet")
SQLITE_PATH  = r"C:\AnalyticsPortal\portal.db"
TMP_DIR      = Path(r"C:\AnalyticsPortal\tmp")
LOG_PATH     = r"C:\AnalyticsPortal\pipeline.log"

SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=10.0.1.71,4000;"
    "Database=HeritageIT;"
    "UID=127903;"
    "PWD=Raghavkumar.j@heritagefoods.in;"
)

# Retention: days to keep per table
RETENTION = {
    "proc_daily_hpc":         730,   # 2 years
    "proc_monthly_hpc":      1095,   # 3 years
    "proc_monthly_farmer":    730,   # 2 years
    "proc_quality_incidents": 365,   # 1 year (23M rows — aggressively pruned)
}
SNAPSHOT_KEEP    = 10   # keep last N snapshots
PARQUET_KEEP_YRS = 2    # delete parquet files older than this many years

# ── LOGGING ───────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ]
)
# Fix Windows console encoding
import sys as _sys
try:
    _sys.stdout.reconfigure(encoding='utf-8')
    _sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

log = logging.getLogger(__name__)

# ── DB / DUCKDB HELPERS ───────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def get_duck():
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    duck = duckdb.connect()
    duck.execute("SET memory_limit='6GB'")
    duck.execute("SET threads=2")
    duck.execute(f"SET temp_directory='{TMP_DIR}'")
    duck.execute("SET max_temp_directory_size='20GB'")
    duck.execute("SET preserve_insertion_order=false")
    return duck

def get_latest_parquet_date():
    """Latest date we have a parquet file for, or None."""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(PARQUET_DIR.glob("proc_*.parquet"))
    if not files:
        return None
    return date.fromisoformat(files[-1].stem.replace("proc_", ""))

# ── STEP 1: EXPORT FROM SQL SERVER ────────────────────────────────────────────

# One day at a time keeps SQL Server happy:
#  - Simple WHERE date = ? on indexed column
#  - ~160K rows per query, ~2 min
#  - Never blocks production systems
EXPORT_SQL = """
SELECT
    CAST([MCS Date] AS DATE)                                              AS proc_date,
    CAST([Plant]    AS VARCHAR(20))                                       AS plant_code,
    CAST([HPC Code] AS VARCHAR(20))                                       AS hpc_code,
    CAST([Plant] AS VARCHAR(20)) + '_' + CAST([HPC Code] AS VARCHAR(20)) AS hpc_plant_key,
    [HPC Name]                                                            AS hpc_name,
    [Plant Name]                                                          AS plant_name,
    [Area]                                                                AS area,
    [Region Code]                                                         AS region_code,
    [Region]                                                              AS region,
    [Zone]                                                                AS zone,
    [Route Code]                                                          AS route_code,
    CAST([Shift]  AS VARCHAR(5))                                          AS shift,
    CAST([Vendor] AS VARCHAR(20))                                         AS farmer_code,
    [Farmer Name]                                                         AS farmer_name,
    [Milk Type]                                                           AS milk_type,
    CAST([Qty(Ltr)]        AS FLOAT)                                      AS qty_ltr,
    CAST([Qty(Kg)]         AS FLOAT)                                      AS qty_kg,
    CAST([Fat]             AS FLOAT)                                      AS fat,
    CAST([SNF]             AS FLOAT)                                      AS snf,
    CAST([Fat Kg]          AS FLOAT)                                      AS fat_kg,
    CAST([SNF Kg]          AS FLOAT)                                      AS snf_kg,
    CAST([Base Price]      AS FLOAT)                                      AS base_price,
    CAST([MCC Incentive]   AS FLOAT)                                      AS mcc_incentive,
    CAST([Qty Incentive]   AS FLOAT)                                      AS qty_incentive,
    CAST([Bonus]           AS FLOAT)                                      AS bonus,
    CAST([Dumping_Amt(QD)] AS FLOAT)                                      AS dumping_amt,
    CAST([Net Price]       AS FLOAT)                                      AS net_price
FROM [HeritageIT].[P&I].[fact_procurement_transactions]
WHERE CAST([MCS Date] AS DATE) = ?
"""

def _fetch_one_day(target_date, sql_conn):
    """Export one day from SQL Server -> parquet. Returns row count (0 = already exists)."""
    path = PARQUET_DIR / f"proc_{target_date}.parquet"
    if path.exists():
        return 0
    df = pd.read_sql(EXPORT_SQL, sql_conn, params=[str(target_date)])
    if df.empty:
        log.warning(f"  {target_date}: no rows from SQL Server")
        return 0
    df.to_parquet(path, index=False, compression="snappy")
    return len(df)

def fetch_new_data(progress_cb=None):
    """
    Fetch all dates from (latest parquet + 1) through yesterday.
    Skips dates that already have a parquet file.
    Returns list of (date, row_count) for newly fetched dates.
    """
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    yesterday = date.today() - timedelta(days=1)
    latest    = get_latest_parquet_date()

    if latest is None:
        raise RuntimeError(
            "No parquet files found. Run: python pipeline_v2.py --backfill-from 2024-01"
        )

    start = latest + timedelta(days=1)
    if start > yesterday:
        log.info(f"Already up to date (latest parquet: {latest})")
        if progress_cb: progress_cb("fetch", 1, 1, f"Already up to date — latest: {latest}")
        return []

    dates = [start + timedelta(days=i) for i in range((yesterday - start).days + 1)]
    log.info(f"Fetching {len(dates)} days: {start} -> {yesterday}")

    if progress_cb: progress_cb("fetch", 0, len(dates), f"Connecting to SQL Server...")
    try:
        sql_conn = pyodbc.connect(SQL_CONN_STR, timeout=30)
    except Exception as e:
        log.error(f"SQL Server connection failed: {e}")
        raise

    results = []
    try:
        for i, d in enumerate(dates):
            if progress_cb:
                progress_cb("fetch", i + 1, len(dates), f"Fetching {d}...")
            rows = _fetch_one_day(d, sql_conn)
            if rows > 0:
                log.info(f"  {d}: {rows:,} rows")
                results.append((d, rows))
    finally:
        sql_conn.close()

    log.info(f"Fetch done — {len(results)} new files, "
             f"{sum(r for _, r in results):,} rows")
    return results

# ── STEP 2: PURGE OLD PARQUETS ────────────────────────────────────────────────

def purge_old_parquets(reference_date=None, progress_cb=None):
    """
    Delete parquet files older than PARQUET_KEEP_YRS from reference_date.
    Called after every fetch so old files disappear automatically.
    """
    if reference_date is None:
        reference_date = get_latest_parquet_date()
    if reference_date is None:
        return 0

    cutoff  = reference_date.replace(year=reference_date.year - PARQUET_KEEP_YRS)
    deleted = 0
    for f in sorted(PARQUET_DIR.glob("proc_*.parquet")):
        try:
            file_date = date.fromisoformat(f.stem.replace("proc_", ""))
        except ValueError:
            continue
        if file_date < cutoff:
            f.unlink()
            log.info(f"  Deleted old parquet: {f.name}")
            deleted += 1

    if deleted:
        log.info(f"Purged {deleted} parquet files older than {cutoff}")
    if progress_cb:
        progress_cb("purge", 1, 1, f"Purged {deleted} old parquet files")
    return deleted

# ── STEP 3: BUILD MONTHLY SUMMARIES ───────────────────────────────────────────
#
# Processes ONE month at a time to avoid OOM on 8GB RAM.
# Each month ≈ 150K rows in DuckDB — well within limits.
# If a month fails, you only rerun that month (not the whole year).

def build_month(yr, mth, conn=None, duck=None, progress_cb=None):
    """
    Aggregate all parquet files for (yr, mth) and write to all 4 summary tables.
    Replaces existing rows for that month.
    """
    prefix = f"proc_{yr}-{mth:02d}-"
    files  = sorted(PARQUET_DIR.glob(f"{prefix}*.parquet"))
    if not files:
        log.warning(f"  No parquet files for {yr}-{mth:02d} — skipping")
        return False

    ym_str     = f"{yr}-{mth:02d}"
    file_list  = ", ".join(f"'{f}'" for f in files)
    days_total = calendar.monthrange(yr, mth)[1]

    log.info(f"  {ym_str}: {len(files)} files, {days_total} days in month")

    own_duck = duck is None
    own_conn = conn is None
    if own_duck: duck = get_duck()
    if own_conn: conn = get_db()

    try:
        duck.execute(
            f"CREATE OR REPLACE VIEW month_raw AS SELECT * FROM read_parquet([{file_list}])"
        )

        # ── proc_daily_hpc ────────────────────────────────────────────────
        # Grain: hpc_plant_key + proc_date + shift
        # Weighted avg FAT/SNF: sum(fat*qty_kg)/sum(qty_kg) — not simple average
        # quality_incidents counts fat<3.5 OR snf<8.0 (dumping_amt is always NULL)
        # shift uses '1'/'2' — not 'M'/'E'
        if progress_cb: progress_cb("build", 0, 4, f"{ym_str}: daily_hpc")

        daily = duck.execute("""
            SELECT
                proc_date,
                CAST(shift AS VARCHAR)   AS shift,
                hpc_plant_key,
                MAX(hpc_code)            AS hpc_code,
                MAX(plant_code)          AS plant_code,
                MAX(hpc_name)            AS hpc_name,
                MAX(plant_name)          AS plant_name,
                MAX(area)                AS area,
                MAX(region_code)         AS region_code,
                MAX(region)              AS region,
                MAX(zone)                AS zone,
                MAX(route_code)          AS route_code,
                ROUND(SUM(qty_ltr), 2)                                     AS qty_ltr,
                ROUND(SUM(qty_kg),  2)                                     AS qty_kg,
                COUNT(DISTINCT farmer_code)                                AS farmer_count,
                COUNT(*)                                                   AS delivery_count,
                ROUND(SUM(fat*qty_kg) / NULLIF(SUM(qty_kg),0), 4)         AS avg_fat,
                ROUND(SUM(snf*qty_kg) / NULLIF(SUM(qty_kg),0), 4)         AS avg_snf,
                ROUND(SUM(fat_kg), 3)                                      AS total_fat_kg,
                ROUND(SUM(snf_kg), 3)                                      AS total_snf_kg,
                ROUND(SUM(base_price),    2)   AS total_base_price,
                ROUND(SUM(mcc_incentive), 2)   AS total_mcc_incentive,
                ROUND(SUM(qty_incentive), 2)   AS total_qty_incentive,
                ROUND(SUM(bonus),         2)   AS total_bonus,
                ROUND(SUM(dumping_amt),   2)   AS total_dumping_amt,
                ROUND(SUM(net_price),     2)   AS total_net_price,
                ROUND(SUM(net_price) / NULLIF(SUM(qty_ltr),0), 4)         AS rate_per_ltr,
                ROUND(SUM(net_price) / NULLIF(SUM(fat_kg), 0), 4)         AS cost_per_fat_kg,
                SUM(CASE WHEN fat < 3.5 OR snf < 8.0 THEN 1 ELSE 0 END)  AS quality_incidents
            FROM month_raw
            GROUP BY proc_date, CAST(shift AS VARCHAR), hpc_plant_key
        """).df()

        conn.execute(
            "DELETE FROM proc_daily_hpc WHERE strftime('%Y-%m', DATE(proc_date)) = ?",
            (ym_str,)
        )
        daily.to_sql("proc_daily_hpc", conn, if_exists="append", index=False)
        log.info(f"    daily_hpc:    {len(daily):,} rows")

        # ── proc_monthly_hpc ──────────────────────────────────────────────
        # Grain: hpc_plant_key + yr + mth
        # morning_ltr uses shift='1', evening_ltr uses shift='2'
        if progress_cb: progress_cb("build", 1, 4, f"{ym_str}: monthly_hpc")

        monthly_hpc = duck.execute(f"""
            WITH base AS (
                SELECT
                    {yr}  AS yr,
                    {mth} AS mth,
                    hpc_plant_key,
                    MAX(hpc_code)     AS hpc_code,
                    MAX(plant_code)   AS plant_code,
                    MAX(hpc_name)     AS hpc_name,
                    MAX(plant_name)   AS plant_name,
                    MAX(area)         AS area,
                    MAX(region_code)  AS region_code,
                    MAX(region)       AS region,
                    MAX(zone)         AS zone,
                    {days_total}      AS days_in_month,
                    COUNT(DISTINCT proc_date)    AS active_days,
                    COUNT(DISTINCT farmer_code)  AS farmer_count,
                    ROUND(SUM(qty_ltr), 2)   AS total_qty_ltr,
                    ROUND(SUM(qty_kg),  2)   AS total_qty_kg,
                    ROUND(SUM(fat*qty_kg)/NULLIF(SUM(qty_kg),0), 4)  AS avg_fat,
                    ROUND(SUM(snf*qty_kg)/NULLIF(SUM(qty_kg),0), 4)  AS avg_snf,
                    ROUND(SUM(fat_kg), 3)    AS total_fat_kg,
                    ROUND(SUM(snf_kg), 3)    AS total_snf_kg,
                    SUM(CASE WHEN fat < 3.5 OR snf < 8.0 THEN 1 ELSE 0 END) AS dumping_incidents,
                    ROUND(SUM(net_price),     2)  AS total_net_price,
                    ROUND(SUM(base_price),    2)  AS total_base_price,
                    ROUND(SUM(mcc_incentive), 2)  AS total_mcc_incentive,
                    ROUND(SUM(qty_incentive), 2)  AS total_qty_incentive,
                    ROUND(SUM(bonus),         2)  AS total_bonus,
                    ROUND(SUM(dumping_amt),   2)  AS total_dumping_amt,
                    ROUND(SUM(CASE WHEN CAST(shift AS VARCHAR)='1' THEN qty_ltr ELSE 0 END),2) AS morning_ltr,
                    ROUND(SUM(CASE WHEN CAST(shift AS VARCHAR)='2' THEN qty_ltr ELSE 0 END),2) AS evening_ltr
                FROM month_raw
                GROUP BY hpc_plant_key
            )
            SELECT *,
                ROUND(total_qty_ltr / {days_total}.0, 3)                              AS lpd,
                ROUND(total_qty_ltr / {days_total}.0 / NULLIF(farmer_count,0), 3)     AS lpd_per_farmer,
                ROUND(total_net_price / NULLIF(total_qty_ltr,0), 4)                   AS avg_rate_per_ltr,
                ROUND(total_net_price / NULLIF(total_fat_kg, 0), 4)                   AS avg_cost_per_fat_kg,
                ROUND((total_mcc_incentive + total_qty_incentive + total_bonus)
                      / NULLIF(total_base_price, 0) * 100, 2)                         AS incentive_pct
            FROM base
        """).df()

        conn.execute("DELETE FROM proc_monthly_hpc WHERE yr=? AND mth=?", (yr, mth))
        monthly_hpc.to_sql("proc_monthly_hpc", conn, if_exists="append", index=False)
        log.info(f"    monthly_hpc:  {len(monthly_hpc):,} rows")

        # ── proc_monthly_farmer ───────────────────────────────────────────
        # Grain: farmer_code + yr + mth
        # Uses [Vendor] as farmer identity (not [Farmer Code] which is 1,2,3... per HPC)
        if progress_cb: progress_cb("build", 2, 4, f"{ym_str}: monthly_farmer")

        monthly_farmer = duck.execute(f"""
            WITH base AS (
                SELECT
                    {yr}  AS yr,
                    {mth} AS mth,
                    farmer_code,
                    MAX(farmer_name)   AS farmer_name,
                    MAX(hpc_plant_key) AS hpc_plant_key,
                    MAX(hpc_code)      AS hpc_code,
                    MAX(plant_code)    AS plant_code,
                    MAX(region)        AS region,
                    MAX(zone)          AS zone,
                    MAX(milk_type)     AS milk_type,
                    {days_total}       AS days_in_month,
                    COUNT(DISTINCT proc_date)                              AS delivery_days,
                    ROUND(SUM(qty_ltr), 2)                                 AS total_qty_ltr,
                    ROUND(SUM(fat*qty_kg)/NULLIF(SUM(qty_kg),0), 4)        AS avg_fat,
                    ROUND(SUM(snf*qty_kg)/NULLIF(SUM(qty_kg),0), 4)        AS avg_snf,
                    ROUND(SUM(fat_kg), 3)                                  AS total_fat_kg,
                    ROUND(SUM(net_price), 2)                               AS total_net_price,
                    ROUND(SUM(net_price)/NULLIF(SUM(qty_ltr),0), 4)        AS avg_rate_per_ltr,
                    SUM(CASE WHEN fat < 3.5 OR snf < 8.0 THEN 1 ELSE 0 END) AS dumping_incidents
                FROM month_raw
                GROUP BY farmer_code
            )
            SELECT *,
                ROUND(total_qty_ltr / {days_total}.0, 3) AS lpd
            FROM base
        """).df()

        conn.execute("DELETE FROM proc_monthly_farmer WHERE yr=? AND mth=?", (yr, mth))
        monthly_farmer.to_sql("proc_monthly_farmer", conn, if_exists="append", index=False)
        log.info(f"    monthly_farmer: {len(monthly_farmer):,} rows")

        # ── proc_quality_incidents ────────────────────────────────────────
        # Individual transactions where fat<3.5 OR snf<8.0
        # Not aggregated — kept at transaction level for drill-down
        # dumping_amt excluded from filter because it's always NULL in Heritage data
        if progress_cb: progress_cb("build", 3, 4, f"{ym_str}: quality_incidents")

        quality = duck.execute("""
            SELECT
                proc_date, hpc_plant_key, hpc_code, plant_code,
                farmer_code, region, zone,
                fat, snf, qty_ltr, dumping_amt,
                CASE
                    WHEN fat < 3.5 THEN 'LOW_FAT'
                    WHEN snf < 8.0 THEN 'LOW_SNF'
                    ELSE 'OTHER'
                END AS incident_type
            FROM month_raw
            WHERE fat < 3.5 OR snf < 8.0
        """).df()

        conn.execute(
            "DELETE FROM proc_quality_incidents WHERE strftime('%Y-%m', DATE(proc_date)) = ?",
            (ym_str,)
        )
        quality.to_sql("proc_quality_incidents", conn, if_exists="append", index=False)
        log.info(f"    quality_incidents: {len(quality):,} rows")

        conn.commit()
        if progress_cb: progress_cb("build", 4, 4, f"{ym_str}: done")
        return True

    except Exception as e:
        log.error(f"  build_month {ym_str} FAILED: {e}")
        conn.rollback()
        raise
    finally:
        if own_duck: duck.close()
        if own_conn: conn.close()


def build_all_summaries(from_ym=None, progress_cb=None):
    """
    Rebuild summary tables for all months found in parquet files.
    from_ym: (year, month) tuple — only rebuild from this month onwards.
    """
    files = sorted(PARQUET_DIR.glob("proc_*.parquet"))
    if not files:
        raise RuntimeError("No parquet files found.")

    # Discover all unique year-months from filenames
    ym_set = set()
    for f in files:
        try:
            d = date.fromisoformat(f.stem.replace("proc_", ""))
            ym_set.add((d.year, d.month))
        except ValueError:
            continue

    months = sorted(ym_set)
    if from_ym:
        months = [(y, m) for y, m in months if (y, m) >= from_ym]

    log.info(f"Building summaries: {months[0]} -> {months[-1]} ({len(months)} months)")

    conn = get_db()
    duck = get_duck()
    _ensure_tables(conn)
    _ensure_indexes(conn)

    try:
        for i, (yr, mth) in enumerate(months):
            if progress_cb:
                progress_cb("months", i + 1, len(months), f"Processing {yr}-{mth:02d}")
            build_month(yr, mth, conn=conn, duck=duck)
    finally:
        duck.close()
        conn.close()

    log.info("All summaries built successfully.")

# ── STEP 4: RETENTION ─────────────────────────────────────────────────────────

def apply_retention(conn=None, progress_cb=None):
    """
    Delete rows older than policy from every summary table.
    Also keeps only SNAPSHOT_KEEP snapshots.
    """
    own = conn is None
    if own: conn = get_db()

    total_deleted = 0

    for table, days in RETENTION.items():
        cutoff_date = date.today() - timedelta(days=days)
        try:
            if table in ("proc_daily_hpc", "proc_quality_incidents"):
                r = conn.execute(
                    f"DELETE FROM {table} WHERE DATE(proc_date) < ?",
                    (str(cutoff_date),)
                )
            else:  # proc_monthly_hpc, proc_monthly_farmer
                r = conn.execute(
                    f"DELETE FROM {table} WHERE yr < ? OR (yr=? AND mth<?)",
                    (cutoff_date.year, cutoff_date.year, cutoff_date.month)
                )
            if r.rowcount:
                log.info(f"  {table}: deleted {r.rowcount:,} rows (before {cutoff_date})")
                total_deleted += r.rowcount
        except Exception as e:
            log.warning(f"  Retention {table} failed: {e}")

    # Snapshot: keep last SNAPSHOT_KEEP by deleting beyond the Nth oldest
    snaps = conn.execute(
        "SELECT DISTINCT snapshot_date FROM proc_period_snapshot ORDER BY snapshot_date DESC"
    ).fetchall()
    if len(snaps) > SNAPSHOT_KEEP:
        keep_until = snaps[SNAPSHOT_KEEP - 1][0]
        r = conn.execute(
            "DELETE FROM proc_period_snapshot WHERE snapshot_date < ?", (keep_until,)
        )
        if r.rowcount:
            log.info(f"  proc_period_snapshot: kept {SNAPSHOT_KEEP}, deleted {r.rowcount:,} rows")
            total_deleted += r.rowcount

    conn.commit()
    if own: conn.close()
    if progress_cb:
        progress_cb("retention", 1, 1, f"Retention done — {total_deleted:,} rows deleted")
    return total_deleted

# ── STEP 5: SNAPSHOT ──────────────────────────────────────────────────────────
#
# The snapshot pre-computes MTD/LM/LMTD/LYMTD/LYSM for every HPC.
# Uses DATE() on proc_date everywhere — proc_date is stored as
# '2024-01-01 00:00:00' so bare BETWEEN comparisons match nothing.
# Always builds for MAX(DATE(proc_date)) — never date.today().

def build_snapshot(conn=None, progress_cb=None):
    """
    Rebuild proc_period_snapshot for the latest available data date.
    Deletes any stale snapshots beyond that date first.
    """
    own = conn is None
    if own: conn = get_db()

    latest = conn.execute(
        "SELECT MAX(DATE(proc_date)) FROM proc_daily_hpc"
    ).fetchone()[0]

    if not latest:
        log.error("proc_daily_hpc is empty — cannot build snapshot")
        if own: conn.close()
        return False

    snap_date  = date.fromisoformat(latest)
    day_num    = snap_date.day
    curr_yr    = snap_date.year
    curr_mth   = snap_date.month
    ly_yr      = curr_yr - 1
    curr_start = snap_date.replace(day=1)
    prev_end   = curr_start - timedelta(days=1)
    prev_start = prev_end.replace(day=1)
    lm_days    = calendar.monthrange(prev_start.year, prev_start.month)[1]
    ly_sm_days = calendar.monthrange(ly_yr, curr_mth)[1]
    lmtd_end   = prev_start + timedelta(days=day_num - 1)
    ly_mtd_end = date(
        ly_yr, curr_mth,
        min(day_num, calendar.monthrange(ly_yr, curr_mth)[1])
    )

    log.info(f"Building snapshot for {snap_date}")
    log.info(f"  MTD:   {curr_start} -> {snap_date}  ({day_num}d)")
    log.info(f"  LM:    {prev_start} -> {prev_end}  ({lm_days}d)")
    log.info(f"  LMTD:  {prev_start} -> {lmtd_end}  ({day_num}d)")
    log.info(f"  LYMTD: {date(ly_yr, curr_mth, 1)} -> {ly_mtd_end}  ({day_num}d)")
    log.info(f"  LYSM:  {date(ly_yr, curr_mth, 1)} -> {date(ly_yr, curr_mth, ly_sm_days)}")

    if progress_cb: progress_cb("snapshot", 0, 1, f"Computing snapshot for {snap_date}...")

    # Remove stale (beyond data) and today's snapshot (force fresh build)
    conn.execute("DELETE FROM proc_period_snapshot WHERE snapshot_date > ?", (str(snap_date),))
    conn.execute("DELETE FROM proc_period_snapshot WHERE snapshot_date = ?", (str(snap_date),))

    sql = f"""
    WITH
    -- All HPCs seen in last 3 months (avoids inflating dark count with old closed HPCs)
    hpc_dim AS (
        SELECT hpc_plant_key, hpc_code, plant_code, hpc_name, plant_name,
               area, region_code, region, zone
        FROM (
            SELECT hpc_plant_key, hpc_code, plant_code, hpc_name, plant_name,
                   area, region_code, region, zone,
                   ROW_NUMBER() OVER (
                       PARTITION BY hpc_plant_key ORDER BY DATE(proc_date) DESC
                   ) AS rn
            FROM proc_daily_hpc
            WHERE DATE(proc_date) >= DATE('{snap_date}', '-3 months')
        ) t WHERE rn = 1
    ),
    -- Current month to date
    mtd AS (
        SELECT hpc_plant_key,
            SUM(qty_ltr) / {day_num}.0                                    AS mtd,
            SUM(farmer_count) / {day_num}.0                               AS mtd_farmers,
            ROUND(SUM(total_fat_kg)/NULLIF(SUM(qty_kg),0)*100, 3)         AS mtd_avg_fat,
            SUM(total_net_price)                                          AS mtd_payout,
            SUM(total_net_price)/NULLIF(SUM(qty_ltr),0)                  AS mtd_rate
        FROM proc_daily_hpc
        WHERE DATE(proc_date) BETWEEN '{curr_start}' AND '{snap_date}'
        GROUP BY hpc_plant_key
    ),
    -- Full last month (for absolute reference and MoM rate comparison)
    lm AS (
        SELECT hpc_plant_key,
            SUM(qty_ltr) / {lm_days}.0                                    AS lm,
            SUM(farmer_count) / {lm_days}.0                               AS lm_farmers,
            SUM(total_net_price)/NULLIF(SUM(qty_ltr),0)                  AS lm_rate
        FROM proc_daily_hpc
        WHERE DATE(proc_date) BETWEEN '{prev_start}' AND '{prev_end}'
        GROUP BY hpc_plant_key
    ),
    -- Last month same day count (fair MoM comparison — same number of days)
    lmtd AS (
        SELECT hpc_plant_key,
            SUM(qty_ltr) / {day_num}.0                                    AS lmtd,
            SUM(farmer_count) / {day_num}.0                               AS lmtd_farmers
        FROM proc_daily_hpc
        WHERE DATE(proc_date) BETWEEN '{prev_start}' AND '{lmtd_end}'
        GROUP BY hpc_plant_key
    ),
    -- Last year same period (YoY comparison)
    lymtd AS (
        SELECT hpc_plant_key,
            SUM(qty_ltr) / {day_num}.0                                    AS lymtd,
            SUM(farmer_count) / {day_num}.0                               AS lymtd_farmers,
            ROUND(SUM(total_fat_kg)/NULLIF(SUM(qty_kg),0)*100, 3)         AS lymtd_avg_fat
        FROM proc_daily_hpc
        WHERE DATE(proc_date) BETWEEN '{date(ly_yr, curr_mth, 1)}' AND '{ly_mtd_end}'
        GROUP BY hpc_plant_key
    ),
    -- Last year full same month
    lysm AS (
        SELECT hpc_plant_key,
            SUM(qty_ltr) / {ly_sm_days}.0                                 AS lysm
        FROM proc_daily_hpc
        WHERE DATE(proc_date) BETWEEN '{date(ly_yr, curr_mth, 1)}'
                                  AND '{date(ly_yr, curr_mth, ly_sm_days)}'
        GROUP BY hpc_plant_key
    )
    SELECT
        '{snap_date}'                  AS snapshot_date,
        h.*,
        COALESCE(m.mtd,    0)          AS mtd,
        COALESCE(lm.lm,    0)          AS lm,
        COALESCE(lt.lmtd,  0)          AS lmtd,
        COALESCE(ly.lymtd, 0)          AS lymtd,
        COALESCE(ls.lysm,  0)          AS lysm,
        COALESCE(m.mtd_farmers,    0)  AS mtd_farmers,
        COALESCE(lm.lm_farmers,    0)  AS lm_farmers,
        COALESCE(lt.lmtd_farmers,  0)  AS lmtd_farmers,
        COALESCE(ly.lymtd_farmers, 0)  AS lymtd_farmers,
        m.mtd_avg_fat,
        ly.lymtd_avg_fat,
        m.mtd_payout,
        m.mtd_rate,
        lm.lm_rate,
        COALESCE(m.mtd,0) - COALESCE(ly.lymtd,0)  AS yoy_lpd_diff,
        COALESCE(m.mtd,0) - COALESCE(lt.lmtd, 0)  AS mom_lpd_diff,
        CASE WHEN COALESCE(ly.lymtd,0) > 0
            THEN ROUND((COALESCE(m.mtd,0) - ly.lymtd) / ly.lymtd, 4)
        END AS yoy_growth_pct,
        CASE WHEN COALESCE(lt.lmtd,0) > 0
            THEN ROUND((COALESCE(m.mtd,0) - lt.lmtd) / lt.lmtd, 4)
        END AS mom_growth_pct,
        COALESCE(m.mtd_farmers,0) - COALESCE(ly.lymtd_farmers,0)  AS yoy_farmer_diff,
        CASE WHEN COALESCE(ly.lymtd_farmers,0) > 0
            THEN ROUND(
                CAST(COALESCE(m.mtd_farmers,0) - ly.lymtd_farmers AS FLOAT)
                / ly.lymtd_farmers, 4)
        END AS yoy_farmer_pct
    FROM hpc_dim h
    LEFT JOIN mtd   m  ON h.hpc_plant_key = m.hpc_plant_key
    LEFT JOIN lm    lm ON h.hpc_plant_key = lm.hpc_plant_key
    LEFT JOIN lmtd  lt ON h.hpc_plant_key = lt.hpc_plant_key
    LEFT JOIN lymtd ly ON h.hpc_plant_key = ly.hpc_plant_key
    LEFT JOIN lysm  ls ON h.hpc_plant_key = ls.hpc_plant_key
    """

    df = pd.read_sql(sql, conn)
    df.to_sql("proc_period_snapshot", conn, if_exists="append", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_date ON proc_period_snapshot(snapshot_date)")
    conn.commit()

    n_snap = len(df)
    log.info(f"Snapshot built -- {n_snap:,} HPCs")
    if progress_cb: progress_cb("snapshot", 1, 1, f"Snapshot built: {n_snap:,} HPCs")

    if own: conn.close()

    # Always rebuild RFM + health after snapshot (uses separate connection)
    build_farmer_health(progress_cb=progress_cb)
    build_farmer_rfm(progress_cb=progress_cb)

    return True


# ── FARMER HEALTH (per HPC) ───────────────────────────────────────────────────

def build_farmer_health(conn=None, progress_cb=None):
    """
    Builds proc_hpc_farmer_health: farmer retention stats per HPC.
    Consistent  = delivery_days >= 20 this month
    Irregular   = delivery_days < 10 this month
    Churned     = active last month, zero this month
    Acquired    = zero last month, active this month
    Attrition % = churned / last month total * 100
    """
    own = conn is None
    if own: conn = get_db()

    snap_date = conn.execute(
        "SELECT MAX(snapshot_date) FROM proc_period_snapshot"
    ).fetchone()[0]
    if not snap_date:
        log.warning("build_farmer_health: no snapshot yet, skipping")
        if own: conn.close()
        return False

    # Get current and last month year/month
    from datetime import date
    sd = date.fromisoformat(snap_date)
    curr_yr, curr_mth = sd.year, sd.month
    if curr_mth == 1:
        prev_yr, prev_mth = curr_yr - 1, 12
    else:
        prev_yr, prev_mth = curr_yr, curr_mth - 1

    log.info(f"Building farmer health for {snap_date}")

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
        -- SQLite has no FULL OUTER JOIN — emulate with LEFT + RIGHT via UNION
        all_pairs AS (
            SELECT c.hpc_plant_key, c.farmer_code, c.delivery_days,
                   CASE WHEN p.farmer_code IS NOT NULL THEN 1 ELSE 0 END AS in_prev
            FROM curr c
            LEFT JOIN prev p
                ON c.hpc_plant_key = p.hpc_plant_key
               AND c.farmer_code   = p.farmer_code
            UNION ALL
            -- Farmers in prev but NOT in curr (churned/dormant candidates)
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
                -- Churned: in prev, not in curr this month
                SUM(CASE WHEN delivery_days = 0 AND in_prev = 1 THEN 1 ELSE 0 END) AS churned,
                -- Acquired: in curr but was not in prev
                SUM(CASE WHEN delivery_days > 0 AND in_prev = 0 THEN 1 ELSE 0 END) AS acquired,
                SUM(in_prev)                                                  AS prev_total
            FROM all_pairs
            GROUP BY hpc_plant_key
        )
        SELECT
            '{snap_date}'                                AS snapshot_date,
            s.hpc_plant_key,
            s.total_farmers,
            s.consistent,
            s.irregular,
            ROUND(CASE WHEN s.total_farmers > 0
                THEN s.consistent * 100.0 / s.total_farmers ELSE 0 END, 1) AS consistency_pct,
            s.churned,
            s.acquired,
            s.acquired - s.churned                      AS net_change,
            ROUND(CASE WHEN s.prev_total > 0
                THEN s.churned * 100.0 / s.prev_total ELSE 0 END, 1) AS attrition_pct,
            CASE
                WHEN s.acquired > s.churned              THEN 'Growing'
                WHEN s.churned * 100.0 / MAX(s.prev_total,1) > 15 THEN 'Churning'
                WHEN s.consistent * 100.0 / MAX(s.total_farmers,1) >= 60 THEN 'Stable'
                ELSE 'At Risk'
            END                                         AS segment
        FROM stats s
    """)
    conn.commit()

    n = conn.execute(
        "SELECT COUNT(*) FROM proc_hpc_farmer_health WHERE snapshot_date=?", (snap_date,)
    ).fetchone()[0]
    log.info(f"Farmer health built — {n:,} HPCs for {snap_date}")

    if own: conn.close()
    if progress_cb: progress_cb("farmer_health", 1, 1, f"Farmer health built — {n:,} HPCs")
    return True


# ── FARMER RFM ────────────────────────────────────────────────────────────────

def build_farmer_rfm(conn=None, progress_cb=None):
    """
    Builds proc_farmer_rfm with proper Dormant + Churned tiers.

    R score (Recency):
      3 = delivered this month (delivery_days > 0)
      2 = delivered last month but NOT this month  -> Dormant candidate
      1 = not in either month                      -> Churned candidate

    F score (Frequency — delivery_days this month, percentile-based):
      3 = top third  (high attendance)
      2 = mid third
      1 = bottom third

    M score (Monetary — total_payout, percentile-based):
      3 = top third
      2 = mid third
      1 = bottom third

    Tiers:
      Champion  : R=3, F=3, M>=2
      Loyal     : R=3, F>=2  (not Champion)
      At Risk   : R=3, any low F or M
      Dormant   : R=2  (active last month, zero this month)
      Churned   : R=1  (not active in either month)
    """
    own = conn is None
    if own: conn = get_db()

    snap_date = conn.execute(
        "SELECT MAX(snapshot_date) FROM proc_period_snapshot"
    ).fetchone()[0]
    if not snap_date:
        log.warning("build_farmer_rfm: no snapshot yet, skipping")
        if own: conn.close()
        return False

    from datetime import date
    sd = date.fromisoformat(snap_date)
    curr_yr, curr_mth = sd.year, sd.month
    if curr_mth == 1:
        prev_yr, prev_mth = curr_yr - 1, 12
    else:
        prev_yr, prev_mth = curr_yr, curr_mth - 1
    # Two months ago — for Churned (absent from both last month AND this month)
    if prev_mth == 1:
        pp_yr, pp_mth = prev_yr - 1, 12
    else:
        pp_yr, pp_mth = prev_yr, prev_mth - 1

    log.info(f"Building farmer RFM for {snap_date}")

    # Compute F/M thresholds from current month active farmers
    th = conn.execute(f"""
        WITH curr AS (
            SELECT delivery_days, total_net_price AS payout
            FROM proc_monthly_farmer
            WHERE yr={curr_yr} AND mth={curr_mth} AND delivery_days > 0
        ),
        f_sorted AS (
            SELECT delivery_days,
                   ROW_NUMBER() OVER (ORDER BY delivery_days) AS rn,
                   COUNT(*) OVER () AS total
            FROM curr
        ),
        m_sorted AS (
            SELECT payout,
                   ROW_NUMBER() OVER (ORDER BY payout) AS rn,
                   COUNT(*) OVER () AS total
            FROM curr
        )
        SELECT
            (SELECT delivery_days FROM f_sorted WHERE rn=MAX(1,total/3) LIMIT 1)   AS f_low,
            (SELECT delivery_days FROM f_sorted WHERE rn=MAX(1,total*2/3) LIMIT 1) AS f_high,
            (SELECT payout FROM m_sorted WHERE rn=MAX(1,total/3) LIMIT 1)          AS m_low,
            (SELECT payout FROM m_sorted WHERE rn=MAX(1,total*2/3) LIMIT 1)        AS m_high
        FROM curr LIMIT 1
    """).fetchone()

    f_low  = (th[0] if th and th[0] else 10)
    f_high = (th[1] if th and th[1] else 20)
    m_low  = (th[2] if th and th[2] else 1000)
    m_high = (th[3] if th and th[3] else 5000)
    log.info(f"  RFM thresholds: F=[{f_low},{f_high}d] M=[{m_low},{m_high} Rs]")

    # Diagnostic: how many dormant farmers exist?
    dormant_check = conn.execute(f"""
        SELECT COUNT(*) FROM proc_monthly_farmer pm
        WHERE pm.yr={prev_yr} AND pm.mth={prev_mth} AND pm.delivery_days > 0
          AND NOT EXISTS (
            SELECT 1 FROM proc_monthly_farmer c2
            WHERE c2.farmer_code   = pm.farmer_code
              AND c2.hpc_plant_key = pm.hpc_plant_key
              AND c2.yr={curr_yr} AND c2.mth={curr_mth}
              AND c2.delivery_days > 0
          )
    """).fetchone()[0]
    log.info(f"  Dormant candidate count: {dormant_check:,}")

    curr_check = conn.execute(f"""
        SELECT COUNT(*) FROM proc_monthly_farmer
        WHERE yr={curr_yr} AND mth={curr_mth} AND delivery_days > 0
    """).fetchone()[0]
    log.info(f"  Current month active farmers: {curr_check:,}")

    # Diagnostic: count candidates before INSERT
    curr_n = conn.execute(f"""
        SELECT COUNT(*) FROM proc_monthly_farmer
        WHERE yr={curr_yr} AND mth={curr_mth} AND delivery_days>0
    """).fetchone()[0]
    dormant_n = conn.execute(f"""
        SELECT COUNT(*) FROM proc_monthly_farmer pm
        WHERE pm.yr={prev_yr} AND pm.mth={prev_mth} AND pm.delivery_days > 0
          AND NOT EXISTS (
            SELECT 1 FROM proc_monthly_farmer c2
            WHERE c2.farmer_code=pm.farmer_code AND c2.hpc_plant_key=pm.hpc_plant_key
              AND c2.yr={curr_yr} AND c2.mth={curr_mth} AND c2.delivery_days>0
          )
    """).fetchone()[0]
    churned_n = conn.execute(f"""
        SELECT COUNT(*) FROM proc_monthly_farmer pm
        WHERE pm.yr={pp_yr} AND pm.mth={pp_mth} AND pm.delivery_days > 0
          AND NOT EXISTS (
            SELECT 1 FROM proc_monthly_farmer p2
            WHERE p2.farmer_code=pm.farmer_code AND p2.hpc_plant_key=pm.hpc_plant_key
              AND p2.yr={prev_yr} AND p2.mth={prev_mth} AND p2.delivery_days>0
          )
          AND NOT EXISTS (
            SELECT 1 FROM proc_monthly_farmer c2
            WHERE c2.farmer_code=pm.farmer_code AND c2.hpc_plant_key=pm.hpc_plant_key
              AND c2.yr={curr_yr} AND c2.mth={curr_mth} AND c2.delivery_days>0
          )
    """).fetchone()[0]
    log.info(f"  Current month active farmers : {curr_n:,}")
    log.info(f"  Dormant candidates (LM->now) : {dormant_n:,}")
    log.info(f"  Churned candidates (2m->now) : {churned_n:,}")

    conn.execute("DELETE FROM proc_farmer_rfm WHERE snapshot_date=?", (snap_date,))

    conn.execute(f"""
        INSERT INTO proc_farmer_rfm
        WITH
        -- Current month active farmers
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
            WHERE mf.yr={curr_yr} AND mf.mth={curr_mth}
              AND mf.delivery_days > 0
        ),
        -- Last month active farmers (for Dormant detection)
        prev AS (
            SELECT DISTINCT farmer_code, hpc_plant_key
            FROM proc_monthly_farmer
            WHERE yr={prev_yr} AND mth={prev_mth}
              AND delivery_days > 0
        ),
        -- Dormant: active last month, zero delivery this month
        -- Uses NOT EXISTS — valid SQLite, no FULL OUTER JOIN needed
        -- Dormant: active last month, NOT active this month (R=2)
        -- Carries last-active-month volume so we can show "volume lost"
        dormant AS (
            SELECT
                pm.farmer_code,
                pm.farmer_name,
                pm.hpc_plant_key,
                s.region, s.zone,
                CAST(s.plant_code AS TEXT) AS plant_code,
                pm.milk_type,
                pm.delivery_days           AS delivery_days,    -- last active days
                pm.total_qty_ltr           AS total_qty_ltr,    -- last active volume
                pm.total_net_price         AS total_payout,     -- last active payout
                pm.avg_fat                 AS avg_fat,
                pm.lpd                     AS lpd,              -- last active LPD
                2                          AS r_score
            FROM proc_monthly_farmer pm
            JOIN proc_period_snapshot s
              ON pm.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date  = '{snap_date}'
            WHERE pm.yr={prev_yr} AND pm.mth={prev_mth}
              AND pm.delivery_days > 0
              AND NOT EXISTS (
                SELECT 1 FROM proc_monthly_farmer c2
                WHERE c2.farmer_code   = pm.farmer_code
                  AND c2.hpc_plant_key = pm.hpc_plant_key
                  AND c2.yr={curr_yr} AND c2.mth={curr_mth}
                  AND c2.delivery_days > 0
              )
        ),
        -- Churned: active 2 months ago, absent from BOTH last month AND this month (R=1)
        -- Carries last-active-month volume so we can show "volume lost"
        churned AS (
            SELECT
                pm.farmer_code,
                pm.farmer_name,
                pm.hpc_plant_key,
                s.region, s.zone,
                CAST(s.plant_code AS TEXT) AS plant_code,
                pm.milk_type,
                pm.delivery_days           AS delivery_days,    -- last active days
                pm.total_qty_ltr           AS total_qty_ltr,    -- last active volume
                pm.total_net_price         AS total_payout,     -- last active payout
                pm.avg_fat                 AS avg_fat,
                pm.lpd                     AS lpd,              -- last active LPD
                1                          AS r_score
            FROM proc_monthly_farmer pm
            JOIN proc_period_snapshot s
              ON pm.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date  = '{snap_date}'
            WHERE pm.yr={pp_yr} AND pm.mth={pp_mth}
              AND pm.delivery_days > 0
              AND NOT EXISTS (
                SELECT 1 FROM proc_monthly_farmer p2
                WHERE p2.farmer_code   = pm.farmer_code
                  AND p2.hpc_plant_key = pm.hpc_plant_key
                  AND p2.yr={prev_yr} AND p2.mth={prev_mth}
                  AND p2.delivery_days > 0
              )
              AND NOT EXISTS (
                SELECT 1 FROM proc_monthly_farmer c2
                WHERE c2.farmer_code   = pm.farmer_code
                  AND c2.hpc_plant_key = pm.hpc_plant_key
                  AND c2.yr={curr_yr} AND c2.mth={curr_mth}
                  AND c2.delivery_days > 0
              )
        ),
        -- Combine: active + dormant + churned
        all_farmers AS (
            SELECT *,
                   CASE WHEN delivery_days >= {f_high} THEN 3
                        WHEN delivery_days >= {f_low}  THEN 2
                        ELSE 1 END AS f_score,
                   CASE WHEN total_payout >= {m_high} THEN 3
                        WHEN total_payout >= {m_low}  THEN 2
                        ELSE 1 END AS m_score
            FROM curr
            UNION ALL
            SELECT *, 1 AS f_score, 1 AS m_score FROM dormant
            UNION ALL
            SELECT *, 1 AS f_score, 1 AS m_score FROM churned
        )
        SELECT
            '{snap_date}'  AS snapshot_date,
            farmer_code, farmer_name, hpc_plant_key, region, zone, plant_code, milk_type,
            r_score, f_score, m_score,
            r_score + f_score + m_score AS rfm_score,
            CASE
                WHEN r_score = 3 AND f_score = 3 AND m_score >= 2 THEN 'Champion'
                WHEN r_score = 3 AND f_score >= 2                 THEN 'Loyal'
                WHEN r_score = 3                                   THEN 'At Risk'
                WHEN r_score = 2                                   THEN 'Dormant'
                ELSE                                                    'Churned'
            END AS tier,
            total_qty_ltr, total_payout, delivery_days, avg_fat, lpd
        FROM all_farmers
    """)
    conn.commit()

    counts = conn.execute(f"""
        SELECT tier, COUNT(*) FROM proc_farmer_rfm
        WHERE snapshot_date='{snap_date}' GROUP BY tier ORDER BY tier
    """).fetchall()
    for t, c in counts:
        log.info(f"  RFM {t}: {c:,}")

    if own: conn.close()
    if progress_cb: progress_cb("farmer_rfm", 1, 1,
        "Farmer RFM built — " + ", ".join(f"{t}:{c:,}" for t,c in counts))
    return True


# ── TABLE SETUP ───────────────────────────────────────────────────────────────

def _ensure_tables(conn):
    # Migrate proc_farmer_rfm if missing plant_code column (schema was updated)
    rfm_cols = [r[1] for r in conn.execute(
        "PRAGMA table_info(proc_farmer_rfm)"
    ).fetchall()]
    if rfm_cols and 'plant_code' not in rfm_cols:
        log.info("Migrating proc_farmer_rfm — dropping old schema (missing plant_code)")
        conn.execute("DROP TABLE IF EXISTS proc_farmer_rfm")
        conn.commit()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS proc_daily_hpc (
            proc_date TEXT, shift TEXT, hpc_plant_key TEXT,
            hpc_code TEXT, plant_code TEXT, hpc_name TEXT, plant_name TEXT,
            area TEXT, region_code TEXT, region TEXT, zone TEXT, route_code TEXT,
            qty_ltr REAL, qty_kg REAL, farmer_count INTEGER, delivery_count INTEGER,
            avg_fat REAL, avg_snf REAL, total_fat_kg REAL, total_snf_kg REAL,
            total_base_price REAL, total_mcc_incentive REAL, total_qty_incentive REAL,
            total_bonus REAL, total_dumping_amt REAL, total_net_price REAL,
            rate_per_ltr REAL, cost_per_fat_kg REAL, quality_incidents INTEGER
        );
        CREATE TABLE IF NOT EXISTS proc_monthly_hpc (
            yr INTEGER, mth INTEGER, hpc_plant_key TEXT,
            hpc_code TEXT, plant_code TEXT, hpc_name TEXT, plant_name TEXT,
            area TEXT, region_code TEXT, region TEXT, zone TEXT,
            days_in_month INTEGER, active_days INTEGER, farmer_count INTEGER,
            total_qty_ltr REAL, total_qty_kg REAL,
            avg_fat REAL, avg_snf REAL, total_fat_kg REAL, total_snf_kg REAL,
            dumping_incidents INTEGER,
            total_net_price REAL, total_base_price REAL,
            total_mcc_incentive REAL, total_qty_incentive REAL,
            total_bonus REAL, total_dumping_amt REAL,
            morning_ltr REAL, evening_ltr REAL,
            lpd REAL, lpd_per_farmer REAL,
            avg_rate_per_ltr REAL, avg_cost_per_fat_kg REAL, incentive_pct REAL
        );
        CREATE TABLE IF NOT EXISTS proc_monthly_farmer (
            yr INTEGER, mth INTEGER, farmer_code TEXT, farmer_name TEXT,
            hpc_plant_key TEXT, hpc_code TEXT, plant_code TEXT,
            region TEXT, zone TEXT, milk_type TEXT,
            days_in_month INTEGER, delivery_days INTEGER,
            total_qty_ltr REAL, avg_fat REAL, avg_snf REAL, total_fat_kg REAL,
            total_net_price REAL, avg_rate_per_ltr REAL,
            dumping_incidents INTEGER, lpd REAL
        );
        CREATE TABLE IF NOT EXISTS proc_quality_incidents (
            proc_date TEXT, hpc_plant_key TEXT, hpc_code TEXT, plant_code TEXT,
            farmer_code TEXT, region TEXT, zone TEXT,
            fat REAL, snf REAL, qty_ltr REAL, dumping_amt REAL, incident_type TEXT
        );
        CREATE TABLE IF NOT EXISTS proc_period_snapshot (
            snapshot_date TEXT, hpc_plant_key TEXT,
            hpc_code TEXT, plant_code TEXT, hpc_name TEXT, plant_name TEXT,
            area TEXT, region_code TEXT, region TEXT, zone TEXT,
            mtd REAL, lm REAL, lmtd REAL, lymtd REAL, lysm REAL,
            mtd_farmers REAL, lm_farmers REAL, lmtd_farmers REAL, lymtd_farmers REAL,
            mtd_avg_fat REAL, lymtd_avg_fat REAL,
            mtd_payout REAL, mtd_rate REAL, lm_rate REAL,
            yoy_lpd_diff REAL, mom_lpd_diff REAL,
            yoy_growth_pct REAL, mom_growth_pct REAL,
            yoy_farmer_diff REAL, yoy_farmer_pct REAL
        );
        CREATE TABLE IF NOT EXISTS pipeline_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT DEFAULT (datetime('now')),
            trigger TEXT, stage TEXT, status TEXT,
            rows_processed INTEGER, duration_sec REAL, message TEXT
        );
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
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin','cxo','zh','rh','plant')),
            scope_zone TEXT DEFAULT '',
            scope_region TEXT DEFAULT '',
            scope_plant TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            last_login TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()

def _ensure_indexes(conn):
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_d_date         ON proc_daily_hpc(DATE(proc_date));
        CREATE INDEX IF NOT EXISTS idx_d_hpc          ON proc_daily_hpc(hpc_plant_key, DATE(proc_date));
        CREATE INDEX IF NOT EXISTS idx_d_region       ON proc_daily_hpc(region, DATE(proc_date));
        CREATE INDEX IF NOT EXISTS idx_d_zone         ON proc_daily_hpc(zone, DATE(proc_date));

        -- ↓ NEW: covers the bootstrap daily trend query (last 90 days grouped by date+zone+region)
        CREATE INDEX IF NOT EXISTS idx_d_zone_rgn_date ON proc_daily_hpc(zone, region, DATE(proc_date));

        CREATE INDEX IF NOT EXISTS idx_mhpc_ym        ON proc_monthly_hpc(yr, mth);
        CREATE INDEX IF NOT EXISTS idx_mhpc_rgn       ON proc_monthly_hpc(yr, mth, region);
        CREATE INDEX IF NOT EXISTS idx_mfmr_ym        ON proc_monthly_farmer(yr, mth);
        CREATE INDEX IF NOT EXISTS idx_mfmr_rgn       ON proc_monthly_farmer(yr, mth, region);
        CREATE INDEX IF NOT EXISTS idx_qi_ym          ON proc_quality_incidents(strftime('%Y-%m', proc_date));
        CREATE INDEX IF NOT EXISTS idx_qi_hpc         ON proc_quality_incidents(hpc_plant_key, proc_date);
        CREATE INDEX IF NOT EXISTS idx_snap_date      ON proc_period_snapshot(snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_snap_hpc       ON proc_period_snapshot(hpc_plant_key, snapshot_date);
    """)
    conn.commit()

def _ensure_default_admin(conn):
    """Create default admin user if no users exist."""
    from werkzeug.security import generate_password_hash
    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        conn.execute("""
            INSERT INTO users (username, full_name, password_hash, role)
            VALUES ('admin', 'Administrator', ?, 'admin')
        """, (generate_password_hash("admin@123"),))
        conn.commit()
        log.info("Default admin user created: admin / admin@123 — CHANGE THIS IMMEDIATELY")

# ── ORCHESTRATORS ─────────────────────────────────────────────────────────────

def run_nightly():
    """
    Standard nightly job: fetch yesterday -> rebuild current month -> retention -> snapshot.
    This runs every night. Total time ~6 minutes.
    """
    import time
    t0 = time.time()
    log.info("=== Nightly pipeline starting ===")

    conn = get_db()
    _ensure_tables(conn)
    _ensure_indexes(conn)
    conn.close()

    fetched = fetch_new_data()
    if not fetched:
        log.info("Nothing new to fetch — rebuilding snapshot only")
        build_snapshot()
        return

    affected = sorted(set((d.year, d.month) for d, _ in fetched))
    conn = get_db()
    duck = get_duck()
    try:
        for yr, mth in affected:
            build_month(yr, mth, conn=conn, duck=duck)
    finally:
        duck.close()
        conn.close()

    apply_retention()
    purge_old_parquets()
    build_snapshot()

    elapsed = int(time.time() - t0)
    log.info(f"=== Nightly done in {elapsed}s ===")

    conn = get_db()
    conn.execute(
        "INSERT INTO pipeline_log(trigger,stage,status,duration_sec,message) VALUES(?,?,?,?,?)",
        ("nightly", "full", "success", elapsed, f"Fetched {len(fetched)} days")
    )
    conn.commit()
    conn.close()


def run_full_fetch(progress_cb=None):
    """
    Admin-triggered full fetch.
    Fetches all missing data -> rebuilds affected months -> retention -> purge -> snapshot.
    Runs in a background thread. Returns result dict.
    """
    import time
    t0 = time.time()
    log.info("=== Admin full fetch starting ===")

    conn = get_db()
    _ensure_tables(conn)
    _ensure_indexes(conn)
    conn.close()

    # 1. Fetch
    fetched = fetch_new_data(progress_cb=progress_cb)
    if not fetched:
        msg = "Already up to date — no new data in SQL Server"
        if progress_cb: progress_cb("done", 1, 1, msg)
        return {"status": "up_to_date", "fetched_days": 0, "fetched_rows": 0}

    total_rows = sum(r for _, r in fetched)

    # 2. Rebuild affected months
    affected = sorted(set((d.year, d.month) for d, _ in fetched))
    if progress_cb: progress_cb("build", 0, len(affected),
                                f"Rebuilding {len(affected)} month(s)...")
    conn = get_db()
    duck = get_duck()
    try:
        for i, (yr, mth) in enumerate(affected):
            if progress_cb:
                progress_cb("build", i + 1, len(affected), f"Building {yr}-{mth:02d}...")
            build_month(yr, mth, conn=conn, duck=duck)
        _ensure_indexes(conn)
    finally:
        duck.close()
        conn.close()

    # 3. Retention
    apply_retention(progress_cb=progress_cb)

    # 4. Purge old parquets
    purge_old_parquets(progress_cb=progress_cb)

    # 5. Snapshot
    build_snapshot(progress_cb=progress_cb)

    elapsed = int(time.time() - t0)
    log.info(f"=== Admin fetch done in {elapsed}s ===")

    conn = get_db()
    conn.execute(
        "INSERT INTO pipeline_log(trigger,stage,status,duration_sec,rows_processed,message) "
        "VALUES(?,?,?,?,?,?)",
        ("admin_fetch", "full", "success", elapsed, total_rows,
         f"Fetched {len(fetched)} days, {total_rows:,} rows")
    )
    conn.commit()
    conn.close()

    return {
        "status":       "success",
        "fetched_days": len(fetched),
        "fetched_rows": total_rows,
        "seconds":      elapsed,
    }

# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heritage Procurement Pipeline v2")
    parser.add_argument("--nightly",        action="store_true",
                        help="Nightly: fetch yesterday + rebuild + snapshot")
    parser.add_argument("--backfill-from",  metavar="YYYY-MM",
                        help="Backfill all months from YYYY-MM onwards")
    parser.add_argument("--rebuild-from",   metavar="YYYY-MM",
                        help="Rebuild summary tables from YYYY-MM onwards (uses existing parquets)")
    parser.add_argument("--snapshot-only",  action="store_true",
                        help="Rebuild snapshot only (no parquet fetch or aggregation)")
    parser.add_argument("--retention-only", action="store_true",
                        help="Apply retention + purge old parquets only")
    args = parser.parse_args()

    conn = get_db()
    _ensure_tables(conn)
    _ensure_indexes(conn)
    _ensure_default_admin(conn)
    conn.close()

    if args.nightly:
        run_nightly()

    elif args.backfill_from:
        yr, mth = map(int, args.backfill_from.split("-"))
        log.info(f"Backfill from {yr}-{mth:02d}")
        fetch_new_data()                        # fetch all missing parquets first
        build_all_summaries(from_ym=(yr, mth))  # then aggregate month by month
        apply_retention()
        purge_old_parquets()
        build_snapshot()

    elif args.rebuild_from:
        yr, mth = map(int, args.rebuild_from.split("-"))
        build_all_summaries(from_ym=(yr, mth))
        build_snapshot()

    elif args.snapshot_only:
        build_snapshot()

    elif args.retention_only:
        apply_retention()
        purge_old_parquets()

    else:
        parser.print_help()