"""
Heritage Dairy — Data Recovery Script
======================================
Run this ONCE after deploying the fixed pipeline.py.
Fixes the corrupt DB state caused by all 3 bugs:

  Bug 1 aftermath : All parquets have milk_type='1'/'2' instead of 'Cow'/'Buffalo'.
                    Fix: delete all parquets → re-fetch → rebuild everything.

  Bug 2 aftermath : proc_daily_hpc is missing Feb 1-7 rows.
                    Fix: covered by the full re-fetch above.

  Bug 3 aftermath : proc_period_snapshot has 2 rows per HPC (milk_type in grain).
                    Fix: delete all snapshots → rebuilt correctly at end.

Run order:
  python data_recovery.py --verify-only    # dry-run: shows what would be deleted
  python data_recovery.py --fix            # actually runs the recovery

PRE-REQUISITES:
  1. Deploy the fixed pipeline.py first.
  2. Confirm SQL Server is reachable.
  3. Take a backup of portal.db before running --fix.

Expected run time: same as a full backfill (~40-90 min depending on months of history).
"""

import argparse
import os
import sys
import sqlite3
from pathlib import Path
from datetime import date

# ── CONFIG — must match pipeline.py ─────────────────────────────────────────
PARQUET_DIR = Path(r"C:\AnalyticsPortal\parquet")
SQLITE_PATH = r"C:\AnalyticsPortal\portal.db"


def verify(verbose=True):
    """Report current state without making any changes."""
    print("\n=== VERIFICATION — current DB state ===\n")

    # 1. Parquet audit
    parquets = sorted(PARQUET_DIR.glob("proc_*.parquet"))
    print(f"Parquet files on disk: {len(parquets)}")
    if parquets:
        dates = []
        for p in parquets:
            try:
                dates.append(date.fromisoformat(p.stem.replace("proc_", "")))
            except ValueError:
                pass
        dates.sort()
        print(f"  Earliest : {dates[0]}")
        print(f"  Latest   : {dates[-1]}")

        # Check for Feb 1-7 gap (Bug 2 symptom)
        import calendar
        for yr in {d.year for d in dates}:
            for mth in {d.month for d in dates if d.year == yr}:
                month_days = calendar.monthrange(yr, mth)[1]
                expected = {date(yr, mth, d) for d in range(1, month_days + 1)}
                have = {d for d in dates if d.year == yr and d.month == mth}
                missing = sorted(expected - have)
                if missing:
                    print(f"\n  *** GAP DETECTED in {yr}-{mth:02d}: {len(missing)} missing days ***")
                    if verbose:
                        print(f"      Missing: {[str(d) for d in missing[:10]]}"
                              + (" ..." if len(missing) > 10 else ""))

    # 2. SQLite audit
    if not os.path.exists(SQLITE_PATH):
        print(f"\nSQLite DB not found at {SQLITE_PATH}")
        return

    conn = sqlite3.connect(SQLITE_PATH)

    # Bug 1: check milk_type values in proc_daily_hpc
    print("\n--- proc_daily_hpc milk_type distribution ---")
    try:
        rows = conn.execute(
            "SELECT milk_type, COUNT(*) AS cnt FROM proc_daily_hpc GROUP BY milk_type ORDER BY cnt DESC"
        ).fetchall()
        for milk_type, cnt in rows:
            flag = " *** BAD VALUE — should be Cow or Buffalo ***" if milk_type in ('1', '2') else ""
            print(f"  milk_type='{milk_type}' : {cnt:,} rows{flag}")
    except Exception as e:
        print(f"  Error: {e}")

    # Bug 2: check Feb row counts in proc_daily_hpc
    print("\n--- proc_daily_hpc rows per month (check Feb vs neighbours) ---")
    try:
        rows = conn.execute("""
            SELECT strftime('%Y-%m', DATE(proc_date)) AS ym,
                   COUNT(DISTINCT DATE(proc_date))     AS days_with_data,
                   COUNT(*)                            AS total_rows
            FROM proc_daily_hpc
            GROUP BY ym
            ORDER BY ym DESC
            LIMIT 6
        """).fetchall()
        for ym, days, total in rows:
            print(f"  {ym}: {days} days with data, {total:,} rows")
    except Exception as e:
        print(f"  Error: {e}")

    # Bug 3: check snapshot grain
    print("\n--- proc_period_snapshot rows per snapshot_date ---")
    try:
        rows = conn.execute("""
            SELECT snapshot_date,
                   COUNT(*)                          AS total_rows,
                   COUNT(DISTINCT hpc_plant_key)     AS distinct_hpcs
            FROM proc_period_snapshot
            GROUP BY snapshot_date
            ORDER BY snapshot_date DESC
            LIMIT 5
        """).fetchall()
        for snap_date, total, distinct_hpcs in rows:
            flag = " *** BUG 3: rows > HPCs → milk_type was in grain ***" \
                   if total > distinct_hpcs else " (OK)"
            print(f"  {snap_date}: {total} rows, {distinct_hpcs} distinct HPCs{flag}")
    except Exception as e:
        print(f"  Error: {e}")

    conn.close()
    print("\n=== VERIFICATION COMPLETE ===")
    print("If you see BAD VALUE or rows > HPCs above, run:  python data_recovery.py --fix")


def fix():
    """
    Full data recovery — run only after verifying what will be changed.

    Step 1: Delete all parquets (they carry wrong milk_type values).
    Step 2: Delete all rows from the 4 summary tables (they were built from bad parquets).
    Step 3: Delete all snapshots (Bug 3 stale data).
    Step 4: Import and run the full pipeline backfill from the earliest month.
    """
    print("\n=== DATA RECOVERY — starting ===")

    # ── STEP 1: Delete all parquets ──────────────────────────────────────────
    parquets = sorted(PARQUET_DIR.glob("proc_*.parquet"))
    print(f"\nStep 1: Deleting {len(parquets)} parquet files...")
    for p in parquets:
        p.unlink()
        if not p.exists():
            pass  # deleted OK
    remaining = len(list(PARQUET_DIR.glob("proc_*.parquet")))
    if remaining:
        print(f"  WARNING: {remaining} parquet files could not be deleted — check file locks.")
        sys.exit(1)
    print(f"  Done — all parquets deleted.")

    # ── STEP 2 & 3: Wipe corrupt summary + snapshot tables ───────────────────
    print("\nStep 2+3: Clearing corrupt summary and snapshot tables...")
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    tables = [
        "proc_daily_hpc",
        "proc_monthly_hpc",
        "proc_monthly_farmer",
        "proc_quality_incidents",
        "proc_period_snapshot",
        "proc_hpc_farmer_health",
        "proc_farmer_rfm",
    ]
    for tbl in tables:
        try:
            r = conn.execute(f"DELETE FROM {tbl}")
            print(f"  {tbl}: {r.rowcount:,} rows deleted")
        except Exception as e:
            print(f"  {tbl}: {e}")
    conn.commit()
    conn.close()
    print("  Done — all tables cleared.")

    # ── STEP 4: Full re-fetch + rebuild ──────────────────────────────────────
    print("\nStep 4: Running full backfill via fixed pipeline.py...")
    print("  This will fetch all missing parquets from SQL Server and rebuild everything.")
    print("  Expected time: 40–90 minutes.\n")

    # Import pipeline (it must be the fixed version in the same directory)
    try:
        import pipeline as pl
    except ImportError:
        # Try loading from known path
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "pipeline",
            Path(__file__).parent / "pipeline.py"
        )
        pl = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(pl)

    conn = pl.get_db()
    pl._ensure_tables(conn)
    pl._ensure_indexes(conn)
    conn.close()

    # Fetch ALL missing parquets from SQL Server
    fetched = pl.fetch_new_data()
    if not fetched:
        print("  No data fetched from SQL Server — check connection or date range.")
        sys.exit(1)

    print(f"\n  Fetched {len(fetched)} days of parquet data.")

    # Determine earliest month from fetched data
    earliest_ym = sorted(set((d.year, d.month) for d, _ in fetched))[0]
    print(f"  Earliest month with data: {earliest_ym[0]}-{earliest_ym[1]:02d}")

    # Build all summaries from earliest month
    pl.build_all_summaries(from_ym=earliest_ym)

    # Retention + purge
    pl.apply_retention()
    pl.purge_old_parquets()

    # Build snapshot (this fixes Bug 3 — 1 row per HPC, no milk_type in grain)
    pl.build_snapshot()

    print("\n=== DATA RECOVERY COMPLETE ===")
    print("Run --verify-only again to confirm all values look correct.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heritage Dairy — Data Recovery")
    parser.add_argument("--verify-only", action="store_true",
                        help="Show current state without making any changes")
    parser.add_argument("--fix", action="store_true",
                        help="Run full data recovery (DESTRUCTIVE — backup DB first)")
    args = parser.parse_args()

    if args.verify_only:
        verify()
    elif args.fix:
        print("WARNING: This will DELETE all parquets and clear all summary tables.")
        print("Make sure you have a backup of portal.db before continuing.")
        confirm = input("Type YES to proceed: ").strip()
        if confirm == "YES":
            fix()
        else:
            print("Aborted.")
    else:
        parser.print_help()