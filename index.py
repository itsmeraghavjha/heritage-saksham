"""
remove_indexes.py
"""
import sqlite3

conn = sqlite3.connect(r"C:\AnalyticsPortal\portal.db")
cursor = conn.cursor()

indexes = [
    'idx_monthly_farmer_yr_mth',
    'idx_monthly_farmer_farmer_code',
    'idx_monthly_farmer_hpc_key',
    'idx_monthly_hpc_yr_mth',
    'idx_monthly_hpc_hpc_key',
    'idx_monthly_farmer_composite'
]

for idx in indexes:
    try:
        cursor.execute(f"DROP INDEX IF EXISTS {idx}")
        print(f"✓ Dropped {idx}")
    except Exception as e:
        print(f"✗ {idx}: {e}")

conn.commit()
conn.close()
print("\n✓ Indexes removed. Restart Flask.")