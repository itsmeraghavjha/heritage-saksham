import sqlite3
conn = sqlite3.connect(r"C:\AnalyticsPortal\portal.db")
conn.row_factory = sqlite3.Row

snap = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()[0]

# Check what sample/9999 farmers exist in proc_hpc_farmer_health
# We can't check directly since health table only has hpc_plant_key, not farmer details
# So let's check what's driving the 839 difference

# Health table total churned
health_churned = conn.execute(f"""
    SELECT SUM(churned) as total_churned,
           SUM(total_farmers - acquired + churned) as total_prev
    FROM proc_hpc_farmer_health
    WHERE snapshot_date = '{snap}'
""").fetchone()

# Direct query churned (our verified correct number)
direct_churned = conn.execute("""
    SELECT COUNT(DISTINCT farmer_code) as n
    FROM proc_monthly_farmer lm
    WHERE yr = 2026 AND mth = 2
      AND delivery_days > 0
      AND farmer_code_seq != '9999'
      AND farmer_name NOT LIKE '%SAMPLE%'
      AND NOT EXISTS (
          SELECT 1 FROM proc_monthly_farmer cm
          WHERE cm.farmer_code = lm.farmer_code
            AND cm.yr = 2026 AND cm.mth = 3
            AND cm.delivery_days > 0
      )
""").fetchone()["n"]

print(f"Health table  — churned : {health_churned['total_churned']:,}")
print(f"Health table  — prev    : {health_churned['total_prev']:,}")
print(f"Direct query  — churned : {direct_churned:,}")
print(f"Difference              : {health_churned['total_churned'] - direct_churned:,}")
print(f"\nHealth table attrition  : {health_churned['total_churned']/health_churned['total_prev']*100:.1f}%")
print(f"Direct query attrition  : {direct_churned/health_churned['total_prev']*100:.1f}%")

conn.close()