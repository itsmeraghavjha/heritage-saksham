"""
Heritage Dairy — Procurement Portal
Flask backend with role-based login, server-side bootstrap cache, and scoped data delivery.

Roles:
  admin  → /admin only (user management + data fetch). Cannot see analytics.
  cxo    → full data, all zones/regions/plants
  zh     → locked to own zone
  rh     → locked to own region
  plant  → locked to own plant

Run:
  python app.py
"""

from flask import Flask, jsonify, request, session, redirect, send_file, render_template
import sqlite3, threading, uuid, json, time, logging, os
from functools import wraps
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta, date

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(r"C:\AnalyticsPortal\portal.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── APP ───────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key               = "heritage-portal-v2-change-this-in-production"
app.permanent_session_lifetime = timedelta(hours=12)

SQLITE_PATH = r"C:\AnalyticsPortal\portal.db"

# ── SAMPLE FARMER FILTER ──────────────────────────────────────────────────────
# Exclude test/sample entries from farmer-level analytics
SAMPLE_FILTER = "farmer_name NOT LIKE '%SAMPLE MILK%'"

# ── DB ────────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── SERVER-SIDE CACHE ─────────────────────────────────────────────────────────
# Built once at startup (background thread), rebuilt after every admin fetch.
# Bootstrap serves pre-built bytes from RAM — zero DB queries per login.

_cache      = {}
_cache_lock = threading.Lock()


METRIC_ORDER = [
    'farmerProductivity', 'hpcProductivity', 'farmerPerHPC',
    'fat', 'snf', 'costPerKg', 'attrition',
]


def  _build_cache():
    log.info("Building bootstrap cache...")
    t0 = time.time()
    try:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row

        def q(sql, params=[]):
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

        snap_date = conn.execute(
            "SELECT MAX(snapshot_date) FROM proc_period_snapshot"
        ).fetchone()[0]
        if not snap_date:
            conn.close()
            log.warning("Cache build skipped — no snapshot data yet")
            return

        yr_mth_hpc = (
            "yr=(SELECT MAX(yr) FROM proc_monthly_hpc) AND "
            "mth=(SELECT MAX(mth) FROM proc_monthly_hpc "
            "WHERE yr=(SELECT MAX(yr) FROM proc_monthly_hpc))"
        )
        yr_mth_fmr = ''  # unused — monthly_farmer removed from bootstrap

        # ── hpc_list ──────────────────────────────────────────────────────
        # ── hpc_list ──────────────────────────────────────────────────────
        # ── hpc_list (FIXED: Distinct Farmer Count) ───────────────────────
        # ── hpc_list (FIXED: Persistent Contacts) ───────────────────────
        # ── hpc_list (FIXED: Distinct Farmer Count) ───────────────────────
        hpc_list = q(f"""
            WITH true_farmers AS (
                -- COUNT(DISTINCT farmer_code) across all milk types per HPC — no milk_type split
                -- because proc_monthly_farmer has one row per farmer (MAX milk_type), and the
                -- snapshot now has one row per HPC. Splitting by milk_type would leave some
                -- farmers uncounted when their MAX milk_type doesn't match the snapshot row.
                SELECT hpc_plant_key, COUNT(DISTINCT farmer_code) AS distinct_farmers
                FROM proc_monthly_farmer
                WHERE yr = (SELECT MAX(yr) FROM proc_monthly_farmer)
                  AND mth = (SELECT MAX(mth) FROM proc_monthly_farmer WHERE yr = (SELECT MAX(yr) FROM proc_monthly_farmer))
                  AND farmer_code_seq != '9999'
                GROUP BY hpc_plant_key
            ),
            true_hpc_farmers AS (
                SELECT hpc_plant_key, COUNT(DISTINCT farmer_code) AS total_hpc_farmers
                FROM proc_monthly_farmer
                WHERE yr = (SELECT MAX(yr) FROM proc_monthly_farmer)
                  AND mth = (SELECT MAX(mth) FROM proc_monthly_farmer WHERE yr = (SELECT MAX(yr) FROM proc_monthly_farmer))
                  AND farmer_code_seq != '9999'
                GROUP BY hpc_plant_key
            )
            SELECT s.hpc_plant_key, s.hpc_name, s.plant_name, s.plant_code, s.region, s.zone,
                   COALESCE(hc.hpr_name, s.hpr_name) AS hpr_name, 
                   COALESCE(hc.mobile_no, s.mobile_no) AS mobile_no,
                   ROUND(s.mtd,1)         AS mtd,
                   ROUND(s.lm,1)          AS lm,
                   ROUND(s.lmtd,1)        AS lmtd,
                   ROUND(s.lymtd,1)       AS lymtd,
                   s.yoy_growth_pct,
                   s.mom_growth_pct,
                   COALESCE(tf.distinct_farmers, 0) AS mtd_farmers,
                   COALESCE(thf.total_hpc_farmers, 0) AS mtd_farmers_total,
                   ROUND(s.lm_farmers,0)  AS lm_farmers,
                   ROUND(s.mtd_avg_fat,3) AS mtd_avg_fat,
                   ROUND(s.mtd_rate,2)    AS mtd_rate,
                   ROUND(s.mtd_payout,0)  AS mtd_payout
            FROM proc_period_snapshot s
            LEFT JOIN true_farmers tf ON s.hpc_plant_key = tf.hpc_plant_key
            LEFT JOIN true_hpc_farmers thf ON s.hpc_plant_key = thf.hpc_plant_key
            LEFT JOIN hpr_contacts hc ON s.hpc_plant_key = hc.hpc_plant_key
            WHERE s.snapshot_date='{snap_date}'
            ORDER BY s.mtd DESC
        """)

        # ── monthly_hpc ───────────────────────────────────────────────────
        # ── monthly_hpc ───────────────────────────────────────────────────
        # ── monthly_hpc ───────────────────────────────────────────────────
        monthly_hpc = q(f"""
    SELECT mh.hpc_plant_key, mh.milk_type, 
           s.zone, s.region, s.plant_code,
           ROUND(mh.total_qty_ltr,0)      AS total_qty_ltr,
           ROUND(mh.avg_fat,3)             AS avg_fat,
           ROUND(mh.avg_snf,3)             AS avg_snf,
           mh.dumping_incidents,
           ROUND(mh.total_mcc_incentive,0) AS total_mcc_incentive,
           ROUND(mh.total_qty_incentive,0) AS total_qty_incentive,
           ROUND(mh.total_bonus,0)         AS total_bonus,
           ROUND(mh.total_net_price,0)     AS total_net_price,
           ROUND(mh.incentive_pct,2)       AS incentive_pct,
           ROUND(mh.avg_cost_per_fat_kg,2) AS avg_cost_per_fat_kg,
           ROUND(mh.total_fat_kg,3)        AS total_fat_kg,  
           ROUND(mh.avg_rate_per_ltr,2)    AS avg_rate_per_ltr,
           mh.morning_ltr, mh.evening_ltr
    FROM proc_monthly_hpc mh
    JOIN proc_period_snapshot s
      ON mh.hpc_plant_key = s.hpc_plant_key
     AND s.snapshot_date  = '{snap_date}'
     -- NOTE: no milk_type join — snapshot is 1-row-per-HPC; monthly_hpc has 2 rows
     -- (Cow + Buffalo) which correctly pass through for the frontend milk_type filter.
    WHERE {yr_mth_hpc}
""")





        # monthly_farmer removed from bootstrap (97k rows = too large).
        # farmer_rfm serves the same purpose on the frontend.
        monthly_farmer = []

        # ── daily — last 90 days, zone+region grain (compact) ────────────
        # Plant users see their region's trend (plant_code grain = 16k rows, too slow)
        daily = q("""
            SELECT DATE(proc_date)        AS proc_date,
                   zone, region,
                   ROUND(SUM(qty_ltr),0)  AS qty_ltr,
                   SUM(farmer_count)       AS farmers,
                   SUM(total_fat_kg)       AS fat_kg,
                   SUM(total_snf_kg)       AS snf_kg,
                   SUM(qty_kg)             AS qty_kg,
                   SUM(quality_incidents)  AS incidents,
                   SUM(total_net_price)    AS net_price
            FROM proc_daily_hpc
            WHERE DATE(proc_date) >= DATE('now', '-90 days')
            GROUP BY DATE(proc_date), zone, region
            ORDER BY proc_date
        """)

        # ── quality_incidents — current month per HPC ─────────────────────
        # ── quality_incidents — current month per HPC ─────────────────────
        # ── quality_incidents — current month per HPC ─────────────────────
        latest_ym = conn.execute(
            "SELECT strftime('%Y-%m', MAX(DATE(proc_date))) FROM proc_quality_incidents"
        ).fetchone()[0]
        quality_incidents = q(f"""
            SELECT qi.hpc_plant_key, qi.milk_type,
                   s.hpc_name, s.region, s.zone, s.plant_code,
                   COUNT(*)             AS incidents,
                   qi.incident_type,
                   ROUND(AVG(qi.fat),3) AS avg_fat,
                   ROUND(AVG(qi.snf),3) AS avg_snf
            FROM proc_quality_incidents qi
            JOIN proc_period_snapshot s
              ON qi.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date  = '{snap_date}'
             -- No milk_type join: snapshot is 1-row-per-HPC; qi.milk_type preserved in SELECT
             -- for frontend milk_type filtering without causing row duplication.
            WHERE strftime('%Y-%m', DATE(qi.proc_date)) = '{latest_ym}'
            GROUP BY qi.hpc_plant_key, qi.milk_type
            ORDER BY incidents DESC
        """) if latest_ym else []

        # ── farmer_health — per HPC ───────────────────────────────────────
        # ── farmer_health — per HPC ───────────────────────────────────────
        # ── farmer_health — per HPC ───────────────────────────────────────
        has_fh = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='proc_hpc_farmer_health'"
        ).fetchone()[0]
        farmer_health = q(f"""
            SELECT fh.hpc_plant_key, fh.total_farmers, fh.consistent,
                   fh.irregular, fh.consistency_pct, fh.churned, fh.acquired,
                   fh.net_change, fh.attrition_pct, fh.segment,
                   s.hpc_name, s.region, s.zone, s.plant_code
            FROM proc_hpc_farmer_health fh
            JOIN (
                SELECT hpc_plant_key, MAX(hpc_name) as hpc_name, 
                       MAX(region) as region, MAX(zone) as zone, MAX(plant_code) as plant_code
                FROM proc_period_snapshot 
                WHERE snapshot_date = '{snap_date}'
                GROUP BY hpc_plant_key
            ) s                                       /* <-- THIS SUBQUERY PREVENTS DUPLICATION */
              ON fh.hpc_plant_key = s.hpc_plant_key
            WHERE fh.snapshot_date = '{snap_date}'
        """) if has_fh else [] 

        # ── farmer_rfm ────────────────────────────────────────────────────
        # ── farmer_rfm ────────────────────────────────────────────────────
        # ── farmer_rfm ────────────────────────────────────────────────────
        # ── farmer_rfm ────────────────────────────────────────────────────
        has_rfm = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='proc_farmer_rfm'"
        ).fetchone()[0]
        
        farmer_rfm = q(f"""
            SELECT r.farmer_code,
                   SUBSTR(r.farmer_name,1,40)     AS farmer_name,
                   r.hpc_plant_key, r.region, r.zone,
                   CAST(s.plant_code AS TEXT)      AS plant_code,
                   r.tier,
                   r.milk_type,
                   ROUND(r.total_qty_ltr,0)        AS total_qty_ltr,
                   ROUND(r.total_payout,0)         AS total_payout,
                   r.delivery_days,
                   ROUND(r.avg_fat,2)              AS avg_fat,
                   ROUND(r.lpd,1)                  AS lpd,
                   CASE WHEN r.avg_fat < 3.5 THEN 1 ELSE 0 END AS quality_risk
            FROM proc_farmer_rfm r
            JOIN proc_period_snapshot s
              ON r.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date = '{snap_date}'
             -- No milk_type join: snapshot is 1-row-per-HPC; r.milk_type in SELECT
             -- lets the frontend filter by milk type without duplicating farmer rows.
            WHERE r.snapshot_date = '{snap_date}'
              AND r.farmer_name NOT LIKE '%SAMPLE MILK%'
        """) if has_rfm else []

        # ── snapshot KPI row ──────────────────────────────────────────────
        snap_row = conn.execute(f"""
            SELECT
                SUM(mtd)           AS mtd,
                SUM(lm)            AS lm,
                SUM(lmtd)          AS lmtd,
                SUM(lymtd)         AS lymtd,
                SUM(mtd_farmers)   AS total_farmers,
                SUM(lm_farmers)    AS lm_farmers,
                SUM(mtd_payout)    AS total_payout,
                COUNT(DISTINCT hpc_plant_key) AS total_hpcs,   -- snapshot is now 1-row-per-HPC; DISTINCT guards against any future milk_type rows
                SUM(CASE WHEN mtd=0 THEN 1 ELSE 0 END) AS dark_hpcs,
                SUM(CASE WHEN yoy_growth_pct < -0.2 AND mtd>0 THEN 1 ELSE 0 END) AS critical_hpcs,
                SUM(CASE WHEN yoy_growth_pct>=-0.2 AND yoy_growth_pct<0 AND mtd>0
                    THEN 1 ELSE 0 END) AS declining_hpcs,
                SUM(CASE WHEN yoy_growth_pct >= 0.3 THEN 1 ELSE 0 END) AS star_hpcs,
                AVG(CASE WHEN mtd_avg_fat>0 THEN mtd_avg_fat END) AS avg_fat,
                AVG(CASE WHEN mtd_rate>0    THEN mtd_rate    END) AS avg_rate
            FROM proc_period_snapshot
            WHERE snapshot_date='{snap_date}'
        """).fetchone()

        mtd   = snap_row["mtd"]   or 0
        lymtd = snap_row["lymtd"] or 0
        lmtd  = snap_row["lmtd"]  or 0

        # ── zone & region summaries — must come before snapshot dict ──────
        snap_d     = date.fromisoformat(snap_date)
        day_num    = snap_d.day
        ly_yr      = snap_d.year - 1
        import calendar as _cal
        ly_days    = min(day_num, _cal.monthrange(ly_yr, snap_d.month)[1])
        curr_start = snap_d.replace(day=1).isoformat()
        ly_start   = date(ly_yr, snap_d.month, 1).isoformat()
        ly_end     = date(ly_yr, snap_d.month, ly_days).isoformat()

        zone_summary = q(f"""
            SELECT zone,
                ROUND(SUM(CASE WHEN DATE(proc_date) BETWEEN '{curr_start}' AND '{snap_date}'
                          THEN qty_ltr ELSE 0 END) / {day_num}.0, 0) AS mtd,
                ROUND(SUM(CASE WHEN DATE(proc_date) BETWEEN '{ly_start}' AND '{ly_end}'
                          THEN qty_ltr ELSE 0 END) / {ly_days}.0, 0) AS lymtd
            FROM proc_daily_hpc WHERE zone IS NOT NULL
            GROUP BY zone ORDER BY mtd DESC
        """)

        region_summary = q(f"""
            SELECT region, zone,
                ROUND(SUM(CASE WHEN DATE(proc_date) BETWEEN '{curr_start}' AND '{snap_date}'
                          THEN qty_ltr ELSE 0 END) / {day_num}.0, 0) AS mtd,
                ROUND(SUM(CASE WHEN DATE(proc_date) BETWEEN '{ly_start}' AND '{ly_end}'
                          THEN qty_ltr ELSE 0 END) / {ly_days}.0, 0) AS lymtd
            FROM proc_daily_hpc WHERE region IS NOT NULL
            GROUP BY region, zone ORDER BY mtd DESC
        """)

        snapshot = {
            "snapshot_date":  snap_date,
            "mtd":            round(mtd,   0),
            "lm":             round(snap_row["lm"] or 0, 0),
            "lmtd":           round(lmtd,  0),
            "lymtd":          round(lymtd, 0),
            "yoy_pct": round((sum(z['mtd'] for z in zone_summary) - sum(z['lymtd'] for z in zone_summary)) / sum(z['lymtd'] for z in zone_summary) * 100, 1) if sum(z['lymtd'] for z in zone_summary) else 0,
            "mom_pct":        round((mtd - lmtd)  / lmtd  * 100, 1) if lmtd  else 0,
            "total_farmers":  int(conn.execute(
                "SELECT COUNT(DISTINCT farmer_code) FROM proc_monthly_farmer "
                "WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer) "
                "AND mth=(SELECT MAX(mth) FROM proc_monthly_farmer WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer)) "
                "AND farmer_code_seq != '9999'"
            ).fetchone()[0] or 0),
            "lm_farmers":     int(conn.execute("""
                SELECT COUNT(DISTINCT farmer_code) FROM proc_monthly_farmer
                WHERE (yr * 100 + mth) = (
                    SELECT MAX(yr * 100 + mth) FROM proc_monthly_farmer
                    WHERE (yr * 100 + mth) < (
                        SELECT MAX(yr * 100 + mth) FROM proc_monthly_farmer
                    )
                )
                AND farmer_code_seq != '9999'
            """).fetchone()[0] or 0),
            "total_payout":   round(snap_row["total_payout"] or 0, 0),
            "total_hpcs":     snap_row["total_hpcs"],
            "dark_hpcs":      snap_row["dark_hpcs"],
            "critical_hpcs":  snap_row["critical_hpcs"],
            "declining_hpcs": snap_row["declining_hpcs"],
            "star_hpcs":      snap_row["star_hpcs"],
            "avg_fat":        round(snap_row["avg_fat"]  or 0, 3),
            "avg_rate":       round(snap_row["avg_rate"] or 0, 2),
        }

        # ── filters (all values for CXO / admin) ──────────────────────────
        filters = {
            "zones":   [r[0] for r in conn.execute(
                f"SELECT DISTINCT zone FROM proc_period_snapshot "
                f"WHERE snapshot_date='{snap_date}' AND zone IS NOT NULL ORDER BY zone"
            ).fetchall()],
            "regions": [r[0] for r in conn.execute(
                f"SELECT DISTINCT region FROM proc_period_snapshot "
                f"WHERE snapshot_date='{snap_date}' AND region IS NOT NULL ORDER BY region"
            ).fetchall()],
            "plants":  [{"code": r[0], "name": r[1]} for r in conn.execute(
                f"SELECT DISTINCT plant_code, plant_name FROM proc_period_snapshot "
                f"WHERE snapshot_date='{snap_date}' AND plant_code IS NOT NULL ORDER BY plant_name"
            ).fetchall()],
        }

        # ── insights ──────────────────────────────────────────────────────
        insights = []
        w_snap = f"WHERE snapshot_date='{snap_date}'"

        dark_row = conn.execute(f"""
            SELECT COUNT(*) AS cnt, GROUP_CONCAT(hpc_name, ', ') AS names
            FROM (SELECT hpc_name FROM proc_period_snapshot {w_snap} AND mtd=0 AND lm>0
                  ORDER BY lm DESC LIMIT 5)
        """).fetchone()
        if dark_row["cnt"] > 0:
            insights.append({
                "priority": "critical", "category": "Volume Risk",
                "title":    f"{dark_row['cnt']} HPC(s) went dark this month",
                "detail":   f"Had procurement last month but zero MTD. Top: {dark_row['names']}",
                "action":   "Immediate field visit — check farmer mobilisation and route issues.",
                "zone": "", "region": "",
            })

        severe = conn.execute(f"""
            SELECT COUNT(*) AS cnt FROM proc_period_snapshot
            {w_snap} AND yoy_growth_pct < -0.3 AND mtd>0
        """).fetchone()
        if severe["cnt"] > 0:
            top = conn.execute(f"""
                SELECT hpc_name, region, zone, ROUND(yoy_growth_pct*100,1) AS yoy
                FROM proc_period_snapshot {w_snap} AND yoy_growth_pct < -0.3 AND mtd>0
                ORDER BY yoy_growth_pct LIMIT 3
            """).fetchall()
            insights.append({
                "priority": "critical", "category": "YoY Decline",
                "title":    f"{severe['cnt']} HPC(s) with >30% YoY decline",
                "detail":   " | ".join([f"{r['hpc_name']} ({r['region']}): {r['yoy']}%" for r in top]),
                "action":   "Investigate rate competitiveness and farmer satisfaction.",
                "zone": "", "region": "",
            })

        bad_regions = conn.execute(f"""
            SELECT region, zone, ROUND(AVG(yoy_growth_pct)*100,1) AS avg_yoy,
                   COUNT(*) AS hpcs,
                   SUM(CASE WHEN yoy_growth_pct<0 THEN 1 ELSE 0 END) AS declining
            FROM proc_period_snapshot {w_snap} AND yoy_growth_pct IS NOT NULL AND lymtd>0
            GROUP BY region HAVING AVG(yoy_growth_pct) < -0.05 ORDER BY avg_yoy LIMIT 3
        """).fetchall()
        for r in bad_regions:
            insights.append({
                "priority": "warning", "category": "Region Alert",
                "title":    f"{r['region']}: {r['avg_yoy']}% avg YoY",
                "detail":   f"{r['declining']} of {r['hpcs']} HPCs declining.",
                "action":   f"Review competitor rates in {r['region']}.",
                "zone": r["zone"] or "", "region": r["region"] or "",
            })

        # Low attendance insight — use monthly_farmer if rfm not ready
        # Low attendance insight — use monthly_farmer if rfm not ready
        if has_rfm:
            _low_att_row = conn.execute(f"""
                SELECT COUNT(*) AS cnt FROM proc_farmer_rfm
                WHERE snapshot_date='{snap_date}' 
                  AND delivery_days < 10
            """).fetchone()
        else:
            yr_c  = "yr=(SELECT MAX(yr) FROM proc_monthly_farmer)"
            mth_c = "mth=(SELECT MAX(mth) FROM proc_monthly_farmer WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))"
            _low_att_row = conn.execute(f"""
                SELECT COUNT(DISTINCT farmer_code) AS cnt
                FROM proc_monthly_farmer 
                WHERE {yr_c} AND {mth_c} 
                  AND delivery_days < 10
                  AND farmer_code_seq != '9999'
            """).fetchone()
        if _low_att_row and _low_att_row["cnt"] > 100:
            insights.append({
                "priority": "warning", "category": "Farmer Risk",
                "title":    f"{_low_att_row['cnt']:,} farmers with <10 delivery days",
                "detail":   "High absenteeism signals churn — may be selling to competitors.",
                "action":   "Flag to field officers. Check if payment delays are an issue.",
                "zone": "", "region": "",
            })

        low_fat = conn.execute(f"""
            SELECT s.region, s.zone, ROUND(AVG(mh.avg_fat),3) AS fat
            FROM proc_monthly_hpc mh
            JOIN proc_period_snapshot s
              ON mh.hpc_plant_key=s.hpc_plant_key AND s.snapshot_date='{snap_date}'
            WHERE {yr_mth_hpc}
            GROUP BY s.region HAVING AVG(mh.avg_fat) < 3.8 ORDER BY fat LIMIT 3
        """).fetchall()
        for r in low_fat:
            insights.append({
                "priority": "warning", "category": "Quality Alert",
                "title":    f"{r['region']}: Avg FAT {r['fat']}% — below 3.8%",
                "detail":   "Low FAT reduces milk value. May indicate adulteration or breed issues.",
                "action":   "Increase testing frequency. Consider breed improvement incentives.",
                "zone": r["zone"] or "", "region": r["region"] or "",
            })

        star_regions = conn.execute(f"""
            SELECT region, zone, ROUND(AVG(yoy_growth_pct)*100,1) AS avg_yoy, COUNT(*) AS hpcs
            FROM proc_period_snapshot {w_snap} AND yoy_growth_pct IS NOT NULL AND lymtd>0
            GROUP BY region HAVING AVG(yoy_growth_pct) > 0.25 ORDER BY avg_yoy DESC LIMIT 2
        """).fetchall()
        for r in star_regions:
            insights.append({
                "priority": "positive", "category": "Growth Opportunity",
                "title":    f"{r['region']}: +{r['avg_yoy']}% YoY — top performing",
                "detail":   f"{r['hpcs']} HPCs growing strongly.",
                "action":   "Document best practices and replicate to lagging regions.",
                "zone": r["zone"] or "", "region": r["region"] or "",
            })

        

        conn.close()

        payload = {
            "snapshot":          snapshot,
            "hpc_list":          hpc_list,
            "monthly_hpc":       monthly_hpc,
            "monthly_farmer":    monthly_farmer,
            "daily":             daily,
            "quality_incidents": quality_incidents,
            "farmer_health":     farmer_health,
            "farmer_rfm":        farmer_rfm,
            "insights":          insights,
            "filters":           filters,
            "zone_summary":      zone_summary,
            "region_summary":    region_summary,
        }

        payload_bytes = json.dumps(payload).encode("utf-8")

        # ── Pre-build zone-level scoped caches ───────────────────────────
        scoped = {}
        for z in filters["zones"]:
            try:
                z_scope = {"zone": z, "region": "", "plant_code": ""}
                z_hpcs  = _filter_by_scope(hpc_list,        z_scope)
                z_mhpc  = _filter_by_scope(monthly_hpc,      z_scope)
                z_daily = _filter_by_scope(daily,             z_scope, skip_plant=True)
                z_qi    = _filter_by_scope(quality_incidents, z_scope)
                z_fh    = _filter_by_scope(farmer_health,     z_scope)
                z_rfm   = _filter_by_scope(farmer_rfm,        z_scope)
                z_snap  = _recompute_snapshot(z_hpcs, z_mhpc, snapshot)
                z_flt   = _build_scoped_filters(z_hpcs)
                z_ins   = [i for i in insights if not i.get("zone") or i["zone"]==z]
                z_zone_summary   = [r for r in zone_summary   if r['zone'] == z]
                z_region_summary = [r for r in region_summary if r['zone'] == z]
                z_zone_summary   = [r for r in zone_summary   if r['zone'] == z]
                z_region_summary = [r for r in region_summary if r['zone'] == z]
                scoped[z] = json.dumps({
                    "snapshot": z_snap, "hpc_list": z_hpcs, "monthly_hpc": z_mhpc,
                    "monthly_farmer": [], "daily": z_daily,
                    "quality_incidents": z_qi, "farmer_health": z_fh,
                    "farmer_rfm": z_rfm, "insights": z_ins, "filters": z_flt,
                    "zone_summary":   z_zone_summary,
                    "region_summary": z_region_summary,
                }).encode("utf-8")
            except Exception as ez:
                log.warning(f"Scoped cache failed for zone {z}: {ez}")

        with _cache_lock:
            _cache["payload"]   = payload_bytes
            _cache["scoped"]    = scoped
            _cache["built_at"]  = time.time()
            _cache["snap_date"] = snap_date

        elapsed = time.time() - t0
        log.info(f"Cache built in {elapsed:.1f}s — {len(payload_bytes)//1024} KB, "
                 f"snap={snap_date}, zone caches: {list(scoped.keys())}")

    except Exception as e:
        log.error(f"Cache build failed: {e}", exc_info=True)


def _build_cache_bg():
    threading.Thread(target=_build_cache, daemon=True).start()



# ── TARGETS TABLE & HELPERS ──────────────────────────────────────────────────

def _ensure_targets_table(conn):
    """
    Create scoped_targets table supporting zone/region/plant/milk-type overrides.
    Auto-migrates existing company_targets rows on first run.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scoped_targets (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_key   TEXT    NOT NULL,
            scope_type   TEXT    NOT NULL DEFAULT 'company',
            scope_value  TEXT    NOT NULL DEFAULT '',
            milk_type    TEXT    NOT NULL DEFAULT 'all',
            target_value REAL    NOT NULL,
            unit         TEXT    NOT NULL,
            description  TEXT    DEFAULT '',
            updated_at   TEXT    DEFAULT (datetime('now')),
            updated_by   INTEGER,
            UNIQUE(metric_key, scope_type, scope_value, milk_type),
            FOREIGN KEY(updated_by) REFERENCES users(id)
        )
    """)
 
    count = conn.execute("""
        SELECT COUNT(*) FROM scoped_targets
        WHERE scope_type='company' AND scope_value='' AND milk_type='all'
    """).fetchone()[0]
 
    if count == 0:
        migrated = False
        # Try to migrate from old company_targets table
        try:
            old = conn.execute(
                "SELECT metric_key, target_value, unit, description FROM company_targets"
            ).fetchall()
            if old:
                conn.executemany("""
                    INSERT OR IGNORE INTO scoped_targets
                        (metric_key, scope_type, scope_value, milk_type, target_value, unit, description)
                    VALUES (?, 'company', '', 'all', ?, ?, ?)
                """, [(r[0], r[1], r[2], r[3] or '') for r in old])
                migrated = True
                log.info("Migrated company_targets → scoped_targets")
        except Exception:
            pass
 
        if not migrated:
            defaults = [
                ('farmerProductivity', 12.0,  'L/Farmer', 'Average litres per farmer per day'),
                ('hpcProductivity',    250.0,  'L/HPC',    'Average litres per HPC per day'),
                ('farmerPerHPC',       25.0,   'Count',    'Average number of farmers per HPC'),
                ('fat',                3.5,    '%',        'Minimum average FAT percentage'),
                ('snf',                8.0,    '%',        'Minimum average SNF percentage'),
                ('costPerKg',          900.0,  '₹/Kg',    'Maximum cost per fat kilogram'),
                ('attrition',          8.0,    '%',        'Maximum acceptable farmer attrition rate'),
            ]
            conn.executemany("""
                INSERT OR IGNORE INTO scoped_targets
                    (metric_key, scope_type, scope_value, milk_type, target_value, unit, description)
                VALUES (?, 'company', '', 'all', ?, ?, ?)
            """, defaults)
 
        conn.commit()
        log.info("Initialized scoped_targets with company defaults")
 


# ── AUTH HELPERS ──────────────────────────────────────────────────────────────

def current_user():
    if "user_id" not in session:
        return None
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE id=? AND is_active=1", (session["user_id"],)
    ).fetchone()
    conn.close()
    return user

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user():
            if request.path.startswith("/api/"):
                return jsonify({"error": "Unauthorized"}), 401
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        if user["role"] != "admin":
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated

def analytics_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        if user["role"] == "admin":
            return jsonify({"error": "Admin accounts cannot access analytics"}), 403
        return f(*args, **kwargs)
    return decorated

# ── SCOPE HELPERS ─────────────────────────────────────────────────────────────

def get_data_scope(user):
    role = user["role"]
    if role in ("admin", "cxo"):
        return {"zone": "", "region": "", "plant_code": ""}
    if role == "zh":
        return {"zone": user["scope_zone"], "region": "", "plant_code": ""}
    if role == "rh":
        return {"zone": user["scope_zone"], "region": user["scope_region"], "plant_code": ""}
    if role == "plant":
        return {"zone": user["scope_zone"], "region": user["scope_region"], "plant_code": str(user["scope_plant"])}
    return {"zone": "", "region": "", "plant_code": ""}


def gf():
    user  = current_user()
    scope = get_data_scope(user)
    f     = dict(scope)
    for key in ("zone", "region", "plant_code"):
        if not f[key]:
            val = request.args.get(key, "")
            if val:
                f[key] = val
    return f


def build_where(f, table_alias="", extra=None):
    clauses, params = [], []
    p = f"{table_alias}." if table_alias else ""
    if f.get("zone"):       clauses.append(f"{p}zone=?");       params.append(f["zone"])
    if f.get("region"):     clauses.append(f"{p}region=?");     params.append(f["region"])
    if f.get("plant_code"): clauses.append(f"{p}plant_code=?"); params.append(f["plant_code"])
    if extra:               clauses.append(extra)
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def _filter_by_scope(arr, scope, skip_plant=False):
    """Filter a list of dicts using zone/region/plant_code scope."""
    zone   = scope.get("zone", "")
    region = scope.get("region", "")
    plant  = scope.get("plant_code", "")
    if not zone and not region and not plant:
        return arr
    out = []
    for item in arr:
        if zone   and item.get("zone",   "") != zone:   continue
        if region and item.get("region", "") != region: continue
        if not skip_plant and plant:
            # Primary: match plant_code column if present
            item_plant = str(item.get("plant_code") or "")
            if item_plant and item_plant != str(plant):
                continue
            # Fallback: match via hpc_plant_key prefix (handles farmer_rfm
            # rows where plant_code column may be null)
            elif not item_plant:
                hpk = str(item.get("hpc_plant_key") or "")
                if hpk and not (hpk.startswith(str(plant)+"-") or
                                hpk.startswith(str(plant)+"_") or
                                hpk == str(plant)):
                    continue
        out.append(item)
    return out


def _recompute_snapshot(hpcs, mhpc, base_snap):
    """Recompute top-level KPIs for a scoped hpc_list."""
    if not hpcs:
        return base_snap
    mtd   = sum(h.get("mtd",   0) or 0 for h in hpcs)
    lymtd = sum(h.get("lymtd", 0) or 0 for h in hpcs)
    lmtd  = sum(h.get("lmtd",  0) or 0 for h in hpcs)
    lm    = sum(h.get("lm",    0) or 0 for h in hpcs)
    pay   = sum(m.get("total_net_price", 0) or 0 for m in mhpc)
    return {
        **base_snap,
        "mtd":            round(mtd,   0),
        "lm":             round(lm,    0),
        "lmtd":           round(lmtd,  0),
        "lymtd":          round(lymtd, 0),
        "yoy_pct": round((mtd - lymtd) / lymtd * 100, 1) if lymtd else 0,
        "mom_pct":        round((mtd - lmtd)  / lmtd  * 100, 1) if lmtd  else 0,
        "total_farmers":  int(sum(h.get("mtd_farmers", 0) or 0 for h in hpcs)),
        "lm_farmers":     int(sum(h.get("lm_farmers",  0) or 0 for h in hpcs)),
        "total_payout":   round(pay, 0),
        "total_hpcs":     len({h.get("hpc_plant_key") for h in hpcs}),   # distinct HPCs, not list rows
        "dark_hpcs":      len({h.get("hpc_plant_key") for h in hpcs if not h.get("mtd")}),
        "star_hpcs":      len([h for h in hpcs if (h.get("yoy_growth_pct") or 0) >= 0.3 and h.get("mtd")]),
        "critical_hpcs":  len([h for h in hpcs if (h.get("yoy_growth_pct") or 0) < -0.2 and h.get("mtd")]),
        "declining_hpcs": len([h for h in hpcs if -0.2 <= (h.get("yoy_growth_pct") or 0) < 0 and h.get("mtd")]),
    }


def _build_scoped_filters(hpcs):
    zones   = sorted({h["zone"]   for h in hpcs if h.get("zone")})
    regions = sorted({h["region"] for h in hpcs if h.get("region")})
    seen, plants = set(), []
    for h in hpcs:
        pc = str(h.get("plant_code", ""))
        if pc and pc not in seen:
            seen.add(pc)
            plants.append({"code": pc, "name": h.get("plant_name", pc)})
    return {"zones": zones, "regions": regions, "plants": plants}

# ── AUTH ROUTES ───────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET"])
def login_page():
    if current_user():
        u = current_user()
        return redirect("/admin" if u["role"] == "admin" else "/")
    return render_template("login.html")


@app.route("/login", methods=["POST"])
def do_login():
    d        = request.get_json(silent=True) or {}
    username = (d.get("username") or "").strip()
    password = d.get("password") or ""

    if not username or not password:
        return jsonify({"success": False, "error": "Username and password required"}), 400

    conn = get_db()
    # Case-insensitive match for email usernames; exact match for plant codes
    user = conn.execute(
        "SELECT * FROM users WHERE (LOWER(username)=LOWER(?) OR username=?) AND is_active=1",
        (username, username)
    ).fetchone()

    if not user or not check_password_hash(user["password_hash"], password):
        conn.close()
        log.warning(f"Failed login: {username}")
        return jsonify({"success": False, "error": "Invalid username or password"}), 401

    conn.execute("UPDATE users SET last_login=datetime('now') WHERE id=?", (user["id"],))
    conn.commit()
    conn.close()

    session.clear()
    session.permanent  = True
    session["user_id"] = user["id"]
    log.info(f"Login: {user['username']} ({user['role']})")

    return jsonify({
        "success":   True,
        "full_name": user["full_name"],
        "role":      user["role"],
        "redirect":  "/admin" if user["role"] == "admin" else "/",
    })


# Keep old endpoint for backwards compat
@app.route("/api/auth/login", methods=["POST"])
def do_login_compat():
    return do_login()


@app.route("/logout")
@app.route("/api/auth/logout", methods=["GET", "POST"])
def do_logout():
    session.clear()
    return redirect("/login")


@app.route("/api/auth/me")
@login_required
def auth_me():
    user = current_user()
    return jsonify({
        "id":           user["id"],
        "username":     user["username"],
        "full_name":    user["full_name"],
        "role":         user["role"],
        "scope_zone":   user["scope_zone"],
        "scope_region": user["scope_region"],
        "scope_plant":  user["scope_plant"],
    })

# ── MAIN PAGES ────────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    user = current_user()
    if user["role"] == "admin":
        return redirect("/admin")
    return render_template("index.html")


@app.route("/admin")
@login_required
def admin_page():
    if current_user()["role"] != "admin":
        return redirect("/")
    return render_template("admin.html")

# ── BOOTSTRAP — scope-filtered ────────────────────────────────────────────────

@app.route("/api/bootstrap")
@login_required
@analytics_required
def bootstrap():
    with _cache_lock:
        payload = _cache.get("payload")
    if not payload:
        _build_cache()
        with _cache_lock:
            payload = _cache.get("payload")
    if not payload:
        return jsonify({"error": "Data not ready yet, please try again in a few seconds"}), 503

    user  = current_user()
    role  = user["role"]
    scope = get_data_scope(user)

    # CXO gets the full pre-built payload directly — fastest path
    if role == "cxo":
        data = json.loads(payload)
        data["user"] = {
            "role": role, "full_name": user["full_name"],
            "scope_zone": "", "scope_region": "", "scope_plant": "",
        }
        return jsonify(data)

    # Scoped users — use pre-built zone cache if available, then narrow further
    my_zone   = scope.get("zone", "")
    my_region = scope.get("region", "")
    my_plant  = scope.get("plant_code", "")

    with _cache_lock:
        scoped_caches = _cache.get("scoped", {})
        zone_bytes = scoped_caches.get(my_zone) if my_zone else None

    # Use zone cache as base (much smaller than full payload)
    base_bytes = zone_bytes if zone_bytes else payload
    data = json.loads(base_bytes)

    # ZH: zone cache is already correct, just inject user
    if role == "zh" and zone_bytes:
        data["user"] = {"role": role, "full_name": user["full_name"],
                        "scope_zone": my_zone, "scope_region": "", "scope_plant": ""}
        return jsonify(data)
    
    # Fallback: if no zone cache yet (first boot), parse full payload
    if not zone_bytes:
        data = json.loads(payload)

    # RH / Plant: narrow further from zone cache
    hpc_list          = _filter_by_scope(data["hpc_list"],           scope)
    monthly_hpc       = _filter_by_scope(data["monthly_hpc"],        scope)
    daily             = _filter_by_scope(data["daily"],              scope, skip_plant=True)
    quality_incidents = _filter_by_scope(data["quality_incidents"],  scope)
    farmer_health     = _filter_by_scope(data.get("farmer_health",[]),scope)
    farmer_rfm        = _filter_by_scope(data.get("farmer_rfm",[]),   scope)
    insights          = [i for i in data["insights"]
                         if not i.get("region") or i["region"]==my_region]

    filtered = {
        "snapshot":          _recompute_snapshot(hpc_list, monthly_hpc, data["snapshot"]),
        "hpc_list":          hpc_list,
        "monthly_hpc":       monthly_hpc,
        "monthly_farmer":    [],
        "daily":             daily,
        "quality_incidents": quality_incidents,
        "farmer_health":     farmer_health,
        "farmer_rfm":        farmer_rfm,
        "insights":          insights,
        "filters":           _build_scoped_filters(hpc_list),
        "zone_summary":      [r for r in data.get("zone_summary",   []) if not my_zone   or r["zone"]   == my_zone],
        "region_summary":    [r for r in data.get("region_summary", []) if not my_region or r["region"] == my_region],
        "user": {
            "role":         role,
            "full_name":    user["full_name"],
            "scope_zone":   my_zone,
            "scope_region": my_region,
            "scope_plant":  my_plant,
        },
    }
    return jsonify(filtered)

# ── HPC DETAIL PANEL ──────────────────────────────────────────────────────────

@app.route("/api/hpc/detail")
@login_required
@analytics_required
def hpc_detail():
    key  = request.args.get("hpc_plant_key", "")
    if not key:
        return jsonify({"error": "hpc_plant_key required"}), 400

    # Scope check — scoped users can only drill into their own centers
    user  = current_user()
    scope = get_data_scope(user)
    conn  = get_db()

    # Verify this HPC belongs to the user's scope (skip for CXO)
    if user["role"] not in ("cxo",):
        row = conn.execute(
            "SELECT zone, region, plant_code FROM proc_daily_hpc WHERE hpc_plant_key=? LIMIT 1",
            (key,)
        ).fetchone()
        if row:
            if scope["zone"]       and row["zone"]       != scope["zone"]:
                conn.close(); return jsonify({"error": "Access denied"}), 403
            if scope["region"]     and row["region"]     != scope["region"]:
                conn.close(); return jsonify({"error": "Access denied"}), 403
            if scope["plant_code"] and str(row["plant_code"]) != str(scope["plant_code"]):
                conn.close(); return jsonify({"error": "Access denied"}), 403

    daily = conn.execute("""
        SELECT DATE(proc_date) AS proc_date,
               SUM(qty_ltr)   AS qty_ltr,
               ROUND(SUM(total_fat_kg)/NULLIF(SUM(qty_kg),0)*100, 3) AS avg_fat,
               SUM(farmer_count) AS farmers
        FROM proc_daily_hpc
        WHERE hpc_plant_key=?
          AND DATE(proc_date) >= DATE('now', '-60 days')
        GROUP BY DATE(proc_date)
        ORDER BY proc_date
    """, (key,)).fetchall()

    # 1. Fetch the latest available year and month ONCE
    latest_ym = conn.execute("""
        SELECT yr, mth FROM proc_monthly_farmer 
        ORDER BY yr DESC, mth DESC LIMIT 1
    """).fetchone()
    
    max_yr = latest_ym['yr'] if latest_ym else 0
    max_mth = latest_ym['mth'] if latest_ym else 0

    # 2. Use those strict parameters in the query to utilize the new index
    top = conn.execute("""
        SELECT farmer_name, farmer_code, delivery_days,
               ROUND(lpd,1) AS lpd, ROUND(avg_fat,3) AS avg_fat,
               ROUND(total_net_price,0) AS payout
        FROM proc_monthly_farmer
        WHERE hpc_plant_key=? AND yr=? AND mth=?
          AND farmer_code_seq != '9999'
        ORDER BY total_net_price DESC LIMIT 10
    """, (key, max_yr, max_mth)).fetchall()

    conn.close()
    return jsonify({
        "daily":       [dict(r) for r in daily],
        "top_farmers": [dict(r) for r in top],
    })



@app.route("/api/trend/plant")
@login_required
@analytics_required
def plant_trend():
    # 1. Capture all possible filters from the frontend
    plant_code = request.args.get("plant_code", "")
    milk_type  = request.args.get("milk_type", "")
    zone       = request.args.get("zone", "")
    region     = request.args.get("region", "")

    conn = get_db()

    # 2. Build a dynamic WHERE clause based on what was selected
    clauses = ["DATE(proc_date) >= DATE('now', '-90 days')"]
    params = []

    if plant_code:
        clauses.append("plant_code = ?")
        params.append(plant_code)
        
    if milk_type and milk_type != 'all':
        clauses.append("milk_type = ?")
        params.append(milk_type)
        
    if zone:
        clauses.append("zone = ?")
        params.append(zone)
        
    if region:
        clauses.append("region = ?")
        params.append(region)

    where_sql = " AND ".join(clauses)

    # 3. Execute the dynamically filtered query
    daily = conn.execute(f"""
        SELECT DATE(proc_date) AS proc_date,
               SUM(qty_ltr)    AS qty_ltr,
               SUM(farmer_count) AS farmers
        FROM proc_daily_hpc
        WHERE {where_sql}
        GROUP BY DATE(proc_date)
        ORDER BY proc_date
    """, params).fetchall()
    
    conn.close()

    return jsonify([dict(r) for r in daily])
# ── ADMIN: USER CRUD ──────────────────────────────────────────────────────────

@app.route("/api/admin/users")
@admin_required
def list_users():
    conn = get_db()
    rows = conn.execute("""
        SELECT id, username, full_name, role,
               scope_zone, scope_region, scope_plant,
               is_active, last_login, created_at
        FROM users ORDER BY role, username
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/admin/users", methods=["POST"])
@admin_required
def create_user():
    d = request.get_json() or {}
    for field in ("username", "full_name", "password", "role"):
        if not d.get(field):
            return jsonify({"error": f"{field} is required"}), 400
    role = d["role"]
    if role not in ("admin", "cxo", "zh", "rh", "plant"):
        return jsonify({"error": "Invalid role"}), 400
    if role == "zh"    and not d.get("scope_zone"):
        return jsonify({"error": "scope_zone required for zh"}), 400
    if role == "rh"    and not d.get("scope_region"):
        return jsonify({"error": "scope_region required for rh"}), 400
    if role == "plant" and not d.get("scope_plant"):
        return jsonify({"error": "scope_plant required for plant"}), 400
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO users (username, full_name, password_hash, role, scope_zone, scope_region, scope_plant)
            VALUES (?,?,?,?,?,?,?)
        """, (
            d["username"].strip(),
            d["full_name"].strip(),
            generate_password_hash(d["password"]),
            role,
            d.get("scope_zone",   ""),
            d.get("scope_region", ""),
            d.get("scope_plant",  ""),
        ))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({"error": "Username already exists"}), 409
    conn.close()
    return jsonify({"ok": True}), 201


@app.route("/api/admin/users/<int:uid>", methods=["PUT"])
@admin_required
def update_user(uid):
    d    = request.get_json() or {}
    conn = get_db()
    if not conn.execute("SELECT id FROM users WHERE id=?", (uid,)).fetchone():
        conn.close()
        return jsonify({"error": "User not found"}), 404
    fields, params = [], []
    for col in ("full_name", "role", "scope_zone", "scope_region", "scope_plant"):
        if col in d:
            fields.append(f"{col}=?"); params.append(d[col])
    if "is_active" in d:
        fields.append("is_active=?"); params.append(1 if d["is_active"] else 0)
    if d.get("password"):
        fields.append("password_hash=?"); params.append(generate_password_hash(d["password"]))
    if fields:
        params.append(uid)
        conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/admin/users/<int:uid>", methods=["DELETE"])
@admin_required
def delete_user(uid):
    if session.get("user_id") == uid:
        return jsonify({"error": "Cannot delete your own account"}), 400
    conn = get_db()
    conn.execute("DELETE FROM users WHERE id=?", (uid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/admin/scope-options")
@admin_required
def scope_options():
    conn = get_db()
    zones   = [r[0] for r in conn.execute(
        "SELECT DISTINCT zone FROM proc_period_snapshot WHERE zone IS NOT NULL ORDER BY zone"
    ).fetchall()]
    regions = [r[0] for r in conn.execute(
        "SELECT DISTINCT region FROM proc_period_snapshot WHERE region IS NOT NULL ORDER BY region"
    ).fetchall()]
    plants  = conn.execute(
        "SELECT DISTINCT plant_code, plant_name FROM proc_period_snapshot "
        "WHERE plant_code IS NOT NULL ORDER BY plant_name"
    ).fetchall()
    conn.close()
    return jsonify({
        "zones":   zones,
        "regions": regions,
        "plants":  [{"code": r["plant_code"], "name": r["plant_name"]} for r in plants],
    })

# ── ADMIN: DATA FETCH ─────────────────────────────────────────────────────────

_jobs      = {}
_jobs_lock = threading.Lock()


@app.route("/api/admin/fetch", methods=["POST"])
@admin_required
def trigger_fetch():
    with _jobs_lock:
        for j in _jobs.values():
            if j["status"] == "running":
                return jsonify({"error": "A fetch is already running"}), 409
    job_id = str(uuid.uuid4())[:8]
    job = {
        "job_id": job_id, "status": "running",
        "started": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stage": "starting", "progress": 0, "total": 1,
        "log": ["Starting fetch..."], "result": None,
    }
    with _jobs_lock:
        _jobs[job_id] = job

    def run():
        from pipeline import run_full_fetch
        def cb(stage, current, total, message):
            with _jobs_lock:
                _jobs[job_id].update({"stage": stage, "progress": current, "total": max(total, 1)})
                _jobs[job_id]["log"].append(message)
                _jobs[job_id]["log"] = _jobs[job_id]["log"][-60:]
        try:
            result = run_full_fetch(progress_cb=cb)
            with _jobs_lock:
                _jobs[job_id]["status"] = "done"
                _jobs[job_id]["result"] = result
                _jobs[job_id]["log"].append("Complete")
            _build_cache_bg()
        except Exception as e:
            with _jobs_lock:
                _jobs[job_id]["status"] = "error"
                _jobs[job_id]["result"] = {"error": str(e)}
                _jobs[job_id]["log"].append(f"Error: {e}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/api/admin/fetch-status")
@admin_required
def fetch_status():
    with _jobs_lock:
        if not _jobs:
            return jsonify({"status": "idle"})
        job_id = sorted(_jobs.keys())[-1]
        return jsonify(_jobs[job_id])


@app.route("/api/admin/data-status")
@admin_required
def data_status():
    conn        = get_db()
    latest_db   = conn.execute("SELECT MAX(DATE(proc_date)) FROM proc_daily_hpc").fetchone()[0]
    latest_snap = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()[0]
    hpcs        = conn.execute("SELECT COUNT(DISTINCT hpc_plant_key) FROM proc_daily_hpc").fetchone()[0]
    farmers     = conn.execute("SELECT COUNT(DISTINCT farmer_code) FROM proc_monthly_farmer").fetchone()[0]
    months      = conn.execute("""
        SELECT strftime('%Y-%m', DATE(proc_date)) AS ym,
               COUNT(DISTINCT hpc_plant_key) AS hpcs,
               ROUND(SUM(qty_ltr)/1000000.0, 2) AS mn_ltr
        FROM proc_daily_hpc GROUP BY ym ORDER BY ym DESC LIMIT 4
    """).fetchall()
    runs = conn.execute("""
        SELECT run_at, trigger, status, duration_sec, message
        FROM pipeline_log ORDER BY run_at DESC LIMIT 5
    """).fetchall()
    conn.close()
    try:
        from pathlib import Path
        from pipeline import PARQUET_DIR
        files          = sorted(Path(PARQUET_DIR).glob("proc_*.parquet"))
        latest_parquet = files[-1].stem.replace("proc_", "") if files else None
        parquet_count  = len(files)
    except Exception:
        latest_parquet, parquet_count = None, 0
    with _cache_lock:
        cache_built_at = _cache.get("built_at")
        cache_snap     = _cache.get("snap_date")
        cache_size_kb  = len(_cache["payload"]) // 1024 if _cache.get("payload") else 0
    return jsonify({
        "latest_parquet":  latest_parquet,
        "parquet_count":   parquet_count,
        "latest_db_date":  latest_db,
        "latest_snapshot": latest_snap,
        "total_hpcs":      hpcs,
        "total_farmers":   farmers,
        "recent_months":   [dict(r) for r in months],
        "recent_runs":     [dict(r) for r in runs],
        "cache": {
            "built_at":  datetime.fromtimestamp(cache_built_at).strftime("%Y-%m-%d %H:%M:%S") if cache_built_at else None,
            "snap_date": cache_snap,
            "size_kb":   cache_size_kb,
        },
    })


# ── ADMIN: HPR CONTACT MANAGEMENT ─────────────────────────────────────────────

@app.route("/api/admin/hpcs", methods=["GET"])
@admin_required
def list_hpcs():
    conn = get_db()
    # Ensure the persistent table exists
    conn.execute("CREATE TABLE IF NOT EXISTS hpr_contacts (hpc_plant_key TEXT PRIMARY KEY, hpr_name TEXT, mobile_no TEXT)")
    
    snap_date = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()
    if not snap_date or not snap_date[0]:
        conn.close()
        return jsonify([])
        
    rows = conn.execute(f"""
        SELECT hpc_plant_key, hpc_name, plant_name, region, zone, hpr_name, mobile_no
        FROM proc_period_snapshot
        WHERE snapshot_date='{snap_date[0]}'
        ORDER BY zone, region, hpc_name
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/admin/hpcs/<path:hpc_key>", methods=["PUT"])
@admin_required
def update_hpc_contact(hpc_key):
    d = request.get_json() or {}
    hpr_name = d.get("hpr_name", "").strip()
    mobile_no = d.get("mobile_no", "").strip()
    
    conn = get_db()
    conn.execute("CREATE TABLE IF NOT EXISTS hpr_contacts (hpc_plant_key TEXT PRIMARY KEY, hpr_name TEXT, mobile_no TEXT)")
    
    # 1. Save permanently to our persistent table (so pipeline rebuilds don't erase it)
    conn.execute(
        "INSERT OR REPLACE INTO hpr_contacts (hpc_plant_key, hpr_name, mobile_no) VALUES (?, ?, ?)",
        (hpc_key, hpr_name, mobile_no)
    )
    
    # 2. Update the current live snapshot so it shows up instantly on the dashboard
    conn.execute(
        "UPDATE proc_period_snapshot SET hpr_name = ?, mobile_no = ? WHERE hpc_plant_key = ?",
        (hpr_name, mobile_no, hpc_key)
    )
    conn.commit()
    conn.close()
    
    # 3. Rebuild the RAM cache in the background so the main dashboard updates instantly
    _build_cache_bg()
    
    return jsonify({"ok": True})




# ── SCORECARD HISTORY — add this route to app.py ─────────────────────────────
# Returns last-month and last-year-same-month aggregates for the scorecard.
# Respects user role scope AND optional ?zone= / ?region= / ?plant_code= params.

# ── SCORECARD HISTORY — updated with computed attrition ──────────────────────
# Replace the existing /api/scorecard/history route in app.py with this.

# ── SCORECARD HISTORY — v3, fixed LY attrition ───────────────────────────────
# Replace the existing /api/scorecard/history route in app.py with this.
#
# Attrition definition:
#   LM attrition  = backward: farmers in (lm-1) who disappeared in (lm)
#   LY attrition  = forward:  farmers in (ly) who disappeared in (ly+1)
#
# "Forward" is used for LY because data 13 months back often doesn't exist,
# but LY+1 (11 months ago) almost always does.

@app.route("/api/scorecard/history")
@login_required
@analytics_required
def scorecard_history():
    import calendar as _cal
    f    = gf()
    conn = get_db()

    snap_date = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()[0]
    if not snap_date:
        conn.close()
        return jsonify({"lm": {}, "ly": {}})

    cur = conn.execute("SELECT yr, mth FROM proc_monthly_hpc ORDER BY yr DESC, mth DESC LIMIT 1").fetchone()
    if not cur:
        conn.close()
        return jsonify({"lm": {}, "ly": {}})

    curr_yr, curr_mth = cur["yr"], cur["mth"]

    lm_yr  = curr_yr  if curr_mth > 1 else curr_yr - 1
    lm_mth = curr_mth - 1 if curr_mth > 1 else 12
    ly_yr, ly_mth = curr_yr - 1, curr_mth

    # ── scope WHERE fragment ──────────────────────────────────────────────────
    # ── scope WHERE fragment ──────────────────────────────────────────────────
    scope_clauses, scope_params = [], []
    if f.get("zone"):
        scope_clauses.append("s.zone = ?")
        scope_params.append(f["zone"])
    if f.get("region"):
        scope_clauses.append("s.region = ?")
        scope_params.append(f["region"])
    if f.get("plant_code"):
        scope_clauses.append("CAST(s.plant_code AS TEXT) = ?")
        scope_params.append(str(f["plant_code"]))
        
    milk_type = request.args.get('milk_type', '')
    milk_type_clause = ""
    milk_type_param  = []
    if milk_type and milk_type != 'all':
        # Filter directly on proc_monthly_hpc/farmer.milk_type — NOT through the snapshot
        # (snapshot is 1-row-per-HPC with no milk_type column after the fix)
        milk_type_clause = "AND mh.milk_type = ?"
        milk_type_param  = [milk_type]

    scope_and = ("AND " + " AND ".join(scope_clauses)) if scope_clauses else ""

    # ── FAST SQL AGGREGATOR ───────────────────────────────────────────────────
    # ── FAST SQL AGGREGATOR ───────────────────────────────────────────────────
    def fetch_metrics(yr, mth):
        # 1. Do all math in SQL instead of looping in Python
        hpc_agg = conn.execute(f"""
            SELECT SUM(mh.total_qty_ltr) as total_ltr,
                   SUM(mh.total_fat_kg) as total_fat_kg,
                   SUM(mh.total_net_price) as total_net_price,
                   SUM(mh.avg_fat * mh.total_qty_ltr) / NULLIF(SUM(mh.total_qty_ltr), 0) as fat,
                   SUM(mh.avg_snf * mh.total_qty_ltr) / NULLIF(SUM(mh.total_qty_ltr), 0) as snf,
                   COUNT(DISTINCT mh.hpc_plant_key) as hpcs
            FROM proc_monthly_hpc mh
            JOIN proc_period_snapshot s ON mh.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date = ?
            WHERE mh.yr = ? AND mh.mth = ? {milk_type_clause} {scope_and}
        """, [snap_date, yr, mth] + milk_type_param + scope_params).fetchone()

        # 2. Optimized Farmer Count using exact index, skipping string wildcard scans
        fmr_milk_clause = milk_type_clause.replace("mh.", "mf.") if milk_type_clause else ""
        farmer_row = conn.execute(f"""
            SELECT COUNT(DISTINCT mf.farmer_code) AS farmers
            FROM proc_monthly_farmer mf
            JOIN proc_period_snapshot s ON mf.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date = ?
            WHERE mf.yr = ? AND mf.mth = ?
              AND mf.farmer_code_seq != '9999'
              {fmr_milk_clause} {scope_and}
        """, [snap_date, yr, mth] + milk_type_param + scope_params).fetchone()

        total_ltr = hpc_agg["total_ltr"] or 0
        hpcs      = hpc_agg["hpcs"] or 1
        farmers   = farmer_row["farmers"] or 0
        days      = _cal.monthrange(yr, mth)[1]
        lpd       = total_ltr / days

        return {
            "farmer_productivity": round(lpd / farmers, 2) if farmers > 0 else None,
            "hpc_productivity"   : round(lpd / hpcs, 2) if hpcs > 0 else None,
            "farmers_per_hpc"    : round(farmers / hpcs, 1) if hpcs > 0 else None,
            "fat"                : round(hpc_agg["fat"], 3) if hpc_agg["fat"] else None,
            "snf"                : round(hpc_agg["snf"], 3) if hpc_agg["snf"] else None,
            "cost_per_fat_kg"    : round(hpc_agg["total_net_price"] / hpc_agg["total_fat_kg"], 2) if hpc_agg["total_fat_kg"] else None
        }

    lm_data = fetch_metrics(lm_yr, lm_mth)
    ly_data = fetch_metrics(ly_yr, ly_mth)
    conn.close()

    return jsonify({"lm": lm_data, "ly": ly_data})


# ── ADMIN: TARGET MANAGEMENT ──────────────────────────────────────────────────

@app.route("/api/admin/targets")
@admin_required
def get_targets():
    """
    Return targets for a specific (scope_type, scope_value, milk_type) combo.
    Each metric shows:
      - target_value   : the effective value (override if set, else company default)
      - is_overridden  : True if an explicit row exists for this exact scope
      - company_value  : always the company/all baseline so the UI can show the diff
    """
    scope_type  = request.args.get('scope_type',  'company')
    scope_value = request.args.get('scope_value', '')
    milk_type   = request.args.get('milk_type',   'all')
 
    conn = get_db()
 
    # Always fetch company/all as the baseline
    company = {
        r['metric_key']: dict(r)
        for r in conn.execute("""
            SELECT metric_key, target_value, unit, description, updated_at
            FROM scoped_targets
            WHERE scope_type='company' AND scope_value='' AND milk_type='all'
        """).fetchall()
    }
 
    # Fetch the explicit override for this scope (if any)
    overrides = {}
    if not (scope_type == 'company' and scope_value == '' and milk_type == 'all'):
        overrides = {
            r['metric_key']: dict(r)
            for r in conn.execute("""
                SELECT metric_key, target_value, unit, description, updated_at, updated_by
                FROM scoped_targets
                WHERE scope_type=? AND scope_value=? AND milk_type=?
            """, (scope_type, scope_value, milk_type)).fetchall()
        }
 
    conn.close()
 
    result = []
    for key in METRIC_ORDER:
        base = company.get(key, {})
        ov   = overrides.get(key)
        result.append({
            'metric_key':    key,
            'target_value':  ov['target_value'] if ov else base.get('target_value', 0),
            'unit':          base.get('unit', ''),
            'description':   base.get('description', ''),
            'is_overridden': ov is not None,
            'company_value': base.get('target_value'),
            'scope_type':    scope_type,
            'scope_value':   scope_value,
            'milk_type':     milk_type,
            'updated_at':    (ov or base).get('updated_at'),
        })
 
    return jsonify(result)



@app.route("/api/admin/targets", methods=["PUT"])
@admin_required
def update_targets():
    """
    Bulk upsert / delete target overrides for a given scope+milk_type.
    Pass reset=True on a target row to remove the override (revert to parent).
    """
    data        = request.get_json() or {}
    targets_in  = data.get('targets', [])
    scope_type  = data.get('scope_type',  'company')
    scope_value = data.get('scope_value', '')
    milk_type   = data.get('milk_type',   'all')
 
    if not targets_in:
        return jsonify({"error": "No targets provided"}), 400
 
    conn    = get_db()
    user_id = session.get("user_id")
 
    try:
        for t in targets_in:
            metric_key   = t.get('metric_key')
            target_value = t.get('target_value')
            reset        = t.get('reset', False)
 
            if not metric_key:
                continue
 
            if reset:
                conn.execute("""
                    DELETE FROM scoped_targets
                    WHERE metric_key=? AND scope_type=? AND scope_value=? AND milk_type=?
                """, (metric_key, scope_type, scope_value, milk_type))
 
            elif target_value is not None:
                # Fetch unit from company defaults
                unit_row = conn.execute("""
                    SELECT unit FROM scoped_targets
                    WHERE metric_key=? AND scope_type='company'
                      AND scope_value='' AND milk_type='all'
                """, (metric_key,)).fetchone()
                unit = unit_row[0] if unit_row else ''
 
                conn.execute("""
                    INSERT INTO scoped_targets
                        (metric_key, scope_type, scope_value, milk_type,
                         target_value, unit, updated_at, updated_by)
                    VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)
                    ON CONFLICT(metric_key, scope_type, scope_value, milk_type)
                    DO UPDATE SET
                        target_value = excluded.target_value,
                        updated_at   = excluded.updated_at,
                        updated_by   = excluded.updated_by
                """, (metric_key, scope_type, scope_value, milk_type,
                      float(target_value), unit, user_id))
 
        conn.commit()
        log.info(
            f"Targets saved: scope={scope_type}/{scope_value} "
            f"milk={milk_type} by user_id={user_id}"
        )
        _build_cache_bg()
 
    except Exception as e:
        conn.rollback()
        conn.close()
        log.error(f"Target update failed: {e}")
        return jsonify({"error": str(e)}), 500
 
    conn.close()
    return jsonify({"success": True})
 
 


@app.route("/api/targets")
@login_required
def get_public_targets():
    """
    Dashboard endpoint: returns the most-specific applicable target for
    each metric given the current zone/region/plant_code/milk_type context.
 
    Priority (highest first):
      plant+specific > plant+all > region+specific > region+all >
      zone+specific  > zone+all  > company+specific > company+all
    """
    zone       = request.args.get('zone',       '')
    region     = request.args.get('region',     '')
    plant_code = request.args.get('plant_code', '')
    milk_type  = request.args.get('milk_type',  'all')
 
    conn = get_db()
 
    rows = conn.execute("""
        SELECT metric_key, scope_type, scope_value, milk_type, target_value, unit
        FROM scoped_targets
        WHERE (
               (scope_type = 'company' AND scope_value = '')
            OR (scope_type = 'zone'    AND scope_value = ?)
            OR (scope_type = 'region'  AND scope_value = ?)
            OR (scope_type = 'plant'   AND scope_value = ?)
        )
          AND (milk_type = 'all' OR milk_type = ?)
    """, (zone, region, str(plant_code), milk_type)).fetchall()
 
    conn.close()
 
    # Resolve: highest (scope_priority, milk_priority) wins per metric
    SCOPE_P = {'plant': 4, 'region': 3, 'zone': 2, 'company': 1}
    best    = {}
 
    for r in rows:
        mk    = r['metric_key']
        score = (
            SCOPE_P.get(r['scope_type'], 0),
            2 if r['milk_type'] != 'all' else 1,
        )
        if mk not in best or score > best[mk]['_score']:
            best[mk] = {
                'value':  r['target_value'],
                'unit':   r['unit'],
                'source': r['scope_type'],
                '_score': score,
            }
 
    # Strip internal score key
    return jsonify({k: {kk: vv for kk, vv in v.items() if kk != '_score'}
                    for k, v in best.items()})
# ── RUN ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_db()
    from pipeline import _ensure_tables, _ensure_indexes, _ensure_default_admin
    _ensure_tables(conn)
    _ensure_indexes(conn)
    _ensure_default_admin(conn)
    _ensure_targets_table(conn)  # ← Add this line
    conn.close()
    _build_cache_bg()
    app.run(debug=True, port=5000, threaded=True)