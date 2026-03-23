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


def _build_cache():
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
        hpc_list = q(f"""
            SELECT hpc_plant_key, hpc_name, plant_name, plant_code, region, zone,
                   hpr_name, mobile_no,  /* <--- ADD THIS LINE HERE */
                   ROUND(mtd,1)         AS mtd,
                   ROUND(lm,1)          AS lm,
                   ROUND(lmtd,1)        AS lmtd,
                   ROUND(lymtd,1)       AS lymtd,
                   yoy_growth_pct,
                   mom_growth_pct,
                   ROUND(mtd_farmers,0) AS mtd_farmers,
                   ROUND(lm_farmers,0)  AS lm_farmers,
                   ROUND(mtd_avg_fat,3) AS mtd_avg_fat,
                   ROUND(mtd_rate,2)    AS mtd_rate,
                   ROUND(mtd_payout,0)  AS mtd_payout
            FROM proc_period_snapshot
            WHERE snapshot_date='{snap_date}'
            ORDER BY mtd DESC
        """)

        # ── monthly_hpc ───────────────────────────────────────────────────
        monthly_hpc = q(f"""
            SELECT mh.hpc_plant_key,
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
                   ROUND(mh.avg_rate_per_ltr,2)    AS avg_rate_per_ltr,
                   mh.morning_ltr, mh.evening_ltr
            FROM proc_monthly_hpc mh
            JOIN proc_period_snapshot s
              ON mh.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date  = '{snap_date}'
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
        latest_ym = conn.execute(
            "SELECT strftime('%Y-%m', MAX(DATE(proc_date))) FROM proc_quality_incidents"
        ).fetchone()[0]
        quality_incidents = q(f"""
            SELECT qi.hpc_plant_key,
                   s.hpc_name, s.region, s.zone, s.plant_code,
                   COUNT(*)             AS incidents,
                   qi.incident_type,
                   ROUND(AVG(qi.fat),3) AS avg_fat,
                   ROUND(AVG(qi.snf),3) AS avg_snf
            FROM proc_quality_incidents qi
            JOIN proc_period_snapshot s
              ON qi.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date  = '{snap_date}'
            WHERE strftime('%Y-%m', DATE(qi.proc_date)) = '{latest_ym}'
            GROUP BY qi.hpc_plant_key
            ORDER BY incidents DESC
        """) if latest_ym else []

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
            JOIN proc_period_snapshot s
              ON fh.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date  = '{snap_date}'
            WHERE fh.snapshot_date = '{snap_date}'
        """) if has_fh else []

        # ── farmer_rfm ────────────────────────────────────────────────────
        has_rfm = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='proc_farmer_rfm'"
        ).fetchone()[0]
        # farmer_rfm: minimal fields only — keeps payload small
        # Join snapshot to get plant_code (proc_farmer_rfm may not have it directly)
        farmer_rfm = q(f"""
            SELECT r.farmer_code,
                   SUBSTR(r.farmer_name,1,40)     AS farmer_name,
                   r.hpc_plant_key, r.region, r.zone,
                   CAST(s.plant_code AS TEXT)      AS plant_code,
                   r.tier,
                   ROUND(r.total_qty_ltr,0)        AS total_qty_ltr,
                   ROUND(r.total_payout,0)          AS total_payout,
                   r.delivery_days,
                   ROUND(r.avg_fat,2)               AS avg_fat,
                   ROUND(r.lpd,1)                   AS lpd,
                   CASE WHEN r.avg_fat < 3.5 THEN 1 ELSE 0 END AS quality_risk
            FROM proc_farmer_rfm r
            JOIN proc_period_snapshot s
              ON r.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date = '{snap_date}'
            WHERE r.snapshot_date = '{snap_date}'
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
                COUNT(*)           AS total_hpcs,
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

        snapshot = {
            "snapshot_date":  snap_date,
            "mtd":            round(mtd,   0),
            "lm":             round(snap_row["lm"] or 0, 0),
            "lmtd":           round(lmtd,  0),
            "lymtd":          round(lymtd, 0),
            "yoy_pct":        round((mtd - lymtd) / lymtd * 100, 1) if lymtd else 0,
            "mom_pct":        round((mtd - lmtd)  / lmtd  * 100, 1) if lmtd  else 0,
            "total_farmers":  int(snap_row["total_farmers"]  or 0),
            "lm_farmers":     int(snap_row["lm_farmers"]     or 0),
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
        if has_rfm:
            _low_att_row = conn.execute(f"""
                SELECT COUNT(*) AS cnt FROM proc_farmer_rfm
                WHERE snapshot_date='{snap_date}' AND delivery_days < 10
            """).fetchone()
        else:
            yr_c  = "yr=(SELECT MAX(yr) FROM proc_monthly_farmer)"
            mth_c = "mth=(SELECT MAX(mth) FROM proc_monthly_farmer WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))"
            _low_att_row = conn.execute(f"""
                SELECT COUNT(DISTINCT farmer_code) AS cnt
                FROM proc_monthly_farmer WHERE {yr_c} AND {mth_c} AND delivery_days < 10
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
                scoped[z] = json.dumps({
                    "snapshot": z_snap, "hpc_list": z_hpcs, "monthly_hpc": z_mhpc,
                    "monthly_farmer": [], "daily": z_daily,
                    "quality_incidents": z_qi, "farmer_health": z_fh,
                    "farmer_rfm": z_rfm, "insights": z_ins, "filters": z_flt,
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
        "yoy_pct":        round((mtd - lymtd) / lymtd * 100, 1) if lymtd else 0,
        "mom_pct":        round((mtd - lmtd)  / lmtd  * 100, 1) if lmtd  else 0,
        "total_farmers":  int(sum(h.get("mtd_farmers", 0) or 0 for h in hpcs)),
        "lm_farmers":     int(sum(h.get("lm_farmers",  0) or 0 for h in hpcs)),
        "total_payout":   round(pay, 0),
        "total_hpcs":     len(hpcs),
        "dark_hpcs":      len([h for h in hpcs if not h.get("mtd")]),
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
    plant_code = request.args.get("plant_code", "")
    if not plant_code:
        return jsonify({"error": "plant_code required"}), 400

    conn = get_db()
    # Fetch just the 90 rows for this specific plant
    daily = conn.execute("""
        SELECT DATE(proc_date) AS proc_date,
               SUM(qty_ltr)    AS qty_ltr,
               SUM(farmer_count) AS farmers
        FROM proc_daily_hpc
        WHERE plant_code = ?
          AND DATE(proc_date) >= DATE('now', '-90 days')
        GROUP BY DATE(proc_date)
        ORDER BY proc_date
    """, (plant_code,)).fetchall()
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


# ── RUN ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_db()
    from pipeline import _ensure_tables, _ensure_indexes, _ensure_default_admin
    _ensure_tables(conn)
    _ensure_indexes(conn)
    _ensure_default_admin(conn)
    conn.close()
    _build_cache_bg()
    app.run(debug=True, port=5000, threaded=True)