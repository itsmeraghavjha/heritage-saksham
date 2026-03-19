"""
Heritage Dairy — Procurement Portal v2
Flask backend with role-based login and admin data fetch trigger.

Roles:
  admin  → /admin only (user management + data fetch). Cannot see analytics.
  cxo    → full data, all zones/regions/plants
  zh     → locked to own zone
  rh     → locked to own region
  plant  → locked to own plant

Scope enforcement:
  get_data_scope() returns the fixed filter for a user's role.
  gf() merges that with UI request args — scope always wins.
  A ZH can narrow to a specific region within their zone,
  but can never select a different zone.
"""

from flask import Flask, render_template, jsonify, request, session, redirect, url_for
import sqlite3, threading, uuid, json, time, logging
from functools import wraps
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash

log = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = "heritage-portal-v2-change-this-in-production"
app.permanent_session_lifetime = timedelta(hours=12)

SQLITE_PATH = r"C:\AnalyticsPortal\portal.db"

# ── SERVER-SIDE CACHE ─────────────────────────────────────────────────────────
# Built once at startup, rebuilt after every admin data fetch.
# Every login just gets served pre-built JSON from RAM — zero DB queries.

_cache      = {}
_cache_lock = threading.Lock()

def _build_cache():
    """
    Queries SQLite once, serialises everything to JSON bytes, stores in _cache.
    Called at startup in a background thread, and again after each admin fetch.
    """
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
        yr_mth_fmr = (
            "yr=(SELECT MAX(yr) FROM proc_monthly_farmer) AND "
            "mth=(SELECT MAX(mth) FROM proc_monthly_farmer "
            "WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))"
        )

        # ── hpc_list — from snapshot, all HPCs ───────────────────────────
        hpc_list = q(f"""
            SELECT hpc_plant_key, hpc_name, plant_name, plant_code, region, zone,
                   ROUND(mtd,1)          AS mtd,
                   ROUND(lm,1)           AS lm,
                   ROUND(lmtd,1)         AS lmtd,
                   ROUND(lymtd,1)        AS lymtd,
                   yoy_growth_pct,
                   mom_growth_pct,
                   ROUND(mtd_farmers,0)  AS mtd_farmers,
                   ROUND(lm_farmers,0)   AS lm_farmers,
                   ROUND(mtd_avg_fat,3)  AS mtd_avg_fat,
                   ROUND(mtd_rate,2)     AS mtd_rate,
                   ROUND(mtd_payout,0)   AS mtd_payout
            FROM proc_period_snapshot
            WHERE snapshot_date='{snap_date}'
            ORDER BY mtd DESC
        """)

        # ── monthly_hpc — join snapshot for geo, never touch proc_daily_hpc
        monthly_hpc = q(f"""
            SELECT mh.hpc_plant_key,
                   s.zone, s.region, s.plant_code,
                   ROUND(mh.total_qty_ltr,0)       AS total_qty_ltr,
                   ROUND(mh.avg_fat,3)              AS avg_fat,
                   ROUND(mh.avg_snf,3)              AS avg_snf,
                   mh.dumping_incidents,
                   ROUND(mh.total_mcc_incentive,0)  AS total_mcc_incentive,
                   ROUND(mh.total_qty_incentive,0)  AS total_qty_incentive,
                   ROUND(mh.total_bonus,0)          AS total_bonus,
                   ROUND(mh.total_net_price,0)      AS total_net_price,
                   ROUND(mh.incentive_pct,2)        AS incentive_pct,
                   ROUND(mh.avg_cost_per_fat_kg,2)  AS avg_cost_per_fat_kg,
                   ROUND(mh.avg_rate_per_ltr,2)     AS avg_rate_per_ltr,
                   mh.morning_ltr, mh.evening_ltr
            FROM proc_monthly_hpc mh
            JOIN proc_period_snapshot s
              ON mh.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date = '{snap_date}'
            WHERE {yr_mth_hpc}
        """)

        # ── monthly_farmer ────────────────────────────────────────────────
        monthly_farmer = q(f"""
            SELECT mf.farmer_code, mf.farmer_name, mf.hpc_plant_key,
                mf.region, mf.zone, mf.plant_code, mf.milk_type,
                ROUND(mf.total_qty_ltr,0)    AS total_qty_ltr,
                ROUND(mf.lpd,1)              AS lpd,
                mf.delivery_days,
                ROUND(mf.avg_fat,3)          AS avg_fat,
                ROUND(mf.avg_snf,3)          AS avg_snf,
                ROUND(mf.total_net_price,0)  AS total_net_price,
                ROUND(mf.avg_rate_per_ltr,2) AS avg_rate_per_ltr,
                mf.dumping_incidents,
                COALESCE(qi.quality_risk, 0) AS quality_risk
            FROM proc_monthly_farmer mf
            LEFT JOIN (
                SELECT farmer_code, COUNT(*) AS quality_risk
                FROM proc_quality_incidents
                WHERE strftime('%Y-%m', DATE(proc_date)) = (
                    SELECT strftime('%Y-%m', MAX(DATE(proc_date)))
                    FROM proc_quality_incidents
                )
                GROUP BY farmer_code
            ) qi ON mf.farmer_code = qi.farmer_code
            WHERE {yr_mth_fmr}
        """)

        # ── daily — last 90 days only, grouped by date+zone+region ───────
        daily = q("""
            SELECT DATE(proc_date)         AS proc_date,
                   zone, region,
                   ROUND(SUM(qty_ltr),0)   AS qty_ltr,
                   SUM(farmer_count)        AS farmers,
                   SUM(total_fat_kg)        AS fat_kg,
                   SUM(total_snf_kg)        AS snf_kg,
                   SUM(qty_kg)              AS qty_kg,
                   SUM(quality_incidents)   AS incidents,
                   SUM(total_net_price)     AS net_price
            FROM proc_daily_hpc
            WHERE DATE(proc_date) >= DATE('now', '-90 days')
            GROUP BY DATE(proc_date), zone, region
            ORDER BY proc_date
        """)

        # ── quality_incidents — current month per HPC, snapshot for geo ──
        latest_ym = conn.execute(
            "SELECT strftime('%Y-%m', MAX(DATE(proc_date))) FROM proc_quality_incidents"
        ).fetchone()[0]

        quality_incidents = q(f"""
            SELECT qi.hpc_plant_key,
                   s.hpc_name, s.region, s.zone, s.plant_code,
                   COUNT(*)              AS incidents,
                   qi.incident_type,
                   ROUND(AVG(qi.fat),3)  AS avg_fat,
                   ROUND(AVG(qi.snf),3)  AS avg_snf
            FROM proc_quality_incidents qi
            JOIN proc_period_snapshot s
              ON qi.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date = '{snap_date}'
            WHERE strftime('%Y-%m', DATE(qi.proc_date)) = '{latest_ym}'
            GROUP BY qi.hpc_plant_key
            ORDER BY incidents DESC
        """) if latest_ym else []


        # ── farmer_health — HPC level attrition + consistency ─────────────
        farmer_health = q(f"""
            SELECT fh.*, s.hpc_name, s.region, s.zone, s.plant_code
            FROM proc_hpc_farmer_health fh
            JOIN proc_period_snapshot s
              ON fh.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date = '{snap_date}'
            WHERE fh.snapshot_date = '{snap_date}'
        """) if conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='proc_hpc_farmer_health'"
        ).fetchone()[0] else []


        farmer_rfm = q(f"""
            SELECT * FROM proc_farmer_rfm
            WHERE snapshot_date = '{snap_date}'
        """) if conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='proc_farmer_rfm'"
        ).fetchone()[0] else []

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

        # ── filters ───────────────────────────────────────────────────────
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

        # ── insights — run all 7 checks unscoped (full company view) ─────
        insights = []
        w_snap = f"WHERE snapshot_date='{snap_date}'"

        dark = conn.execute(f"""
            SELECT COUNT(*) AS cnt, GROUP_CONCAT(hpc_name, ', ') AS names
            FROM (SELECT hpc_name FROM proc_period_snapshot {w_snap} AND mtd=0 AND lm>0
                  ORDER BY lm DESC LIMIT 5)
        """).fetchone()
        if dark["cnt"] > 0:
            insights.append({"priority": "critical", "category": "Volume Risk",
                "title":  f"{dark['cnt']} HPC(s) went dark this month",
                "detail": f"Had procurement last month but zero MTD. Top: {dark['names']}",
                "action": "Immediate field visit — check farmer mobilisation and route issues."})

        severe = conn.execute(f"""
            SELECT COUNT(*) AS cnt FROM proc_period_snapshot
            {w_snap} AND yoy_growth_pct < -0.3 AND mtd>0
        """).fetchone()
        if severe["cnt"] > 0:
            top = conn.execute(f"""
                SELECT hpc_name, region, ROUND(yoy_growth_pct*100,1) AS yoy
                FROM proc_period_snapshot {w_snap} AND yoy_growth_pct < -0.3 AND mtd>0
                ORDER BY yoy_growth_pct LIMIT 3
            """).fetchall()
            insights.append({"priority": "critical", "category": "YoY Decline",
                "title":  f"{severe['cnt']} HPC(s) with >30% YoY decline",
                "detail": " | ".join([f"{r['hpc_name']} ({r['region']}): {r['yoy']}%" for r in top]),
                "action": "Investigate rate competitiveness and farmer satisfaction."})

        bad_regions = conn.execute(f"""
            SELECT region, ROUND(AVG(yoy_growth_pct)*100,1) AS avg_yoy,
                   COUNT(*) AS hpcs,
                   SUM(CASE WHEN yoy_growth_pct<0 THEN 1 ELSE 0 END) AS declining
            FROM proc_period_snapshot {w_snap} AND yoy_growth_pct IS NOT NULL AND lymtd>0
            GROUP BY region HAVING AVG(yoy_growth_pct) < -0.05 ORDER BY avg_yoy LIMIT 3
        """).fetchall()
        for r in bad_regions:
            insights.append({"priority": "warning", "category": "Region Alert",
                "title":  f"{r['region']}: {r['avg_yoy']}% avg YoY",
                "detail": f"{r['declining']} of {r['hpcs']} HPCs declining.",
                "action": f"Review competitor rates in {r['region']}. Check MCC incentive structure."})

        yr_clause  = "yr=(SELECT MAX(yr) FROM proc_monthly_farmer)"
        mth_clause = "mth=(SELECT MAX(mth) FROM proc_monthly_farmer WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))"
        low_att = conn.execute(f"""
            SELECT COUNT(DISTINCT farmer_code) AS cnt
            FROM proc_monthly_farmer
            WHERE {yr_clause} AND {mth_clause} AND delivery_days < 10
        """).fetchone()
        if low_att["cnt"] > 100:
            insights.append({"priority": "warning", "category": "Farmer Risk",
                "title":  f"{low_att['cnt']:,} farmers with <10 delivery days this month",
                "detail": "High absenteeism signals churn — these farmers may be selling to competitors.",
                "action": "Flag to field officers. Check if payment delays are an issue."})

        low_fat = conn.execute(f"""
            SELECT s.region, ROUND(AVG(mh.avg_fat),3) AS fat
            FROM proc_monthly_hpc mh
            JOIN proc_period_snapshot s
              ON mh.hpc_plant_key = s.hpc_plant_key
             AND s.snapshot_date = '{snap_date}'
            WHERE {yr_mth_hpc}
            GROUP BY s.region HAVING AVG(mh.avg_fat) < 3.8 ORDER BY fat LIMIT 3
        """).fetchall()
        for r in low_fat:
            insights.append({"priority": "warning", "category": "Quality Alert",
                "title":  f"{r['region']}: Avg FAT {r['fat']}% — below 3.8%",
                "detail": "Low FAT reduces milk value. May indicate adulteration or breed issues.",
                "action": "Increase testing frequency. Consider breed improvement incentives."})

        star_regions = conn.execute(f"""
            SELECT region, ROUND(AVG(yoy_growth_pct)*100,1) AS avg_yoy, COUNT(*) AS hpcs
            FROM proc_period_snapshot {w_snap} AND yoy_growth_pct IS NOT NULL AND lymtd>0
            GROUP BY region HAVING AVG(yoy_growth_pct) > 0.25 ORDER BY avg_yoy DESC LIMIT 2
        """).fetchall()
        for r in star_regions:
            insights.append({"priority": "positive", "category": "Growth Opportunity",
                "title":  f"{r['region']}: +{r['avg_yoy']}% YoY — top performing",
                "detail": f"{r['hpcs']} HPCs growing strongly.",
                "action": "Document best practices here and replicate to lagging regions."})

        total_mtd = conn.execute(
            f"SELECT SUM(mtd) FROM proc_period_snapshot {w_snap}"
        ).fetchone()[0] or 1
        top20_n = max(1, int(conn.execute(
            f"SELECT COUNT(*) FROM proc_period_snapshot {w_snap}"
        ).fetchone()[0] * 0.2))
        top20_vol = conn.execute(f"""
            SELECT SUM(mtd) FROM (
                SELECT mtd FROM proc_period_snapshot {w_snap} ORDER BY mtd DESC LIMIT {top20_n}
            )
        """).fetchone()[0] or 0
        conc = round(top20_vol / total_mtd * 100, 1)
        if conc > 70:
            insights.append({"priority": "warning", "category": "Concentration Risk",
                "title":  f"Top 20% HPCs contribute {conc}% of volume",
                "detail": "High concentration means any disruption has outsized impact.",
                "action": "Prioritise farmer expansion in medium-sized HPCs."})

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

        # Pre-serialise to bytes — served as-is, zero per-request serialisation cost
        payload_bytes = json.dumps(payload).encode("utf-8")

        with _cache_lock:
            _cache["payload"]   = payload_bytes
            _cache["built_at"]  = time.time()
            _cache["snap_date"] = snap_date

        elapsed = time.time() - t0
        log.info(
            f"Cache built in {elapsed:.1f}s — "
            f"{len(payload_bytes) / 1024:.0f} KB, snap={snap_date}"
        )

    except Exception as e:
        log.error(f"Cache build failed: {e}", exc_info=True)


def _build_cache_bg():
    """Fire _build_cache in a daemon thread so startup / fetch is not blocked."""
    threading.Thread(target=_build_cache, daemon=True).start()


# ── DB ────────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

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
            return redirect(url_for("login_page"))
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

# ── FILTER HELPERS ────────────────────────────────────────────────────────────

def get_data_scope(user):
    """Return the fixed data boundary for this user's role."""
    role = user["role"]
    if role in ("admin", "cxo"):
        return {"zone": "", "region": "", "plant_code": ""}
    if role == "zh":
        return {"zone": user["scope_zone"], "region": "", "plant_code": ""}
    if role == "rh":
        return {"zone": "", "region": user["scope_region"], "plant_code": ""}
    if role == "plant":
        return {"zone": "", "region": "", "plant_code": user["scope_plant"]}
    return {"zone": "", "region": "", "plant_code": ""}

def gf():
    """
    Get effective filter: role scope merged with UI request args.
    Scope always wins — scoped users can narrow further but never broaden.
    """
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

# ── AUTH ENDPOINTS ────────────────────────────────────────────────────────────

@app.route("/login")
def login_page():
    user = current_user()
    if user:
        return redirect("/admin" if user["role"] == "admin" else "/")
    return render_template("login.html")

@app.route("/api/auth/login", methods=["POST"])
def do_login():
    d        = request.get_json() or {}
    username = (d.get("username") or "").strip().lower()
    password = d.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400

    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE LOWER(username)=? AND is_active=1", (username,)
    ).fetchone()

    if not user or not check_password_hash(user["password_hash"], password):
        conn.close()
        return jsonify({"error": "Invalid username or password"}), 401

    conn.execute("UPDATE users SET last_login=datetime('now') WHERE id=?", (user["id"],))
    conn.commit()
    conn.close()

    session.clear()
    session.permanent  = True
    session["user_id"]   = user["id"]
    session["username"]  = user["username"]
    session["role"]      = user["role"]
    session["full_name"] = user["full_name"]

    return jsonify({
        "ok":        True,
        "role":      user["role"],
        "full_name": user["full_name"],
        "redirect":  "/admin" if user["role"] == "admin" else "/",
    })

@app.route("/api/auth/logout", methods=["POST"])
def do_logout():
    session.clear()
    return jsonify({"ok": True})

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

# ── ADMIN PAGES ───────────────────────────────────────────────────────────────

@app.route("/admin")
@login_required
def admin_page():
    if current_user()["role"] != "admin":
        return redirect("/")
    return render_template("admin.html")

@app.route("/")
@login_required
def index():
    if current_user()["role"] == "admin":
        return redirect("/admin")
    return render_template("index.html")

# ── ADMIN: USER CRUD ──────────────────────────────────────────────────────────

@app.route("/api/admin/users")
@admin_required
def list_users():
    conn = get_db()
    rows = conn.execute("""
        SELECT id, username, full_name, role,
               scope_zone, scope_region, scope_plant,
               is_active, last_login, created_at
        FROM users ORDER BY created_at DESC
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
            INSERT INTO users
              (username, full_name, password_hash, role, scope_zone, scope_region, scope_plant)
            VALUES (?,?,?,?,?,?,?)
        """, (
            d["username"].strip().lower(),
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
    user = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not user:
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
    conn    = get_db()
    zones   = [r[0] for r in conn.execute(
        "SELECT DISTINCT zone FROM proc_period_snapshot "
        "WHERE zone IS NOT NULL AND zone!='' ORDER BY zone"
    ).fetchall()]
    regions = [r[0] for r in conn.execute(
        "SELECT DISTINCT region FROM proc_period_snapshot "
        "WHERE region IS NOT NULL AND region!='' ORDER BY region"
    ).fetchall()]
    plants  = conn.execute(
        "SELECT DISTINCT plant_code, plant_name FROM proc_period_snapshot "
        "WHERE plant_code IS NOT NULL AND plant_code!='' ORDER BY plant_name"
    ).fetchall()
    conn.close()
    return jsonify({
        "zones":   zones,
        "regions": regions,
        "plants":  [{"code": r["plant_code"], "name": r["plant_name"]} for r in plants],
    })

# ── ADMIN: DATA FETCH ─────────────────────────────────────────────────────────

_jobs:      dict = {}
_jobs_lock       = threading.Lock()

@app.route("/api/admin/fetch", methods=["POST"])
@admin_required
def trigger_fetch():
    with _jobs_lock:
        for j in _jobs.values():
            if j["status"] == "running":
                return jsonify({"error": "A fetch is already running"}), 409

    job_id = str(uuid.uuid4())[:8]
    job = {
        "job_id":   job_id,
        "status":   "running",
        "started":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stage":    "starting",
        "progress": 0,
        "total":    1,
        "log":      ["Starting fetch..."],
        "result":   None,
    }
    with _jobs_lock:
        _jobs[job_id] = job

    def run():
        from pipeline_v2 import run_full_fetch

        def cb(stage, current, total, message):
            with _jobs_lock:
                _jobs[job_id].update({
                    "stage":    stage,
                    "progress": current,
                    "total":    max(total, 1),
                })
                _jobs[job_id]["log"].append(message)
                _jobs[job_id]["log"] = _jobs[job_id]["log"][-60:]

        try:
            result = run_full_fetch(progress_cb=cb)
            with _jobs_lock:
                _jobs[job_id]["status"] = "done"
                _jobs[job_id]["result"] = result
                _jobs[job_id]["log"].append("✓ Complete")
            # Rebuild cache so next login gets fresh data instantly
            _build_cache_bg()
        except Exception as e:
            with _jobs_lock:
                _jobs[job_id]["status"] = "error"
                _jobs[job_id]["result"] = {"error": str(e)}
                _jobs[job_id]["log"].append(f"✗ Error: {e}")

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
        FROM proc_daily_hpc
        GROUP BY ym ORDER BY ym DESC LIMIT 4
    """).fetchall()

    try:
        from pathlib import Path
        from pipeline_v2 import PARQUET_DIR
        files          = sorted(Path(PARQUET_DIR).glob("proc_*.parquet"))
        latest_parquet = files[-1].stem.replace("proc_", "") if files else None
        parquet_count  = len(files)
    except Exception:
        latest_parquet, parquet_count = None, 0

    runs = conn.execute("""
        SELECT run_at, trigger, status, duration_sec, message
        FROM pipeline_log ORDER BY run_at DESC LIMIT 5
    """).fetchall()
    conn.close()

    # Include cache status so admin can see when cache was last built
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
        }
    })

# ── BOOTSTRAP — served from RAM, zero DB hit ──────────────────────────────────

@app.route("/api/bootstrap")
@login_required
@analytics_required
def bootstrap():
    with _cache_lock:
        payload = _cache.get("payload")
    if not payload:
        # Cache not ready yet (server just started) — try building synchronously
        _build_cache()
        with _cache_lock:
            payload = _cache.get("payload")
    if not payload:
        return jsonify({"error": "Data not ready yet, please try again in a few seconds"}), 503
    return app.response_class(payload, mimetype="application/json")

# ── FILTERS META ──────────────────────────────────────────────────────────────
# Kept for backwards compatibility — not called by the new frontend

@app.route("/api/filters")
@login_required
@analytics_required
def get_filters():
    user  = current_user()
    scope = get_data_scope(user)
    f     = dict(scope)
    conn  = get_db()
    w, params = build_where(f)

    snap_extra = f"snapshot_date=(SELECT MAX(snapshot_date) FROM proc_period_snapshot)"
    snap_w     = (w + f" AND {snap_extra}") if w else ("WHERE " + snap_extra)

    zones   = [r[0] for r in conn.execute(
        f"SELECT DISTINCT zone FROM proc_period_snapshot {snap_w} AND zone IS NOT NULL ORDER BY zone",
        params).fetchall()]
    regions = [r[0] for r in conn.execute(
        f"SELECT DISTINCT region FROM proc_period_snapshot {snap_w} AND region IS NOT NULL ORDER BY region",
        params).fetchall()]
    plants  = conn.execute(
        f"SELECT DISTINCT plant_code, plant_name FROM proc_period_snapshot {snap_w} "
        f"AND plant_code IS NOT NULL ORDER BY plant_name", params).fetchall()
    conn.close()

    return jsonify({
        "zones":   zones,
        "regions": regions,
        "plants":  [{"code": r["plant_code"], "name": r["plant_name"]} for r in plants],
        "scope":   scope,
        "role":    user["role"],
    })

# ── OVERVIEW ──────────────────────────────────────────────────────────────────
# These individual routes are kept for backwards compatibility.
# The new frontend uses /api/bootstrap and never calls these.

@app.route("/api/overview/kpis")
@login_required
@analytics_required
def overview_kpis():
    f    = gf()
    conn = get_db()
    snap = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()[0]
    w, params = build_where(f, extra=f"snapshot_date='{snap}'")

    kpis = conn.execute(f"""
        SELECT
            SUM(mtd)           AS total_mtd,
            SUM(lm)            AS total_lm,
            SUM(lmtd)          AS total_lmtd,
            SUM(lymtd)         AS total_lymtd,
            SUM(mtd_farmers)   AS total_farmers,
            SUM(lm_farmers)    AS total_lm_farmers,
            SUM(lymtd_farmers) AS total_ly_farmers,
            SUM(mtd_payout)    AS total_payout,
            COUNT(*)           AS total_hpcs,
            SUM(CASE WHEN mtd=0 THEN 1 ELSE 0 END)                              AS dark_hpcs,
            SUM(CASE WHEN yoy_growth_pct < -0.2 AND mtd>0 THEN 1 ELSE 0 END)    AS critical_hpcs,
            SUM(CASE WHEN yoy_growth_pct >= -0.2 AND yoy_growth_pct < 0 AND mtd>0 THEN 1 ELSE 0 END) AS declining_hpcs,
            SUM(CASE WHEN yoy_growth_pct >= 0.3 THEN 1 ELSE 0 END)              AS star_hpcs,
            AVG(CASE WHEN mtd_avg_fat > 0 THEN mtd_avg_fat END)                 AS avg_fat,
            AVG(CASE WHEN mtd_rate > 0 THEN mtd_rate END)                       AS avg_rate
        FROM proc_period_snapshot {w}
    """, params).fetchone()
    conn.close()

    mtd   = kpis["total_mtd"]   or 0
    lm    = kpis["total_lm"]    or 0
    lmtd  = kpis["total_lmtd"]  or 0
    lymtd = kpis["total_lymtd"] or 0

    return jsonify({
        "snapshot_date":  snap,
        "mtd":            round(mtd,   0),
        "lm":             round(lm,    0),
        "lmtd":           round(lmtd,  0),
        "lymtd":          round(lymtd, 0),
        "yoy_pct":        round((mtd - lymtd) / lymtd * 100, 1) if lymtd else 0,
        "mom_pct":        round((mtd - lmtd)  / lmtd  * 100, 1) if lmtd  else 0,
        "lm_pct":         round((mtd - lm)    / lm    * 100, 1) if lm    else 0,
        "total_farmers":  int(kpis["total_farmers"]    or 0),
        "lm_farmers":     int(kpis["total_lm_farmers"] or 0),
        "ly_farmers":     int(kpis["total_ly_farmers"] or 0),
        "total_payout":   round(kpis["total_payout"] or 0, 0),
        "total_hpcs":     kpis["total_hpcs"],
        "dark_hpcs":      kpis["dark_hpcs"],
        "critical_hpcs":  kpis["critical_hpcs"],
        "declining_hpcs": kpis["declining_hpcs"],
        "star_hpcs":      kpis["star_hpcs"],
        "avg_fat":        round(kpis["avg_fat"]  or 0, 3),
        "avg_rate":       round(kpis["avg_rate"] or 0, 2),
    })

@app.route("/api/overview/by_zone")
@login_required
@analytics_required
def overview_by_zone():
    f    = gf()
    conn = get_db()
    snap = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()[0]
    w, params = build_where(f, extra=f"snapshot_date='{snap}'")
    rows = conn.execute(f"""
        SELECT zone,
            ROUND(SUM(mtd),0)   AS mtd,   ROUND(SUM(lm),0)    AS lm,
            ROUND(SUM(lymtd),0) AS lymtd, SUM(mtd_farmers)    AS farmers,
            COUNT(*)            AS hpcs,
            SUM(CASE WHEN mtd=0 THEN 1 ELSE 0 END) AS dark,
            AVG(CASE WHEN yoy_growth_pct IS NOT NULL THEN yoy_growth_pct END) AS avg_yoy
        FROM proc_period_snapshot {w}
        GROUP BY zone ORDER BY mtd DESC
    """, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/overview/by_region")
@login_required
@analytics_required
def overview_by_region():
    f    = gf()
    conn = get_db()
    snap = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()[0]
    w, params = build_where(f, extra=f"snapshot_date='{snap}'")
    rows = conn.execute(f"""
        SELECT region, zone,
            ROUND(SUM(mtd),0)    AS mtd,   ROUND(SUM(lm),0)    AS lm,
            ROUND(SUM(lmtd),0)   AS lmtd,  ROUND(SUM(lymtd),0) AS lymtd,
            SUM(mtd_farmers)     AS farmers,
            COUNT(*)             AS hpcs,
            SUM(CASE WHEN mtd=0 THEN 1 ELSE 0 END) AS dark,
            AVG(yoy_growth_pct)  AS avg_yoy,
            AVG(mom_growth_pct)  AS avg_mom,
            AVG(CASE WHEN mtd_avg_fat>0 THEN mtd_avg_fat END) AS avg_fat,
            ROUND(SUM(mtd_payout),0) AS total_payout
        FROM proc_period_snapshot {w}
        GROUP BY region, zone ORDER BY mtd DESC
    """, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/overview/daily_trend")
@login_required
@analytics_required
def overview_daily_trend():
    f    = gf()
    conn = get_db()
    w, params = build_where(f)
    rows = conn.execute(f"""
        SELECT proc_date,
            ROUND(SUM(qty_ltr),0) AS total_ltr,
            SUM(farmer_count)     AS farmers,
            ROUND(AVG(avg_fat),3) AS avg_fat
        FROM proc_daily_hpc {w}
        GROUP BY proc_date ORDER BY proc_date
    """, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

# ── INSIGHTS ──────────────────────────────────────────────────────────────────

@app.route("/api/insights")
@login_required
@analytics_required
def get_insights():
    f    = gf()
    conn = get_db()
    snap = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()[0]
    w, params = build_where(f, extra=f"snapshot_date='{snap}'")
    insights  = []

    dark = conn.execute(f"""
        SELECT COUNT(*) AS cnt, GROUP_CONCAT(hpc_name, ', ') AS names
        FROM (SELECT hpc_name FROM proc_period_snapshot {w} AND mtd=0 AND lm>0
              ORDER BY lm DESC LIMIT 5)
    """, params).fetchone()
    if dark["cnt"] > 0:
        insights.append({"priority": "critical", "category": "Volume Risk",
            "title":  f"{dark['cnt']} HPC(s) went dark this month",
            "detail": f"Had procurement last month but zero MTD. Top: {dark['names']}",
            "action": "Immediate field visit — check farmer mobilisation and route issues."})

    severe = conn.execute(f"""
        SELECT COUNT(*) AS cnt FROM proc_period_snapshot {w} AND yoy_growth_pct < -0.3 AND mtd>0
    """, params).fetchone()
    if severe["cnt"] > 0:
        top = conn.execute(f"""
            SELECT hpc_name, region, ROUND(yoy_growth_pct*100,1) AS yoy
            FROM proc_period_snapshot {w} AND yoy_growth_pct < -0.3 AND mtd>0
            ORDER BY yoy_growth_pct LIMIT 3
        """, params).fetchall()
        insights.append({"priority": "critical", "category": "YoY Decline",
            "title":  f"{severe['cnt']} HPC(s) with >30% YoY decline",
            "detail": " | ".join([f"{r['hpc_name']} ({r['region']}): {r['yoy']}%" for r in top]),
            "action": "Investigate rate competitiveness and farmer satisfaction."})

    bad_regions = conn.execute(f"""
        SELECT region, ROUND(AVG(yoy_growth_pct)*100,1) AS avg_yoy,
               COUNT(*) AS hpcs,
               SUM(CASE WHEN yoy_growth_pct<0 THEN 1 ELSE 0 END) AS declining
        FROM proc_period_snapshot {w} AND yoy_growth_pct IS NOT NULL AND lymtd>0
        GROUP BY region HAVING AVG(yoy_growth_pct) < -0.05 ORDER BY avg_yoy LIMIT 3
    """, params).fetchall()
    for r in bad_regions:
        insights.append({"priority": "warning", "category": "Region Alert",
            "title":  f"{r['region']}: {r['avg_yoy']}% avg YoY",
            "detail": f"{r['declining']} of {r['hpcs']} HPCs declining.",
            "action": f"Review competitor rates in {r['region']}. Check MCC incentive structure."})

    yr_clause  = "yr=(SELECT MAX(yr) FROM proc_monthly_farmer)"
    mth_clause = "mth=(SELECT MAX(mth) FROM proc_monthly_farmer WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))"
    zone_cl    = "AND zone=?"   if f.get("zone")   else ""
    rgn_cl     = "AND region=?" if f.get("region") else ""
    att_params = ([f["zone"]] if f.get("zone") else []) + ([f["region"]] if f.get("region") else [])
    low_att = conn.execute(f"""
        SELECT COUNT(DISTINCT farmer_code) AS cnt
        FROM proc_monthly_farmer
        WHERE {yr_clause} AND {mth_clause} AND delivery_days < 10
        {zone_cl} {rgn_cl}
    """, att_params).fetchone()
    if low_att["cnt"] > 100:
        insights.append({"priority": "warning", "category": "Farmer Risk",
            "title":  f"{low_att['cnt']:,} farmers with <10 delivery days this month",
            "detail": "High absenteeism signals churn — these farmers may be selling to competitors.",
            "action": "Flag to field officers. Check if payment delays are an issue."})

    lf_zone   = "AND h.zone=?"   if f.get("zone")   else ""
    lf_rgn    = "AND h.region=?" if f.get("region") else ""
    lf_params = ([f["zone"]] if f.get("zone") else []) + ([f["region"]] if f.get("region") else [])
    low_fat = conn.execute(f"""
        SELECT h.region, ROUND(AVG(mh.avg_fat),3) AS fat
        FROM proc_monthly_hpc mh
        JOIN (SELECT DISTINCT hpc_plant_key, zone, region FROM proc_daily_hpc) h
          ON mh.hpc_plant_key=h.hpc_plant_key
        WHERE mh.yr=(SELECT MAX(yr) FROM proc_monthly_hpc)
          AND mh.mth=(SELECT MAX(mth) FROM proc_monthly_hpc WHERE yr=(SELECT MAX(yr) FROM proc_monthly_hpc))
          {lf_zone} {lf_rgn}
        GROUP BY h.region HAVING AVG(mh.avg_fat) < 3.8 ORDER BY fat LIMIT 3
    """, lf_params).fetchall()
    for r in low_fat:
        insights.append({"priority": "warning", "category": "Quality Alert",
            "title":  f"{r['region']}: Avg FAT {r['fat']}% — below 3.8%",
            "detail": "Low FAT reduces milk value. May indicate adulteration or breed issues.",
            "action": "Increase testing frequency. Consider breed improvement incentives."})

    star_regions = conn.execute(f"""
        SELECT region, ROUND(AVG(yoy_growth_pct)*100,1) AS avg_yoy, COUNT(*) AS hpcs
        FROM proc_period_snapshot {w} AND yoy_growth_pct IS NOT NULL AND lymtd>0
        GROUP BY region HAVING AVG(yoy_growth_pct) > 0.25 ORDER BY avg_yoy DESC LIMIT 2
    """, params).fetchall()
    for r in star_regions:
        insights.append({"priority": "positive", "category": "Growth Opportunity",
            "title":  f"{r['region']}: +{r['avg_yoy']}% YoY — top performing",
            "detail": f"{r['hpcs']} HPCs growing strongly.",
            "action": "Document best practices here and replicate to lagging regions."})

    total_mtd = conn.execute(f"SELECT SUM(mtd) FROM proc_period_snapshot {w}", params).fetchone()[0] or 1
    top20_n   = max(1, int(conn.execute(f"SELECT COUNT(*) FROM proc_period_snapshot {w}", params).fetchone()[0] * 0.2))
    top20_vol = conn.execute(f"""
        SELECT SUM(mtd) FROM (
            SELECT mtd FROM proc_period_snapshot {w} ORDER BY mtd DESC LIMIT {top20_n}
        )
    """, params).fetchone()[0] or 0
    conc = round(top20_vol / total_mtd * 100, 1)
    if conc > 70:
        insights.append({"priority": "warning", "category": "Concentration Risk",
            "title":  f"Top 20% HPCs contribute {conc}% of volume",
            "detail": "High concentration means any disruption has outsized impact.",
            "action": "Prioritise farmer expansion in medium-sized HPCs."})

    conn.close()
    return jsonify(insights)

# ── HPC DRILLDOWN ─────────────────────────────────────────────────────────────

@app.route("/api/hpc/list")
@login_required
@analytics_required
def hpc_list():
    f    = gf()
    conn = get_db()
    snap = conn.execute("SELECT MAX(snapshot_date) FROM proc_period_snapshot").fetchone()[0]
    w, params = build_where(f, extra=f"snapshot_date='{snap}'")
    rows = conn.execute(f"""
        SELECT hpc_plant_key, hpc_name, plant_name, region, zone,
            ROUND(mtd,1)  AS mtd,  ROUND(lm,1)    AS lm,
            ROUND(lmtd,1) AS lmtd, ROUND(lymtd,1) AS lymtd,
            ROUND(yoy_growth_pct*100,1) AS yoy_pct,
            ROUND(mom_growth_pct*100,1) AS mom_pct,
            ROUND(mtd_farmers,0) AS farmers,
            ROUND(mtd_avg_fat,3) AS avg_fat,
            ROUND(mtd_rate,2)    AS rate,
            ROUND(mtd_payout,0)  AS payout
        FROM proc_period_snapshot {w}
        ORDER BY mtd DESC
    """, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/hpc/detail")
@login_required
@analytics_required
def hpc_detail():
    hpc = request.args.get("hpc_plant_key", "")
    if not hpc:
        return jsonify({"error": "hpc_plant_key required"}), 400
    conn = get_db()

    daily = conn.execute("""
        SELECT proc_date,
            ROUND(SUM(qty_ltr),0) AS qty_ltr,
            SUM(farmer_count)     AS farmers,
            ROUND(AVG(avg_fat),3) AS avg_fat
        FROM proc_daily_hpc WHERE hpc_plant_key=?
        GROUP BY proc_date ORDER BY proc_date DESC LIMIT 60
    """, (hpc,)).fetchall()

    monthly = conn.execute("""
        SELECT yr, mth, ROUND(lpd,1) AS lpd,
               ROUND(avg_fat,3) AS avg_fat,
               ROUND(avg_rate_per_ltr,2) AS rate,
               farmer_count, dumping_incidents
        FROM proc_monthly_hpc WHERE hpc_plant_key=?
        ORDER BY yr DESC, mth DESC LIMIT 24
    """, (hpc,)).fetchall()

    top_farmers = conn.execute("""
        SELECT farmer_code, farmer_name, milk_type,
               ROUND(lpd,1) AS lpd, delivery_days,
               ROUND(avg_fat,3) AS avg_fat,
               ROUND(total_net_price,0) AS payout
        FROM proc_monthly_farmer
        WHERE hpc_plant_key=?
          AND yr=(SELECT MAX(yr) FROM proc_monthly_farmer)
          AND mth=(SELECT MAX(mth) FROM proc_monthly_farmer WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))
        ORDER BY total_qty_ltr DESC LIMIT 20
    """, (hpc,)).fetchall()

    conn.close()
    return jsonify({
        "daily":       [dict(r) for r in daily],
        "monthly":     [dict(r) for r in monthly],
        "top_farmers": [dict(r) for r in top_farmers],
    })

# ── FARMER ANALYTICS ─────────────────────────────────────────────────────────

@app.route("/api/farmers/kpis")
@login_required
@analytics_required
def farmers_kpis():
    f      = gf()
    conn   = get_db()
    yr_mth = ("yr=(SELECT MAX(yr) FROM proc_monthly_farmer) AND "
               "mth=(SELECT MAX(mth) FROM proc_monthly_farmer "
               "WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))")
    zone_cl = "AND zone=?"   if f.get("zone")   else ""
    rgn_cl  = "AND region=?" if f.get("region") else ""
    params  = ([f["zone"]] if f.get("zone") else []) + ([f["region"]] if f.get("region") else [])

    r = conn.execute(f"""
        SELECT COUNT(DISTINCT farmer_code) AS total_farmers,
               ROUND(AVG(lpd),1)           AS avg_lpd,
               ROUND(AVG(avg_fat),3)       AS avg_fat,
               SUM(CASE WHEN delivery_days >= 25 THEN 1 ELSE 0 END) AS consistent,
               SUM(CASE WHEN delivery_days < 10  THEN 1 ELSE 0 END) AS irregular,
               ROUND(SUM(total_qty_ltr),0)   AS total_ltr,
               ROUND(SUM(total_net_price),0) AS total_payout
        FROM proc_monthly_farmer
        WHERE {yr_mth} {zone_cl} {rgn_cl}
    """, params).fetchone()
    conn.close()
    return jsonify(dict(r))

@app.route("/api/farmers/attendance")
@login_required
@analytics_required
def farmers_attendance():
    f      = gf()
    conn   = get_db()
    yr_mth = ("yr=(SELECT MAX(yr) FROM proc_monthly_farmer) AND "
               "mth=(SELECT MAX(mth) FROM proc_monthly_farmer "
               "WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))")
    zone_cl = "AND zone=?"   if f.get("zone")   else ""
    rgn_cl  = "AND region=?" if f.get("region") else ""
    params  = ([f["zone"]] if f.get("zone") else []) + ([f["region"]] if f.get("region") else [])

    rows = conn.execute(f"""
        SELECT
            CASE
                WHEN delivery_days <= 5  THEN '1-5 days'
                WHEN delivery_days <= 10 THEN '6-10 days'
                WHEN delivery_days <= 15 THEN '11-15 days'
                WHEN delivery_days <= 20 THEN '16-20 days'
                WHEN delivery_days <= 25 THEN '21-25 days'
                ELSE '26+ days'
            END AS bucket,
            COUNT(*) AS farmers
        FROM proc_monthly_farmer
        WHERE {yr_mth} {zone_cl} {rgn_cl}
        GROUP BY bucket ORDER BY MIN(delivery_days)
    """, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/farmers/top")
@login_required
@analytics_required
def farmers_top():
    f    = gf()
    sort = request.args.get("sort", "total_qty_ltr")
    if sort not in ("total_qty_ltr", "lpd", "avg_fat", "delivery_days", "total_net_price"):
        sort = "total_qty_ltr"

    conn   = get_db()
    yr_mth = ("yr=(SELECT MAX(yr) FROM proc_monthly_farmer) AND "
               "mth=(SELECT MAX(mth) FROM proc_monthly_farmer "
               "WHERE yr=(SELECT MAX(yr) FROM proc_monthly_farmer))")
    zone_cl = "AND zone=?"   if f.get("zone")   else ""
    rgn_cl  = "AND region=?" if f.get("region") else ""
    params  = ([f["zone"]] if f.get("zone") else []) + ([f["region"]] if f.get("region") else [])

    rows = conn.execute(f"""
        SELECT farmer_code, farmer_name, hpc_plant_key, region, zone, milk_type,
               ROUND(total_qty_ltr,0)    AS total_qty_ltr,
               ROUND(lpd,1)              AS lpd,
               delivery_days,
               ROUND(avg_fat,3)          AS avg_fat,
               ROUND(avg_snf,3)          AS avg_snf,
               ROUND(total_net_price,0)  AS total_net_price,
               ROUND(avg_rate_per_ltr,2) AS avg_rate_per_ltr,
               dumping_incidents
        FROM proc_monthly_farmer
        WHERE {yr_mth} {zone_cl} {rgn_cl}
        ORDER BY {sort} DESC LIMIT 200
    """, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

# ── QUALITY & COST ────────────────────────────────────────────────────────────

@app.route("/api/quality/summary")
@login_required
@analytics_required
def quality_summary():
    f      = gf()
    conn   = get_db()
    yr_mth = ("mh.yr=(SELECT MAX(yr) FROM proc_monthly_hpc) AND "
               "mh.mth=(SELECT MAX(mth) FROM proc_monthly_hpc "
               "WHERE yr=(SELECT MAX(yr) FROM proc_monthly_hpc))")
    join_clause = extra_where = ""
    params = []
    if f.get("zone") or f.get("region") or f.get("plant_code"):
        join_clause = """JOIN (SELECT DISTINCT hpc_plant_key, zone, region, plant_code
                               FROM proc_daily_hpc) h ON mh.hpc_plant_key=h.hpc_plant_key"""
        parts = []
        if f.get("zone"):       parts.append("h.zone=?");       params.append(f["zone"])
        if f.get("region"):     parts.append("h.region=?");     params.append(f["region"])
        if f.get("plant_code"): parts.append("h.plant_code=?"); params.append(f["plant_code"])
        extra_where = " AND " + " AND ".join(parts)

    fs = conn.execute(f"""
        SELECT ROUND(AVG(avg_fat),3) AS avg_fat, ROUND(AVG(avg_snf),3) AS avg_snf,
               ROUND(MIN(avg_fat),3) AS min_fat, ROUND(MAX(avg_fat),3) AS max_fat,
               SUM(dumping_incidents) AS dumps,
               ROUND(SUM(total_mcc_incentive),0)  AS mcc,
               ROUND(SUM(total_qty_incentive),0)  AS qty_inc,
               ROUND(SUM(total_bonus),0)           AS bonus,
               ROUND(SUM(total_base_price),0)      AS base,
               ROUND(SUM(total_net_price),0)       AS net,
               ROUND(AVG(incentive_pct),2)         AS avg_incentive_pct,
               ROUND(AVG(avg_cost_per_fat_kg),2)   AS avg_cost_fat_kg,
               SUM(morning_ltr) AS morning_ltr,
               SUM(evening_ltr) AS evening_ltr
        FROM proc_monthly_hpc mh {join_clause}
        WHERE {yr_mth}{extra_where}
    """, params).fetchone()

    latest_ym = conn.execute("""
        SELECT yr || '-' || printf('%02d',mth)
        FROM proc_monthly_hpc ORDER BY yr DESC, mth DESC LIMIT 1
    """).fetchone()[0]

    incidents = conn.execute(f"""
        SELECT incident_type, COUNT(*) AS cnt
        FROM proc_quality_incidents qi
        {("JOIN (SELECT DISTINCT hpc_plant_key, zone, region, plant_code FROM proc_daily_hpc) h "
          "ON qi.hpc_plant_key=h.hpc_plant_key") if (f.get("zone") or f.get("region") or f.get("plant_code")) else ""}
        WHERE strftime('%Y-%m', qi.proc_date) = ?
        {("AND h.zone='"   + f["zone"]       + "'") if f.get("zone")   else ""}
        {("AND h.region='" + f["region"]     + "'") if f.get("region") else ""}
        GROUP BY incident_type ORDER BY cnt DESC
    """, [latest_ym]).fetchall()

    conn.close()
    return jsonify({"fat_snf": dict(fs), "incidents": [dict(r) for r in incidents]})

@app.route("/api/quality/by_region")
@login_required
@analytics_required
def quality_by_region():
    conn = get_db()
    rows = conn.execute("""
        SELECT h.region,
            ROUND(AVG(mh.avg_fat),3)           AS avg_fat,
            ROUND(AVG(mh.avg_snf),3)           AS avg_snf,
            SUM(mh.dumping_incidents)           AS dumps,
            COUNT(*)                            AS hpcs,
            ROUND(AVG(mh.incentive_pct),2)      AS incentive_pct,
            ROUND(AVG(mh.avg_cost_per_fat_kg),2) AS cost_fat_kg,
            ROUND(AVG(mh.avg_rate_per_ltr),2)   AS rate_per_ltr
        FROM proc_monthly_hpc mh
        JOIN (SELECT DISTINCT hpc_plant_key, region FROM proc_daily_hpc) h
          ON mh.hpc_plant_key=h.hpc_plant_key
        WHERE mh.yr=(SELECT MAX(yr) FROM proc_monthly_hpc)
          AND mh.mth=(SELECT MAX(mth) FROM proc_monthly_hpc WHERE yr=(SELECT MAX(yr) FROM proc_monthly_hpc))
          AND h.region IS NOT NULL
        GROUP BY h.region ORDER BY avg_fat DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/quality/daily_trend")
@login_required
@analytics_required
def quality_daily_trend():
    f    = gf()
    conn = get_db()
    w, params = build_where(f)
    rows = conn.execute(f"""
        SELECT proc_date,
            ROUND(SUM(total_fat_kg)/NULLIF(SUM(qty_kg),0)*100, 3) AS fat,
            ROUND(SUM(total_snf_kg)/NULLIF(SUM(qty_kg),0)*100, 3) AS snf,
            SUM(quality_incidents)                                  AS incidents,
            ROUND(SUM(total_net_price)/NULLIF(SUM(qty_ltr),0), 2)  AS rate
        FROM proc_daily_hpc {w}
        GROUP BY proc_date ORDER BY proc_date
    """, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/quality/top_offenders")
@login_required
@analytics_required
def quality_offenders():
    f    = gf()
    conn = get_db()
    latest_ym = conn.execute("""
        SELECT yr || '-' || printf('%02d',mth)
        FROM proc_monthly_hpc ORDER BY yr DESC, mth DESC LIMIT 1
    """).fetchone()[0]
    w, params = build_where(f, "h")
    rows = conn.execute(f"""
        SELECT q.hpc_plant_key,
            MAX(d.hpc_name)   AS hpc_name,
            MAX(d.plant_name) AS plant_name,
            h.region, h.zone,
            COUNT(*)            AS incidents,
            ROUND(AVG(q.fat),3) AS avg_fat,
            ROUND(AVG(q.snf),3) AS avg_snf
        FROM proc_quality_incidents q
        JOIN (SELECT DISTINCT hpc_plant_key, zone, region, plant_code FROM proc_daily_hpc) h
          ON q.hpc_plant_key=h.hpc_plant_key
        JOIN (SELECT DISTINCT hpc_plant_key, hpc_name, plant_name FROM proc_daily_hpc) d
          ON q.hpc_plant_key=d.hpc_plant_key
        {w}
        AND strftime('%Y-%m', q.proc_date) = ?
        GROUP BY q.hpc_plant_key ORDER BY incidents DESC LIMIT 20
    """, params + [latest_ym]).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/quality/incentive_by_region")
@login_required
@analytics_required
def incentive_by_region():
    conn = get_db()
    rows = conn.execute("""
        SELECT h.region,
            ROUND(SUM(mh.total_base_price),0)    AS base,
            ROUND(SUM(mh.total_mcc_incentive),0) AS mcc,
            ROUND(SUM(mh.total_qty_incentive),0) AS qty_inc,
            ROUND(SUM(mh.total_bonus),0)          AS bonus,
            ROUND(SUM(mh.total_net_price),0)      AS net,
            ROUND(AVG(mh.incentive_pct),2)        AS incentive_pct,
            ROUND(AVG(mh.avg_cost_per_fat_kg),2)  AS cost_fat_kg,
            ROUND(AVG(mh.avg_rate_per_ltr),2)     AS rate
        FROM proc_monthly_hpc mh
        JOIN (SELECT DISTINCT hpc_plant_key, region FROM proc_daily_hpc) h
          ON mh.hpc_plant_key=h.hpc_plant_key
        WHERE mh.yr=(SELECT MAX(yr) FROM proc_monthly_hpc)
          AND mh.mth=(SELECT MAX(mth) FROM proc_monthly_hpc WHERE yr=(SELECT MAX(yr) FROM proc_monthly_hpc))
        GROUP BY h.region ORDER BY net DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

# ── RUN ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_db()
    from pipeline import _ensure_tables, _ensure_indexes, _ensure_default_admin
    _ensure_tables(conn)
    _ensure_indexes(conn)
    _ensure_default_admin(conn)
    conn.close()
    _build_cache_bg()   # build cache in background while Flask starts up
    app.run(debug=True, port=5001, threaded=True)