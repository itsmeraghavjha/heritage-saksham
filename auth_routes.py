"""
auth_routes.py — Drop these routes into app.py
Replace the existing login/logout routes with these.
Also ensure app.secret_key is set.
"""

# ── At the top of app.py, add if not already present: ──────────────────────────
#   from flask import Flask, request, jsonify, session, redirect, send_file
#   from werkzeug.security import check_password_hash
#   app.secret_key = 'heritage-procurement-secret-2024-changeme'

# ── HELPER ─────────────────────────────────────────────────────────────────────
def get_current_user():
    """Return user dict from session, or None."""
    if 'user_id' not in session:
        return None
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE id=? AND is_active=1", (session['user_id'],)
    ).fetchone()
    conn.close()
    return dict(user) if user else None

def require_login(f):
    """Decorator: redirect to /login if not logged in."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not get_current_user():
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({'error': 'Unauthorised', 'redirect': '/login'}), 401
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated

# ── ROUTES ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    user = get_current_user()
    if not user:
        return redirect('/login')
    if user['role'] == 'admin':
        return redirect('/admin')
    return send_file('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        if get_current_user():
            return redirect('/')
        return send_file('login.html')

    # POST — JSON body from fetch()
    data     = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''

    if not username or not password:
        return jsonify({'success': False, 'error': 'Username and password required'}), 400

    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE username=? AND is_active=1", (username,)
    ).fetchone()

    if not user or not check_password_hash(user['password_hash'], password):
        conn.close()
        log.warning(f"Failed login for: {username}")
        return jsonify({'success': False, 'error': 'Invalid username or password'}), 401

    # Record login time
    conn.execute("UPDATE users SET last_login=datetime('now') WHERE id=?", (user['id'],))
    conn.commit()
    conn.close()

    session.permanent = True
    session['user_id'] = user['id']
    log.info(f"Login: {username} ({user['role']})")

    redirect_url = '/admin' if user['role'] == 'admin' else '/'
    return jsonify({
        'success':   True,
        'full_name': user['full_name'],
        'role':      user['role'],
        'redirect':  redirect_url,
    })

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

# ── CURRENT USER API (for frontend to know who is logged in) ───────────────────
@app.route('/api/me')
def api_me():
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Not logged in'}), 401
    return jsonify({
        'username':     user['username'],
        'full_name':    user['full_name'],
        'role':         user['role'],
        'scope_zone':   user['scope_zone'],
        'scope_region': user['scope_region'],
        'scope_plant':  user['scope_plant'],
    })

# ── BOOTSTRAP — enforce scope server-side ─────────────────────────────────────
# Replace your existing /api/bootstrap with this:

@app.route('/api/bootstrap')
@require_login
def api_bootstrap():
    user = get_current_user()
    # Return cached payload filtered by user scope
    with _cache_lock:
        payload = _cache.get('payload')
    if payload is None:
        return jsonify({'error': 'Cache not ready — try again in a few seconds'}), 503

    import json, copy
    data = json.loads(payload)

    role   = user['role']
    zone   = user['scope_zone']
    region = user['scope_region']
    plant  = user['scope_plant']

    # CXO and admin see everything
    if role in ('admin', 'cxo'):
        return payload, 200, {'Content-Type': 'application/json'}

    # Scoped filter
    def f_zone(item):
        return not zone or item.get('zone') == zone

    def f_region(item):
        return (not zone   or item.get('zone')   == zone) and \
               (not region or item.get('region') == region)

    def f_plant(item):
        return (not zone   or item.get('zone')      == zone) and \
               (not region or item.get('region')    == region) and \
               (not plant  or str(item.get('plant_code','')) == str(plant))

    def pick(arr, fn):
        return [x for x in arr if fn(x)]

    filtered = {
        'snapshot':          data['snapshot'],           # company-wide always shown for context
        'hpc_list':          pick(data.get('hpc_list',[]),          f_plant  if role=='plant' else f_region if role=='rh' else f_zone),
        'monthly_hpc':       pick(data.get('monthly_hpc',[]),       f_plant  if role=='plant' else f_region if role=='rh' else f_zone),
        'monthly_farmer':    pick(data.get('monthly_farmer',[]),    f_plant  if role=='plant' else f_region if role=='rh' else f_zone),
        'daily':             pick(data.get('daily',[]),              f_region if role in ('rh','plant') else f_zone),
        'quality_incidents': pick(data.get('quality_incidents',[]), f_plant  if role=='plant' else f_region if role=='rh' else f_zone),
        'farmer_health':     pick(data.get('farmer_health',[]),     f_plant  if role=='plant' else f_region if role=='rh' else f_zone),
        'farmer_rfm':        pick(data.get('farmer_rfm',[]),        f_plant  if role=='plant' else f_region if role=='rh' else f_zone),
        'insights':          data.get('insights', []),
        'filters':           _build_scoped_filters(data, role, zone, region, plant),
        'user': {
            'role':         role,
            'full_name':    user['full_name'],
            'scope_zone':   zone,
            'scope_region': region,
            'scope_plant':  plant,
        },
    }

    # Recompute snapshot for scoped users from their filtered hpc_list
    if filtered['hpc_list']:
        hpcs = filtered['hpc_list']
        mhpc = filtered['monthly_hpc']
        mtd   = sum(h.get('mtd',0) or 0 for h in hpcs)
        lymtd = sum(h.get('lymtd',0) or 0 for h in hpcs)
        lmtd  = sum(h.get('lmtd',0) or 0 for h in hpcs)
        pay   = sum(m.get('total_net_price',0) or 0 for m in mhpc)
        farmers = sum(h.get('mtd_farmers',0) or 0 for h in hpcs)
        lm_farmers = sum(h.get('lm_farmers',0) or 0 for h in hpcs)
        dark  = len([h for h in hpcs if not h.get('mtd')])
        stars = len([h for h in hpcs if (h.get('yoy_growth_pct') or 0) >= 0.3 and h.get('mtd')])
        crits = len([h for h in hpcs if (h.get('yoy_growth_pct') or 0) < -0.3 and h.get('mtd')])
        filtered['snapshot'] = {
            **data['snapshot'],
            'mtd': mtd, 'lymtd': lymtd, 'lmtd': lmtd,
            'total_payout': pay,
            'total_farmers': farmers, 'lm_farmers': lm_farmers,
            'total_hpcs': len(hpcs), 'dark_hpcs': dark,
            'star_hpcs': stars, 'critical_hpcs': crits,
            'yoy_pct': round((mtd-lymtd)/lymtd*100, 1) if lymtd else 0,
            'mom_pct': round((mtd-lmtd)/lmtd*100, 1) if lmtd else 0,
        }

    return jsonify(filtered)

def _build_scoped_filters(data, role, zone, region, plant):
    """Return filter options limited to the user's scope."""
    base = data.get('filters', {})
    if role in ('admin', 'cxo'):
        return base

    hpcs = data.get('hpc_list', [])
    if zone:
        hpcs = [h for h in hpcs if h.get('zone') == zone]
    if region:
        hpcs = [h for h in hpcs if h.get('region') == region]
    if plant:
        hpcs = [h for h in hpcs if str(h.get('plant_code','')) == str(plant)]

    zones   = sorted({h['zone']   for h in hpcs if h.get('zone')})
    regions = sorted({h['region'] for h in hpcs if h.get('region')})
    plants  = []
    seen = set()
    for h in hpcs:
        pc = str(h.get('plant_code',''))
        if pc and pc not in seen:
            seen.add(pc)
            plants.append({'code': pc, 'name': h.get('plant_name', pc)})

    return {'zones': zones, 'regions': regions, 'plants': plants}