import os
import time
import json
from math import radians, sin, cos, sqrt, atan2, asin, pi
from datetime import datetime, timezone

from flask import Flask, request, jsonify
import requests
import gpxpy
import gpxpy.gpx

# ======================================
# CONFIGURAZIONE
# ======================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")
VALHALLA_URL = os.environ.get("VALHALLA_URL", "").rstrip("/")
VALHALLA_URL_FALLBACK = os.environ.get("VALHALLA_URL_FALLBACK", "").rstrip("/")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
AUTH_USERS_CSV = os.environ.get("AUTH_USERS_CSV", "").strip()

MAX_WAYPOINTS = 4
MAX_ROUTE_KM = 120
MAX_RADIUS_KM = 80
RT_TARGET_MIN = 70
RT_TARGET_MAX = 80
RATE_LIMIT_DAYS = 7

app = Flask(__name__)

# ======================================
# STATO UTENTE
# ======================================

USER_STATE = {}
AUTHORIZED = set()
PENDING = set()
LAST_DOWNLOAD = {}

# Carica utenti autorizzati da variabile d'ambiente
if AUTH_USERS_CSV:
    for _id in AUTH_USERS_CSV.split(","):
        _id = _id.strip()
        if _id.isdigit():
            AUTHORIZED.add(int(_id))

# Owner sempre autorizzato
if OWNER_ID:
    AUTHORIZED.add(OWNER_ID)

# ======================================
# UTILITY
# ======================================

def now_epoch():
    """Restituisce timestamp corrente in secondi."""
    return time.time()

def epoch_to_str(e):
    """Converte timestamp in stringa leggibile."""
    try:
        dt = datetime.fromtimestamp(e, tz=timezone.utc).astimezone()
        return dt.strftime("%d/%m/%Y %H:%M")
    except:
        return "più tardi"

def haversine_km(a, b):
    """Distanza approssimata tra due coordinate (km)."""
    R = 6371.0
    lat1, lon1 = radians(a[0]), radians(a[1])
    lat2, lon2 = radians(b[0]), radians(b[1])
    dlat = lat2 - lat1
    dlon = radians(b[1] - a[1])
    h = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * atan2(sqrt(h), sqrt(1-h))

def approx_total_km_from_locs(locs, roundtrip):
    """Calcola distanza approssimata tra i punti forniti."""
    if not locs or len(locs) < 2:
        return 0.0
    total = 0.0
    for i in range(len(locs)-1):
        a = (locs[i]["lat"], locs[i]["lon"])
        b = (locs[i+1]["lat"], locs[i+1]["lon"])
        total += haversine_km(a, b)
    if roundtrip:
        a = (locs[-1]["lat"], locs[-1]["lon"])
        b = (locs[0]["lat"], locs[0]["lon"])
        total += haversine_km(a, b)
    return total

# ======================================
# TELEGRAM HELPERS
# ======================================

def send_message(chat_id, text, reply_markup=None):
    """Invia messaggio Telegram."""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    requests.post(url, json=payload, timeout=15)

def send_document(chat_id, file_bytes, filename, caption=None):
    """Invia file GPX."""
    url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
    files = {"document": (filename, file_bytes, "application/octet-stream")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    requests.post(url, data=data, files=files, timeout=30)

def send_photo(chat_id, file_bytes, caption=None):
    """Invia immagine PNG."""
    url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
    files = {"photo": ("route.png", file_bytes, "image/png")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    requests.post(url, data=data, files=files, timeout=30)

def answer_callback_query(cq_id, text=None):
    """Risponde ai pulsanti inline."""
    url = f"https://api.telegram.org/bot{TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": cq_id}
    if text:
        payload["text"] = text
    requests.post(url, json=payload, timeout=10)

# ======================================
# GEOCODING
# ======================================

def geocode_address(q):
    """Converte indirizzo in coordinate tramite Nominatim."""
    if not q:
        return None
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": q, "format": "json", "limit": 1}
    headers = {"User-Agent": "MotoRouteBot/1.1"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data:
            return None
        return (float(data[0]["lat"]), float(data[0]["lon"]))
    except:
        return None

def parse_location_from_message(msg):
    """Estrae coordinate da messaggio Telegram."""
    if "location" in msg:
        loc = msg["location"]
        return (loc["latitude"], loc["longitude"])
    text = (msg.get("text") or "").strip()
    if not text:
        return None
    return geocode_address(text)

# ======================================
# GPX
# ======================================

def build_gpx_with_turns(coords, maneuvers, name="Percorso Moto"):
    """Genera GPX con turn-by-turn."""
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    for lat, lon in coords:
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon))

    for m in maneuvers:
        lat = m.get("lat")
        lon = m.get("lon")
        instr = m.get("instruction", "")
        if lat is None or lon is None:
            continue
        gpx.waypoints.append(gpxpy.gpx.GPXWaypoint(latitude=lat, longitude=lon, name=instr))

    return gpx.to_xml().encode("utf-8")

def build_gpx_simple(coords, name="Percorso Moto (semplice)"):
    """Genera GPX semplice senza turn-by-turn."""
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    for lat, lon in coords:
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon))

    return gpx.to_xml().encode("utf-8")

# ======================================
# POLYLINE DECODER
# ======================================

def decode_polyline6(polyline_str):
    """Decodifica polyline Valhalla (precisione 1e-6)."""
    index, lat, lng, coords = 0, 0, 0, []
    changes = {"lat": 0, "lng": 0}
    while index < len(polyline_str):
        for unit in ("lat", "lng"):
            shift, result = 0, 0
            while True:
                b = ord(polyline_str[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            changes[unit] = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += changes["lat"]
        lng += changes["lng"]
        coords.append((lat / 1e6, lng / 1e6))
    return coords
# ======================================
# VALHALLA COSTING
# ======================================

def post_valhalla(url, payload, timeout=30, retries=1):
    """Invia richiesta a Valhalla con retry."""
    last_err = None
    for _ in range(retries + 1):
        try:
            r = requests.post(f"{url}/route", json=payload, timeout=timeout)
            if r.ok:
                return r.json()
            last_err = f"{r.status_code} {r.text[:200]}"
        except Exception as e:
            last_err = str(e)
        time.sleep(0.4)
    raise RuntimeError(last_err or "Errore Valhalla")

def build_motorcycle_costing(style, is_owner):
    """Restituisce i parametri di costing per i vari livelli di curvatura."""
    
    # STANDARD (per tutti)
    if style == "standard":
        return {
            "use_highways": 0.5,
            "use_primary": 0.8,
            "use_secondary": 1.0,
            "use_tertiary": 1.0,
            "use_curves": 0.0,
            "use_hills": 0.0,
            "exclude_unpaved": True,
            "avoid_bad_surfaces": True
        }

    # CURVY LEGGERO (per tutti)
    if style == "curvy_light":
        return {
            "use_highways": 0.0,
            "use_primary": 0.4,
            "use_secondary": 0.9,
            "use_tertiary": 1.1,
            "use_curves": 0.6,
            "use_hills": 0.4,
            "exclude_unpaved": True,
            "avoid_bad_surfaces": True
        }

    # CURVY (solo owner)
    if style == "curvy":
        if not is_owner:
            return None
        return {
            "use_highways": 0.0,
            "use_primary": 0.2,
            "use_secondary": 1.0,
            "use_tertiary": 1.3,
            "use_curves": 1.4,
            "use_hills": 0.8,
            "exclude_unpaved": True,
            "avoid_bad_surfaces": True
        }

    # SUPER CURVY (solo owner)
    if style == "super_curvy":
        if not is_owner:
            return None
        return {
            "use_highways": 0.0,
            "use_primary": 0.1,
            "use_secondary": 0.8,
            "use_tertiary": 1.5,
            "use_living_streets": 1.6,
            "use_curves": 2.2,
            "use_hills": 1.2,
            "exclude_unpaved": True,
            "avoid_bad_surfaces": True
        }

    return None

# ======================================
# VALHALLA ROUTING
# ======================================

def valhalla_route(locations, style="standard", roundtrip=False, is_owner=False):
    """Invia richiesta a Valhalla e decodifica percorso, distanza, tempo e manovre."""
    if not VALHALLA_URL:
        raise RuntimeError("VALHALLA_URL non configurato.")

    locs = list(locations)

    # Chiudi anello se roundtrip
    if roundtrip:
        s = locs[0]
        locs.append({"lat": s["lat"], "lon": s["lon"]})

    costing_opts = build_motorcycle_costing(style, is_owner)
    if costing_opts is None:
        raise PermissionError("premium_blocked")

    payload = {
        "locations": locs,
        "costing": "motorcycle",
        "costing_options": {"motorcycle": costing_opts},
        "directions_options": {"units": "kilometers"},
    }

    urls_to_try = [VALHALLA_URL]
    if VALHALLA_URL_FALLBACK and VALHALLA_URL_FALLBACK != VALHALLA_URL:
        urls_to_try.append(VALHALLA_URL_FALLBACK)

    resp_json = None
    last_err = None

    for url in urls_to_try:
        try:
            j = post_valhalla(url, payload, timeout=30, retries=1)
            if "trip" in j:
                resp_json = j
                break
            last_err = json.dumps(j)[:200]
        except Exception as e:
            last_err = str(e)

    if not resp_json:
        raise RuntimeError(f"Valhalla error: {last_err}")

    trip = resp_json.get("trip", {})
    coords = []
    total_km = 0.0
    total_min = 0.0
    maneuvers_out = []

    for leg in trip.get("legs", []):
        shape = leg.get("shape")
        if shape:
            coords.extend(decode_polyline6(shape))

        summary = leg.get("summary", {})
        total_km += float(summary.get("length", 0.0))
        total_min += float(summary.get("time", 0.0)) / 60.0

        for m in leg.get("maneuvers", []):
            instr = m.get("instruction", "")
            idx = m.get("begin_shape_index", 0)
            if 0 <= idx < len(coords):
                lat, lon = coords[idx]
                maneuvers_out.append({
                    "instruction": instr,
                    "lat": lat,
                    "lon": lon
                })

    return coords, round(total_km, 1), round(total_min, 1), maneuvers_out

# ======================================
# STATIC MAP (OSM)
# ======================================

def simplify_coords(coords, max_points=80):
    """Riduce numero punti per evitare URL troppo lunghe."""
    if len(coords) <= max_points:
        return coords
    step = max(1, len(coords) // max_points)
    return [coords[i] for i in range(0, len(coords), step)]

def build_static_map_url(coords, start, end, waypoints):
    """Costruisce URL static map OSM."""
    if not coords:
        return None

    mid = coords[len(coords)//2]
    center_lat, center_lon = mid[0], mid[1]

    zoom = 12
    size = "800x800"

    path = "weight:3|color:red"
    for lat, lon in coords:
        path += f"|{lat},{lon}"

    markers = []
    if start:
        markers.append(f"color:green|{start[0]},{start[1]}")
    if end:
        markers.append(f"color:red|{end[0]},{end[1]}")
    for w in waypoints or []:
        markers.append(f"color:yellow|{w[0]},{w[1]}")

    markers_param = "&".join([f"markers={m}" for m in markers]) if markers else ""

    url = (
        "https://staticmap.openstreetmap.de/staticmap.php?"
        f"center={center_lat},{center_lon}"
        f"&zoom={zoom}"
        f"&size={size}"
        f"&path={path}"
    )
    if markers_param:
        url += f"&{markers_param}"

    return url

def download_static_map(url):
    """Scarica PNG della mappa."""
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.content
    except:
        pass
    return None

# ======================================
# ROUTES DI SERVIZIO
# ======================================

@app.route("/", methods=["GET"])
def home():
    return "OK - MotoRoute Bot (Valhalla) online."

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify(
        status="ok",
        valhalla=bool(VALHALLA_URL),
        fallback=bool(VALHALLA_URL_FALLBACK),
        max_wp=MAX_WAYPOINTS,
        owner=bool(OWNER_ID),
        authorized=len(AUTHORIZED)
    )
# ======================================
# WEBHOOK
# ======================================

@app.route("/webhook/<path:token>", methods=["POST"])
def webhook(token):
    if token != TOKEN:
        return jsonify(ok=False, error="forbidden"), 403

    update = request.get_json(silent=True) or {}

    # ---------------------------------------------------------
    # CALLBACK QUERY
    # ---------------------------------------------------------
    if "callback_query" in update:
        cq = update["callback_query"]
        data = cq.get("data", "")
        chat_id = cq["message"]["chat"]["id"]
        uid = cq["from"]["id"]
        uname = cq["from"].get("first_name", "Utente")

        answer_callback_query(cq["id"])

        if uid not in USER_STATE:
            reset_state(uid)
        state = USER_STATE[uid]

        # --- Admin approval ---
        if data.startswith("admin:"):
            if uid != OWNER_ID:
                return jsonify(ok=True)
            parts = data.split(":")
            if len(parts) == 3 and parts[1] in ("approve", "deny") and parts[2].isdigit():
                target = int(parts[2])
                if parts[1] == "approve":
                    AUTHORIZED.add(target)
                    PENDING.discard(target)
                    send_message(target, ACCESS_GRANTED)
                    send_message(chat_id, f"✅ Autorizzato: {target}")
                else:
                    PENDING.discard(target)
                    send_message(target, ACCESS_DENIED)
                    send_message(chat_id, f"🚫 Rifiutato: {target}")
            return jsonify(ok=True)

        # --- Cancel / Restart ---
        if data == "action:cancel":
            reset_state(uid)
            send_message(chat_id, CANCELLED)
            return jsonify(ok=True)

        if data == "action:restart":
            reset_state(uid)
            send_message(chat_id, RESTARTED)
            return jsonify(ok=True)

        # --- Access control ---
        if uid != OWNER_ID and uid not in AUTHORIZED:
            if uid not in PENDING:
                PENDING.add(uid)
                send_message(
                    OWNER_ID,
                    f"📩 Richiesta accesso da {uname} (id `{uid}`)",
                    reply_markup=admin_request_keyboard(uid, uname)
                )
            send_message(chat_id, NOT_AUTH)
            return jsonify(ok=True)

        # --- Set destination ---
        if data == "action:set_end":
            state["roundtrip"] = False
            state["phase"] = "end"
            send_message(chat_id, ASK_END, reply_markup=cancel_restart_keyboard())
            return jsonify(ok=True)

        # --- Round Trip ---
        if data == "action:roundtrip_now":
            state["roundtrip"] = True
            state["end"] = None
            state["phase"] = "direction"
            state["direction"] = None
            send_message(chat_id, ASK_DIRECTION, reply_markup=direction_keyboard())
            return jsonify(ok=True)

        # --- Direction for roundtrip ---
        if data.startswith("dir:"):
            key = data.split(":", 1)[1]
            state["direction"] = "NE" if key == "skip" else key
            send_message(chat_id, f"Direzione impostata: *{state['direction']}*")
            state["phase"] = "waypoints"
            send_message(
                chat_id,
                "Round Trip dalla partenza.\nAggiungi waypoint opzionali oppure premi *Fine*.",
                reply_markup=waypoints_keyboard()
            )
            return jsonify(ok=True)

        # --- Finish waypoints ---
        if data == "action:finish_waypoints":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
            return jsonify(ok=True)

        # --- Style selection ---
        if data.startswith("style:"):
            style = data.split(":", 1)[1]

            if style in ("curvy", "super_curvy") and uid != OWNER_ID:
                send_message(chat_id, "🔒 *Funzione Premium riservata all’owner.*")
                return jsonify(ok=True)

            state["style"] = style
            state["phase"] = "style_selected"
            send_message(chat_id, PROCESSING)
            return jsonify(ok=True)

        return jsonify(ok=True)

    # ---------------------------------------------------------
    # STYLE SELECTED → CALCOLO PERCORSO
    # ---------------------------------------------------------
    if "callback_query" in update:
        cq = update["callback_query"]
        chat_id = cq["message"]["chat"]["id"]
        uid = cq["from"]["id"]
        state = USER_STATE.get(uid, {})

        if state.get("phase") == "style_selected":
            style = state["style"]
            is_owner = (uid == OWNER_ID)

            start = state["start"]
            end = state["end"]
            wps = state["waypoints"]
            roundtrip = state["roundtrip"]

            # --- Build locations ---
            if roundtrip:
                locs = [{"lat": start[0], "lon": start[1]}]
                for w in wps:
                    locs.append({"lat": w[0], "lon": w[1]})
                bearing = BEARING_MAP.get(state["direction"], 45)
                locs, used_base, approx_km = tune_roundtrip_length(
                    start[0], start[1],
                    wps,
                    desired_min=RT_TARGET_MIN,
                    desired_max=RT_TARGET_MAX,
                    bearing_deg=bearing
                )
            else:
                locs = [{"lat": start[0], "lon": start[1]}]
                for w in wps:
                    locs.append({"lat": w[0], "lon": w[1]})
                locs.append({"lat": end[0], "lon": end[1]})

            # --- Limits ---
            approx = approx_total_km_from_locs(locs, roundtrip)
            if approx > MAX_ROUTE_KM:
                send_message(chat_id, LIMITS_EXCEEDED)
                reset_state(uid)
                return jsonify(ok=True)

            # --- Route calculation ---
            try:
                coords, total_km, total_min, maneuvers = valhalla_route(
                    locs,
                    style=style,
                    roundtrip=roundtrip,
                    is_owner=is_owner
                )
            except PermissionError:
                send_message(chat_id, "🔒 *Funzione Premium riservata all’owner.*")
                return jsonify(ok=True)
            except Exception:
                send_message(chat_id, ROUTE_NOT_FOUND)
                reset_state(uid)
                return jsonify(ok=True)

            # --- PNG ---
            coords_simplified = simplify_coords(coords, max_points=80)
            map_url = build_static_map_url(
                coords_simplified,
                start=start,
                end=(start if roundtrip else end),
                waypoints=wps
            )
            png_bytes = download_static_map(map_url) if map_url else None

            # --- GPX ---
            gpx_turns = build_gpx_with_turns(coords, maneuvers)
            gpx_simple = build_gpx_simple(coords)

            # --- Rate limit ---
            last = LAST_DOWNLOAD.get(uid, 0)
            now = now_epoch()
            if uid != OWNER_ID and (now - last) < RATE_LIMIT_DAYS * 86400:
                when = epoch_to_str(last + RATE_LIMIT_DAYS * 86400)
                send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                reset_state(uid)
                return jsonify(ok=True)

            LAST_DOWNLOAD[uid] = now

            # --- Send PNG ---
            if png_bytes:
                send_photo(chat_id, png_bytes, caption="🗺️ *Mappa del percorso*")

            # --- Send GPX ---
            send_document(chat_id, gpx_turns, "percorso_turns.gpx", caption="📍 GPX con istruzioni")
            send_document(chat_id, gpx_simple, "percorso_simple.gpx", caption="📍 GPX semplice")

            # --- Summary ---
            send_message(
                chat_id,
                f"🏁 *Percorso generato!*\n\n"
                f"📏 *Distanza:* {total_km} km\n"
                f"⏱️ *Tempo stimato:* {total_min} min\n"
                f"🌀 *Stile:* {style.replace('_', ' ')}\n"
                f"📍 *Round Trip:* {'Sì' if roundtrip else 'No'}"
            )

            reset_state(uid)
            return jsonify(ok=True)

    # ---------------------------------------------------------
    # MESSAGGI NORMALI
    # ---------------------------------------------------------
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        uid = msg["from"]["id"]
        uname = msg["from"].get("first_name", "Utente")

        if uid not in USER_STATE:
            reset_state(uid)
        state = USER_STATE[uid]

        # --- Access control ---
        if uid != OWNER_ID and uid not in AUTHORIZED:
            if uid not in PENDING:
                PENDING.add(uid)
                send_message(
                    OWNER_ID,
                    f"📩 Richiesta accesso da {uname} (id `{uid}`)",
                    reply_markup=admin_request_keyboard(uid, uname)
                )
            send_message(chat_id, NOT_AUTH)
            return jsonify(ok=True)

        text = msg.get("text", "")

        # --- /start ---
        if text == "/start":
            reset_state(uid)
            send_message(chat_id, WELCOME)
            return jsonify(ok=True)

        # --- /cancel ---
        if text == "/cancel":
            reset_state(uid)
            send_message(chat_id, CANCELLED)
            return jsonify(ok=True)

        # --- Phase: start ---
        if state["phase"] == "start":
            loc = parse_location_from_message(msg)
            if not loc:
                send_message(chat_id, INVALID_INPUT)
                return jsonify(ok=True)
            state["start"] = loc
            state["phase"] = "choose_mode"
            send_message(chat_id, "Partenza impostata!", reply_markup=start_options_keyboard())
            return jsonify(ok=True)

        # --- Phase: end ---
        if state["phase"] == "end":
            loc = parse_location_from_message(msg)
            if not loc:
                send_message(chat_id, INVALID_INPUT)
                return jsonify(ok=True)
            state["end"] = loc
            state["phase"] = "waypoints"
            send_message(chat_id, ASK_WAYPOINTS, reply_markup=waypoints_keyboard())
            return jsonify(ok=True)

        # --- Phase: waypoints ---
        if state["phase"] == "waypoints":
            if len(state["waypoints"]) >= MAX_WAYPOINTS:
                send_message(chat_id, f"⚠️ Hai già {MAX_WAYPOINTS} waypoint. Premi *Fine*.")
                return jsonify(ok=True)

            loc = parse_location_from_message(msg)
            if not loc:
                send_message(chat_id, INVALID_INPUT)
                return jsonify(ok=True)

            state["waypoints"].append(loc)
            send_message(chat_id, f"Waypoint aggiunto! ({len(state['waypoints'])}/{MAX_WAYPOINTS})")
            return jsonify(ok=True)

    return jsonify(ok=True)

# ======================================
# AVVIO FLASK
# ======================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
