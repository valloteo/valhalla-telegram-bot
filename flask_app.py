import os
import time
import json
import urllib.parse
from math import radians, sin, cos, sqrt, atan2, asin, log, tan, pi
from datetime import datetime, timezone

from flask import Flask, request, jsonify
import requests
import gpxpy
import gpxpy.gpx
from io import BytesIO
from PIL import Image, ImageDraw

# ======================================
# CONFIG
# ======================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")
VALHALLA_URL = os.environ.get("VALHALLA_URL", "").rstrip("/")
VALHALLA_URL_FALLBACK = os.environ.get("VALHALLA_URL_FALLBACK", "").rstrip("/")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
AUTH_USERS_CSV = os.environ.get("AUTH_USERS_CSV", "").strip()

MAX_WAYPOINTS = 4
MAX_ROUTE_KM = 120              # limite assoluto hard
MAX_RADIUS_KM = 80              # solo A→B raggio
RT_TARGET_MIN = 70              # round trip minimo
RT_TARGET_MAX = 80              # round trip massimo
RATE_LIMIT_DAYS = 7             # 1 download/sett (owner escluso)

# PNG MAP CONFIG
PNG_SIZE = 800
OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
DEFAULT_ZOOM = 12  # zoom di base

app = Flask(__name__)

# Memorie in-process (Render free: reset a cold start)
USER_STATE = {}
AUTHORIZED = set()
PENDING = set()
LAST_DOWNLOAD = {}

if AUTH_USERS_CSV:
    for _id in AUTH_USERS_CSV.split(","):
        _id = _id.strip()
        if _id.isdigit():
            AUTHORIZED.add(int(_id))
if OWNER_ID:
    AUTHORIZED.add(OWNER_ID)

# ======================================
# UTILS TEMPO / DISTANZA
# ======================================

def now_epoch() -> float:
    return time.time()

def epoch_to_str(e: float) -> str:
    try:
        dt = datetime.fromtimestamp(e, tz=timezone.utc).astimezone()
        return dt.strftime("%d/%m/%Y %H:%M")
    except:
        return "più tardi"

def haversine_km(a, b):
    R = 6371.0
    lat1, lon1 = radians(a[0]), radians(a[1])
    lat2, lon2 = radians(b[0]), radians(b[1])
    dlat = lat2 - lat1
    dlon = radians(b[1] - a[1])
    h = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * atan2(sqrt(h), sqrt(1-h))

def approx_total_km_from_locs(locs, roundtrip: bool) -> float:
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
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    requests.post(url, json=payload, timeout=15)

def send_document(chat_id, file_bytes, filename, caption=None):
    url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
    files = {"document": (filename, file_bytes, "application/octet-stream")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    requests.post(url, data=data, files=files, timeout=30)

def send_photo(chat_id, file_bytes, caption=None):
    url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
    files = {"photo": ("route.png", file_bytes, "image/png")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    requests.post(url, data=data, files=files, timeout=30)

def answer_callback_query(cq_id, text=None):
    url = f"https://api.telegram.org/bot{TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": cq_id}
    if text:
        payload["text"] = text
    requests.post(url, json=payload, timeout=10)

# ======================================
# GEO / GEOCODING
# ======================================

def geocode_address(q):
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
    # 1) Posizione Telegram
    if "location" in msg:
        loc = msg["location"]
        return (loc["latitude"], loc["longitude"])

    # 2) Testo come indirizzo (niente più parsing coordinate)
    text = (msg.get("text") or "").strip()
    if not text:
        return None

    loc = geocode_address(text)
    return loc

# ======================================
# GPX
# ======================================

def build_gpx_with_turns(coords, maneuvers, name="Percorso Moto"):
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
        wpt = gpxpy.gpx.GPXWaypoint(latitude=lat, longitude=lon, name=instr)
        gpx.waypoints.append(wpt)

    return gpx.to_xml().encode("utf-8")

def build_gpx_simple(coords, name="Percorso Moto (semplice)"):
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    for lat, lon in coords:
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon))

    return gpx.to_xml().encode("utf-8")

# ======================================
# POLYLINE Decoder
# ======================================

def decode_polyline6(polyline_str):
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
# BEARING / SEED (Round Trip)
# ======================================

BEARING_MAP = {
    "N": 0, "NE": 45, "E": 90, "SE": 135,
    "S": 180, "SW": 225, "W": 270, "NW": 315
}

def offset_point(lat, lon, km, bearing_deg):
    R = 6371.0
    br = radians(bearing_deg)
    lat1 = radians(lat); lon1 = radians(lon)
    d = km / R
    lat2 = asin(sin(lat1)*cos(d) + cos(lat1)*sin(d)*cos(br))
    lon2 = lon1 + atan2(
        sin(br)*sin(d)*cos(lat1),
        cos(d) - sin(lat1)*sin(lat2)
    )
    return (lat2*180/pi, lon2*180/pi)

def seed_roundtrip_locations(start_lat, start_lon, base_km, bearing_deg):
    a_bearing = (bearing_deg + 90) % 360
    b_bearing = (bearing_deg + 180) % 360
    wp_a = offset_point(start_lat, start_lon, base_km, a_bearing)
    wp_b = offset_point(start_lat, start_lon, base_km * 0.9, b_bearing)
    return [{"lat": wp_a[0], "lon": wp_a[1]},
            {"lat": wp_b[0], "lon": wp_b[1]}]

def tune_roundtrip_length(start_lat, start_lon, waypoints, desired_min=RT_TARGET_MIN, desired_max=RT_TARGET_MAX, bearing_deg=45):
    if waypoints:
        base_candidates = [18, 20, 22]
    else:
        base_candidates = [18, 20, 22, 24, 26]

    best = None
    for base in base_candidates:
        locs = [{"lat": start_lat, "lon": start_lon}]
        if waypoints:
            locs += [{"lat": w[0], "lon": w[1]} for w in waypoints]
        else:
            locs += seed_roundtrip_locations(start_lat, start_lon, base, bearing_deg)
        approx = approx_total_km_from_locs(locs, roundtrip=True)
        score = 0
        if desired_min <= approx <= desired_max:
            score = 1000
        else:
            if approx < desired_min:
                score = - (desired_min - approx)
            else:
                score = - (approx - desired_max)
        if (best is None) or (score > best[0]):
            best = (score, base, locs, approx)
        if desired_min <= approx <= desired_max:
            break

    _, used_base, best_locs, approx_km = best
    return best_locs, used_base, approx_km

# ======================================
# UI / MESSAGGI / PULSANTI
# ======================================

WELCOME = (
    "🏍️ *Benvenuto nel MotoRoute Bot!*\n\n"
    "Genera *GPX (con turn-by-turn)* e un *GPX semplice*, più una *mappa PNG* del percorso.\n\n"
    "1️⃣ Invia la *partenza* (posizione o indirizzo)\n"
    "2️⃣ Scegli *Round Trip* subito oppure imposta la *destinazione*\n"
    "3️⃣ (Opz.) aggiungi fino a *4 waypoint*, poi *Fine*\n"
    "4️⃣ Scegli *Standard* o *Curvy leggero*\n\n"
    "🔒 Limiti: 120 km totali · 80 km raggio A→B · RT 70–80 km · 4 waypoint"
)

ASK_END = "Perfetto! Ora manda la *destinazione* (posizione o indirizzo)."
ASK_WAYPOINTS = "Vuoi aggiungere *waypoint*? Fino a 4.\nInvia una posizione/indirizzo, oppure premi *Fine*."
ASK_STYLE_TEXT = "Seleziona lo *stile* del percorso:"
ASK_DIRECTION = "Scegli una *direzione iniziale* per l'anello (opzionale). Se salti, userò *NE* (45°)."
PROCESSING = "⏳ Sto calcolando il percorso…"
INVALID_INPUT = "❌ Non riesco a capire questo indirizzo/posizione. Prova con un indirizzo più preciso o una posizione da mappa."
LIMITS_EXCEEDED = "⚠️ Supera i limiti. Riduci distanza/waypoint."
ROUTE_NOT_FOUND = "❌ Nessun percorso trovato. Modifica i punti e riprova."
CANCELLED = "🛑 Operazione annullata. Usa /start per ricominciare."
RESTARTED = "🔄 Conversazione ricominciata! Invia la *partenza*."
NOT_AUTH = "🔒 Bot ad accesso riservato. Ho inviato la tua *richiesta di autorizzazione* all'owner."
ALREADY_PENDING = "⏳ Hai già una richiesta di accesso in revisione. Attendi conferma."
ACCESS_GRANTED = "✅ Sei stato abilitato! Ora puoi usare il bot."
ACCESS_DENIED = "❌ La tua richiesta di accesso è stata rifiutata."
RATE_LIMIT_MSG = "⏱️ Limite *1 download/settimana*. Riprova dopo: *{when}*."

def style_inline_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "🛣️ Standard", "callback_data": "style:standard"},
                {"text": "🌀 Curvy leggero", "callback_data": "style:curvy"},
            ],
            [
                {"text": "❌ Annulla", "callback_data": "action:cancel"},
                {"text": "🔄 Ricomincia", "callback_data": "action:restart"}
            ]
        ]
    }

def start_options_keyboard():
    return {
        "inline_keyboard": [[
            {"text": "🔄 Round Trip da qui", "callback_data": "action:roundtrip_now"},
            {"text": "➡️ Imposta Destinazione", "callback_data": "action:set_end"}
        ],
        [
            {"text": "❌ Annulla", "callback_data": "action:cancel"}
        ]]
    }

def waypoints_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "✅ Fine", "callback_data": "action:finish_waypoints"}],
            [
                {"text": "❌ Annulla", "callback_data": "action:cancel"},
                {"text": "🔄 Ricomincia", "callback_data": "action:restart"}
            ]
        ]
    }

def direction_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "⬆️ N", "callback_data": "dir:N"},
                {"text": "↗️ NE", "callback_data": "dir:NE"},
                {"text": "➡️ E", "callback_data": "dir:E"},
                {"text": "↘️ SE", "callback_data": "dir:SE"},
            ],
            [
                {"text": "⬇️ S", "callback_data": "dir:S"},
                {"text": "↙️ SW", "callback_data": "dir:SW"},
                {"text": "⬅️ W", "callback_data": "dir:W"},
                {"text": "↖️ NW", "callback_data": "dir:NW"},
            ],
            [
                {"text": "⏭️ Salta (NE)", "callback_data": "dir:skip"}
            ]
        ]
    }

def cancel_restart_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "❌ Annulla", "callback_data": "action:cancel"},
                {"text": "🔄 Ricomincia", "callback_data": "action:restart"}
            ]
        ]
    }

def admin_request_keyboard(uid:int, name:str):
    return {
        "inline_keyboard": [
            [
                {"text": f"✅ Approva {name} ({uid})", "callback_data": f"admin:approve:{uid}"},
                {"text": "❌ Rifiuta", "callback_data": f"admin:deny:{uid}"}
            ]
        ]
    }

def reset_state(uid):
    USER_STATE[uid] = {
        "phase": "start",
        "start": None,
        "end": None,
        "waypoints": [],
        "style": None,
        "roundtrip": False,
        "direction": None
    }

# ======================================
# VALHALLA (motorcycle only) + FALLBACK URL + VARIANTI CURVY
# ======================================

def post_valhalla(url, payload, timeout=30, retries=1):
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

def valhalla_route(locations, style="standard", roundtrip=False):
    if not VALHALLA_URL:
        raise RuntimeError("VALHALLA_URL non configurato.")

    locs = list(locations)
    if roundtrip:
        s = locs[0]
        locs = locs + [{"lat": s["lat"], "lon": s["lon"]}]

    def build_payload_motorcycle(curvy_variant: int):
        co = {}
        if style == "curvy":
            opts = {"use_highways": 0.0}
            if curvy_variant in (2, 4):
                opts["exclude_unpaved"] = True
            if curvy_variant in (3, 4):
                opts["avoid_bad_surfaces"] = True
            co["motorcycle"] = opts

        body = {
            "locations": locs,
            "costing": "motorcycle",
            "directions_options": {"units": "kilometers"},
        }
        if co:
            body["costing_options"] = co
        return body

    variants = [0] if style == "standard" else [4, 2, 3, 1]
    urls_to_try = [VALHALLA_URL] + (
        [VALHALLA_URL_FALLBACK]
        if VALHALLA_URL_FALLBACK and VALHALLA_URL_FALLBACK != VALHALLA_URL
        else []
    )

    resp_json = None
    last_err = None
    for v in variants:
        payload = build_payload_motorcycle(v)
        for url in urls_to_try:
            try:
                j = post_valhalla(url, payload, timeout=30, retries=1)
                if "trip" in j:
                    resp_json = j
                    break
                last_err = json.dumps(j)[:200]
            except Exception as e:
                last_err = str(e)
        if resp_json:
            break

    if not resp_json:
        raise RuntimeError(f"Valhalla error (motorcycle): {last_err}")

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
            ll = m.get("begin_shape_index", 0)
            if 0 <= ll < len(coords):
                lat, lon = coords[ll]
                maneuvers_out.append({
                    "instruction": instr,
                    "lat": lat,
                    "lon": lon
                })

    return coords, round(total_km, 1), round(total_min, 1), maneuvers_out

# ======================================
# PNG MAP GENERATION (OSM CARTO)
# ======================================

def latlon_to_tile(lat, lon, z):
    lat_rad = radians(lat)
    n = 2.0 ** z
    xtile = int((lon + 180.0) / 360.0 * n)
    ytile = int((1.0 - (log(tan(lat_rad) + 1/cos(lat_rad)) / pi)) / 2.0 * n)
    return xtile, ytile

def tile_to_pixel(lat, lon, z, xtile0, ytile0, tiles_x, tiles_y, img_size):
    lat_rad = radians(lat)
    n = 2.0 ** z
    x = (lon + 180.0) / 360.0 * n
    y = (1.0 - (log(tan(lat_rad) + 1/cos(lat_rad)) / pi)) / 2.0 * n

    dx = (x - xtile0) * 256
    dy = (y - ytile0) * 256

    scale_x = img_size / (tiles_x * 256)
    scale_y = img_size / (tiles_y * 256)

    px = int(dx * scale_x)
    py = int(dy * scale_y)
    return px, py

def build_png_map(coords, start, end, waypoints):
    if not coords:
        return None

    lats = [c[0] for c in coords]
    lons = [c[1] for c in coords]
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)

    lat_pad = (max_lat - min_lat) * 0.2 or 0.01
    lon_pad = (max_lon - min_lon) * 0.2 or 0.01
    min_lat -= lat_pad
    max_lat += lat_pad
    min_lon -= lon_pad
    max_lon += lon_pad

    z = DEFAULT_ZOOM

    xt_min, yt_max = latlon_to_tile(min_lat, min_lon, z)
    xt_max, yt_min = latlon_to_tile(max_lat, max_lon, z)

    tiles_x = max(1, xt_max - xt_min + 1)
    tiles_y = max(1, yt_max - yt_min + 1)

    base_img = Image.new("RGB", (tiles_x * 256, tiles_y * 256), (255, 255, 255))
    for x in range(xt_min, xt_min + tiles_x):
        for y in range(yt_min, yt_min + tiles_y):
            url = OSM_TILE_URL.format(z=z, x=x, y=y)
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    tile = Image.open(BytesIO(r.content)).convert("RGB")
                    base_img.paste(tile, ((x - xt_min) * 256, (y - yt_min) * 256))
            except:
                pass

    base_img = base_img.resize((PNG_SIZE, PNG_SIZE), Image.LANCZOS)
    draw = ImageDraw.Draw(base_img)

    pts = []
    for lat, lon in coords:
        px, py = tile_to_pixel(lat, lon, z, xt_min, yt_min, tiles_x, tiles_y, PNG_SIZE)
        pts.append((px, py))
    if len(pts) >= 2:
        draw.line(pts, fill=(255, 0, 0), width=4)

    if start:
        sx, sy = tile_to_pixel(start[0], start[1], z, xt_min, yt_min, tiles_x, tiles_y, PNG_SIZE)
        r = 6
        draw.ellipse((sx-r, sy-r, sx+r, sy+r), fill=(0, 200, 0), outline=(0, 0, 0))

    if end:
        ex, ey = tile_to_pixel(end[0], end[1], z, xt_min, yt_min, tiles_x, tiles_y, PNG_SIZE)
        r = 6
        draw.ellipse((ex-r, ey-r, ex+r, ey+r), fill=(200, 0, 0), outline=(0, 0, 0))

    for w in waypoints or []:
        wx, wy = tile_to_pixel(w[0], w[1], z, xt_min, yt_min, tiles_x, tiles_y, PNG_SIZE)
        r = 5
        draw.ellipse((wx-r, wy-r, wx+r, wy+r), fill=(255, 215, 0), outline=(0, 0, 0))

    buf = BytesIO()
    base_img.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()

# ======================================
# ROUTES
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

@app.route("/webhook/<path:token>", methods=["POST"])
def webhook(token):
    if token != os.environ.get("TELEGRAM_TOKEN"):
        return jsonify(ok=False, error="forbidden"), 403

    update = request.get_json(silent=True) or {}

    # ---------- CALLBACK ----------
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

        # Admin approval
        if data.startswith("admin:"):
            if uid != OWNER_ID:
                return jsonify(ok=True)
            parts = data.split(":")
            if len(parts) == 3 and parts[1] in ("approve", "deny") and parts[2].isdigit():
                target = int(parts[2])
                if parts[1] == "approve":
                    AUTHORIZED.add(target); PENDING.discard(target)
                    send_message(target, ACCESS_GRANTED)
                    send_message(chat_id, f"✅ Autorizzato: {target}")
                else:
                    PENDING.discard(target)
                    send_message(target, ACCESS_DENIED)
                    send_message(chat_id, f"🚫 Rifiutato: {target}")
            return jsonify(ok=True)

        # Cancel/Restart
        if data == "action:cancel":
            reset_state(uid)
            send_message(chat_id, CANCELLED)
            return jsonify(ok=True)
        if data == "action:restart":
            reset_state(uid)
            send_message(chat_id, RESTARTED)
            return jsonify(ok=True)

        # Access control (oltre annulla/ricomincia)
        if uid != OWNER_ID and uid not in AUTHORIZED:
            if uid not in PENDING:
                PENDING.add(uid)
                try:
                    send_message(
                        OWNER_ID,
                        f"📩 Richiesta accesso da {uname} (id `{uid}`)",
                        reply_markup=admin_request_keyboard(uid, uname)
                    )
                except:
                    pass
            send_message(chat_id, NOT_AUTH)
            return jsonify(ok=True)

        # Scelte flusso
        if data == "action:set_end":
            state["roundtrip"] = False
            state["phase"] = "end"
            send_message(chat_id, ASK_END, reply_markup=cancel_restart_keyboard())
            return jsonify(ok=True)

        if data == "action:roundtrip_now":
            state["roundtrip"] = True
            state["end"] = None
            state["phase"] = "direction"
            state["direction"] = None
            send_message(chat_id, ASK_DIRECTION, reply_markup=direction_keyboard())
            return jsonify(ok=True)

        # Direzione roundtrip
        if data.startswith("dir:"):
            key = data.split(":", 1)[1]
            if key == "skip":
                state["direction"] = "NE"
            else:
                state["direction"] = key if key in BEARING_MAP else "NE"
            send_message(chat_id, f"Direzione impostata: *{state['direction']}*")
            state["phase"] = "waypoints"
            send_message(
                chat_id,
                "Round Trip dalla partenza.\nAggiungi waypoint opzionali oppure premi *Fine*.\n"
                f"Puoi aggiungere fino a *{MAX_WAYPOINTS}* waypoint.",
                reply_markup=waypoints_keyboard()
            )
            return jsonify(ok=True)

        # Fine waypoint → stile
        if data == "action:finish_waypoints":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
            return jsonify(ok=True)

        # Scelta stile (solo standard/curvy)
        if data.startswith("style:"):
            style = data.split(":", 1)[1]  # standard | curvy
            is_roundtrip = bool(state.get("roundtrip"))

            # Rate limit (owner escluso)
            if uid != OWNER_ID:
                last = LAST_DOWNLOAD.get(uid, 0)
                if now_epoch() - last < RATE_LIMIT_DAYS * 86400:
                    when = epoch_to_str(last + RATE_LIMIT_DAYS*86400)
                    send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                    return jsonify(ok=True)

            start = state["start"]
            locs = [{"lat": start[0], "lon": start[1]}]
            for wp in state["waypoints"]:
                locs.append({"lat": wp[0], "lon": wp[1]})

            approx_txt = ""
            if is_roundtrip:
                dir_key = state.get("direction") or "NE"
                bearing = BEARING_MAP.get(dir_key, 45)
                tuned_locs, used_base, approx_km_seed = tune_roundtrip_length(
                    start[0], start[1],
                    state["waypoints"],
                    desired_min=RT_TARGET_MIN,
                    desired_max=RT_TARGET_MAX,
                    bearing_deg=bearing
                )
                locs = tuned_locs
                approx_txt = f"(stima iniziale ~{round(approx_km_seed,1)} km, raggio seed {used_base} km)"
            else:
                if not state.get("end"):
                    send_message(chat_id, "⚠️ Imposta una *destinazione* prima di calcolare A→B.")
                    return jsonify(ok=True)
                locs.append({"lat": state["end"][0], "lon": state["end"][1]})

            approx_km = approx_total_km_from_locs(locs, roundtrip=is_roundtrip)
            if approx_km > MAX_ROUTE_KM * 1.25:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nStima: ~{round(approx_km,1)} km")
                return jsonify(ok=True)

            if is_roundtrip:
                if not (RT_TARGET_MIN*0.85 <= approx_km <= RT_TARGET_MAX*1.15):
                    send_message(
                        chat_id,
                        f"⚠️ Non riesco a stimare un anello 70–80 km. {approx_txt}\n"
                        "Aggiungi 1 waypoint oppure cambia direzione e riprova."
                    )
                    return jsonify(ok=True)
                if approx_km < RT_TARGET_MIN:
                    send_message(
                        chat_id,
                        f"ℹ️ Il giro stimato è piuttosto corto (~{round(approx_km,1)} km). "
                        "Puoi aggiungere un waypoint per allungarlo, oppure procedo comunque con il calcolo."
                    )

            send_message(chat_id, PROCESSING)

            try:
                coords, dist_km, time_min, maneuvers = valhalla_route(
                    locs,
                    style=("curvy" if style == "curvy" else "standard"),
                    roundtrip=is_roundtrip
                )
            except Exception:
                send_message(
                    chat_id,
                    "❌ Non sono riuscito a calcolare il percorso.\n"
                    "Ho provato diverse varianti ma Valhalla ha restituito errore.\n"
                    "Prova a modificare i punti o ridurre la distanza."
                )
                return jsonify(ok=True)

            if dist_km > MAX_ROUTE_KM:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
                return jsonify(ok=True)

            if is_roundtrip and not (RT_TARGET_MIN <= dist_km <= RT_TARGET_MAX):
                send_message(
                    chat_id,
                    f"⚠️ L'anello calcolato è di *{dist_km} km* (target 70–80).\n"
                    "Puoi riprovare cambiando direzione o aggiungendo un waypoint, "
                    "ma intanto ti invio comunque i file del percorso."
                )

            if not coords or len(coords) < 2:
                send_message(chat_id, ROUTE_NOT_FOUND)
                return jsonify(ok=True)

            # PNG MAP
            png_bytes = build_png_map(
                coords,
                start=state["start"],
                end=(state["end"] if not is_roundtrip else state["start"]),
                waypoints=state["waypoints"]
            )
            if png_bytes:
                send_photo(
                    chat_id,
                    png_bytes,
                    caption=f"🗺️ Anteprima percorso\nDistanza stimata: {dist_km} km · Durata: {time_min} min"
                )

            # GPX con turn-by-turn
            gpx_bytes = build_gpx_with_turns(coords, maneuvers, "Percorso Moto")
            send_document(
                chat_id,
                gpx_bytes,
                "route_turns.gpx",
                caption=f"GPX con turn-by-turn\nDistanza: {dist_km} km · Durata: {time_min} min"
            )

            # GPX semplice
            gpx_simple = build_gpx_simple(coords, "Percorso Moto (semplice)")
            send_document(
                chat_id,
                gpx_simple,
                "route_simple.gpx",
                caption="GPX semplice (solo traccia)"
            )

            # riepilogo finale
            wp_names = []
            for i, w in enumerate(state["waypoints"], start=1):
                wp_names.append(f"WP{i}: {w[0]:.5f}, {w[1]:.5f}")
            wp_text = "\n".join(wp_names) if wp_names else "Nessun waypoint."

            if is_roundtrip:
                dir_key = state.get("direction") or "NE"
                extra = f"Round Trip · Direzione iniziale: *{dir_key}*"
            else:
                extra = "A→B"

            send_message(
                chat_id,
                "✅ *Percorso generato!*\n\n"
                f"• Tipo: {extra}\n"
                f"• Stile: *{'Curvy leggero' if style=='curvy' else 'Standard'}*\n"
                f"• Distanza: *{dist_km} km*\n"
                f"• Durata stimata: *{time_min} min*\n"
                f"• Waypoint:\n{wp_text}\n"
                f"{('• ' + approx_txt) if approx_txt else ''}"
            )

            LAST_DOWNLOAD[uid] = now_epoch()
            reset_state(uid)
            return jsonify(ok=True)

        return jsonify(ok=True)

    # ---------- MESSAGGI ----------
    msg = update.get("message")
    if not msg:
        return jsonify(ok=True)

    chat_id = msg["chat"]["id"]
    uid = msg["from"]["id"]
    uname = msg["from"].get("first_name", "Utente")
    text = (msg.get("text") or "").strip()

    if uid not in USER_STATE:
        reset_state(uid)
    state = USER_STATE[uid]

    # Comandi
    if text.lower() == "/start":
        reset_state(uid)
        if uid != OWNER_ID and uid not in AUTHORIZED:
            if uid in PENDING:
                send_message(chat_id, ALREADY_PENDING)
            else:
                PENDING.add(uid)
                try:
                    send_message(
                        OWNER_ID,
                        f"📩 Richiesta accesso da {uname} (id `{uid}`)",
                        reply_markup=admin_request_keyboard(uid, uname)
                    )
                except:
                    pass
                send_message(chat_id, NOT_AUTH)
            return jsonify(ok=True)
        send_message(chat_id, WELCOME)
        return jsonify(ok=True)

    if text.lower() in ("annulla", "/cancel"):
        reset_state(uid)
        send_message(chat_id, CANCELLED)
        return jsonify(ok=True)

    if text.lower() in ("ricomincia", "/restart"):
        reset_state(uid)
        send_message(chat_id, RESTARTED)
        return jsonify(ok=True)

    # Blocca non autorizzati
    if uid != OWNER_ID and uid not in AUTHORIZED:
        send_message(chat_id, "🔒 Non sei autorizzato. Usa /start per richiedere l'accesso.")
        return jsonify(ok=True)

    phase = state["phase"]

    # START
    if phase == "start":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)
        state["start"] = loc
        state["phase"] = "choose_route_type"
        send_message(
            chat_id,
            "Perfetto, ho impostato la *partenza*.\n\n"
            "Vuoi partire subito con un *Round Trip* o impostare una *destinazione*?",
            reply_markup=start_options_keyboard()
        )
        return jsonify(ok=True)

    # END
    if phase == "end":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)
        if haversine_km(state["start"], loc) > MAX_RADIUS_KM:
            send_message(
                chat_id,
                "⚠️ La destinazione è oltre *80 km* in linea d’aria dalla partenza.\n"
                "Riduci la distanza o scegli un punto più vicino."
            )
            return jsonify(ok=True)
        state["end"] = loc
        state["phase"] = "waypoints"
        send_message(
            chat_id,
            ASK_WAYPOINTS + f"\nPuoi aggiungere fino a *{MAX_WAYPOINTS}* waypoint.",
            reply_markup=waypoints_keyboard()
        )
        return jsonify(ok=True)

    # WAYPOINTS
    if phase == "waypoints":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)

        if len(state["waypoints"]) >= MAX_WAYPOINTS:
            send_message(
                chat_id,
                f"Hai già raggiunto il numero massimo di *{MAX_WAYPOINTS}* waypoint.\n"
                "Premi *Fine* per procedere al calcolo del percorso.",
                reply_markup=waypoints_keyboard()
            )
            return jsonify(ok=True)

        state["waypoints"].append(loc)
        remaining = MAX_WAYPOINTS - len(state["waypoints"])
        wp_list = "\n".join(
            [f"• WP{i}: {w[0]:.5f}, {w[1]:.5f}" for i, w in enumerate(state["waypoints"], start=1)]
        ) or "Nessun waypoint."

        send_message(
            chat_id,
            "✅ Waypoint aggiunto.\n\n"
            f"Waypoints attuali:\n{wp_list}\n\n"
            f"Puoi aggiungere ancora *{remaining}* waypoint, oppure premere *Fine*.",
            reply_markup=waypoints_keyboard()
        )
        return jsonify(ok=True)

    # CHOOSE ROUTE TYPE
    if phase == "choose_route_type":
        send_message(
            chat_id,
            "Usa i pulsanti per scegliere *Round Trip* o *Destinazione*.",
            reply_markup=start_options_keyboard()
        )
        return jsonify(ok=True)

    # STYLE
    if phase == "style":
        send_message(
            chat_id,
            "Seleziona lo *stile* del percorso usando i pulsanti.",
            reply_markup=style_inline_keyboard()
        )
        return jsonify(ok=True)

    # DIRECTION
    if phase == "direction":
        send_message(
            chat_id,
            "Scegli la *direzione iniziale* usando i pulsanti.",
            reply_markup=direction_keyboard()
        )
        return jsonify(ok=True)

    return jsonify(ok=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
