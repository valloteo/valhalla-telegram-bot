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

# ======================================
# CONFIG
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

if AUTH_USERS_CSV:
    for _id in AUTH_USERS_CSV.split(","):
        _id = _id.strip()
        if _id.isdigit():
            AUTHORIZED.add(int(_id))
if OWNER_ID:
    AUTHORIZED.add(OWNER_ID)

# ======================================
# UTILITY
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
# GEOCODING
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
    if "location" in msg:
        loc = msg["location"]
        return (loc["latitude"], loc["longitude"])

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
# POLYLINE DECODER
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
# ROUND TRIP LOGIC
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
    "Genera *GPX (con turn-by-turn)*, un *GPX semplice* e una *mappa PNG* del percorso.\n\n"
    "1️⃣ Invia la *partenza* (posizione o indirizzo)\n"
    "2️⃣ Scegli *Round Trip* o imposta la *destinazione*\n"
    "3️⃣ (Opz.) aggiungi fino a *4 waypoint*, poi *Fine*\n"
    "4️⃣ Scegli lo *stile del percorso*\n\n"
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
                {"text": "🛣️ Standard", "callback_data": "style:standard"}
            ],
            [
                {"text": "🌀 Curvy leggero", "callback_data": "style:curvy_light"},
                {"text": "🌀🌀 Curvy", "callback_data": "style:curvy"}
            ],
            [
                {"text": "🌀🌀🌀 Super Curvy (Premium)", "callback_data": "style:super_curvy"}
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
# VALHALLA (4 LIVELLI DI CURVOSITÀ)
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


def build_motorcycle_costing(style, is_owner):
    """
    Restituisce le opzioni di costing per i 4 livelli:
    - standard
    - curvy leggero
    - curvy (solo owner)
    - super curvy (solo owner)
    """

    # --- STANDARD ---
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

    # --- CURVY LEGGERO (per tutti) ---
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

    # --- CURVY (solo owner) ---
    if style == "curvy":
        if not is_owner:
            return None  # blocco premium
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

    # --- SUPER CURVY (solo owner) ---
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


def valhalla_route(locations, style="standard", roundtrip=False, is_owner=False):
    if not VALHALLA_URL:
        raise RuntimeError("VALHALLA_URL non configurato.")

    locs = list(locations)
    if roundtrip:
        s = locs[0]
        locs = locs + [{"lat": s["lat"], "lon": s["lon"]}]

    # Costing options
    costing_opts = build_motorcycle_costing(style, is_owner)
    if costing_opts is None:
        raise PermissionError("premium_blocked")

    payload = {
        "locations": locs,
        "costing": "motorcycle",
        "costing_options": {"motorcycle": costing_opts},
        "directions_options": {"units": "kilometers"},
    }

    urls_to_try = [VALHALLA_URL] + (
        [VALHALLA_URL_FALLBACK]
        if VALHALLA_URL_FALLBACK and VALHALLA_URL_FALLBACK != VALHALLA_URL
        else []
    )

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
# STATIC MAP (OSM) + SEMPLIFICAZIONE POLYLINE
# ======================================

def simplify_coords(coords, max_points=80):
    """
    Riduce il numero di punti mantenendo la forma generale del percorso.
    Evita URL troppo lunghe per staticmap.openstreetmap.de.
    """
    if len(coords) <= max_points:
        return coords
    step = max(1, len(coords) // max_points)
    return [coords[i] for i in range(0, len(coords), step)]


def build_static_map_url(coords, start, end, waypoints):
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
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.content
    except:
        pass
    return None


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

        # Access control
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

        # Scelta stile
        if data.startswith("style:"):
            style = data.split(":", 1)[1]  # standard | curvy_light | curvy | super_curvy

            # Premium lock
            if style in ("curvy", "super_curvy") and uid != OWNER_ID:
                send_message(chat_id, "🔒 *Funzione Premium riservata all’owner.*")
                return jsonify(ok=True)

            state["style"] = style
            # Il resto della logica continua nel blocco 6
            state["phase"] = "style_selected"
            send_message(chat_id, PROCESSING)
            # Il calcolo del percorso prosegue nel blocco 6
            return jsonify(ok=True)

        return jsonify(ok=True)

        # --- STYLE SELECTED → CALCOLO PERCORSO ---
        if state["phase"] == "style_selected":
            style = state["style"]
            is_owner = (uid == OWNER_ID)

            start = state["start"]
            end = state["end"]
            wps = state["waypoints"]
            roundtrip = state["roundtrip"]

            # Costruzione locations Valhalla
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

            # Limiti
            approx = approx_total_km_from_locs(locs, roundtrip)
            if approx > MAX_ROUTE_KM:
                send_message(chat_id, LIMITS_EXCEEDED)
                reset_state(uid)
                return jsonify(ok=True)

            # Calcolo percorso
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
            except Exception as e:
                send_message(chat_id, ROUTE_NOT_FOUND)
                reset_state(uid)
                return jsonify(ok=True)

            # PNG
            coords_simplified = simplify_coords(coords, max_points=80)
            map_url = build_static_map_url(
                coords_simplified,
                start=start,
                end=(start if roundtrip else end),
                waypoints=wps
            )
            png_bytes = download_static_map(map_url) if map_url else None

            # GPX
            gpx_turns = build_gpx_with_turns(coords, maneuvers, name="Percorso Moto")
            gpx_simple = build_gpx_simple(coords, name="Percorso Moto (semplice)")

            # Rate limit
            last = LAST_DOWNLOAD.get(uid, 0)
            now = now_epoch()
            if uid != OWNER_ID and (now - last) < RATE_LIMIT_DAYS * 86400:
                when = epoch_to_str(last + RATE_LIMIT_DAYS * 86400)
                send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                reset_state(uid)
                return jsonify(ok=True)

            LAST_DOWNLOAD[uid] = now

            # Invio PNG
            if png_bytes:
                send_photo(chat_id, png_bytes, caption="🗺️ *Mappa del percorso*")

            # Invio GPX
            send_document(chat_id, gpx_turns, "percorso_turns.gpx", caption="📍 GPX con istruzioni")
            send_document(chat_id, gpx_simple, "percorso_simple.gpx", caption="📍 GPX semplice")

            # Riepilogo
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

    # ---------- MESSAGGI NORMALI ----------
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        uid = msg["from"]["id"]
        uname = msg["from"].get("first_name", "Utente")

        if uid not in USER_STATE:
            reset_state(uid)
        state = USER_STATE[uid]

        # Access control
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

        text = msg.get("text", "")

        # /start
        if text == "/start":
            reset_state(uid)
            send_message(chat_id, WELCOME)
            return jsonify(ok=True)

        # /cancel
        if text == "/cancel":
            reset_state(uid)
            send_message(chat_id, CANCELLED)
            return jsonify(ok=True)

        # Fasi
        if state["phase"] == "start":
            loc = parse_location_from_message(msg)
            if not loc:
                send_message(chat_id, INVALID_INPUT)
                return jsonify(ok=True)
            state["start"] = loc
            state["phase"] = "choose_mode"
            send_message(chat_id, "Partenza impostata!", reply_markup=start_options_keyboard())
            return jsonify(ok=True)

        if state["phase"] == "end":
            loc = parse_location_from_message(msg)
            if not loc:
                send_message(chat_id, INVALID_INPUT)
                return jsonify(ok=True)
            state["end"] = loc
            state["phase"] = "waypoints"
            send_message(chat_id, ASK_WAYPOINTS, reply_markup=waypoints_keyboard())
            return jsonify(ok=True)

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


# =====================================
# AVVIO FLASK
# ======================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))VERSIONE 8:

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

# ======================================
# CONFIG
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

if AUTH_USERS_CSV:
    for _id in AUTH_USERS_CSV.split(","):
        _id = _id.strip()
        if _id.isdigit():
            AUTHORIZED.add(int(_id))
if OWNER_ID:
    AUTHORIZED.add(OWNER_ID)

# ======================================
# UTILITY
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
# GEOCODING
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
    if "location" in msg:
        loc = msg["location"]
        return (loc["latitude"], loc["longitude"])

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
# POLYLINE DECODER
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
# ROUND TRIP LOGIC
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
    "Genera *GPX (con turn-by-turn)*, un *GPX semplice* e una *mappa PNG* del percorso.\n\n"
    "1️⃣ Invia la *partenza* (posizione o indirizzo)\n"
    "2️⃣ Scegli *Round Trip* o imposta la *destinazione*\n"
    "3️⃣ (Opz.) aggiungi fino a *4 waypoint*, poi *Fine*\n"
    "4️⃣ Scegli lo *stile del percorso*\n\n"
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
                {"text": "🛣️ Standard", "callback_data": "style:standard"}
            ],
            [
                {"text": "🌀 Curvy leggero", "callback_data": "style:curvy_light"},
                {"text": "🌀🌀 Curvy", "callback_data": "style:curvy"}
            ],
            [
                {"text": "🌀🌀🌀 Super Curvy (Premium)", "callback_data": "style:super_curvy"}
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
# VALHALLA (4 LIVELLI DI CURVOSITÀ)
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


def build_motorcycle_costing(style, is_owner):
    """
    Restituisce le opzioni di costing per i 4 livelli:
    - standard
    - curvy leggero
    - curvy (solo owner)
    - super curvy (solo owner)
    """

    # --- STANDARD ---
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

    # --- CURVY LEGGERO (per tutti) ---
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

    # --- CURVY (solo owner) ---
    if style == "curvy":
        if not is_owner:
            return None  # blocco premium
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

    # --- SUPER CURVY (solo owner) ---
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


def valhalla_route(locations, style="standard", roundtrip=False, is_owner=False):
    if not VALHALLA_URL:
        raise RuntimeError("VALHALLA_URL non configurato.")

    locs = list(locations)
    if roundtrip:
        s = locs[0]
        locs = locs + [{"lat": s["lat"], "lon": s["lon"]}]

    # Costing options
    costing_opts = build_motorcycle_costing(style, is_owner)
    if costing_opts is None:
        raise PermissionError("premium_blocked")

    payload = {
        "locations": locs,
        "costing": "motorcycle",
        "costing_options": {"motorcycle": costing_opts},
        "directions_options": {"units": "kilometers"},
    }

    urls_to_try = [VALHALLA_URL] + (
        [VALHALLA_URL_FALLBACK]
        if VALHALLA_URL_FALLBACK and VALHALLA_URL_FALLBACK != VALHALLA_URL
        else []
    )

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
# STATIC MAP (OSM) + SEMPLIFICAZIONE POLYLINE
# ======================================

def simplify_coords(coords, max_points=80):
    """
    Riduce il numero di punti mantenendo la forma generale del percorso.
    Evita URL troppo lunghe per staticmap.openstreetmap.de.
    """
    if len(coords) <= max_points:
        return coords
    step = max(1, len(coords) // max_points)
    return [coords[i] for i in range(0, len(coords), step)]


def build_static_map_url(coords, start, end, waypoints):
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
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.content
    except:
        pass
    return None


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

        # Access control
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

        # Scelta stile
        if data.startswith("style:"):
            style = data.split(":", 1)[1]  # standard | curvy_light | curvy | super_curvy

            # Premium lock
            if style in ("curvy", "super_curvy") and uid != OWNER_ID:
                send_message(chat_id, "🔒 *Funzione Premium riservata all’owner.*")
                return jsonify(ok=True)

            state["style"] = style
            # Il resto della logica continua nel blocco 6
            state["phase"] = "style_selected"
            send_message(chat_id, PROCESSING)
            # Il calcolo del percorso prosegue nel blocco 6
            return jsonify(ok=True)

        return jsonify(ok=True)

        # --- STYLE SELECTED → CALCOLO PERCORSO ---
        if state["phase"] == "style_selected":
            style = state["style"]
            is_owner = (uid == OWNER_ID)

            start = state["start"]
            end = state["end"]
            wps = state["waypoints"]
            roundtrip = state["roundtrip"]

            # Costruzione locations Valhalla
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

            # Limiti
            approx = approx_total_km_from_locs(locs, roundtrip)
            if approx > MAX_ROUTE_KM:
                send_message(chat_id, LIMITS_EXCEEDED)
                reset_state(uid)
                return jsonify(ok=True)

            # Calcolo percorso
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
            except Exception as e:
                send_message(chat_id, ROUTE_NOT_FOUND)
                reset_state(uid)
                return jsonify(ok=True)

            # PNG
            coords_simplified = simplify_coords(coords, max_points=80)
            map_url = build_static_map_url(
                coords_simplified,
                start=start,
                end=(start if roundtrip else end),
                waypoints=wps
            )
            png_bytes = download_static_map(map_url) if map_url else None

            # GPX
            gpx_turns = build_gpx_with_turns(coords, maneuvers, name="Percorso Moto")
            gpx_simple = build_gpx_simple(coords, name="Percorso Moto (semplice)")

            # Rate limit
            last = LAST_DOWNLOAD.get(uid, 0)
            now = now_epoch()
            if uid != OWNER_ID and (now - last) < RATE_LIMIT_DAYS * 86400:
                when = epoch_to_str(last + RATE_LIMIT_DAYS * 86400)
                send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                reset_state(uid)
                return jsonify(ok=True)

            LAST_DOWNLOAD[uid] = now

            # Invio PNG
            if png_bytes:
                send_photo(chat_id, png_bytes, caption="🗺️ *Mappa del percorso*")

            # Invio GPX
            send_document(chat_id, gpx_turns, "percorso_turns.gpx", caption="📍 GPX con istruzioni")
            send_document(chat_id, gpx_simple, "percorso_simple.gpx", caption="📍 GPX semplice")

            # Riepilogo
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

    # ---------- MESSAGGI NORMALI ----------
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        uid = msg["from"]["id"]
        uname = msg["from"].get("first_name", "Utente")

        if uid not in USER_STATE:
            reset_state(uid)
        state = USER_STATE[uid]

        # Access control
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

        text = msg.get("text", "")

        # /start
        if text == "/start":
            reset_state(uid)
            send_message(chat_id, WELCOME)
            return jsonify(ok=True)

        # /cancel
        if text == "/cancel":
            reset_state(uid)
            send_message(chat_id, CANCELLED)
            return jsonify(ok=True)

        # Fasi
        if state["phase"] == "start":
            loc = parse_location_from_message(msg)
            if not loc:
                send_message(chat_id, INVALID_INPUT)
                return jsonify(ok=True)
            state["start"] = loc
            state["phase"] = "choose_mode"
            send_message(chat_id, "Partenza impostata!", reply_markup=start_options_keyboard())
            return jsonify(ok=True)

        if state["phase"] == "end":
            loc = parse_location_from_message(msg)
            if not loc:
                send_message(chat_id, INVALID_INPUT)
                return jsonify(ok=True)
            state["end"] = loc
            state["phase"] = "waypoints"
            send_message(chat_id, ASK_WAYPOINTS, reply_markup=waypoints_keyboard())
            return jsonify(ok=True)

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


# =====================================
# AVVIO FLASK
# ======================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
