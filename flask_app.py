import os
import time
import json
import urllib.parse
from math import radians, sin, cos, sqrt, atan2, asin
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import requests
import gpxpy
import gpxpy.gpx

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
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
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
    if "," in text:
        try:
            parts = text.split(",")
            return (float(parts[0].strip()), float(parts[1].strip()))
        except:
            pass
    return geocode_address(text)

# ======================================
# MAPS LINKS
# ======================================

def build_gmaps_url_ab(start, end, waypoints):
    """ API=1 per A→B (funziona con waypoints) """
    origin = f"{start[0]},{start[1]}"
    destination = f"{end[0]},{end[1]}"
    wp_list = [f"{lat},{lon}" for (lat, lon) in waypoints] if waypoints else []
    params = {"api": "1", "origin": origin, "destination": destination}
    if wp_list:
        params["waypoints"] = "|".join(wp_list)
    return "https://www.google.com/maps/dir/?" + urllib.parse.urlencode(params, safe="|,")  # OK

def build_gmaps_url_roundtrip(start, waypoints):
    """
    Per roundtrip Google non supporta API=1 con origin==destination+waypoints.
    Usare path-style: /lat1,lon1/lat2,lon2/.../lat1,lon1
    """
    parts = [f"{start[0]},{start[1]}"]
    if waypoints:
        parts.extend([f"{lat},{lon}" for (lat, lon) in waypoints])
    parts.append(f"{start[0]},{start[1]}")
    path = "/".join(parts)
    return f"https://www.google.com/maps/dir/{path}"

# ======================================
# GPX / KML
# ======================================

def build_gpx_with_turns(coords, maneuvers, name="Percorso Moto"):
    """
    Crea GPX con:
     - traccia (track) della shape
     - waypoints per ogni manovra (turn-by-turn)
    """
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    # Track points
    for lat, lon in coords:
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon))

    # Maneuver waypoints
    for m in maneuvers:
        lat = m.get("lat")
        lon = m.get("lon")
        instr = m.get("instruction", "")
        if lat is None or lon is None:
            continue
        wpt = gpxpy.gpx.GPXWaypoint(latitude=lat, longitude=lon, name=instr)
        gpx.waypoints.append(wpt)

    return gpx.to_xml().encode("utf-8")

def build_kml(coords):
    kml_points = ""
    for lat, lon in coords:
        kml_points += f"{lon},{lat},0\n"

    kml = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
    <name>Percorso Moto</name>
    <Placemark>
        <name>Percorso</name>
        <Style><LineStyle><color>ff0000ff</color><width>4</width></LineStyle></Style>
        <LineString><tessellate>1</tessellate><coordinates>
{kml_points}
        </coordinates></LineString>
    </Placemark>
</Document>
</kml>
"""
    return kml.encode("utf-8")

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
    """
    Sposta un punto di km su bearing (gradi) sulla sfera
    """
    R = 6371.0
    br = radians(bearing_deg)
    lat1 = radians(lat); lon1 = radians(lon)
    d = km / R
    lat2 = asin(sin(lat1)*cos(d) + cos(lat1)*sin(d)*cos(br))
    lon2 = lon1 + atan2(sin(br)*sin(d)*cos(lat1),
                        cos(d) - sin(lat1)*sin(lat2))
    return (lat2*180/3.1415926535, lon2*180/3.1415926535)

def seed_roundtrip_locations(start_lat, start_lon, base_km, bearing_deg):
    """
    Genera 2 seed 'anti-overlap':
     - seed A a distanza base_km in direzione (bearing + 90°)
     - seed B a distanza base_km * 0.9 in direzione (bearing + 180°)
    """
    a_bearing = (bearing_deg + 90) % 360
    b_bearing = (bearing_deg + 180) % 360
    wp_a = offset_point(start_lat, start_lon, base_km, a_bearing)
    wp_b = offset_point(start_lat, start_lon, base_km * 0.9, b_bearing)
    return [{"lat": wp_a[0], "lon": wp_a[1]},
            {"lat": wp_b[0], "lon": wp_b[1]}]

def tune_roundtrip_length(start_lat, start_lon, waypoints, desired_min=RT_TARGET_MIN, desired_max=RT_TARGET_MAX, bearing_deg=45):
    """
    Tenta di portare la stima Haversine dell'anello tra 70–80 km
    variando il raggio seed 'base_km' (coarse search).
    Ritorna (locs, used_base_km)
    """
    # Se l'utente ha già messo dei waypoint, usiamo solo un tuning leggero dei seed
    if waypoints:
        base_candidates = [18, 20, 22]  # km
    else:
        base_candidates = [18, 20, 22, 24, 26]

    best = None
    for base in base_candidates:
        locs = [{"lat": start_lat, "lon": start_lon}]
        if waypoints:
            locs += [{"lat": w[0], "lon": w[1]} for w in waypoints]
        else:
            # genera seed euristici
            locs += seed_roundtrip_locations(start_lat, start_lon, base, bearing_deg)
        # stima (chiuderemo anello nella chiamata Valhalla)
        approx = approx_total_km_from_locs(locs, roundtrip=True)
        # keep best distanza in [min, max] o il più vicino
        score = 0
        if desired_min <= approx <= desired_max:
            score = 1000  # perfetto
        else:
            # penalizza distanza da range
            if approx < desired_min:
                score = - (desired_min - approx)
            else:
                score = - (approx - desired_max)
        if (best is None) or (score > best[0]):
            best = (score, base, locs, approx)

        # early exit se siamo dentro range
        if desired_min <= approx <= desired_max:
            break

    _, used_base, best_locs, approx_km = best
    return best_locs, used_base, approx_km

# ======================================
# UI / MESSAGGI / PULSANTI
# ======================================

WELCOME = (
    "🏍️ *Benvenuto nel MotoRoute Bot!*\n\n"
    "Genera *GPX (con turn-by-turn)*, *KML* e un link *Google Maps*.\n\n"
    "1️⃣ Invia la *partenza* (posizione, `lat,lon` o indirizzo)\n"
    "2️⃣ Scegli *Round Trip* subito oppure imposta la *destinazione*\n"
    "3️⃣ (Opz.) aggiungi fino a *4 waypoint*, poi *Fine*\n"
    "4️⃣ Scegli *Standard* o *Curvy*\n\n"
    "🔒 Limiti: 120 km totali · 80 km raggio A→B · RT 70–80 km · 4 waypoint"
)

ASK_END = "Perfetto! Ora manda la *destinazione* (posizione, `lat,lon` o indirizzo)."
ASK_WAYPOINTS = "Vuoi aggiungere *waypoint*? Fino a 4.\nInvia una posizione/indirizzo, oppure premi *Fine*."
ASK_STYLE_TEXT = "Seleziona lo *stile* (solo routing, Round Trip è già stato scelto se attivo):"
ASK_DIRECTION = "Scegli una *direzione iniziale* per l'anello (opzionale). Se salti, userò *NE* (45°)."
PROCESSING = "⏳ Sto calcolando il percorso…"
INVALID_INPUT = "❌ Formato non valido. Invia posizione, `lat,lon` oppure un indirizzo."
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
        "direction": None   # "N", "NE", ...
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

    # chiudi l'anello duplicando start in coda
    locs = list(locations)
    if roundtrip:
        s = locs[0]
        locs = locs + [{"lat": s["lat"], "lon": s["lon"]}]

    def build_payload_motorcycle(curvy_variant: int):
        """
        curvy_variant:
          0 -> standard moto
          1 -> use_highways=0.0
          2 -> use_highways=0.0 + exclude_unpaved
          3 -> use_highways=0.0 + avoid_bad_surfaces
          4 -> use_highways=0.0 + exclude_unpaved + avoid_bad_surfaces
        """
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
    urls_to_try = [VALHALLA_URL] + ([VALHALLA_URL_FALLBACK] if VALHALLA_URL_FALLBACK and VALHALLA_URL_FALLBACK != VALHALLA_URL else [])

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

        # turn-by-turn (maneuvers)
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
            reset_state(uid); send_message(chat_id, CANCELLED); return jsonify(ok=True)
        if data == "action:restart":
            reset_state(uid); send_message(chat_id, RESTARTED); return jsonify(ok=True)

        # Access control (oltre annulla/ricomincia)
        if uid != OWNER_ID and uid not in AUTHORIZED:
            if uid not in PENDING:
                PENDING.add(uid)
                try:
                    send_message(OWNER_ID, f"📩 Richiesta accesso da {uname} (id `{uid}`)",
                                 reply_markup=admin_request_keyboard(uid, uname))
                except: pass
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
            state["phase"] = "waypoints"
            # Chiedi direzione (opzionale)
            state["direction"] = None
            send_message(chat_id, ASK_DIRECTION, reply_markup=direction_keyboard())
            # Poi apri la fase waypoint
            send_message(chat_id,
                         "Round Trip dalla partenza.\nAggiungi waypoint opzionali oppure premi *Fine*.",
                         reply_markup=waypoints_keyboard())
            return jsonify(ok=True)

        # Direzione roundtrip
        if data.startswith("dir:"):
            key = data.split(":",1)[1]
            if key == "skip":
                state["direction"] = "NE"
            else:
                state["direction"] = key if key in BEARING_MAP else "NE"
            send_message(chat_id, f"Direzione impostata: *{state['direction']}*")
            return jsonify(ok=True)

        # Fine waypoint → stile
        if data == "action:finish_waypoints":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
            return jsonify(ok=True)

        # Scelta stile (solo standard/curvy)
        if data.startswith("style:"):
            style = data.split(":",1)[1]  # standard | curvy
            is_roundtrip = bool(state.get("roundtrip"))

            # Rate limit (owner escluso)
            if uid != OWNER_ID:
                last = LAST_DOWNLOAD.get(uid, 0)
                if now_epoch() - last < RATE_LIMIT_DAYS * 86400:
                    when = epoch_to_str(last + RATE_LIMIT_DAYS*86400)
                    send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                    return jsonify(ok=True)

            # Prepara locations (start + wps [+ end])
            start = state["start"]
            locs = [{"lat": start[0], "lon": start[1]}]
            for wp in state["waypoints"]:
                locs.append({"lat": wp[0], "lon": wp[1]})

            if is_roundtrip:
                # Direzione
                dir_key = state.get("direction") or "NE"
                bearing = BEARING_MAP.get(dir_key, 45)
                # Genera locs con seed/tuning per 70–80 km
                tuned_locs, used_base, approx_km = tune_roundtrip_length(
                    start[0], start[1],
                    state["waypoints"],
                    desired_min=RT_TARGET_MIN,
                    desired_max=RT_TARGET_MAX,
                    bearing_deg=bearing
                )
                locs = tuned_locs  # contiene già start e (wps|seed)
                approx_txt = f"(stima ~{round(approx_km,1)} km, raggio {used_base} km)"
            else:
                # A→B → aggiungi end
                if not state.get("end"):
                    send_message(chat_id, "⚠️ Imposta una *destinazione* prima di calcolare A→B.")
                    return jsonify(ok=True)
                locs.append({"lat": state["end"][0], "lon": state["end"][1]})
                approx_txt = ""

            # Pre-check limiti (hard)
            approx_km = approx_total_km_from_locs(locs, roundtrip=is_roundtrip)
            if approx_km > MAX_ROUTE_KM * 1.25:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nStima: ~{round(approx_km,1)} km")
                return jsonify(ok=True)

            # Pre-check specifico Round Trip (70–80 km)
            if is_roundtrip:
                # permettiamo ±10% sulla stima prima del calcolo reale
                if not (RT_TARGET_MIN*0.85 <= approx_km <= RT_TARGET_MAX*1.15):
                    send_message(chat_id, f"⚠️ Non riesco a stimare un anello 70–80 km. {approx_txt}\n"
                                          "Aggiungi 1 waypoint oppure cambia direzione e riprova.")
                    return jsonify(ok=True)

            send_message(chat_id, PROCESSING)

            try:
                coords, dist_km, time_min, maneuvers = valhalla_route(
                    locs,
                    style=("curvy" if style == "curvy" else "standard"),
                    roundtrip=is_roundtrip
                )
            except Exception as e:
                send_message(chat_id, f"Errore routing:\n{str(e)[:250]}")
                return jsonify(ok=True)

            # Hard limits
            if dist_km > MAX_ROUTE_KM:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
                return jsonify(ok=True)

            if is_roundtrip and not (RT_TARGET_MIN <= dist_km <= RT_TARGET_MAX):
                send_message(chat_id, f"⚠️ L'anello calcolato è di *{dist_km} km* (target 70–80).\n"
                                      "Riprova modificando direzione o aggiungendo un waypoint.")
                # proseguo comunque con i file, ma avviso
            if not coords or len(coords) < 2:
                send_message(chat_id, ROUTE_NOT_FOUND)
                return jsonify(ok=True)

            # File GPX (traccia + maneuvers)
            gpx_bytes = build_gpx_with_turns(coords, maneuvers, "Percorso Moto")
            send_document(chat_id, gpx_bytes, "route.gpx",
                          caption=f"Distanza: {dist_km} km · Durata: {time_min} min {('· ' + approx_txt) if approx_txt else ''}")

            # File KML
            kml_bytes = build_kml(coords)
            send_document(chat_id, kml_bytes, "route.kml")

            # Link Google Maps
            if is_roundtrip:
                gmaps_url = build_gmaps_url_roundtrip(
                    start=state["start"],
                    waypoints=state["waypoints"]
                )
            else:
                gmaps_url = build_gmaps_url_ab(
                    start=state["start"],
                    end=state["end"],
                    waypoints=state["waypoints"]
                )
            send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

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
                    send_message(OWNER_ID, f"📩 Richiesta accesso da {uname} (id `{uid}`)",
                                 reply_markup=admin_request_keyboard(uid, uname))
                except: pass
                send_message(chat_id, NOT_AUTH)
            return jsonify(ok=True)
        send_message(chat_id, WELCOME)
        return jsonify(ok=True)

    if text.lower() == "annulla":
        reset_state(uid); send_message(chat_id, CANCELLED); return jsonify(ok=True)

    if text.lower() == "ricomincia":
        reset_state(uid); send_message(chat_id, RESTARTED); return jsonify(ok=True)

    # Blocca non autorizzati
    if uid != OWNER_ID and uid not in AUTHORIZED:
        send_message(chat_id, "🔒 Non sei autorizzato. Usa /start per richiedere l'accesso.")
        return jsonify(ok=True)

    phase = state["phase"]

    # START
    if phase == "start":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT); return jsonify(ok=True)
        state["start"] = loc
        state["phase"] = "choose_route_type"
        send_message(chat_id,
                     "Vuoi partire subito con un *Round Trip* o impostare una *destinazione*?",
                     reply_markup=start_options_keyboard())
        return jsonify(ok=True)

    # END
    if phase == "end":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT); return jsonify(ok=True)
        if haversine_km(state["start"], loc) > MAX_RADIUS_KM:
            send_message(chat_id, "⚠️ La destinazione è oltre *80 km* in linea d’aria dalla partenza.")
            return jsonify(ok=True)
        state["end"] = loc
        state["phase"] = "waypoints"
        send_message(chat_id, ASK_WAYPOINTS, reply_markup=waypoints_keyboard())
        return jsonify(ok=True)

    # WAYPOINTS
    if phase == "waypoints":
        if text.lower() == "fine":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
            return jsonify(ok=True)

        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT); return jsonify(ok=True)

        if len(state["waypoints"]) >= MAX_WAYPOINTS:
            send_message(chat_id, f"Hai già {MAX_WAYPOINTS} waypoint.\nPremi *Fine* per continuare.")
            return jsonify(ok=True)

        state["waypoints"].append(loc)
        send_message(chat_id,
                     f"Waypoint aggiunto ({len(state['waypoints'])}/{MAX_WAYPOINTS}). "
                     "Aggiungine un altro oppure premi *Fine*.",
                     reply_markup=waypoints_keyboard())
        return jsonify(ok=True)

    # STYLE fallback con testo
    if phase == "style":
        if text.lower() not in ("standard", "curvy"):
            send_message(chat_id, "Scegli `standard` o `curvy`, oppure usa i pulsanti.")
            return jsonify(ok=True)

        is_roundtrip = bool(state.get("roundtrip"))

        # Rate limit
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

        if is_roundtrip:
            dir_key = state.get("direction") or "NE"
            bearing = BEARING_MAP.get(dir_key, 45)
            tuned_locs, used_base, approx_km = tune_roundtrip_length(
                start[0], start[1], state["waypoints"],
                desired_min=RT_TARGET_MIN, desired_max=RT_TARGET_MAX,
                bearing_deg=bearing
            )
            locs = tuned_locs
        else:
            if not state.get("end"):
                send_message(chat_id, "⚠️ Imposta una *destinazione* prima di calcolare A→B.")
                return jsonify(ok=True)
            locs.append({"lat": state["end"][0], "lon": state["end"][1]})

        approx_km = approx_total_km_from_locs(locs, roundtrip=is_roundtrip)
        if approx_km > MAX_ROUTE_KM * 1.25:
            send_message(chat_id, f"{LIMITS_EXCEEDED}\nStima: ~{round(approx_km,1)} km")
            return jsonify(ok=True)

        if is_roundtrip and not (RT_TARGET_MIN*0.85 <= approx_km <= RT_TARGET_MAX*1.15):
            send_message(chat_id, "⚠️ Non riesco a stimare un anello 70–80 km. "
                                  "Cambia direzione o aggiungi un waypoint.")
            return jsonify(ok=True)

        send_message(chat_id, PROCESSING)

        try:
            coords, dist_km, time_min, maneuvers = valhalla_route(
                locs,
                style=("curvy" if text.lower() == "curvy" else "standard"),
                roundtrip=is_roundtrip
            )
        except Exception as e:
            send_message(chat_id, f"Errore routing:\n{str(e)[:250]}")
            return jsonify(ok=True)

        if dist_km > MAX_ROUTE_KM:
            send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
            return jsonify(ok=True)

        # File GPX (turn-by-turn) + KML
        gpx_bytes = build_gpx_with_turns(coords, maneuvers, "Percorso Moto")
        send_document(chat_id, gpx_bytes, "route.gpx",
                      caption=f"Distanza: {dist_km} km · Durata: {time_min} min")

        kml_bytes = build_kml(coords)
        send_document(chat_id, kml_bytes, "route.kml")

        # Link Google Maps
        if is_roundtrip:
            gmaps_url = build_gmaps_url_roundtrip(
                start=state["start"],
                waypoints=state["waypoints"]
            )
        else:
            gmaps_url = build_gmaps_url_ab(
                start=state["start"],
                end=state["end"],
                waypoints=state["waypoints"]
            )
        send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

        LAST_DOWNLOAD[uid] = now_epoch()
        reset_state(uid)
        return jsonify(ok=True)

    send_message(chat_id, "❓ Usa /start per cominciare.")
    return jsonify(ok=True)
