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

# Nuovo provider mappe
STADIA_TOKEN = os.environ.get("STADIA_TOKEN")  # <-- NON mettere la chiave nel codice

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

# Carica utenti autorizzati
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
    return time.time()

def epoch_to_str(e):
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

def approx_total_km_from_locs(locs, roundtrip):
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
    return geocode_address(text)

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
        gpx.waypoints.append(gpxpy.gpx.GPXWaypoint(latitude=lat, longitude=lon, name=instr))

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
# MESSAGGI STANDARD
# ======================================

WELCOME = (
    "🏍 *Benvenuto nel MotoRoute Bot!*\n\n"
    "Invia la *partenza* (posizione o indirizzo)."
)

ASK_END = "📍 Ora invia la *destinazione*."
ASK_WAYPOINTS = "➕ Aggiungi waypoint opzionali oppure premi *Fine*."
ASK_DIRECTION = "🧭 Scegli la direzione preferita per il Round Trip."
ASK_STYLE_TEXT = "🎨 Scegli lo stile del percorso."
PROCESSING = "⏳ Sto calcolando il percorso..."
INVALID_INPUT = "⚠️ Non ho capito. Invia una posizione o un indirizzo valido."
CANCELLED = "❌ Operazione annullata."
RESTARTED = "🔄 Ricominciamo! Invia la *partenza*."
NOT_AUTH = "🔒 Non sei autorizzato. Ho inviato la richiesta all’admin."
ACCESS_GRANTED = "✅ Accesso approvato! Ora puoi usare il bot."
ACCESS_DENIED = "❌ La tua richiesta di accesso è stata rifiutata."
LIMITS_EXCEEDED = "🚫 Il percorso supera i limiti consentiti."

# ======================================
# TASTIERE
# ======================================

def start_options_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "Imposta destinazione", "callback_data": "action:set_end"}],
            [{"text": "Round Trip", "callback_data": "action:roundtrip_now"}],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}]
        ]
    }

def waypoints_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "➕ Aggiungi waypoint", "callback_data": "action:add_wp"}],
            [{"text": "✔️ Fine", "callback_data": "action:finish_waypoints"}],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}]
        ]
    }

def direction_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Nord", "callback_data": "dir:N"},
                {"text": "Est", "callback_data": "dir:E"},
                {"text": "Sud", "callback_data": "dir:S"},
                {"text": "Ovest", "callback_data": "dir:W"},
            ],
            [{"text": "Lascia decidere al bot", "callback_data": "dir:skip"}],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}]
        ]
    }

def style_inline_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Normale", "callback_data": "style:normal"},
                {"text": "Curvy", "callback_data": "style:curvy"},
                {"text": "Super Curvy ⭐", "callback_data": "style:super_curvy"},
            ],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}]
        ]
    }

def cancel_restart_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}],
            [{"text": "🔄 Ricomincia", "callback_data": "action:restart"}]
        ]
    }

def admin_request_keyboard(uid, uname):
    return {
        "inline_keyboard": [
            [{"text": f"✔️ Approva {uname}", "callback_data": f"admin:approve:{uid}"}],
            [{"text": f"❌ Rifiuta {uname}", "callback_data": f"admin:deny:{uid}"}]
        ]
    }

# ======================================
# RESET STATO
# ======================================

def reset_state(uid):
    USER_STATE[uid] = {
        "phase": "start",
        "start": None,
        "end": None,
        "waypoints": [],
        "roundtrip": False,
        "direction": None,
        "style": None,
    }

# ======================================
# ROUND TRIP — GENERAZIONE WAYPOINT AUTOMATICI
# ======================================

def generate_roundtrip_waypoints(start, direction, count=3, radius_km=25):
    lat, lon = start["lat"], start["lon"]

    # Direzioni cardinali
    dirs = {
        "N": 0,
        "E": 90,
        "S": 180,
        "W": 270,
        "NE": 45,
    }

    if direction not in dirs:
        direction = "NE"

    base_angle = dirs[direction]
    angles = [
        base_angle - 40,
        base_angle,
        base_angle + 40
    ]

    wps = []
    for ang in angles[:count]:
        ang_rad = ang * pi / 180
        d = radius_km / 6371.0

        lat2 = asin(sin(radians(lat)) * cos(d) +
                    cos(radians(lat)) * sin(d) * cos(ang_rad))
        lon2 = radians(lon) + atan2(
            sin(ang_rad) * sin(d) * cos(radians(lat)),
            cos(d) - sin(radians(lat)) * sin(lat2)
        )

        wps.append({
            "lat": lat2 * 180 / pi,
            "lon": lon2 * 180 / pi
        })

    return wps
# ======================================
# VALHALLA — CHIAMATE API
# ======================================

def post_valhalla(url, payload):
    try:
        r = requests.post(url, json=payload, timeout=25)
        if r.status_code != 200:
            return None
        return r.json()
    except:
        return None

def route_valhalla(locations, style="normal"):
    costing = "motorcycle"

    # Stili personalizzati
    if style == "curvy":
        costing_options = {"use_hills": 0.6, "use_trails": 0.4, "use_highways": 0.1}
    elif style == "super_curvy":
        costing_options = {"use_hills": 1.0, "use_trails": 0.8, "use_highways": 0.0}
    else:
        costing_options = {"use_hills": 0.3, "use_trails": 0.2, "use_highways": 0.5}

    payload = {
        "locations": locations,
        "costing": costing,
        "costing_options": {costing: costing_options},
        "directions_options": {"units": "kilometers"},
    }

    urls_to_try = [VALHALLA_URL]
    if VALHALLA_URL_FALLBACK:
        urls_to_try.append(VALHALLA_URL_FALLBACK)

    for u in urls_to_try:
        data = post_valhalla(f"{u}/route", payload)
        if data:
            return data

    return None

# ======================================
# PNG — STADIA MAPS + FALLBACK OSM
# ======================================

def build_stadia_url(coords, markers):
    """
    coords = lista di (lat, lon) del percorso
    markers = lista di (lat, lon) per start/end/waypoints
    """
    if not STADIA_TOKEN:
        return None

    # Linea del percorso
    path = "|".join([f"{lat},{lon}" for lat, lon in coords])

    # Marker
    mk = "|".join([f"{lat},{lon}" for lat, lon in markers])

    url = (
        "https://tiles.stadiamaps.com/static?"
        f"api_key={STADIA_TOKEN}"
        f"&path=color:red|weight:3|{path}"
        f"&markers=color:blue|{mk}"
        "&zoom=12"
        "&size=800x800"
    )
    return url

def build_osm_url(coords, markers):
    """
    Fallback OSM static map
    """
    base = "https://staticmap.openstreetmap.de/staticmap.php"
    path = "|".join([f"{lat},{lon}" for lat, lon in coords])
    mk = "|".join([f"{lat},{lon}" for lat, lon in markers])

    url = (
        f"{base}?size=800x800"
        f"&path=color:red|weight:3|{path}"
        f"&markers={mk}"
    )
    return url

def download_png(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            return r.content
        return None
    except:
        return None

def build_static_map(coords, markers):
    """
    1) Prova Stadia Maps
    2) Se fallisce → OSM
    """
    # 1) Stadia
    if STADIA_TOKEN:
        url = build_stadia_url(coords, markers)
        if url:
            img = download_png(url)
            if img:
                return img

    # 2) Fallback OSM
    url = build_osm_url(coords, markers)
    return download_png(url)

# ======================================
# ESTRARRE COORDINATE E MANOVRE DA VALHALLA
# ======================================

def extract_coords_and_maneuvers(valhalla_json):
    if not valhalla_json:
        return None, None

    try:
        shape = valhalla_json["trip"]["legs"][0]["shape"]
        coords = decode_polyline6(shape)

        maneuvers = []
        for m in valhalla_json["trip"]["legs"][0]["maneuvers"]:
            lat = m.get("begin_shape_index")
            if lat is None:
                continue
            idx = m["begin_shape_index"]
            if idx < len(coords):
                maneuvers.append({
                    "lat": coords[idx][0],
                    "lon": coords[idx][1],
                    "instruction": m.get("instruction", "")
                })

        return coords, maneuvers

    except:
        return None, None
# ======================================
# RATE LIMIT (solo utenti normali)
# ======================================

def check_rate_limit(uid):
    if uid == OWNER_ID:
        return True  # Owner illimitato

    last = LAST_DOWNLOAD.get(uid)
    if not last:
        return True

    now = now_epoch()
    if (now - last) >= RATE_LIMIT_DAYS * 86400:
        return True

    return False

def update_rate_limit(uid):
    LAST_DOWNLOAD[uid] = now_epoch()

# ======================================
# COSTRUZIONE LOCATIONS PER VALHALLA
# ======================================

def build_locations(start, end, waypoints, roundtrip):
    locs = []

    # Start
    locs.append({"lat": start["lat"], "lon": start["lon"]})

    # Waypoints
    for w in waypoints:
        locs.append({"lat": w["lat"], "lon": w["lon"]})

    # End
    if end:
        locs.append({"lat": end["lat"], "lon": end["lon"]})

    # Round trip → ritorno al punto di partenza
    if roundtrip:
        locs.append({"lat": start["lat"], "lon": start["lon"]})

    return locs

# ======================================
# CONTROLLO LIMITI KM
# ======================================

def check_distance_limits(locs, roundtrip):
    approx = approx_total_km_from_locs(locs, roundtrip)
    return approx <= MAX_ROUTE_KM

# ======================================
# CALCOLO PERCORSO COMPLETO
# ======================================

def compute_and_send_route(uid, chat_id):
    st = USER_STATE.get(uid)
    if not st:
        send_message(chat_id, "Errore interno. Riparti con /start.")
        return

    start = st["start"]
    end = st["end"]
    wps = st["waypoints"]
    roundtrip = st["roundtrip"]
    direction = st["direction"]
    style = st["style"]

    # Se round trip → genera waypoint automatici
    if roundtrip and not wps:
        wps = generate_roundtrip_waypoints(start, direction, count=3, radius_km=25)

    # Costruisci locations
    locs = build_locations(start, end, wps, roundtrip)

    # Controllo limiti km (valido anche per te)
    if not check_distance_limits(locs, roundtrip):
        send_message(chat_id, LIMITS_EXCEEDED)
        return

    # Rate limit (solo utenti normali)
    if not check_rate_limit(uid):
        last = LAST_DOWNLOAD.get(uid)
        send_message(
            chat_id,
            f"⏳ Hai già scaricato un percorso di recente.\n"
            f"Puoi riprovare dopo: *{epoch_to_str(last + RATE_LIMIT_DAYS*86400)}*"
        )
        return

    send_message(chat_id, PROCESSING)

    # Chiamata Valhalla
    val = route_valhalla(locs, style=style)
    if not val:
        send_message(chat_id, "❌ Errore Valhalla. Riprova più tardi.")
        return

    coords, maneuvers = extract_coords_and_maneuvers(val)
    if not coords:
        send_message(chat_id, "❌ Errore nel percorso.")
        return

    # GPX
    gpx_bytes = build_gpx_with_turns(coords, maneuvers)

    # PNG (Stadia + fallback OSM)
    markers = [(start["lat"], start["lon"])]
    for w in wps:
        markers.append((w["lat"], w["lon"]))
    if end:
        markers.append((end["lat"], end["lon"]))

    png_bytes = build_static_map(coords, markers)

    # Aggiorna rate limit
    update_rate_limit(uid)

    # Invio GPX
    send_document(chat_id, gpx_bytes, "route.gpx", caption="📄 GPX pronto!")

    # Invio PNG
    if png_bytes:
        send_photo(chat_id, png_bytes, caption="🗺 Mappa del percorso")
    else:
        send_message(chat_id, "⚠️ PNG non disponibile (OSM e Stadia non rispondono).")

    # Reset stato
    reset_state(uid)
# ======================================
# CALLBACK QUERY HANDLER
# ======================================

def handle_callback(uid, chat_id, cq_id, data):
    st = USER_STATE.get(uid)

    # Admin actions
    if data.startswith("admin:"):
        _, action, target = data.split(":")
        target = int(target)

        if uid != OWNER_ID:
            answer_callback_query(cq_id, "Non autorizzato.")
            return

        if action == "approve":
            AUTHORIZED.add(target)
            if target in PENDING:
                PENDING.remove(target)
            send_message(target, ACCESS_GRANTED)
            answer_callback_query(cq_id, "Utente approvato.")
        else:
            if target in PENDING:
                PENDING.remove(target)
            send_message(target, ACCESS_DENIED)
            answer_callback_query(cq_id, "Utente rifiutato.")
        return

    # Normal user actions
    if data == "action:cancel":
        reset_state(uid)
        send_message(chat_id, CANCELLED)
        return

    if data == "action:restart":
        reset_state(uid)
        send_message(chat_id, RESTARTED)
        return

    if data == "action:set_end":
        st["phase"] = "await_end"
        send_message(chat_id, ASK_END)
        return

    if data == "action:add_wp":
        st["phase"] = "await_wp"
        send_message(chat_id, "📍 Invia il waypoint.")
        return

    if data == "action:finish_waypoints":
        st["phase"] = "choose_style"
        send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
        return

    if data == "action:roundtrip_now":
        st["roundtrip"] = True
        st["phase"] = "choose_direction"
        send_message(chat_id, ASK_DIRECTION, reply_markup=direction_keyboard())
        return

    # Direzione round trip
    if data.startswith("dir:"):
        direction = data.split(":")[1]
        st["direction"] = direction
        st["phase"] = "choose_style"
        send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
        return

    # Stile percorso
    if data.startswith("style:"):
        style = data.split(":")[1]

        # Owner può usare super_curvy
        if style == "super_curvy" and uid != OWNER_ID:
            answer_callback_query(cq_id, "Solo l’admin può usare Super Curvy.")
            return

        st["style"] = style
        answer_callback_query(cq_id, "Stile selezionato!")
        compute_and_send_route(uid, chat_id)
        return

    answer_callback_query(cq_id, "Comando non riconosciuto.")


# ======================================
# MESSAGE HANDLER
# ======================================

def handle_message(uid, chat_id, msg):
    # Controllo accesso
    if uid not in AUTHORIZED:
        if uid not in PENDING:
            PENDING.add(uid)
            send_message(
                OWNER_ID,
                f"🔔 Richiesta accesso da {uid}",
                reply_markup=admin_request_keyboard(uid, f"user_{uid}")
            )
        send_message(chat_id, NOT_AUTH)
        return

    text = (msg.get("text") or "").strip()

    # 👉 GESTIONE /start (mancava!)
    if text == "/start":
        reset_state(uid)
        send_message(chat_id, WELCOME, reply_markup=start_options_keyboard())
        return

    # Stato utente
    st = USER_STATE.get(uid)
    if not st:
        reset_state(uid)
        st = USER_STATE[uid]

    phase = st["phase"]
    loc = parse_location_from_message(msg)

    # 👉 FASE START (ora funziona)
    if phase == "start":
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return
        st["start"] = {"lat": loc[0], "lon": loc[1]}
        st["phase"] = "await_end"
        send_message(chat_id, ASK_END)
        return

    if phase == "await_end":
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return
        st["end"] = {"lat": loc[0], "lon": loc[1]}
        st["phase"] = "waypoints"
        send_message(chat_id, ASK_WAYPOINTS, reply_markup=waypoints_keyboard())
        return

    if phase == "await_wp":
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return
        if len(st["waypoints"]) >= MAX_WAYPOINTS:
            send_message(chat_id, f"⚠️ Puoi aggiungere massimo {MAX_WAYPOINTS} waypoint.")
            return
        st["waypoints"].append({"lat": loc[0], "lon": loc[1]})
        st["phase"] = "waypoints"
        send_message(chat_id, ASK_WAYPOINTS, reply_markup=waypoints_keyboard())
        return

    send_message(chat_id, INVALID_INPUT)


# ======================================
# WEBHOOK
# ======================================

@app.route("/", methods=["POST"])
def webhook():
    data = request.get_json()

    if "callback_query" in data:
        cq = data["callback_query"]
        uid = cq["from"]["id"]
        chat_id = cq["message"]["chat"]["id"]
        cq_id = cq["id"]
        handle_callback(uid, chat_id, cq_id, cq["data"])
        return jsonify({"ok": True})

    if "message" in data:
        msg = data["message"]
        uid = msg["from"]["id"]
        chat_id = msg["chat"]["id"]
        handle_message(uid, chat_id, msg)
        return jsonify({"ok": True})

    return jsonify({"ok": True})


# ======================================
# AVVIO FLASK
# ======================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
