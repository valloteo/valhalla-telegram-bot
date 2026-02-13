import os
import json
from math import radians, sin, cos, sqrt, atan2
from flask import Flask, request, jsonify
import requests
import gpxpy
import gpxpy.gpx

# ======= Config =======
TOKEN = os.environ.get("TELEGRAM_TOKEN")  # Imposta su Render → Environment
VALHALLA_URL = os.environ.get("VALHALLA_URL", "").rstrip("/")  # https://valhalla1.openstreetmap.de
MAX_WAYPOINTS = 3
MAX_ROUTE_KM = 120
MAX_RADIUS_KM = 80

app = Flask(__name__)
USER_STATE = {}  # stato conversazione in RAM

# ======= Utils =======
def haversine_km(a, b):
    R = 6371.0
    lat1, lon1 = radians(a[0]), radians(a[1])
    lat2, lon2 = radians(b[0]), radians(b[1])
    dlat = lat2 - lat1
    dlon = b[1] - a[1]
    dlon = radians(dlon)
    h = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * atan2(sqrt(h), sqrt(1-h))

def send_message(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    requests.post(url, json=payload, timeout=15)

def send_document(chat_id, file_bytes, filename="route.gpx", caption=None):
    url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
    files = {"document": (filename, file_bytes, "application/gpx+xml")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    requests.post(url, data=data, files=files, timeout=30)

def parse_location_from_message(message):
    # Accetta posizione Telegram o "lat,lon" come testo
    if "location" in message:
        loc = message["location"]
        return (loc["latitude"], loc["longitude"])
    elif "text" in message:
        txt = (message["text"] or "").strip()
        if "," in txt:
            parts = txt.split(",")
            try:
                lat = float(parts[0].strip())
                lon = float(parts[1].strip())
                return (lat, lon)
            except:
                return None
    return None

def build_gpx(coords, name="Percorso"):
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)
    for lat, lon in coords:
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon))
    return gpx.to_xml().encode("utf-8")

def decode_polyline6(polyline_str):
    index, lat, lng, coordinates = 0, 0, 0, []
    changes = {'lat': 0, 'lng': 0}
    while index < len(polyline_str):
        for unit in ['lat', 'lng']:
            shift, result = 0, 0
            while True:
                b = ord(polyline_str[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            changes[unit] = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += changes['lat']
        lng += changes['lng']
        coordinates.append((lat / 1e6, lng / 1e6))
    return coordinates

def valhalla_route(locations, style="standard"):
    if not VALHALLA_URL:
        raise RuntimeError("VALHALLA_URL non configurato")

    costing_options = {}
    if style == "curvy":
        costing_options = {
            "auto": {
                "use_highways": 0.0,
                "avoid_bad_surfaces": True
            }
        }

    payload = {
        "locations": locations,
        "costing": "auto",
        "costing_options": costing_options,
        "directions_options": {"units": "km"}
    }
    r = requests.post(f"{VALHALLA_URL}/route", json=payload, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Valhalla error: {r.status_code} {r.text[:200]}")

    data = r.json()
    trip = data.get("trip", {})
    coords = []
    total_km = 0.0
    total_min = 0.0

    for leg in trip.get("legs", []):
        shape = leg.get("shape")
        if shape:
            coords.extend(decode_polyline6(shape))
        summary = leg.get("summary", {})
        total_km += float(summary.get("length", 0.0))  # km se units=km
        total_min += float(summary.get("time", 0.0)) / 60.0

    return coords, round(total_km, 1), round(total_min, 1)

# ======= Conversazione =======
WELCOME = (
    "Benvenuto! Bot base per creare percorsi GPX via Valhalla.\n\n"
    "*Come iniziare*\n"
    "1) Invia il *punto di partenza* come posizione Telegram o `lat,lon`\n"
    "2) Poi la *destinazione*\n"
    "3) Opzionale: fino a 3 *waypoint* (poi scrivi `fine`)\n"
    "4) Scegli lo stile: `standard` o `curvy`\n\n"
    "*Limiti*\n"
    "• Percorso massimo: 120 km\n"
    "• Destinazione entro 80 km (raggio) dal punto di partenza\n"
    "• Max 3 waypoint\n"
    "• Solo asfaltato (profilo auto)\n"
)

ASK_END = "Perfetto! Ora manda la *destinazione* (posizione o `lat,lon`)."
ASK_WAYPOINTS = (
    "Vuoi aggiungere waypoint? Fino a 3 posizioni (`lat,lon`).\n"
    "Quando hai finito, scrivi: `fine`."
)
ASK_STYLE = "Scegli lo *stile*: scrivi `standard` oppure `curvy`"
PROCESSING = "Sto calcolando il percorso…"
INVALID_INPUT = "Formato non valido. Invia posizione o testo `lat,lon` (es: `45.123, 8.456`)."
LIMITS_EXCEEDED = "Supera i limiti della versione gratuita. Riduci distanza/waypoint."
ROUTE_NOT_FOUND = "Nessun percorso valido trovato entro i limiti. Prova a cambiare punti."

def reset_state(uid):
    USER_STATE[uid] = {"phase": "start", "start": None, "end": None, "waypoints": [], "style": None}

@app.route("/", methods=["GET"])
def home():
    return "OK - Bot Valhalla online."

@app.route(f"/webhook/{os.environ.get('TELEGRAM_TOKEN', 'token')}", methods=["POST"])
def webhook():
    update = request.get_json(silent=True) or {}
    msg = update.get("message")
    if not msg:
        return jsonify(ok=True)

    chat_id = msg["chat"]["id"]
    uid = msg["from"]["id"]
    text = (msg.get("text") or "").strip()

    if uid not in USER_STATE:
        reset_state(uid)
    state = USER_STATE[uid]

    # Comando /start
    if text.lower().startswith("/start"):
        reset_state(uid)
        send_message(chat_id, WELCOME)
        return jsonify(ok=True)

    phase = state["phase"]

    if phase == "start":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)
        state["start"] = loc
        state["phase"] = "end"
        send_message(chat_id, ASK_END)
        return jsonify(ok=True)

    if phase == "end":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)
        if haversine_km(state["start"], loc) > MAX_RADIUS_KM:
            send_message(chat_id, "La destinazione è oltre 80 km in linea d'aria dal punto di partenza.")
            return jsonify(ok=True)
        state["end"] = loc
        state["phase"] = "waypoints"
        send_message(chat_id, ASK_WAYPOINTS)
        return jsonify(ok=True)

    if phase == "waypoints":
        if text.lower() == "fine":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE)
            return jsonify(ok=True)
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, "Invia waypoint come posizione o `lat,lon`, oppure scrivi `fine`.")
            return jsonify(ok=True)
        if len(state["waypoints"]) >= MAX_WAYPOINTS:
            send_message(chat_id, "Hai già 3 waypoint. Scrivi `fine` per continuare.")
            return jsonify(ok=True)
        state["waypoints"].append(loc)
        send_message(chat_id, f"Waypoint aggiunto ({len(state['waypoints'])}/{MAX_WAYPOINTS}). Invia un altro o scrivi `fine`.")
        return jsonify(ok=True)

    if phase == "style":
        style = text.lower()
        if style not in ("standard", "curvy"):
            send_message(chat_id, "Scrivi `standard` oppure `curvy`.")
            return jsonify(ok=True)

        state["style"] = style
        send_message(chat_id, PROCESSING)

        # Prepara locations per Valhalla
        locs = [{"lat": state["start"][0], "lon": state["start"][1]}]
        for wp in state["waypoints"]:
            locs.append({"lat": wp[0], "lon": wp[1]})
        locs.append({"lat": state["end"][0], "lon": state["end"][1]})

        try:
            coords, dist_km, time_min = valhalla_route(locs, style=style)
        except Exception as e:
            send_message(chat_id, f"Errore routing: {str(e)[:200]}")
            return jsonify(ok=True)

        if dist_km > MAX_ROUTE_KM:
            send_message(chat_id, f"{LIMITS_EXCEEDED} (stima: {dist_km} km)")
            return jsonify(ok=True)

        if not coords or len(coords) < 2:
            send_message(chat_id, ROUTE_NOT_FOUND)
            return jsonify(ok=True)

        gpx_bytes = build_gpx(coords, name=f"Percorso ({style})")
        caption = f"Distanza: {dist_km} km · Durata: {time_min} min · Stile: {style}"
        send_document(chat_id, gpx_bytes, filename="route.gpx", caption=caption)

        reset_state(uid)
        return jsonify(ok=True)

    send_message(chat_id, "Usa /start per iniziare.")
    return jsonify(ok=True)
