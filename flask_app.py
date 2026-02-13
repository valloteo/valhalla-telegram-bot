import os
import urllib.parse
from math import radians, sin, cos, sqrt, atan2
from flask import Flask, request, jsonify
import requests
import gpxpy
import gpxpy.gpx

# ======================================
# CONFIGURAZIONE
# ======================================

TOKEN = os.environ.get("TELEGRAM_TOKEN")
VALHALLA_URL = os.environ.get("VALHALLA_URL", "").rstrip("/")

MAX_WAYPOINTS = 4       # <-- NUOVO LIMITE
MAX_ROUTE_KM = 120
MAX_RADIUS_KM = 80      # solo per Start → End (non per roundtrip)

app = Flask(__name__)
USER_STATE = {}         # memoria conversazione in RAM

# ======================================
# UTILS GENERALI
# ======================================

def haversine_km(a, b):
    """Distanza linea d’aria tra due coordinate."""
    R = 6371.0
    lat1, lon1 = radians(a[0]), radians(a[1])
    lat2, lon2 = radians(b[0]), radians(b[1])
    dlat = lat2 - lat1
    dlon = radians(b[1] - a[1])
    h = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * atan2(sqrt(h), sqrt(1-h))

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
    """Invia un file al bot."""
    url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
    files = {"document": (filename, file_bytes, "application/octet-stream")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    requests.post(url, data=data, files=files, timeout=25)

def answer_callback_query(cq_id, text=None):
    url = f"https://api.telegram.org/bot{TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": cq_id}
    if text:
        payload["text"] = text
    requests.post(url, json=payload, timeout=10)

# ======================================
# GEOLOCATION / GEOCODING
# ======================================

def geocode_address(q):
    """
    Converte un indirizzo testuale in coordinate usando Nominatim (OSM).
    Restituisce (lat, lon) oppure None.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": q, "format": "json", "limit": 1}
    headers = {"User-Agent": "MotoGPXBot/1.0"}
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
    """
    Accetta:
    - posizione telegram
    - testo "lat,lon"
    - indirizzo (geocoding)
    """
    # 1) posizione Telegram
    if "location" in msg:
        loc = msg["location"]
        return (loc["latitude"], loc["longitude"])

    text = (msg.get("text") or "").strip()

    # 2) lat,lon
    if "," in text:
        try:
            parts = text.split(",")
            return (float(parts[0].strip()), float(parts[1].strip()))
        except:
            pass

    # 3) indirizzo → geocoding
    coords = geocode_address(text)
    return coords  # può essere None

# ======================================
# FORMATO GOOGLE MAPS
# ======================================

def build_gmaps_url(start, end, waypoints):
    """Crea link Google Maps Directions"""
    origin = f"{start[0]},{start[1]}"
    destination = f"{end[0]},{end[1]}"
    wp_list = [f"{lat},{lon}" for (lat, lon) in waypoints] if waypoints else []

    params = {
        "api": "1",
        "origin": origin,
        "destination": destination,
    }
    if wp_list:
        params["waypoints"] = "|".join(wp_list)

    return (
        "https://www.google.com/maps/dir/?"
        + urllib.parse.urlencode(params, safe="|,")
    )

# ======================================
# FILE GPX
# ======================================

def build_gpx(coords, name="Percorso"):
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    for lat, lon in coords:
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon))

    return gpx.to_xml().encode("utf-8")

# ======================================
# FILE KML (per Google Maps / MyMaps)
# ======================================

def build_kml(coords):
    """
    Genera un file KML compatibile con Google MyMaps.
    """
    kml_points = ""
    for lat, lon in coords:
        kml_points += f"{lon},{lat},0\n"

    kml = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
    <name>Percorso Moto</name>
    <Placemark>
        <name>Percorso</name>
        <Style>
            <LineStyle>
                <color>ff0000ff</color>
                <width>4</width>
            </LineStyle>
        </Style>
        <LineString>
            <tessellate>1</tessellate>
            <coordinates>
{kml_points}
            </coordinates>
        </LineString>
    </Placemark>
</Document>
</kml>
"""
    return kml.encode("utf-8")

# ======================================
# DECODE POLYLINE VALHALLA
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
# MESSAGGI E UI (PULSANTI)
# ======================================

WELCOME = (
    "🏍️ *Benvenuto nel MotoRoute Bot!*\n\n"
    "Posso generare un percorso *GPX*, *KML* e un link per *Google Maps*.\n\n"
    "📌 *Come funziona:*\n"
    "1️⃣ Invia la *partenza* (posizione, `lat,lon` o indirizzo)\n"
    "2️⃣ Invia la *destinazione*\n"
    "3️⃣ (Opzionale) Aggiungi fino a *4 waypoint*\n"
    "4️⃣ Scegli lo *stile di guida* o il *Round Trip*\n\n"
    "🔒 *Limiti versione base*\n"
    "• Distanza max: 120 km\n"
    "• Destinazione entro 80 km (solo per A → B)\n"
    "• Max 4 waypoint\n"
    "• Solo asfalto\n\n"
    "Puoi iniziare inviando la partenza! 🗺️"
)

ASK_END = "Perfetto! Ora manda la *destinazione* (posizione, `lat,lon` o indirizzo)."
ASK_WAYPOINTS = (
    "Vuoi aggiungere *waypoint*? Puoi inserirne fino a 4.\n"
    "👉 Invia una posizione, coordinate o indirizzo\n"
    "👉 Quando hai finito, scrivi: `fine`\n\n"
    "Oppure usa i pulsanti qui sotto:"
)

ASK_STYLE_TEXT = "Seleziona lo *stile di guida* oppure un *Round Trip*:"

PROCESSING = "⏳ Sto calcolando il percorso…"

INVALID_INPUT = "❌ Formato non valido. Invia posizione, `lat,lon` oppure un indirizzo valido."
LIMITS_EXCEEDED = "⚠️ Supera i limiti della versione free. Riduci distanza o waypoint."
ROUTE_NOT_FOUND = "❌ Nessun percorso trovato. Modifica i punti e riprova."
CANCELLED = "🛑 Operazione annullata. Usa /start per ricominciare."
RESTARTED = "🔄 Conversazione ricominciata! Invia la *partenza*."

# ======================================
# FUNZIONI PULSANTI INLINE
# ======================================

def style_inline_keyboard():
    """Pulsanti per selezione stile + roundtrip."""
    return {
        "inline_keyboard": [
            [
                {"text": "🛣️ Standard", "callback_data": "style:standard"},
                {"text": "🌀 Curvy leggero", "callback_data": "style:curvy"},
            ],
            [
                {"text": "🔄 Round Trip", "callback_data": "style:roundtrip"},
            ]
        ]
    }

def cancel_restart_keyboard():
    """Pulsanti Annulla / Ricomincia"""
    return {
        "inline_keyboard": [
            [
                {"text": "❌ Annulla", "callback_data": "action:cancel"},
                {"text": "🔄 Ricomincia", "callback_data": "action:restart"}
            ]
        ]
    }

# ======================================
# RESET DELLO STATO
# ======================================

def reset_state(uid):
    USER_STATE[uid] = {
        "phase": "start",
        "start": None,
        "end": None,
        "waypoints": [],
        "style": None
    }
    # ======================================
# FUNZIONE DI ROUTING CON VALHALLA
# ======================================

def valhalla_route(locations, style="standard", roundtrip=False):
    """
    locations = [{"lat":..., "lon":...}, ...]
    style = standard | curvy | roundtrip
    roundtrip=True → aggiunge automaticamente ultimo punto = start
    """
    if not VALHALLA_URL:
        raise RuntimeError("VALHALLA_URL non configurato.")

    # Se roundtrip attivo → chiudiamo il loop con Start come ultimo punto
    if roundtrip:
        start = locations[0]
        locations = locations + [{"lat": start["lat"], "lon": start["lon"]}]

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

    r = requests.post(f"{VALHALLA_URL}/route", json=payload, timeout=30)
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
        total_km += float(summary.get("length", 0.0))      # km
        total_min += float(summary.get("time", 0.0)) / 60  # minuti

    return coords, round(total_km, 1), round(total_min, 1)


# ======================================
# WEBHOOK TELEGRAM (cuore del bot)
# ======================================

@app.route("/webhook/<path:token>", methods=["POST"])
def webhook(token):

    # Sicurezza: conferma che token nel path = token configurato
    if token != os.environ.get("TELEGRAM_TOKEN"):
        return jsonify(ok=False, error="forbidden"), 403

    update = request.get_json(silent=True) or {}

    # =====================================================================
    # 1) CALLBACK QUERY (pulsanti)
    # =====================================================================
    if "callback_query" in update:
        cq = update["callback_query"]
        data = cq.get("data", "")
        chat_id = cq["message"]["chat"]["id"]
        uid = cq["from"]["id"]

        answer_callback_query(cq["id"])

        if uid not in USER_STATE:
            reset_state(uid)
        state = USER_STATE[uid]

        # ---------------------
        # Pulsanti Annulla / Ricomincia
        # ---------------------
        if data == "action:cancel":
            reset_state(uid)
            send_message(chat_id, CANCELLED)
            return jsonify(ok=True)

        if data == "action:restart":
            reset_state(uid)
            send_message(chat_id, RESTARTED)
            return jsonify(ok=True)

        # ---------------------
        # Pulsanti stile
        # ---------------------
        if data.startswith("style:"):
            style = data.split(":", 1)[1]

            # Round Trip?
            is_roundtrip = (style == "roundtrip")

            # Messaggio di attesa
            send_message(chat_id, PROCESSING)

            # Prepara locations
            locs = [{"lat": state["start"][0], "lon": state["start"][1]}]
            for wp in state["waypoints"]:
                locs.append({"lat": wp[0], "lon": wp[1]})

            # Se NON roundtrip, aggiungi End
            if not is_roundtrip:
                locs.append({"lat": state["end"][0], "lon": state["end"][1]})

            # Esegui routing
            try:
                coords, dist_km, time_min = valhalla_route(
                    locs,
                    style="curvy" if style == "curvy" else "standard",
                    roundtrip=is_roundtrip
                )
            except Exception as e:
                send_message(chat_id, f"Errore routing:\n{str(e)[:250]}")
                return jsonify(ok=True)

            # Validazione distanza 120 km
            if dist_km > MAX_ROUTE_KM:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
                return jsonify(ok=True)

            if not coords or len(coords) < 2:
                send_message(chat_id, ROUTE_NOT_FOUND)
                return jsonify(ok=True)

            # Invia GPX
            gpx_bytes = build_gpx(coords, "Percorso Moto")
            send_document(chat_id, gpx_bytes, "route.gpx",
                          caption=f"Distanza: {dist_km} km · Durata: {time_min} min")

            # Invia KML
            kml_bytes = build_kml(coords)
            send_document(chat_id, kml_bytes, "route.kml")

            # Link Google Maps
            if is_roundtrip:
                # In un roundtrip End = Start
                gmaps_url = build_gmaps_url(state["start"], state["start"], state["waypoints"])
            else:
                gmaps_url = build_gmaps_url(state["start"], state["end"], state["waypoints"])

            send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

            reset_state(uid)
            return jsonify(ok=True)

        # Callback sconosciuto
        return jsonify(ok=True)

    # =====================================================================
    # 2) MESSAGGI NORMALI
    # =====================================================================
    msg = update.get("message")
    if not msg:
        return jsonify(ok=True)

    chat_id = msg["chat"]["id"]
    uid = msg["from"]["id"]
    text = (msg.get("text") or "").lower().strip()

    if uid not in USER_STATE:
        reset_state(uid)
    state = USER_STATE[uid]

    # ---------------------
    # Comandi globali
    # ---------------------
    if text == "/start":
        reset_state(uid)
        send_message(chat_id, WELCOME)
        return jsonify(ok=True)

    if text == "annulla":
        reset_state(uid)
        send_message(chat_id, CANCELLED)
        return jsonify(ok=True)

    if text == "ricomincia":
        reset_state(uid)
        send_message(chat_id, RESTARTED)
        return jsonify(ok=True)

    # ---------------------
    # Fasi conversazione
    # ---------------------
    phase = state["phase"]

    # === PHASE: START ===
    if phase == "start":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)

        state["start"] = loc
        state["phase"] = "end"
        send_message(chat_id, ASK_END, reply_markup=cancel_restart_keyboard())
        return jsonify(ok=True)

    # === PHASE: END ===
    if phase == "end":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)

        # Limite 80 km (solo percorso A → B — non roundtrip)
        if haversine_km(state["start"], loc) > MAX_RADIUS_KM:
            send_message(chat_id,
                         "⚠️ La destinazione è oltre *80 km* in linea d’aria dal punto di partenza.")
            return jsonify(ok=True)

        state["end"] = loc
        state["phase"] = "waypoints"
        send_message(chat_id, ASK_WAYPOINTS, reply_markup=cancel_restart_keyboard())
        return jsonify(ok=True)

    # === PHASE: WAYPOINTS ===
    if phase == "waypoints":

        # Utente termina l'aggiunta
        if text == "fine":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
            return jsonify(ok=True)

        # Aggiunge un waypoint
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)

        if len(state["waypoints"]) >= MAX_WAYPOINTS:
            send_message(chat_id,
                         f"Hai già {MAX_WAYPOINTS} waypoint.\nScrivi `fine` per continuare.")
            return jsonify(ok=True)

        state["waypoints"].append(loc)
        send_message(chat_id,
                     f"Waypoint aggiunto ({len(state['waypoints'])}/{MAX_WAYPOINTS}).\n"
                     "Invia un altro oppure scrivi `fine`.",
                     reply_markup=cancel_restart_keyboard())
        return jsonify(ok=True)

    # === PHASE: STYLE (se l’utente scrive invece di premere pulsanti) ===
    if phase == "style":
        if text not in ("standard", "curvy", "roundtrip"):
            send_message(chat_id, "Scegli `standard`, `curvy` o `roundtrip`, oppure usa i pulsanti.")
            return jsonify(ok=True)

        # Emula callback style:
        style = text
        is_roundtrip = (style == "roundtrip")
        send_message(chat_id, PROCESSING)

        locs = [{"lat": state["start"][0], "lon": state["start"][1]}]
        for wp in state["waypoints"]:
            locs.append({"lat": wp[0], "lon": wp[1]})

        if not is_roundtrip:
            locs.append({"lat": state["end"][0], "lon": state["end"][1]})

        try:
            coords, dist_km, time_min = valhalla_route(
                locs,
                style="curvy" if style == "curvy" else "standard",
                roundtrip=is_roundtrip
            )
        except Exception as e:
            send_message(chat_id, f"Errore routing:\n{str(e)[:250]}")
            return jsonify(ok=True)

        if dist_km > MAX_ROUTE_KM:
            send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
            return jsonify(ok=True)

        # GPX + KML
        gpx_bytes = build_gpx(coords, "Percorso Moto")
        send_document(chat_id, gpx_bytes, "route.gpx",
                      caption=f"Distanza: {dist_km} km · Durata: {time_min} min")

        kml_bytes = build_kml(coords)
        send_document(chat_id, kml_bytes, "route.kml")

        # Google Maps link
        if is_roundtrip:
            gmaps_url = build_gmaps_url(state["start"], state["start"], state["waypoints"])
        else:
            gmaps_url = build_gmaps_url(state["start"], state["end"], state["waypoints"])

        send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

        reset_state(uid)
        return jsonify(ok=True)

    # Caso imprevisto
    send_message(chat_id, "❓ Usa /start per cominciare.")
    return jsonify(ok=True)
    # ======================================
# ROUTE DI SERVIZIO (HOME / HEALTH)
# ======================================

@app.route("/", methods=["GET"])
def home():
    return "OK - MotoRoute Bot (Valhalla) online."

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify(status="ok", valhalla=bool(VALHALLA_URL), max_wp=MAX_WAYPOINTS)
