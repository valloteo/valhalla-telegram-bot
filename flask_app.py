import os
import time
import json
import urllib.parse
from math import radians, sin, cos, sqrt, atan2
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

MAX_WAYPOINTS = 4
MAX_ROUTE_KM = 120
MAX_RADIUS_KM = 80  # solo per A→B (non roundtrip)

app = Flask(__name__)
USER_STATE = {}

# ======================================
# UTILS
# ======================================

def haversine_km(a, b):
    R = 6371.0
    lat1, lon1 = radians(a[0]), radians(a[1])
    lat2, lon2 = radians(b[0]), radians(b[1])
    dlat = lat2 - lat1
    dlon = radians(b[1] - a[1])
    h = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * atan2(sqrt(h), sqrt(1-h))

def approx_total_km_from_locs(locs, roundtrip: bool) -> float:
    """Somma Haversine tra locations [{'lat':..,'lon':..}], chiude ad anello se roundtrip."""
    if not locs or len(locs) < 2:
        return 0.0
    total = 0.0
    for i in range(len(locs) - 1):
        a = (locs[i]["lat"], locs[i]["lon"])
        b = (locs[i+1]["lat"], locs[i+1]["lon"])
        total += haversine_km(a, b)
    if roundtrip:
        a = (locs[-1]["lat"], locs[-1]["lon"])
        b = (locs[0]["lat"], locs[0]["lon"])
        total += haversine_km(a, b)
    return total

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

# ---------- Geocoding indirizzi ----------
def geocode_address(q):
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
    except Exception:
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
        except Exception:
            pass

    return geocode_address(text)

# ---------- Google Maps link ----------
def build_gmaps_url(start, end, waypoints):
    origin = f"{start[0]},{start[1]}"
    destination = f"{end[0]},{end[1]}"
    wp_list = [f"{lat},{lon}" for (lat, lon) in waypoints] if waypoints else []

    params = {"api": "1", "origin": origin, "destination": destination}
    if wp_list:
        params["waypoints"] = "|".join(wp_list)

    return "https://www.google.com/maps/dir/?" + urllib.parse.urlencode(params, safe="|,")

# ---------- GPX ----------
def build_gpx(coords, name="Percorso"):
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    for lat, lon in coords:
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon))

    return gpx.to_xml().encode("utf-8")

# ---------- KML ----------
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

# ---------- Polyline decoder (precisione 6) ----------
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

# ---------- Seed per Round Trip se mancano waypoint ----------
def add_roundtrip_seed_if_needed(locs, d_km=5.0):
    # Se c'è solo lo start, aggiunge 2 waypoint sintetici per creare un anello corto
    if len(locs) == 1:
        lat = locs[0]["lat"]
        lon = locs[0]["lon"]
        # conversioni approssimative km→gradi
        dlat = d_km / 110.574
        denom = max(0.1, cos(radians(lat)))  # evita divisione per ~0
        dlon = d_km / (111.320 * denom)
        wp1 = {"lat": lat + dlat, "lon": lon + dlon}
        wp2 = {"lat": lat + dlat * 0.5, "lon": lon - dlon}
        locs.extend([wp1, wp2])
    return locs

# ======================================
# UI / MESSAGGI / PULSANTI
# ======================================

WELCOME = (
    "🏍️ *Benvenuto nel MotoRoute Bot!*\n\n"
    "Genera *GPX*, *KML* e un link *Google Maps*.\n\n"
    "1️⃣ Invia la *partenza* (posizione, `lat,lon` o indirizzo)\n"
    "2️⃣ Scegli *Round Trip* subito oppure imposta la *destinazione*\n"
    "3️⃣ (Opz.) aggiungi fino a *4 waypoint*, poi *Fine*\n"
    "4️⃣ Scegli *Standard* o *Curvy*\n\n"
    "🔒 *Limiti*: 120 km totali · 80 km raggio per A→B · 4 waypoint · solo asfalto"
)

ASK_END = "Perfetto! Ora manda la *destinazione* (posizione, `lat,lon` o indirizzo)."
ASK_WAYPOINTS = (
    "Vuoi aggiungere *waypoint*? Fino a 4.\n"
    "Invia una posizione/indirizzo, oppure premi *Fine*."
)
ASK_STYLE_TEXT = "Seleziona lo *stile*:"
PROCESSING = "⏳ Sto calcolando il percorso…"
INVALID_INPUT = "❌ Formato non valido. Invia posizione, `lat,lon` oppure un indirizzo."
LIMITS_EXCEEDED = "⚠️ Supera i limiti della versione free. Riduci distanza/waypoint."
ROUTE_NOT_FOUND = "❌ Nessun percorso trovato. Modifica i punti e riprova."
CANCELLED = "🛑 Operazione annullata. Usa /start per ricominciare."
RESTARTED = "🔄 Conversazione ricominciata! Invia la *partenza*."

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

def cancel_restart_keyboard():
    return {
        "inline_keyboard": [
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

def reset_state(uid):
    USER_STATE[uid] = {
        "phase": "start",
        "start": None,
        "end": None,
        "waypoints": [],
        "style": None,
        "roundtrip": False
    }

# ======================================
# VALHALLA ROUTING (motorcycle only, retry + failover)
# ======================================

def post_valhalla(url, payload, timeout=30, retries=1):
    """
    POST con retry semplice. Ritorna response JSON o solleva eccezione.
    """
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.post(f"{url}/route", json=payload, timeout=timeout)
            if r.ok:
                return r.json()
            last_err = f"{r.status_code} {r.text[:200]}"
        except Exception as e:
            last_err = str(e)
        time.sleep(0.5)
    raise RuntimeError(last_err or "Errore sconosciuto Valhalla")

def valhalla_route(locations, style="standard", roundtrip=False):
    if not VALHALLA_URL:
        raise RuntimeError("VALHALLA_URL non configurato.")

    # Chiudi l’anello se roundtrip (Valhalla richiede esplicita chiusura)
    if roundtrip:
        start = locations[0]
        locations = locations + [{"lat": start["lat"], "lon": start["lon"]}]

    def build_payload_motorcycle(curvy_variant: int):
        """
        curvy_variant:
          0 -> standard motorcycle (nessuna opzione)
          1 -> curvy: use_highways=0.0
          2 -> curvy: use_highways=0.0 + exclude_unpaved=true
          3 -> curvy: use_highways=0.0 + avoid_bad_surfaces=true
          4 -> curvy: use_highways=0.0 + exclude_unpaved=true + avoid_bad_surfaces=true
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
            "locations": locations,
            "costing": "motorcycle",
            "directions_options": {"units": "kilometers"},
        }
        if co:
            body["costing_options"] = co
        return body

    # Sequenza tentativi SOLO motorcycle (niente auto)
    variants = [0] if style == "standard" else [4, 2, 3, 1]
    last_err = None
    resp_json = None

    urls_to_try = [VALHALLA_URL]
    if VALHALLA_URL_FALLBACK and VALHALLA_URL_FALLBACK != VALHALLA_URL:
        urls_to_try.append(VALHALLA_URL_FALLBACK)

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

    for leg in trip.get("legs", []):
        shape = leg.get("shape")
        if shape:
            coords.extend(decode_polyline6(shape))
        summary = leg.get("summary", {})
        total_km += float(summary.get("length", 0.0))      # km
        total_min += float(summary.get("time", 0.0)) / 60  # minuti

    return coords, round(total_km, 1), round(total_min, 1)

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
        valhalla_fallback=bool(VALHALLA_URL_FALLBACK),
        max_wp=MAX_WAYPOINTS
    )

# Webhook flessibile: /webhook/<token> con check del token
@app.route("/webhook/<path:token>", methods=["POST"])
def webhook(token):
    if token != os.environ.get("TELEGRAM_TOKEN"):
        return jsonify(ok=False, error="forbidden"), 403

    update = request.get_json(silent=True) or {}

    # ---------- CALLBACK (pulsanti) ----------
    if "callback_query" in update:
        cq = update["callback_query"]
        data = cq.get("data", "")
        chat_id = cq["message"]["chat"]["id"]
        uid = cq["from"]["id"]
        answer_callback_query(cq["id"])

        if uid not in USER_STATE:
            reset_state(uid)
        state = USER_STATE[uid]

        # Annulla / Ricomincia
        if data == "action:cancel":
            reset_state(uid)
            send_message(chat_id, CANCELLED)
            return jsonify(ok=True)

        if data == "action:restart":
            reset_state(uid)
            send_message(chat_id, RESTARTED)
            return jsonify(ok=True)

        # Scelta dopo Start
        if data == "action:set_end":
            state["roundtrip"] = False
            state["phase"] = "end"
            send_message(chat_id, ASK_END, reply_markup=cancel_restart_keyboard())
            return jsonify(ok=True)

        if data == "action:roundtrip_now":
            state["roundtrip"] = True
            state["end"] = None
            state["phase"] = "waypoints"
            send_message(
                chat_id,
                "Round Trip dalla partenza.\nAggiungi waypoint opzionali oppure premi *Fine*.",
                reply_markup=waypoints_keyboard()
            )
            return jsonify(ok=True)

        # Fine waypoint (passa alla scelta stile)
        if data == "action:finish_waypoints":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
            return jsonify(ok=True)

        # Selezione stile (standard/curvy)
        if data.startswith("style:"):
            style_choice = data.split(":", 1)[1]  # "standard" | "curvy"
            is_roundtrip = bool(state.get("roundtrip"))

            # Prepara locations
            locs = [{"lat": state["start"][0], "lon": state["start"][1]}]
            for wp in state["waypoints"]:
                locs.append({"lat": wp[0], "lon": wp[1]})

            if is_roundtrip:
                locs = add_roundtrip_seed_if_needed(locs)
                approx_km = approx_total_km_from_locs(locs, roundtrip=True)
            else:
                if not state.get("end"):
                    send_message(chat_id, "⚠️ Devi impostare una destinazione prima di calcolare A→B.")
                    return jsonify(ok=True)
                locs.append({"lat": state["end"][0], "lon": state["end"][1]})
                approx_km = approx_total_km_from_locs(locs, roundtrip=False)

            # Pre-check limiti prima della chiamata a Valhalla (con piccolo cuscinetto)
            if approx_km > MAX_ROUTE_KM * 1.25:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nStima: ~{round(approx_km,1)} km")
                return jsonify(ok=True)

            send_message(chat_id, PROCESSING)

            try:
                coords, dist_km, time_min = valhalla_route(
                    locs,
                    style=("curvy" if style_choice == "curvy" else "standard"),
                    roundtrip=is_roundtrip
                )
            except Exception as e:
                send_message(chat_id, f"Errore routing:\n{str(e)[:250]}")
                return jsonify(ok=True)

            if dist_km > MAX_ROUTE_KM:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
                return jsonify(ok=True)

            if not coords or len(coords) < 2:
                send_message(chat_id, ROUTE_NOT_FOUND)
                return jsonify(ok=True)

            # File
            gpx_bytes = build_gpx(coords, "Percorso Moto")
            send_document(chat_id, gpx_bytes, "route.gpx",
                          caption=f"Distanza: {dist_km} km · Durata: {time_min} min")

            kml_bytes = build_kml(coords)
            send_document(chat_id, kml_bytes, "route.kml")

            # Link Google Maps
            if is_roundtrip:
                gmaps_url = build_gmaps_url(state["start"], state["start"], state["waypoints"])
            else:
                gmaps_url = build_gmaps_url(state["start"], state["end"], state["waypoints"])

            send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

            reset_state(uid)
            return jsonify(ok=True)

        # Callback non gestito
        return jsonify(ok=True)

    # ---------- MESSAGGI NORMALI ----------
    msg = update.get("message")
    if not msg:
        return jsonify(ok=True)

    chat_id = msg["chat"]["id"]
    uid = msg["from"]["id"]
    text = (msg.get("text") or "").strip()
    lower = text.lower()

    if uid not in USER_STATE:
        reset_state(uid)
    state = USER_STATE[uid]

    # Comandi globali
    if lower == "/start":
        reset_state(uid)
        send_message(chat_id, WELCOME)
        return jsonify(ok=True)

    if lower == "annulla":
        reset_state(uid)
        send_message(chat_id, CANCELLED)
        return jsonify(ok=True)

    if lower == "ricomincia":
        reset_state(uid)
        send_message(chat_id, RESTARTED)
        return jsonify(ok=True)

    phase = state["phase"]

    # START → scegli RoundTrip subito o End
    if phase == "start":
        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)

        state["start"] = loc
        state["phase"] = "choose_route_type"
        send_message(
            chat_id,
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
            send_message(chat_id, "⚠️ La destinazione è oltre *80 km* in linea d’aria dalla partenza.")
            return jsonify(ok=True)

        state["end"] = loc
        state["phase"] = "waypoints"
        send_message(chat_id, ASK_WAYPOINTS, reply_markup=waypoints_keyboard())
        return jsonify(ok=True)

    # WAYPOINTS
    if phase == "waypoints":
        # Preferiamo il pulsante ✅ Fine, ma gestiamo anche input testuale "fine"
        if lower == "fine":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
            return jsonify(ok=True)

        loc = parse_location_from_message(msg)
        if not loc:
            send_message(chat_id, INVALID_INPUT)
            return jsonify(ok=True)

        if len(state["waypoints"]) >= MAX_WAYPOINTS:
            send_message(chat_id, f"Hai già {MAX_WAYPOINTS} waypoint.\nPremi *Fine* per continuare.")
            return jsonify(ok=True)

        state["waypoints"].append(loc)
        send_message(
            chat_id,
            f"Waypoint aggiunto ({len(state['waypoints'])}/{MAX_WAYPOINTS}). "
            "Aggiungine un altro oppure premi *Fine*.",
            reply_markup=waypoints_keyboard()
        )
        return jsonify(ok=True)

    # STYLE (fallback se l’utente scrive invece di premere i pulsanti)
    if phase == "style":
        if lower not in ("standard", "curvy"):
            send_message(chat_id, "Scegli `standard` o `curvy`, oppure usa i pulsanti.")
            return jsonify(ok=True)

        is_roundtrip = bool(state.get("roundtrip"))

        locs = [{"lat": state["start"][0], "lon": state["start"][1]}]
        for wp in state["waypoints"]:
            locs.append({"lat": wp[0], "lon": wp[1]})

        if is_roundtrip:
            locs = add_roundtrip_seed_if_needed(locs)
            approx_km = approx_total_km_from_locs(locs, roundtrip=True)
        else:
            if not state.get("end"):
                send_message(chat_id, "⚠️ Devi impostare una destinazione prima di calcolare A→B.")
                return jsonify(ok=True)
            locs.append({"lat": state["end"][0], "lon": state["end"][1]})
            approx_km = approx_total_km_from_locs(locs, roundtrip=False)

        if approx_km > MAX_ROUTE_KM * 1.25:
            send_message(chat_id, f"{LIMITS_EXCEEDED}\nStima: ~{round(approx_km,1)} km")
            return jsonify(ok=True)

        send_message(chat_id, PROCESSING)

        try:
            coords, dist_km, time_min = valhalla_route(
                locs,
                style=("curvy" if lower == "curvy" else "standard"),
                roundtrip=is_roundtrip
            )
        except Exception as e:
            send_message(chat_id, f"Errore routing:\n{str(e)[:250]}")
            return jsonify(ok=True)

        if dist_km > MAX_ROUTE_KM:
            send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
            return jsonify(ok=True)

        if not coords or len(coords) < 2:
            send_message(chat_id, ROUTE_NOT_FOUND)
            return jsonify(ok=True)

        gpx_bytes = build_gpx(coords, "Percorso Moto")
        send_document(chat_id, gpx_bytes, "route.gpx",
                      caption=f"Distanza: {dist_km} km · Durata: {time_min} min")

        kml_bytes = build_kml(coords)
        send_document(chat_id, kml_bytes, "route.kml")

        if is_roundtrip:
            gmaps_url = build_gmaps_url(state["start"], state["start"], state["waypoints"])
        else:
            gmaps_url = build_gmaps_url(state["start"], state["end"], state["waypoints"])
        send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

        reset_state(uid)
        return jsonify(ok=True)

    send_message(chat_id, "❓ Usa /start per cominciare.")
    return jsonify(ok=True)
