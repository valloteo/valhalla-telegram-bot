import os
import time
import json
import urllib.parse
from math import radians, sin, cos, sqrt, atan2, asin, degrees
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
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))  # imposta il tuo Telegram user id

MAX_WAYPOINTS = 4
MAX_ROUTE_KM = 120
MAX_RADIUS_KM = 80      # solo per A→B (non roundtrip)
RATE_LIMIT_DAYS = 7     # 1 download a settimana (owner escluso)

# Round Trip hard limits richiesti
RT_MIN_KM = 70.0
RT_MAX_KM = 80.0
RT_TARGET_KM = (RT_MIN_KM + RT_MAX_KM) / 2.0

app = Flask(__name__)

# Memorie in-process (Render free: si azzerano ai riavvii)
USER_STATE = {}
AUTHORIZED = set()
PENDING = set()
LAST_DOWNLOAD = {}  # uid -> epoch seconds

if OWNER_ID:
    AUTHORIZED.add(OWNER_ID)

# ======================================
# UTILS
# ======================================

def now_epoch() -> float:
    return time.time()

def epoch_to_local(e: float) -> str:
    try:
        dt = datetime.fromtimestamp(e, tz=timezone.utc).astimezone()
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
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
    """Somma Haversine tra locations Valhalla [{'lat':..,'lon':..}] + chiusura."""
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

# ---------- Helpers stringhe/coordinate ----------
def fmt_latlon(lat: float, lon: float) -> str:
    return f"{lat:.6f},{lon:.6f}"

def build_gmaps_url(start, end, waypoints):
    """
    Costruisce Google Maps Directions URL.
    - start/end: tuple (lat, lon)
    - waypoints: list[tuple(lat, lon)]
    """
    origin = fmt_latlon(start[0], start[1])
    destination = fmt_latlon(end[0], end[1])
    params = {
        "api": "1",
        "origin": origin,
        "destination": destination,
        "travelmode": "driving",
    }
    if waypoints:
        wps = [fmt_latlon(lat, lon) for (lat, lon) in waypoints]
        params["waypoints"] = "|".join(wps)

    # Nota: safe='|,'
    return "https://www.google.com/maps/dir/?" + urllib.parse.urlencode(params, safe="|,")

# ---------- GPX ----------
def build_gpx_with_turns(coords, maneuvers, name="Percorso"):
    """
    Crea un GPX con:
    - Track della geometria
    - Waypoint per le manovre (turn-by-turn)
    """
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    for lat, lon in coords:
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon))

    # Aggiungi waypoint manovre
    for m in maneuvers:
        ll = m.get("begin_shape_index")
        title = m.get("instruction") or m.get("street_names", [""])[0] if m.get("street_names") else m.get("type", "maneuver")
        # posizione approssimativa dal shape_index
        if isinstance(ll, int) and 0 <= ll < len(coords):
            lat, lon = coords[ll]
        else:
            # fallback: usa il center LL se presente
            lat = m.get("lat", coords[0][0])
            lon = m.get("lon", coords[0][1])
        wp = gpxpy.gpx.GPXWaypoint(latitude=lat, longitude=lon, name=title)
        # descrizione con distanza del segmento (se disponibile)
        dist = m.get("length", 0.0)  # km
        if dist:
            wp.description = f"{title} — {dist:.1f} km"
        gpx.waypoints.append(wp)

    return gpx.to_xml().encode("utf-8")

# ---------- KML ----------
def build_kml(coords):
    kml_points = ""
    for lat, lon in coords:
        kml_points += f"{lon:.6f},{lat:.6f},0\n"

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

# ---------- Geo helpers (bearing / offset) ----------
def offset_point(lat, lon, km, bearing_deg):
    """
    Sposta il punto (lat, lon) di 'km' con bearing 'bearing_deg' (sfera).
    """
    R = 6371.0
    d = km / R
    br = radians(bearing_deg)
    lat1 = radians(lat)
    lon1 = radians(lon)
    lat2 = asin(sin(lat1)*cos(d) + cos(lat1)*sin(d)*cos(br))
    lon2 = lon1 + atan2(sin(br)*sin(d)*cos(lat1), cos(d) - sin(lat1)*sin(lat2))
    return (degrees(lat2), degrees(lon2))

BEARING_BY_DIR = {
    "N": 0, "NE": 45, "E": 90, "SE": 135,
    "S": 180, "SW": 225, "W": 270, "NW": 315
}

def seeds_from_bearing(start_lat, start_lon, d_km, bearing_deg):
    """
    Genera due seed a partire dal bearing iniziale (±60°) per creare un anello.
    """
    b1 = (bearing_deg + 60) % 360
    b2 = (bearing_deg - 60) % 360
    s1 = offset_point(start_lat, start_lon, d_km, b1)
    s2 = offset_point(start_lat, start_lon, d_km, b2)
    return [{"lat": s1[0], "lon": s1[1]}, {"lat": s2[0], "lon": s2[1]}]

# ---------- Overlap heuristic ----------
def overlap_score(coords, min_sep=50, near_m=15.0):
    """
    Euristica: conteggia quante volte la traccia passa entro 'near_m' metri
    da un punto non adiacente (separazione di almeno 'min_sep' punti).
    Maggiore = peggio (più overlap / ritorni).
    """
    if not coords or len(coords) < (min_sep + 5):
        return 0
    near_count = 0
    # campiona con passo per non esplodere O(n^2)
    step = max(1, len(coords)//2000)
    for i in range(0, len(coords), step):
        a = coords[i]
        for j in range(i + min_sep, len(coords), step):
            b = coords[j]
            if haversine_km(a, b) * 1000.0 <= near_m:
                near_count += 1
    return near_count

# ======================================
# UI / MESSAGGI / PULSANTI
# ======================================

WELCOME = (
    "🏍️ *MotoRoute Bot*\n\n"
    "Genera *GPX*, *KML* e un link *Google Maps*.\n\n"
    "1️⃣ Invia la *partenza* (posizione, `lat,lon` o indirizzo)\n"
    "2️⃣ Scegli *Round Trip* subito oppure imposta la *destinazione*\n"
    "3️⃣ (Opz.) aggiungi fino a *4 waypoint*, poi *Fine*\n"
    "4️⃣ Scegli *Standard* o *Curvy*\n\n"
    "🔒 Limiti: 120 km totali · 80 km raggio per A→B · 4 waypoint.\n"
    "🔁 Round Trip: *min 70 km – max 80 km*."
)

ASK_END = "Perfetto! Ora manda la *destinazione* (posizione, `lat,lon` o indirizzo)."
ASK_WAYPOINTS = (
    "Vuoi aggiungere *waypoint*? Fino a 4.\n"
    "Invia una posizione/indirizzo, oppure premi *Fine*."
)
ASK_STYLE_TEXT = "Seleziona lo *stile*:"
ASK_DIR_TEXT = "Scegli la *direzione iniziale* del giro (orienta l'anello):"
PROCESSING = "⏳ Sto calcolando il percorso…"
INVALID_INPUT = "❌ Formato non valido. Invia posizione, `lat,lon` oppure un indirizzo."
LIMITS_EXCEEDED = "⚠️ Supera i limiti della versione free. Riduci distanza/waypoint."
ROUTE_NOT_FOUND = "❌ Nessun percorso trovato. Modifica i punti e riprova."
CANCELLED = "🛑 Operazione annullata. Usa /start per ricominciare."
RESTARTED = "🔄 Conversazione ricominciata! Invia la *partenza*."
NOT_AUTH = (
    "🔒 Questo bot è ad accesso riservato.\n"
    "Ho inviato una *richiesta di autorizzazione* all'owner. Ti avviso quando sarai abilitato."
)
ALREADY_PENDING = "⏳ Hai già una richiesta di accesso in revisione. Attendi conferma."
ACCESS_GRANTED = "✅ Sei stato abilitato! Ora puoi usare il bot."
ACCESS_DENIED = "❌ La tua richiesta di accesso è stata rifiutata."
RATE_LIMIT_MSG = "⏱️ Hai raggiunto il limite di *1 download a settimana*. Potrai riprovare dopo: *{when}*."

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

def direction_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "N",  "callback_data": "dir:N"},
                {"text": "NE", "callback_data": "dir:NE"},
                {"text": "E",  "callback_data": "dir:E"},
                {"text": "SE", "callback_data": "dir:SE"},
            ],
            [
                {"text": "S",  "callback_data": "dir:S"},
                {"text": "SW", "callback_data": "dir:SW"},
                {"text": "W",  "callback_data": "dir:W"},
                {"text": "NW", "callback_data": "dir:NW"},
            ],
            [
                {"text": "🤖 Auto", "callback_data": "dir:AUTO"},
                {"text": "❌ Annulla", "callback_data": "action:cancel"}
            ]
        ]
    }

def admin_request_keyboard(uid:int, name:str):
    return {
        "inline_keyboard": [
            [
                {"text": f"✅ Approva {name} ({uid})", "callback_data": f"admin:approve:{uid}"},
                {"text": f"❌ Rifiuta {name}", "callback_data": f"admin:deny:{uid}"}
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
        "initial_bearing": None  # gradi (0..359) o None per auto
    }

# ======================================
# VALHALLA ROUTING (motorcycle only, retry + failover)
# ======================================

def post_valhalla(url, payload, timeout=30, retries=1):
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.post(f"{url}/route", json=payload, timeout=timeout)
            if r.ok:
                return r.json()
            last_err = f"{r.status_code} {r.text[:200]}"
        except Exception as e:
            last_err = str(e)
        time.sleep(0.4)
    raise RuntimeError(last_err or "Errore sconosciuto Valhalla")

def valhalla_route(locations, style="standard", roundtrip=False):
    if not VALHALLA_URL:
        raise RuntimeError("VALHALLA_URL non configurato.")

    # Chiudi anello per roundtrip
    if roundtrip:
        start = locations[0]
        locations = locations + [{"lat": start["lat"], "lon": start["lon"]}]

    def build_payload_motorcycle(curvy_variant: int):
        """
        0 -> standard
        1..4 -> varianti curvy (compatibili con istanze pubbliche)
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

    variants = [0] if style == "standard" else [4, 2, 3, 1]
    urls_to_try = [VALHALLA_URL] + ([VALHALLA_URL_FALLBACK] if VALHALLA_URL_FALLBACK and VALHALLA_URL_FALLBACK != VALHALLA_URL else [])

    last_err = None
    resp_json = None

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
    all_maneuvers = []

    for leg in trip.get("legs", []):
        shape = leg.get("shape")
        if shape:
            coords.extend(decode_polyline6(shape))
        summary = leg.get("summary", {})
        total_km += float(summary.get("length", 0.0))      # km
        total_min += float(summary.get("time", 0.0)) / 60  # min
        # maneuvers
        mans = leg.get("maneuvers", [])
        # Valhalla maniuvrs: length è in km; begin_shape_index per localizzazione
        all_maneuvers.extend(mans)

    return coords, round(total_km, 1), round(total_min, 1), all_maneuvers

# ---------- Round Trip planner (70–80 km, anti-overlap) ----------
def plan_roundtrip_with_direction(start, user_wps, style, initial_bearing_deg=None):
    """
    Trova un roundtrip 70–80 km:
    - Seleziona la distanza seed con ricerca adattiva (3..30 km)
    - Prova angoli alternati per ridurre overlap
    - Ritorna best (coords, dist_km, time_min, maneuvers)
    """
    s_lat, s_lon = start
    base_locs = [{"lat": s_lat, "lon": s_lon}]
    for wp in user_wps:
        base_locs.append({"lat": wp[0], "lon": wp[1]})

    # Se l'utente non ha scelto direzione, proviamo in auto (8 direzioni cardinali)
    primary_dirs = []
    if initial_bearing_deg is not None:
        primary_dirs = [initial_bearing_deg]
    else:
        primary_dirs = [0, 45, 90, 135, 180, 225, 270, 315]

    # Ricerca su distanza seed
    d_low, d_high = 3.0, 30.0
    best = None  # (score_overlap, |dist-75|, coords, dist_km, time, mans)

    # fare massimo N iterazioni sulla distanza
    for _ in range(6):
        d_try = (d_low + d_high) / 2.0

        # su ogni distanza, proviamo angoli +/- {0,45,90} rispetto a ciascuna direzione principale
        angle_offsets = [0, 45, -45, 90, -90]
        candidates = []

        for base_bearing in primary_dirs:
            for ao in angle_offsets:
                bearing = (base_bearing + ao) % 360
                # costruisci locs = start + user_wps + seeds
                locs = list(base_locs)
                seeds = seeds_from_bearing(s_lat, s_lon, d_try, bearing)
                locs.extend(seeds)
                # controllo approssimativo prima della chiamata (veloce)
                approx_km = approx_total_km_from_locs(locs + [{"lat": s_lat, "lon": s_lon}], roundtrip=False)
                # filtro molto lasco per evitare chiamate inutili: nell'ordine giusto di grandezza
                if approx_km < RT_MIN_KM * 0.5 or approx_km > RT_MAX_KM * 1.8:
                    continue

                try:
                    coords, dist_km, time_min, mans = valhalla_route(locs, style=style, roundtrip=True)
                except Exception:
                    continue

                # scarta se fuori range
                if dist_km < RT_MIN_KM or dist_km > RT_MAX_KM:
                    # aggiorna bounds per iterazioni successive
                    if dist_km < RT_MIN_KM:
                        d_low = max(d_low, d_try)
                    else:
                        d_high = min(d_high, d_try)
                    continue

                # calcola overlap score (più basso è meglio) e distanza da target
                ov = overlap_score(coords)
                delta = abs(dist_km - RT_TARGET_KM)
                candidates.append((ov, delta, coords, dist_km, time_min, mans))

        if candidates:
            # seleziona migliore
            candidates.sort(key=lambda x: (x[0], x[1]))
            best = candidates[0]
            break
        else:
            # se nessun candidato nel range, ritenta con nuova distanza
            # euristica: allarga un po' se d_low, d_high sono troppo vicini
            if (d_high - d_low) < 1.0:
                d_low = max(2.0, d_low - 1.0)
                d_high = min(35.0, d_high + 1.0)
            # sposta d_try secondo la logica precedente (già aggiornata nei tentativi)
            # altrimenti, prova a forzare verso target medio
            d_mid = (d_low + d_high) / 2.0
            d_low = max(2.0, d_mid - 2.5)
            d_high = min(35.0, d_mid + 2.5)

    # se non si è trovato nulla nel range, prova una volta a prendere il più vicino alla banda 70–80
    if not best:
        # fallback: sceglirotta più vicina al target provando alcune distanze fisse
        for d_try in [8, 10, 12, 15, 18, 20, 22]:
            for base_bearing in primary_dirs:
                for ao in [0, 45, -45]:
                    bearing = (base_bearing + ao) % 360
                    locs = list(base_locs)
                    seeds = seeds_from_bearing(s_lat, s_lon, d_try, bearing)
                    locs.extend(seeds)
                    try:
                        coords, dist_km, time_min, mans = valhalla_route(locs, style=style, roundtrip=True)
                    except Exception:
                        continue
                    ov = overlap_score(coords)
                    delta_to_band = 0 if RT_MIN_KM <= dist_km <= RT_MAX_KM else min(abs(dist_km-RT_MIN_KM), abs(dist_km-RT_MAX_KM))
                    cand = (ov, delta_to_band, coords, dist_km, time_min, mans)
                    if (best is None) or (cand < best):
                        best = cand

    if not best:
        raise RuntimeError("Impossibile trovare un anello 70–80 km. Prova a cambiare zona/waypoint.")

    # ritorna migliore
    _, _, coords, dist_km, time_min, mans = best
    return coords, dist_km, time_min, mans

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
        max_wp=MAX_WAYPOINTS,
        owner=bool(OWNER_ID),
        authorized=len(AUTHORIZED)
    )

# Webhook: /webhook/<token>
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

        # Admin (owner)
        if data.startswith("admin:"):
            if uid != OWNER_ID:
                return jsonify(ok=True)
            parts = data.split(":")
            if len(parts) == 3 and parts[1] in ("approve", "deny") and parts[2].isdigit():
                target_id = int(parts[2])
                if parts[1] == "approve":
                    AUTHORIZED.add(target_id)
                    PENDING.discard(target_id)
                    send_message(target_id, ACCESS_GRANTED)
                    send_message(chat_id, f"✅ Utente {target_id} autorizzato.")
                else:
                    PENDING.discard(target_id)
                    send_message(target_id, ACCESS_DENIED)
                    send_message(chat_id, f"🚫 Richiesta di {target_id} rifiutata.")
            return jsonify(ok=True)

        # Annulla / Ricomincia
        if data == "action:cancel":
            reset_state(uid)
            send_message(chat_id, CANCELLED)
            return jsonify(ok=True)

        if data == "action:restart":
            reset_state(uid)
            send_message(chat_id, RESTARTED)
            return jsonify(ok=True)

        # Access control (owner/autorizzati)
        if uid != OWNER_ID and uid not in AUTHORIZED:
            if uid not in PENDING:
                PENDING.add(uid)
                try:
                    send_message(
                        OWNER_ID,
                        f"📩 *Richiesta accesso* da {uname} (id: `{uid}`).",
                        reply_markup=admin_request_keyboard(uid, uname)
                    )
                except:
                    pass
            send_message(chat_id, NOT_AUTH)
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
            state["phase"] = "pick_direction"
            send_message(chat_id, ASK_DIR_TEXT, reply_markup=direction_keyboard())
            return jsonify(ok=True)

        # Direzione roundtrip
        if data.startswith("dir:"):
            val = data.split(":", 1)[1]
            if val == "AUTO":
                state["initial_bearing"] = None
            else:
                state["initial_bearing"] = BEARING_BY_DIR.get(val, None)
            state["phase"] = "waypoints"
            send_message(chat_id, "Ok! Ora puoi aggiungere *waypoint* oppure premere *Fine*.", reply_markup=waypoints_keyboard())
            return jsonify(ok=True)

        # Fine waypoint → stile
        if data == "action:finish_waypoints":
            state["phase"] = "style"
            send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
            return jsonify(ok=True)

        # Selezione stile
        if data.startswith("style:"):
            style = data.split(":", 1)[1]  # "standard" | "curvy"
            is_roundtrip = bool(state.get("roundtrip"))

            # Rate limit (owner escluso)
            if uid != OWNER_ID:
                last = LAST_DOWNLOAD.get(uid, 0)
                if now_epoch() - last < RATE_LIMIT_DAYS * 86400:
                    when = epoch_to_local(last + RATE_LIMIT_DAYS * 86400)
                    send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                    return jsonify(ok=True)

            start = state["start"]
            wps = state["waypoints"]

            if is_roundtrip:
                # Calcolo roundtrip 70–80 km con direzione
                send_message(chat_id, PROCESSING)
                try:
                    coords, dist_km, time_min, mans = plan_roundtrip_with_direction(
                        start=start,
                        user_wps=wps,
                        style=("curvy" if style == "curvy" else "standard"),
                        initial_bearing_deg=state.get("initial_bearing")
                    )
                except Exception as e:
                    send_message(chat_id, f"Errore routing (roundtrip):\n{str(e)[:250]}")
                    return jsonify(ok=True)

                # File
                gpx_bytes = build_gpx_with_turns(coords, mans, "Round Trip Moto")
                send_document(chat_id, gpx_bytes, "route.gpx",
                              caption=f"Round Trip · Distanza: {dist_km} km · Durata: {time_min} min")

                kml_bytes = build_kml(coords)
                send_document(chat_id, kml_bytes, "route.kml")

                # Link Google Maps (origin=start, destination=start, waypoints = solo i WP utente)
                gmaps_url = build_gmaps_url(start, start, wps)
                send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

                LAST_DOWNLOAD[uid] = now_epoch()
                reset_state(uid)
                return jsonify(ok=True)

            else:
                # A → B
                if not state.get("end"):
                    send_message(chat_id, "⚠️ Devi impostare una destinazione prima di calcolare A→B.")
                    return jsonify(ok=True)

                # Prepara locations
                locs = [{"lat": start[0], "lon": start[1]}]
                for wp in wps:
                    locs.append({"lat": wp[0], "lon": wp[1]})
                locs.append({"lat": state["end"][0], "lon": state["end"][1]})

                send_message(chat_id, PROCESSING)
                try:
                    coords, dist_km, time_min, mans = valhalla_route(
                        locs, style=("curvy" if style == "curvy" else "standard"), roundtrip=False
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

                gpx_bytes = build_gpx_with_turns(coords, mans, "Percorso Moto")
                send_document(chat_id, gpx_bytes, "route.gpx",
                              caption=f"Distanza: {dist_km} km · Durata: {time_min} min")

                kml_bytes = build_kml(coords)
                send_document(chat_id, kml_bytes, "route.kml")

                gmaps_url = build_gmaps_url(start, state["end"], wps)
                send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

                LAST_DOWNLOAD[uid] = now_epoch()
                reset_state(uid)
                return jsonify(ok=True)

        return jsonify(ok=True)

    # ---------- MESSAGGI TESTUALI ----------
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

    # /start
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
                        f"📩 *Richiesta accesso* da {uname} (id: `{uid}`).",
                        reply_markup=admin_request_keyboard(uid, uname)
                    )
                except:
                    pass
                send_message(chat_id, NOT_AUTH)
            return jsonify(ok=True)
        send_message(chat_id, WELCOME)
        return jsonify(ok=True)

    # Annulla / Ricomincia
    if text.lower() == "annulla":
        reset_state(uid)
        send_message(chat_id, CANCELLED)
        return jsonify(ok=True)

    if text.lower() == "ricomincia":
        reset_state(uid)
        send_message(chat_id, RESTARTED)
        return jsonify(ok=True)

    # Blocca non autorizzati
    if uid != OWNER_ID and uid not in AUTHORIZED:
        send_message(chat_id, "🔒 Non sei autorizzato. Usa /start per richiedere l'accesso.")
        return jsonify(ok=True)

    phase = state["phase"]

    # START → ricevi partenza
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

    # END → ricevi destinazione
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
        if text.lower() == "fine":
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

    # STYLE (fallback testuale)
    if phase == "style":
        if text.lower() not in ("standard", "curvy"):
            send_message(chat_id, "Scegli `standard` o `curvy`, oppure usa i pulsanti.")
            return jsonify(ok=True)

        # Rate limit (owner escluso)
        if uid != OWNER_ID:
            last = LAST_DOWNLOAD.get(uid, 0)
            if now_epoch() - last < RATE_LIMIT_DAYS * 86400:
                when = epoch_to_local(last + RATE_LIMIT_DAYS * 86400)
                send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                return jsonify(ok=True)

        is_roundtrip = bool(state.get("roundtrip"))
        start = state["start"]
        wps = state["waypoints"]

        if is_roundtrip:
            send_message(chat_id, PROCESSING)
            try:
                coords, dist_km, time_min, mans = plan_roundtrip_with_direction(
                    start=start,
                    user_wps=wps,
                    style=("curvy" if text.lower() == "curvy" else "standard"),
                    initial_bearing_deg=state.get("initial_bearing")
                )
            except Exception as e:
                send_message(chat_id, f"Errore routing (roundtrip):\n{str(e)[:250]}")
                return jsonify(ok=True)

            gpx_bytes = build_gpx_with_turns(coords, mans, "Round Trip Moto")
            send_document(chat_id, gpx_bytes, "route.gpx",
                          caption=f"Round Trip · Distanza: {dist_km} km · Durata: {time_min} min")

            kml_bytes = build_kml(coords)
            send_document(chat_id, kml_bytes, "route.kml")

            gmaps_url = build_gmaps_url(start, start, wps)
            send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

            LAST_DOWNLOAD[uid] = now_epoch()
            reset_state(uid)
            return jsonify(ok=True)
        else:
            if not state.get("end"):
                send_message(chat_id, "⚠️ Devi impostare una destinazione prima di calcolare A→B.")
                return jsonify(ok=True)

            locs = [{"lat": start[0], "lon": start[1]}]
            for wp in wps:
                locs.append({"lat": wp[0], "lon": wp[1]})
            locs.append({"lat": state["end"][0], "lon": state["end"][1]})

            send_message(chat_id, PROCESSING)
            try:
                coords, dist_km, time_min, mans = valhalla_route(
                    locs, style=("curvy" if text.lower() == "curvy" else "standard"), roundtrip=False
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

            gpx_bytes = build_gpx_with_turns(coords, mans, "Percorso Moto")
            send_document(chat_id, gpx_bytes, "route.gpx",
                          caption=f"Distanza: {dist_km} km · Durata: {time_min} min")

            kml_bytes = build_kml(coords)
            send_document(chat_id, kml_bytes, "route.kml")

            gmaps_url = build_gmaps_url(start, state["end"], wps)
            send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

            LAST_DOWNLOAD[uid] = now_epoch()
            reset_state(uid)
            return jsonify(ok=True)

    send_message(chat_id, "❓ Usa /start per cominciare.")
    return jsonify(ok=True)
