import os
import time
import json
import urllib.parse
from math import radians, sin, cos, sqrt, atan2
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
MAX_ROUTE_KM = 120
MAX_RADIUS_KM = 80   # solo per A→B (non roundtrip)
RATE_LIMIT_DAYS = 7  # 1 download/settimana (owner escluso)

# Altimetria (OpenTopoData)
ELEV_PROVIDER_URL = "https://api.opentopodata.org/v1/srtm90m"
ELEV_SAMPLE_M = 50.0     # campionamento dettagliato lungo il percorso
ELEV_QUERY_STEP = 4      # interroga provider ~ogni 200m (4 * 50m), il resto interpolato
ELEV_BATCH = 100         # punti per batch
ELEV_BATCH_SLEEP = 1.0   # secondi tra batch (rate limit)

app = Flask(__name__)

# Stati in memoria (Render free: si svuotano ai riavvii)
USER_STATE = {}
AUTHORIZED = set()
PENDING = set()
LAST_DOWNLOAD = {}  # uid -> epoch seconds

# Pre-semina allowlist statica
if AUTH_USERS_CSV:
    for _id in AUTH_USERS_CSV.split(","):
        _id = _id.strip()
        if _id.isdigit():
            AUTHORIZED.add(int(_id))
if OWNER_ID:
    AUTHORIZED.add(OWNER_ID)

# ======================================
# UTILS
# ======================================

def now_epoch() -> float:
    return time.time()

def epoch_to_str(e: float) -> str:
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
def build_gpx(coords, name="Percorso", elevations=None, maneuvers=None):
    """
    coords: [(lat, lon), ...]
    elevations: [ele_m, ...] (opzionale, stessa lunghezza di coords)
    maneuvers: [{"lat":..., "lon":..., "text":...}, ...] (opzionale)
    """
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    for i, (lat, lon) in enumerate(coords):
        ele = None
        if elevations and i < len(elevations):
            ele = elevations[i]
        seg.points.append(gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon, elevation=ele))

    # Route con turn-by-turn (maneuvers)
    if maneuvers:
        rte = gpxpy.gpx.GPXRoute(name="Istruzioni")
        for m in maneuvers:
            try:
                rte.points.append(
                    gpxpy.gpx.GPXRoutePoint(latitude=m["lat"], longitude=m["lon"], name=m.get("text", ""))
                )
            except:
                pass
        gpx.routes.append(rte)

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

# ---------- Offsets e seed per Round Trip ----------
def offset_point(lat, lon, km, bearing_deg):
    R = 6371.0
    br = radians(bearing_deg)
    lat1 = radians(lat)
    lon1 = radians(lon)
    d = km / R
    lat2 = asin_clamped(sin(lat1)*cos(d) + cos(lat1)*sin(d)*cos(br))
    lon2 = lon1 + atan2(sin(br)*sin(d)*cos(lat1), cos(d)-sin(lat1)*sin(lat2))
    return (degrees_clamped(lat2), degrees_clamped(lon2))

def asin_clamped(x):
    if x > 1: x = 1
    if x < -1: x = -1
    return atan2(x, sqrt(1 - x*x))

def degrees_clamped(rad):
    return rad * 180.0 / 3.141592653589793

def add_roundtrip_seed_if_needed(locs, d_km=5.0, variant=1):
    # Se c'è solo lo start, aggiunge 2 waypoint sintetici per creare un anello corto
    if len(locs) == 1:
        lat = locs[0]["lat"]
        lon = locs[0]["lon"]
        # 2 varianti per ridurre overlap
        if variant == 1:
            bearings = (60, 300)
        else:
            bearings = (120, 240)
        # seed a ~5 km
        (lat1, lon1) = offset_point(lat, lon, d_km, bearings[0])
        (lat2, lon2) = offset_point(lat, lon, d_km, bearings[1])
        wp1 = {"lat": lat1, "lon": lon1}
        wp2 = {"lat": lat2, "lon": lon2}
        locs.extend([wp1, wp2])
    return locs

# ---------- Resample path every ~50m ----------
def resample_path(coords, spacing_m=50.0):
    if not coords or len(coords) < 2:
        return coords[:]
    out = [coords[0]]
    accum = 0.0
    i = 0
    while i < len(coords) - 1:
        a = coords[i]
        b = coords[i+1]
        seg_km = haversine_km(a, b)
        seg_m = seg_km * 1000.0
        if seg_m + accum >= spacing_m:
            # frazione lungo il segmento
            need = spacing_m - accum
            f = need / seg_m if seg_m > 0 else 0
            lat = a[0] + (b[0] - a[0]) * f
            lon = a[1] + (b[1] - a[1]) * f
            out.append((lat, lon))
            # spostiamo il punto "a" a questa nuova posizione
            coords[i] = (lat, lon)
            accum = 0.0
        else:
            accum += seg_m
            i += 1
    if out[-1] != coords[-1]:
        out.append(coords[-1])
    return out

# ---------- Elevation: batch calls + interpolation ----------
def fetch_elevations_batched(points):
    """
    points: [(lat, lon), ...]
    Ritorna [ele_m or None, ...] stessa lunghezza, usando batch e backoff.
    """
    if not points:
        return []

    elevations = [None] * len(points)
    # Query ogni ELEV_QUERY_STEP punto (es. 200m), il resto lo interpoliamo
    idx_to_query = list(range(0, len(points), ELEV_QUERY_STEP))
    if idx_to_query[-1] != len(points)-1:
        idx_to_query.append(len(points)-1)

    # Batch
    for start in range(0, len(idx_to_query), ELEV_BATCH):
        batch_idx = idx_to_query[start:start+ELEV_BATCH]
        locs_param = "|".join(f"{points[i][0]},{points[i][1]}" for i in batch_idx)
        try:
            r = requests.get(ELEV_PROVIDER_URL, params={"locations": locs_param}, timeout=20)
            if r.ok:
                data = r.json()
                res = data.get("results", [])
                for k, i_pt in enumerate(batch_idx):
                    if k < len(res):
                        elevations[i_pt] = res[k].get("elevation")
            else:
                # fallisce: restituisce tutti None per questo batch
                pass
        except Exception:
            pass
        time.sleep(ELEV_BATCH_SLEEP)

    # Interpolazione lineare dei None compresi tra due noti
    # (semplice e robusta per uso moto)
    last_known = None
    for i in range(len(elevations)):
        if elevations[i] is not None:
            if last_known is None:
                # riempi fino al primo noto
                for j in range(0, i):
                    elevations[j] = elevations[i]
            else:
                # interpola tra last_known e i
                j0 = last_known
                j1 = i
                v0 = elevations[j0]
                v1 = elevations[j1]
                span = j1 - j0
                if span > 1:
                    for j in range(j0+1, j1):
                        f = (j - j0) / span
                        elevations[j] = v0 + f * (v1 - v0)
            last_known = i

    # se gli ultimi sono None, riempi con l'ultimo noto
    if last_known is not None and last_known < len(elevations)-1:
        for j in range(last_known+1, len(elevations)):
            elevations[j] = elevations[last_known]

    return elevations

def elevation_gain_loss(elevations, min_step=1.0):
    """
    Ritorna (gain_m, loss_m). Scarta oscillazioni < min_step m.
    """
    if not elevations or len(elevations) < 2:
        return (0, 0)
    up = 0.0
    down = 0.0
    prev = elevations[0]
    for e in elevations[1:]:
        if e is None or prev is None:
            prev = e if e is not None else prev
            continue
        diff = e - prev
        if diff > min_step:
            up += diff
        elif diff < -min_step:
            down -= diff  # diff è negativo
        prev = e
    return (int(round(up)), int(round(down)))

# ---------- Misura overlap semplice ----------
def overlap_score(coords, grid=1e-4):
    """
    Stima overlappo: quante volte ripassiamo su "celle" già viste (grid ~ 11m).
    """
    seen = {}
    score = 0
    for (lat, lon) in coords:
        key = (round(lat / grid), round(lon / grid))
        cnt = seen.get(key, 0)
        score += 1 if cnt > 0 else 0
        seen[key] = cnt + 1
    return score

# ======================================
# UI / MESSAGGI / PULSANTI
# ======================================

WELCOME = (
    "🏍️ *Benvenuto nel MotoRoute Bot!*\n\n"
    "Genera *GPX* (anche turn‑by‑turn), *KML* e un link *Google Maps*.\n\n"
    "1️⃣ Invia la *partenza* (posizione, `lat,lon` o indirizzo)\n"
    "2️⃣ Scegli *Round Trip* subito oppure imposta la *destinazione*\n"
    "3️⃣ (Opz.) aggiungi fino a *4 waypoint*, poi *Fine*\n"
    "4️⃣ Scegli *Standard* o *Curvy*\n\n"
    "🔒 *Limiti*: 120 km totali · 80 km raggio per A→B · 4 waypoint\n"
    "🛣️ *Profilo*: motorcycle (evita autostrade / fondi scarsi, predilige secondarie)"
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
        "roundtrip": False
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

    if roundtrip:
        start = locations[0]
        locations = locations + [{"lat": start["lat"], "lon": start["lon"]}]

    def build_payload_motorcycle(curvy_variant: int):
        """
        curvy_variant:
          0 -> standard motorcycle
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

    variants = [0] if style == "standard" else [4, 2, 3, 1]
    urls_to_try = [VALHALLA_URL]
    if VALHALLA_URL_FALLBACK and VALHALLA_URL_FALLBACK != VALHALLA_URL:
        urls_to_try.append(VALHALLA_URL_FALLBACK)

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
    maneuvers_out = []

    for leg in trip.get("legs", []):
        shape = leg.get("shape")
        coords_leg = decode_polyline6(shape) if shape else []
        # manovre
        mans = leg.get("maneuvers", [])
        for m in mans:
            idx = m.get("begin_shape_index", 0)
            if 0 <= idx < len(coords_leg):
                lat, lon = coords_leg[idx]
                maneuvers_out.append({
                    "lat": lat,
                    "lon": lon,
                    "text": m.get("instruction", "")
                })
        # append coords e summary
        coords.extend(coords_leg)
        summary = leg.get("summary", {})
        total_km += float(summary.get("length", 0.0))
        total_min += float(summary.get("time", 0.0)) / 60.0

    return coords, round(total_km, 1), round(total_min, 1), maneuvers_out

def compute_best_roundtrip(start_tuple, style, waypoints):
    """
    Per roundtrip senza waypoint, prova due seed e scegli quello con meno overlap.
    """
    base = [{"lat": start_tuple[0], "lon": start_tuple[1]}]
    # Variante 1
    locs1 = add_roundtrip_seed_if_needed(base.copy(), d_km=5.0, variant=1)
    coords1, dist1, time1, mans1 = valhalla_route(locs1, style=style, roundtrip=True)
    score1 = overlap_score(coords1)

    # Variante 2
    base2 = [{"lat": start_tuple[0], "lon": start_tuple[1]}]
    locs2 = add_roundtrip_seed_if_needed(base2.copy(), d_km=5.0, variant=2)
    coords2, dist2, time2, mans2 = valhalla_route(locs2, style=style, roundtrip=True)
    score2 = overlap_score(coords2)

    if score1 <= score2:
        return coords1, dist1, time1, mans1
    else:
        return coords2, dist2, time2, mans2

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

# Webhook flessibile: /webhook/<token> con check del token
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

        # Access control
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

        # Selezione stile
        if data.startswith("style:"):
            style = data.split(":", 1)[1]  # "standard" | "curvy"
            is_roundtrip = bool(state.get("roundtrip"))

            # Rate limit (owner escluso)
            if uid != OWNER_ID:
                last = LAST_DOWNLOAD.get(uid, 0)
                if now_epoch() - last < RATE_LIMIT_DAYS * 86400:
                    when = epoch_to_str(last + RATE_LIMIT_DAYS * 86400)
                    send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                    return jsonify(ok=True)

            # Prepara locations
            locs_base = [{"lat": state["start"][0], "lon": state["start"][1]}]
            for wp in state["waypoints"]:
                locs_base.append({"lat": wp[0], "lon": wp[1]})

            # Pre-check limiti
            if is_roundtrip:
                tmp = add_roundtrip_seed_if_needed(locs_base.copy(), 5.0, 1)
                approx_km = approx_total_km_from_locs(tmp, roundtrip=True)
            else:
                if not state.get("end"):
                    send_message(chat_id, "⚠️ Devi impostare una destinazione prima di calcolare A→B.")
                    return jsonify(ok=True)
                tmp = locs_base + [{"lat": state["end"][0], "lon": state["end"][1]}]
                approx_km = approx_total_km_from_locs(tmp, roundtrip=False)

            if approx_km > MAX_ROUTE_KM * 1.25:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nStima: ~{round(approx_km,1)} km")
                return jsonify(ok=True)

            send_message(chat_id, PROCESSING)

            # Calcolo percorso (con anti-overlap per roundtrip senza waypoint)
            try:
                if is_roundtrip and len(state["waypoints"]) == 0:
                    coords, dist_km, time_min, maneuvers = compute_best_roundtrip(state["start"], style, state["waypoints"])
                else:
                    locs = locs_base.copy()
                    if is_roundtrip:
                        locs = add_roundtrip_seed_if_needed(locs, 5.0, 1)
                        coords, dist_km, time_min, maneuvers = valhalla_route(locs, style=style, roundtrip=True)
                    else:
                        locs.append({"lat": state["end"][0], "lon": state["end"][1]})
                        coords, dist_km, time_min, maneuvers = valhalla_route(locs, style=style, roundtrip=False)
            except Exception as e:
                send_message(chat_id, f"Errore routing:\n{str(e)[:250]}")
                return jsonify(ok=True)

            if dist_km > MAX_ROUTE_KM:
                send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
                return jsonify(ok=True)

            if not coords or len(coords) < 2:
                send_message(chat_id, ROUTE_NOT_FOUND)
                return jsonify(ok=True)

            # ===== Altimetria: resample 50m + fetch + gain/loss =====
            coords50 = resample_path(coords, ELEV_SAMPLE_M)
            ele = None
            gain_m = loss_m = 0
            try:
                ele = fetch_elevations_batched(coords50)
                gain_m, loss_m = elevation_gain_loss(ele)
            except Exception:
                ele = None

            # File GPX con elevazioni (se disponibili) + route turn-by-turn
            gpx_bytes = build_gpx(coords50, "Percorso Moto", elevations=ele, maneuvers=maneuvers)
            cap = f"Distanza: {dist_km} km · Durata: {time_min} min"
            if ele:
                cap += f" · ↑{gain_m} m / ↓{loss_m} m"
            send_document(chat_id, gpx_bytes, "route.gpx", caption=cap)

            # KML solo polilinea (niente ele)
            kml_bytes = build_kml(coords)
            send_document(chat_id, kml_bytes, "route.kml")

            # Link Google Maps
            if is_roundtrip:
                gmaps_url = build_gmaps_url(state["start"], state["start"], state["waypoints"])
            else:
                gmaps_url = build_gmaps_url(state["start"], state["end"], state["waypoints"])
            send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

            # Aggiorna rate-limit
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

    # Comandi globali
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
                when = epoch_to_str(last + RATE_LIMIT_DAYS * 86400)
                send_message(chat_id, RATE_LIMIT_MSG.format(when=when))
                return jsonify(ok=True)

        style = "curvy" if text.lower() == "curvy" else "standard"
        is_roundtrip = bool(state.get("roundtrip"))

        locs_base = [{"lat": state["start"][0], "lon": state["start"][1]}]
        for wp in state["waypoints"]:
            locs_base.append({"lat": wp[0], "lon": wp[1]})

        if is_roundtrip:
            tmp = add_roundtrip_seed_if_needed(locs_base.copy(), 5.0, 1)
            approx_km = approx_total_km_from_locs(tmp, roundtrip=True)
        else:
            if not state.get("end"):
                send_message(chat_id, "⚠️ Devi impostare una destinazione prima di calcolare A→B.")
                return jsonify(ok=True)
            tmp = locs_base + [{"lat": state["end"][0], "lon": state["end"][1]}]
            approx_km = approx_total_km_from_locs(tmp, roundtrip=False)

        if approx_km > MAX_ROUTE_KM * 1.25:
            send_message(chat_id, f"{LIMITS_EXCEEDED}\nStima: ~{round(approx_km,1)} km")
            return jsonify(ok=True)

        send_message(chat_id, PROCESSING)

        try:
            if is_roundtrip and len(state["waypoints"]) == 0:
                coords, dist_km, time_min, maneuvers = compute_best_roundtrip(state["start"], style, state["waypoints"])
            else:
                locs = locs_base.copy()
                if is_roundtrip:
                    locs = add_roundtrip_seed_if_needed(locs, 5.0, 1)
                    coords, dist_km, time_min, maneuvers = valhalla_route(locs, style=style, roundtrip=True)
                else:
                    locs.append({"lat": state["end"][0], "lon": state["end"][1]})
                    coords, dist_km, time_min, maneuvers = valhalla_route(locs, style=style, roundtrip=False)
        except Exception as e:
            send_message(chat_id, f"Errore routing:\n{str(e)[:250]}")
            return jsonify(ok=True)

        if dist_km > MAX_ROUTE_KM:
            send_message(chat_id, f"{LIMITS_EXCEEDED}\nPercorso: {dist_km} km")
            return jsonify(ok=True)

        if not coords or len(coords) < 2:
            send_message(chat_id, ROUTE_NOT_FOUND)
            return jsonify(ok=True)

        # Altimetria
        coords50 = resample_path(coords, ELEV_SAMPLE_M)
        ele = None
        gain_m = loss_m = 0
        try:
            ele = fetch_elevations_batched(coords50)
            gain_m, loss_m = elevation_gain_loss(ele)
        except Exception:
            ele = None

        gpx_bytes = build_gpx(coords50, "Percorso Moto", elevations=ele, maneuvers=maneuvers)
        cap = f"Distanza: {dist_km} km · Durata: {time_min} min"
        if ele:
            cap += f" · ↑{gain_m} m / ↓{loss_m} m"
        send_document(chat_id, gpx_bytes, "route.gpx", caption=cap)

        kml_bytes = build_kml(coords)
        send_document(chat_id, kml_bytes, "route.kml")

        if is_roundtrip:
            gmaps_url = build_gmaps_url(state["start"], state["start"], state["waypoints"])
        else:
            gmaps_url = build_gmaps_url(state["start"], state["end"], state["waypoints"])
        send_message(chat_id, f"🔗 *Apri in Google Maps:*\n{gmaps_url}")

        LAST_DOWNLOAD[uid] = now_epoch()
        reset_state(uid)
        return jsonify(ok=True)

    send_message(chat_id, "❓ Usa /start per cominciare.")
    return jsonify(ok=True)
