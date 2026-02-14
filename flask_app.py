# -*- coding: utf-8 -*-
import os
import time
import json
from math import radians, sin, cos, sqrt, atan2, asin, pi, floor
from datetime import datetime, timezone

from flask import Flask, request, jsonify
import requests
import gpxpy
import gpxpy.gpx

# ======================================
# CONFIGURAZIONE
# ======================================

TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
VALHALLA_URL = os.environ.get("VALHALLA_URL", "").rstrip("/")
VALHALLA_URL_FALLBACK = os.environ.get("VALHALLA_URL_FALLBACK", "").rstrip("/")
OWNER_ID = int(os.environ.get("OWNER_ID", "0") or "0")
AUTH_USERS_CSV = os.environ.get("AUTH_USERS_CSV", "").strip()
STADIA_TOKEN = os.environ.get("STADIA_TOKEN", "").strip()
WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET", "").strip()

# Geocoding UA conforme alla policy Nominatim
GEOCODING_UA = os.environ.get(
    "GEOCODING_UA",
    "MotoRouteBot/1.1 (contact: youremail@example.com)"
)

# Limiti
MAX_WAYPOINTS_STANDARD = 4
MAX_WAYPOINTS_ROUNDTRIP = 2       # manuali
RT_TOTAL_WP_TARGET = 3            # manuali + auto
MAX_ROUTE_KM = 120
MAX_RADIUS_KM = 80                # solo per A→B (linea d’aria)
RATE_LIMIT_DAYS = 7

# Elevazione
ELEVATION_ENABLED = True
ELEVATION_SAMPLE_M = 50  # campionamento ogni ~50 m lungo la traccia
ELEVATION_TIMEOUT = 10   # sec

# Riduzione automatica
REDUCE_MAX_TRIES = 3
RT_MIN_RADIUS_KM = 8.0

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

def clamp(v, vmin, vmax):
    return max(vmin, min(vmax, v))

# ======================================
# TELEGRAM HELPERS
# ======================================

def send_message(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        requests.post(url, json=payload, timeout=15).raise_for_status()
    except Exception:
        pass

def send_document(chat_id, file_bytes, filename, caption=None):
    url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
    files = {"document": (filename, file_bytes, "application/octet-stream")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    try:
        requests.post(url, data=data, files=files, timeout=30).raise_for_status()
    except Exception:
        pass

def send_photo(chat_id, file_bytes, caption=None):
    url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
    files = {"photo": ("route.png", file_bytes, "image/png")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    try:
        requests.post(url, data=data, files=files, timeout=30).raise_for_status()
    except Exception:
        pass

def answer_callback_query(cq_id, text=None):
    url = f"https://api.telegram.org/bot{TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": cq_id}
    if text:
        payload["text"] = text
    try:
        requests.post(url, json=payload, timeout=10).raise_for_status()
    except Exception:
        pass

# ======================================
# MESSAGGI STANDARD
# ======================================

WELCOME = (
    "🏍 *Benvenuto nel MotoRoute Bot!*\n\n"
    "Cosa puoi fare:\n"
    "• 🧭 *Percorso standard*: da A a B con waypoint opzionali.\n"
    "• 🔁 *Round Trip*: un giro ad anello da A con direzione preferita.\n\n"
    "⏳ _Se il bot non parte subito, attendi qualche minuto: il server potrebbe essere in avvio._"
)
CHOOSE_MODE = "🧭 Scegli il *tipo di percorso*:"
ASK_START = "📍 Invia il *punto di partenza*."
ASK_END = "🎯 Ora invia la *destinazione*."
ASK_WAYPOINTS_STD = f"➕ Aggiungi waypoint (max *{MAX_WAYPOINTS_STANDARD}*) oppure premi *✅ Fine*."
ASK_WAYPOINTS_RT = f"➕ Aggiungi waypoint *Round Trip* (max *{MAX_WAYPOINTS_ROUNDTRIP}*). Quando hai finito premi *✅ Fine*."
ASK_DIRECTION = "🧭 Scegli la *direzione* preferita per il Round Trip."
ASK_STYLE_TEXT = "🎨 Scegli lo *stile del percorso*."
PROCESSING = "⏳ Sto calcolando il percorso..."
INVALID_INPUT = "⚠️ Non ho capito. Invia una *posizione* o un *indirizzo* valido."
CANCELLED = "❌ Operazione annullata."
RESTARTED = "🔄 Ricominciamo! Invia la *partenza*."
NOT_AUTH = "🔒 Non sei autorizzato. Ho inviato la *richiesta* all’admin."
ACCESS_GRANTED = "✅ Accesso approvato! Ora puoi usare il bot."
ACCESS_DENIED = "❌ La tua richiesta di accesso è stata rifiutata."
LIMITS_EXCEEDED = f"🚫 Il percorso supera i limiti consentiti (max *{MAX_ROUTE_KM} km*)."
RT_TOO_FAR_WP = f"⚠️ Waypoint troppo lontano dalla partenza (max ~{MAX_RADIUS_KM} km in linea d’aria)."

# ISTRUZIONI PER L'UTENTE (aggiunta)
HOW_TO_POSITION = (
    "ℹ️ *Come inserire una posizione*\n"
    "Puoi inviare:\n"
    "• Un *indirizzo* (es. `Via Roma 10, Milano`)\n"
    "• Delle *coordinate* `lat,lon` (es. `45.4642, 9.1900`)\n"
    "• La *posizione* usando la graffetta 📎 di Telegram → *Posizione*\n\n"
    "_Suggerimento_: aggiungi *città* e *provincia* per risultati migliori.\n"
)

ASK_REPHRASE_OR_SUGGEST = (
    "⚠️ Non ho trovato un indirizzo valido.\n"
    "Riprova scrivendo *via + numero + città* (es. `Via Garibaldi 25, Como`).\n"
    "Oppure invia direttamente la tua *posizione* 📍."
)

CHOOSE_SUGGESTION = "🔎 Ho trovato questi risultati. Scegli quello giusto:"

# ======================================
# TASTIERE
# ======================================

def cancel_restart_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}],
            [{"text": "🔄 Ricomincia", "callback_data": "action:restart"}],
        ]
    }

def main_menu_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "🧭 Percorso standard", "callback_data": "mode:standard"}],
            [{"text": "🔁 Round Trip", "callback_data": "mode:roundtrip"}],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}],
            [{"text": "🔄 Ricomincia", "callback_data": "action:restart"}],
        ]
    }

DIRECTIONS_8 = ["N", "NE", "E", "SE", "S", "SO", "O", "NO"]
def direction_8_keyboard():
    rows = []
    for i in range(0, len(DIRECTIONS_8), 4):
        chunk = DIRECTIONS_8[i:i+4]
        row = [{"text": d, "callback_data": f"dir:{d}"} for d in chunk]
        rows.append(row)
    rows.append([{"text": "🎲 Lascia decidere al bot", "callback_data": "dir:skip"}])
    rows.append([{"text": "❌ Annulla", "callback_data": "action:cancel"}])
    rows.append([{"text": "🔄 Ricomincia", "callback_data": "action:restart"}])
    return {"inline_keyboard": rows}

def waypoints_keyboard_std():
    return {
        "inline_keyboard": [
            [{"text": "➕ Aggiungi waypoint", "callback_data": "action:add_wp_std"}],
            [{"text": "✅ Fine", "callback_data": "action:finish_waypoints_std"}],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}],
            [{"text": "🔄 Ricomincia", "callback_data": "action:restart"}],
        ]
    }

def waypoints_keyboard_rt():
    return {
        "inline_keyboard": [
            [{"text": "➕ Aggiungi waypoint (RT)", "callback_data": "action:add_wp_rt"}],
            [{"text": "✅ Fine", "callback_data": "action:finish_waypoints_rt"}],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}],
            [{"text": "🔄 Ricomincia", "callback_data": "action:restart"}],
        ]
    }

def style_inline_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "⚡ Rapido", "callback_data": "style:rapido"},
                {"text": "🌀 Curvy leggero", "callback_data": "style:curvy_light"},
            ],
            [
                {"text": "🧷 Curvy", "callback_data": "style:curvy"},
                {"text": "⭐ Super curvy", "callback_data": "style:super_curvy"},
                {"text": "🔥 Extreme (premium)", "callback_data": "style:extreme"},
            ],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}],
            [{"text": "🔄 Ricomincia", "callback_data": "action:restart"}],
        ]
    }

def reduce_confirm_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "✅ Accetto versione ridotta", "callback_data": "reduce:accept"}],
            [{"text": "❌ Rifiuto", "callback_data": "reduce:reject"}],
            [{"text": "❌ Annulla", "callback_data": "action:cancel"}],
            [{"text": "🔄 Ricomincia", "callback_data": "action:restart"}],
        ]
    }

def admin_request_keyboard(uid, uname):
    return {
        "inline_keyboard": [
            [{"text": f"✔️ Approva {uname}", "callback_data": f"admin:approve:{uid}"}],
            [{"text": f"❌ Rifiuta {uname}", "callback_data": f"admin:deny:{uid}"}]
        ]
    }

def geocode_suggestions_keyboard(candidates):
    # Mostra max 5 bottoni numerati
    buttons = []
    for i, (_, _, name) in enumerate(candidates[:5], start=1):
        label = f"{i}. {name[:50]}{'…' if len(name) > 50 else ''}"
        buttons.append([{"text": label, "callback_data": f"geo_pick:{i-1}"}])
    buttons.append([{"text": "❌ Annulla", "callback_data": "action:cancel"}])
    buttons.append([{"text": "🔄 Ricomincia", "callback_data": "action:restart"}])
    return {"inline_keyboard": buttons}

# ======================================
# RESET STATO
# ======================================

def reset_state(uid):
    USER_STATE[uid] = {
        "phase": "choose_mode",
        "mode": None,                 # "standard" | "roundtrip"
        "start": None,
        "end": None,
        "waypoints_std": [],
        "waypoints_rt": [],           # manuali
        "rt_radius_km": 25.0,         # raggio base RT (adattivo)
        "roundtrip": False,
        "direction": None,            # "N", "NE", ..., "NO" | "skip"
        "style": None,                # "rapido"|"curvy_light"|"curvy"|"super_curvy"|"extreme"
        "pending_delivery": None,     # contenuto pronto se ridotto e in attesa Accetta
        # Geocoding migliorato
        "last_geo_candidates": None,  # lista [(lat,lon,name), ...]
        "geo_pick_phase": None,       # "start"|"end"|"wp_std"|"wp_rt"
    }

# ======================================
# ROUND TRIP — DIREZIONI & GENERAZIONE WAYPOINT AUTO
# ======================================

DIR_ANGLES = {
    "N": 0.0, "NE": 45.0, "E": 90.0, "SE": 135.0,
    "S": 180.0, "SO": 225.0, "O": 270.0, "NO": 315.0,
    "NE_DEF": 45.0
}

def generate_roundtrip_waypoints_auto(start, direction, count=3, radius_km=25.0):
    """Genera 'count' waypoint distribuiti attorno alla direzione base (−40°, 0°, +40°)."""
    lat, lon = start["lat"], start["lon"]
    base_angle = DIR_ANGLES.get(direction, DIR_ANGLES["NE_DEF"])
    angles = [base_angle - 40.0, base_angle, base_angle + 40.0]

    wps = []
    for ang in angles[:count]:
        ang_rad = ang * pi / 180.0
        d = radius_km / 6371.0
        lat2 = asin(sin(radians(lat)) * cos(d) +
                    cos(radians(lat)) * sin(d) * cos(ang_rad))
        lon2 = radians(lon) + atan2(
            sin(ang_rad) * sin(d) * cos(radians(lat)),
            cos(d) - sin(radians(lat)) * sin(lat2)
        )
        wps.append({"lat": lat2 * 180.0 / pi, "lon": lon2 * 180.0 / pi})
    return wps

def distribute_rt_waypoints(start, dir_code, manual_wps, total_target, radius_km):
    """
    Distribuisce manuali + auto sui 3 slot angolari (-40,0,+40).
    I manuali “vincono” lo slot più vicino; i rimanenti slot vengono auto-generati.
    """
    base_angle = DIR_ANGLES.get(dir_code, DIR_ANGLES["NE_DEF"])
    slots_deg = [base_angle - 40.0, base_angle, base_angle + 40.0]
    assigned = [None, None, None]

    def bearing_from_start(p):
        lat1, lon1 = radians(start["lat"]), radians(start["lon"])
        lat2, lon2 = radians(p["lat"]), radians(p["lon"])
        dlon = lon2 - lon1
        y = sin(dlon) * cos(lat2)
        x = cos(lat1)*sin(lat2) - sin(lat1)*cos(lat2)*cos(dlon)
        brng = (atan2(y, x) * 180.0/pi + 360.0) % 360.0
        return brng

    # assegna manuali
    for p in manual_wps:
        b = bearing_from_start(p)
        diffs = [abs(((b - s + 180) % 360) - 180) for s in slots_deg]
        idx = diffs.index(min(diffs))
        if assigned[idx] is None:
            assigned[idx] = p
        else:
            for j in range(3):
                if assigned[j] is None:
                    assigned[j] = p
                    break

    # genera auto per slot vuoti
    for i in range(3):
        if assigned[i] is None:
            ang = slots_deg[i]
            ang_rad = ang * pi / 180.0
            d = radius_km / 6371.0
            lat2 = asin(sin(radians(start["lat"])) * cos(d) +
                        cos(radians(start["lat"])) * sin(d) * cos(ang_rad))
            lon2 = radians(start["lon"]) + atan2(
                sin(ang_rad) * sin(d) * cos(radians(start["lat"])),
                cos(d) - sin(radians(start["lat"])) * sin(lat2)
            )
            assigned[i] = {"lat": lat2 * 180.0 / pi, "lon": lon2 * 180.0 / pi}

    return assigned[:total_target]

# ======================================
# VALHALLA — CHIAMATE API
# ======================================

def post_valhalla(url, payload):
    try:
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def route_valhalla(locations, style="rapido"):
    costing = "motorcycle"

    if style == "rapido":
        co = {"use_highways": 0.9, "use_tolls": 0.0, "shortest": False}
    elif style == "curvy_light":
        co = {"use_highways": 0.5, "use_tolls": 0.0, "shortest": False}
    elif style == "curvy":
        co = {"use_highways": 0.2, "use_tolls": 0.0, "shortest": False}
    elif style == "super_curvy":
        co = {"use_highways": 0.1, "use_tolls": 0.0, "shortest": False}
    elif style == "extreme":
        co = {"use_highways": 0.05, "use_tolls": 0.0, "shortest": False}
    else:
        co = {"use_highways": 0.5, "use_tolls": 0.0, "shortest": False}

    payload = {
        "locations": locations,
        "costing": costing,
        "costing_options": {costing: co},
        "directions_options": {"units": "kilometers"},
    }

    urls_to_try = [VALHALLA_URL] if VALHALLA_URL else []
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

def subsample(coords, step=20, max_points=300):
    if not coords:
        return coords
    out = coords[::step]
    if len(out) > max_points:
        ratio = max(1, len(out)//max_points)
        out = out[::ratio]
    if out[-1] != coords[-1]:
        out.append(coords[-1])
    return out

def build_stadia_url(coords, markers):
    if not STADIA_TOKEN:
        return None
    path = "|".join([f"{lat},{lon}" for lat, lon in coords]) if coords else ""
    mk = "|".join([f"{lat},{lon}" for lat, lon in markers]) if markers else ""
    url = (
        "https://tiles.stadiamaps.com/static?"
        f"api_key={STADIA_TOKEN}"
        + (f"&path=color:red|weight:3|{path}" if path else "")
        + (f"&markers=color:blue|{mk}" if mk else "")
        + "&zoom=12"
        + "&size=800x800"
    )
    return url

def build_osm_url(coords, markers):
    base = "https://staticmap.openstreetmap.de/staticmap.php"
    path = "|".join([f"{lat},{lon}" for lat, lon in coords]) if coords else ""
    mk = "|".join([f"{lat},{lon}" for lat, lon in markers]) if markers else ""
    url = (
        f"{base}?size=800x800"
        + (f"&path=color:red|weight:3|{path}" if path else "")
        + (f"&markers={mk}" if mk else "")
    )
    return url

def download_png(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            return r.content
        return None
    except Exception:
        return None

def build_static_map(coords, markers):
    # 1) Stadia con sottocampionamento (GET)
    if STADIA_TOKEN:
        coords_small = subsample(coords, step=20, max_points=300)
        url = build_stadia_url(coords_small, markers)
        if url:
            img = download_png(url)
            if img:
                return img
    # 2) OSM fallback
    url = build_osm_url(subsample(coords, step=20, max_points=300), markers)
    return download_png(url)

# ======================================
# GEOCODING
# ======================================

def geocode_address(q, limit=5, countrycodes="it"):
    """
    Ritorna una lista di candidati [(lat, lon, display_name), ...] ordinati per rilevanza.
    Se vuoto, nessun risultato.
    """
    if not q:
        return []

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": q,
        "format": "json",
        "limit": str(limit),
        "accept-language": "it",
    }
    if countrycodes:
        params["countrycodes"] = countrycodes

    headers = {"User-Agent": GEOCODING_UA, "Referer": "https://t.me/your_bot"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json() or []
        out = []
        for it in data:
            try:
                lat = float(it["lat"])
                lon = float(it["lon"])
                name = it.get("display_name", "").strip()
                out.append((lat, lon, name))
            except:
                continue
        return out
    except Exception:
        return []

def parse_location_from_message(msg):
    # 1) Posizione Telegram
    if "location" in msg:
        loc = msg["location"]
        return (loc["latitude"], loc["longitude"])

    # 2) Testo (indirizzo o coordinate)
    text = (msg.get("text") or "").strip()
    if not text:
        return None

    # 2.a) coordinate "lat,lon"
    if "," in text:
        parts = text.split(",")
        if len(parts) == 2:
            try:
                lat = float(parts[0].strip())
                lon = float(parts[1].strip())
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return (lat, lon)
            except:
                pass

    # 2.b) geocoding con suggerimenti
    candidates = geocode_address(text, limit=5, countrycodes="it")
    if not candidates:
        return None
    if len(candidates) == 1:
        lat, lon, _ = candidates[0]
        return (lat, lon)

    # più candidati → chiediamo all'utente di scegliere
    return ("SUGGEST", candidates)

# ======================================
# ESTRARRE COORDINATE E MANOVRE DA VALHALLA (FIX: tutte le legs)
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

def extract_coords_and_maneuvers(valhalla_json):
    """
    Estrae coordinate e manovre da TUTTE le legs del trip Valhalla,
    concatenando le shape e correggendo gli indici delle manovre.
    """
    if not valhalla_json:
        return None, None

    try:
        trip = valhalla_json.get("trip", {})
        legs = trip.get("legs", [])
        if not legs:
            return None, None

        coords = []
        maneuvers_out = []
        global_len_before_leg = 0

        for leg in legs:
            shape = leg.get("shape")
            if not shape:
                continue

            leg_coords = decode_polyline6(shape)
            if not leg_coords:
                continue

            # Evita il punto duplicato di giunzione con la leg precedente
            if coords and leg_coords and coords[-1] == leg_coords[0]:
                leg_coords = leg_coords[1:]

            # Riallinea le manovre a indice globale
            for m in leg.get("maneuvers", []):
                idx = m.get("begin_shape_index")
                if idx is None:
                    continue
                global_idx = global_len_before_leg + idx

                if 0 <= global_idx < len(coords):
                    lat, lon = coords[global_idx]
                else:
                    rel = global_idx - len(coords)
                    if 0 <= rel < len(leg_coords):
                        lat, lon = leg_coords[rel]
                    else:
                        continue

                maneuvers_out.append({
                    "lat": lat,
                    "lon": lon,
                    "instruction": m.get("instruction", "")
                })

            coords.extend(leg_coords)
            global_len_before_leg = len(coords)

        if not coords:
            return None, None

        return coords, maneuvers_out

    except Exception:
        return None, None

def ensure_closed_loop_if_roundtrip(coords, start, is_roundtrip):
    """Se Round Trip, assicura che l'ultimo punto coincida con la partenza."""
    if not is_roundtrip or not coords or not start:
        return coords
    s = (round(start["lat"], 6), round(start["lon"], 6))
    e = (round(coords[-1][0], 6), round(coords[-1][1], 6))
    if s != e:
        coords.append((start["lat"], start["lon"]))
    return coords

# ======================================
# GPX
# ======================================

def build_gpx_with_turns(coords, maneuvers, ele_list=None, name="Percorso Moto"):
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    n = len(coords)
    for i, (lat, lon) in enumerate(coords):
        p = gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon)
        if ele_list is not None and n > 1:
            try:
                ele = ele_list[i]
            except:
                ele = None
            if ele is not None:
                p.elevation = float(ele)
        seg.points.append(p)

    # Waypoint per le manovre
    for m in maneuvers or []:
        lat = m.get("lat")
        lon = m.get("lon")
        instr = m.get("instruction", "")
        if lat is None or lon is None:
            continue
        gpx.waypoints.append(gpxpy.gpx.GPXWaypoint(latitude=lat, longitude=lon, name=instr))

    return gpx.to_xml().encode("utf-8")

def build_gpx_simple(coords, ele_list=None, name="Percorso Moto (semplice)"):
    gpx = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=name)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    gpx.tracks.append(trk)

    n = len(coords)
    for i, (lat, lon) in enumerate(coords):
        p = gpxpy.gpx.GPXTrackPoint(latitude=lat, longitude=lon)
        if ele_list is not None and n > 1:
            try:
                ele = ele_list[i]
            except:
                ele = None
            if ele is not None:
                p.elevation = float(ele)
        seg.points.append(p)

    return gpx.to_xml().encode("utf-8")

# ======================================
# ELEVATION (OpenTopoData / OpenElevation)
# ======================================

def sample_along_coords(coords, step_m=50.0):
    """Restituisce una lista di punti campionati lungo la polyline ogni ~step_m."""
    if not coords or len(coords) < 2:
        return coords[:]
    sampled = [coords[0]]
    acc = 0.0
    for i in range(1, len(coords)):
        a = coords[i-1]
        b = coords[i]
        seg_km = haversine_km(a, b)
        seg_m = seg_km * 1000.0
        if seg_m <= 0:
            continue
        needed = int((acc + seg_m) // step_m)
        for n in range(1, needed+1):
            dist_m = n*step_m - acc
            if dist_m < 0 or dist_m > seg_m:
                continue
            t = dist_m / seg_m
            lat = a[0] + (b[0]-a[0]) * t
            lon = a[1] + (b[1]-a[1]) * t
            sampled.append((lat, lon))
        acc = (acc + seg_m) % step_m
    if sampled[-1] != coords[-1]:
        sampled.append(coords[-1])
    return sampled

def elevation_opentopodata(points):
    """Prova OpenTopoData (dataset EUDEM25m); batch di 100 location."""
    base = "https://api.opentopodata.org/v1/eudem25m"
    out = []
    batch = 100
    headers = {"User-Agent": GEOCODING_UA}
    for i in range(0, len(points), batch):
        chunk = points[i:i+batch]
        locs = "|".join([f"{p[0]},{p[1]}" for p in chunk])
        params = {"locations": locs}
        try:
            r = requests.get(base, params=params, headers=headers, timeout=ELEVATION_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                for res in data.get("results", []):
                    out.append(res.get("elevation"))
            else:
                out.extend([None]*len(chunk))
        except Exception:
            out.extend([None]*len(chunk))
    return out

def elevation_openelevation(points):
    """Fallback Open-Elevation (batch via GET)."""
    base = "https://api.open-elevation.com/api/v1/lookup"
    out = []
    batch = 100
    headers = {"User-Agent": GEOCODING_UA}
    for i in range(0, len(points), batch):
        chunk = points[i:i+batch]
        locs = "|".join([f"{p[0]},{p[1]}" for p in chunk])
        params = {"locations": locs}
        try:
            r = requests.get(base, params=params, headers=headers, timeout=ELEVATION_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                for res in data.get("results", []):
                    out.append(res.get("elevation"))
            else:
                out.extend([None]*len(chunk))
        except Exception:
            out.extend([None]*len(chunk))
    return out

def compute_elevation_for_route(coords):
    """Calcola elevazioni (campionate) + dislivello + quota min/max e produce lista ele per ogni coord."""
    if not ELEVATION_ENABLED or not coords or len(coords) < 2:
        return None, None

    sampled_pts = sample_along_coords(coords, step_m=ELEVATION_SAMPLE_M)
    elev = elevation_opentopodata(sampled_pts)
    if all(e is None for e in elev):
        elev = elevation_openelevation(sampled_pts)

    gain = 0.0
    loss = 0.0
    last = None
    elev_clean = []
    for e in elev:
        if e is None:
            elev_clean.append(None)
            continue
        elev_clean.append(float(e))
        if last is not None:
            delta = float(e) - last
            if delta > 0.5:
                gain += delta
            elif delta < -0.5:
                loss += -delta
        last = float(e) if e is not None else last

    valid = [x for x in elev_clean if x is not None]
    min_ele = min(valid) if valid else None
    max_ele = max(valid) if valid else None

    ele_full = []
    m = len(sampled_pts)
    n = len(coords)
    for i in range(n):
        if m <= 1:
            ele_full.append(None)
            continue
        j = int(round(i * (m-1) / (n-1)))
        ele_full.append(elev_clean[j] if 0 <= j < m else None)

    summary = {
        "gain": round(gain, 1),
        "loss": round(loss, 1),
        "min": round(min_ele, 1) if min_ele is not None else None,
        "max": round(max_ele, 1) if max_ele is not None else None
    }
    return ele_full, summary

# ======================================
# RATE LIMIT
# ======================================

def check_rate_limit(uid):
    if uid == OWNER_ID:
        return True
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
# COSTRUZIONE LOCATIONS PER VALHALLA (con type: "break")
# ======================================

def build_locations_standard(start, end, waypoints):
    locs = []
    locs.append({"lat": start["lat"], "lon": start["lon"], "type": "break"})
    for w in waypoints:
        locs.append({"lat": w["lat"], "lon": w["lon"], "type": "break"})
    locs.append({"lat": end["lat"], "lon": end["lon"], "type": "break"})
    return locs

def build_locations_roundtrip(start, wps):
    locs = []
    locs.append({"lat": start["lat"], "lon": start["lon"], "type": "break"})
    for w in wps:
        locs.append({"lat": w["lat"], "lon": w["lon"], "type": "break"})
    locs.append({"lat": start["lat"], "lon": start["lon"], "type": "break"})
    return locs

# ======================================
# CONTROLLO LIMITI PRE-CHECK
# ======================================

def precheck_radius_standard(start, end):
    d = haversine_km((start["lat"], start["lon"]), (end["lat"], end["lon"]))
    return d <= MAX_RADIUS_KM

def precheck_approx_distance(locs, roundtrip):
    approx = approx_total_km_from_locs(locs, roundtrip)
    return approx <= MAX_ROUTE_KM

# ======================================
# RIDUZIONE AUTOMATICA
# ======================================

def try_reduce_roundtrip(st, start, wps, style, tries_left):
    radius = st.get("rt_radius_km", 25.0)
    direction = st.get("direction") or "NE"
    manual = st.get("waypoints_rt", [])[:]
    total_target = RT_TOTAL_WP_TARGET

    for attempt in range(REDUCE_MAX_TRIES):
        auto_wps = distribute_rt_waypoints(start, direction, manual, total_target, radius)
        locs = build_locations_roundtrip(start, auto_wps)
        val = route_valhalla(locs, style=style)
        if not val:
            radius = max(RT_MIN_RADIUS_KM, radius * 0.85)
            st["rt_radius_km"] = radius
            continue

        trip_km = val.get("trip", {}).get("summary", {}).get("length")
        if isinstance(trip_km, (int, float)) and trip_km <= MAX_ROUTE_KM:
            coords, maneuvers = extract_coords_and_maneuvers(val)
            if coords:
                st["rt_radius_km"] = radius
                return val, coords, maneuvers, auto_wps, style

        radius = max(RT_MIN_RADIUS_KM, radius * 0.85)
        st["rt_radius_km"] = radius
        if attempt >= 1:
            if style == "curvy":
                style = "curvy_light"
            elif style == "curvy_light":
                style = "rapido"

    return None, None, None, None, style

def try_reduce_standard(start, end, wps, style):
    def deviation_score(w):
        ab = haversine_km((start["lat"], start["lon"]), (end["lat"], end["lon"]))
        aw = haversine_km((start["lat"], start["lon"]), (w["lat"], w["lon"]))
        wb = haversine_km((w["lat"], w["lon"]), (end["lat"], end["lon"]))
        return (aw + wb) - ab

    local_wps = wps[:]
    local_style = style

    for attempt in range(REDUCE_MAX_TRIES):
        locs = build_locations_standard(start, end, local_wps)
        val = route_valhalla(locs, style=local_style)
        if val:
            trip_km = val.get("trip", {}).get("summary", {}).get("length")
            if isinstance(trip_km, (int, float)) and trip_km <= MAX_ROUTE_KM:
                coords, maneuvers = extract_coords_and_maneuvers(val)
                if coords:
                    return val, coords, maneuvers, local_wps, local_style

        if local_wps:
            worst = max(local_wps, key=deviation_score)
            local_wps.remove(worst)
        else:
            if local_style == "curvy":
                local_style = "curvy_light"
            elif local_style == "curvy_light":
                local_style = "rapido"

    return None, None, None, None, local_style

# ======================================
# CALCOLO PERCORSO (e gestione riduzione/accetta)
# ======================================

def format_time(secs):
    try:
        m = int(secs // 60)
        h, m = divmod(m, 60)
        return f"~{h}h {m}m" if h > 0 else f"~{m}m"
    except:
        return "n/d"

def compute_and_maybe_reduce(uid, chat_id):
    st = USER_STATE.get(uid)
    if not st:
        send_message(chat_id, "Errore interno. Riparti con /start.")
        return

    mode = st["mode"]
    start = st["start"]
    style = st["style"]

    # Premium check (blocca super_curvy / extreme a non owner)
    if style in ("super_curvy", "extreme") and uid != OWNER_ID:
        answer_callback_query(st.get("last_cq_id", ""), "Solo utenti premium possono usare Super curvy")
        send_message(chat_id, "Scegli uno stile tra ⚡ Rapido, 🌀 Curvy leggero, 🧷 Curvy.", reply_markup=style_inline_keyboard())
        return

    # Round Trip
    if mode == "roundtrip":
        manual = st["waypoints_rt"]
        direction = st.get("direction") or "NE"
        total_target = RT_TOTAL_WP_TARGET
        radius = st.get("rt_radius_km", 25.0)
        auto_wps = distribute_rt_waypoints(start, direction, manual, total_target, radius)
        locs = build_locations_roundtrip(start, auto_wps)
        send_message(chat_id, PROCESSING)
        val = route_valhalla(locs, style=style)
        if not val:
            send_message(chat_id, "❌ Errore Valhalla. Riprova più tardi.")
            return
        trip_km = val.get("trip", {}).get("summary", {}).get("length")
        trip_time = val.get("trip", {}).get("summary", {}).get("time")

        if isinstance(trip_km, (int, float)) and trip_km > MAX_ROUTE_KM:
            send_message(
                chat_id,
                f"🚫 Il percorso calcolato è di ~{trip_km:.1f} km e supera il limite di {MAX_ROUTE_KM} km.\n"
                f"Provo a ridurlo automaticamente…"
            )
            val2, coords2, man2, wps2, style2 = try_reduce_roundtrip(st, start, auto_wps, style, REDUCE_MAX_TRIES)
            if not val2 or not coords2:
                send_message(
                    chat_id,
                    "⚠️ Non riesco a rientrare nei limiti senza modifiche ulteriori. "
                    "Riduci i waypoint oppure scegli uno stile più rapido (⚡ Rapido / 🌀 Curvy leggero)."
                )
                reset_state(uid)
                return
            trip_km2 = val2.get("trip", {}).get("summary", {}).get("length")
            trip_time2 = val2.get("trip", {}).get("summary", {}).get("time")
            coords2, _ = (ensure_closed_loop_if_roundtrip(coords2, start, True), man2)
            ele_list, elev_summary = compute_elevation_for_route(coords2) if ELEVATION_ENABLED else (None, None)
            gpx_turns = build_gpx_with_turns(coords2, man2, ele_list)
            gpx_simple = build_gpx_simple(coords2, ele_list)
            markers = [(start["lat"], start["lon"])] + [(w["lat"], w["lon"]) for w in wps2]
            png_bytes = build_static_map(coords2, markers)
            st["pending_delivery"] = {
                "gpx_turns": gpx_turns,
                "gpx_simple": gpx_simple,
                "png": png_bytes,
                "summary": {
                    "mode": "Round Trip",
                    "direction": direction if direction != "skip" else None,
                    "style": style2,
                    "km": trip_km2,
                    "secs": trip_time2,
                    "elev": elev_summary
                }
            }
            msg = (
                f"✅ Riduzione completata: ora ~{trip_km2:.1f} km "
                f"(prima ~{trip_km:.1f} km).\n"
                f"Vuoi procedere con la *versione ridotta*?"
            )
            send_message(chat_id, msg, reply_markup=reduce_confirm_keyboard())
            return

        coords, maneuvers = extract_coords_and_maneuvers(val)
        if not coords:
            send_message(chat_id, "❌ Errore nel percorso.")
            return
        coords = ensure_closed_loop_if_roundtrip(coords, start, True)

        ele_list, elev_summary = compute_elevation_for_route(coords) if ELEVATION_ENABLED else (None, None)
        gpx_turns = build_gpx_with_turns(coords, maneuvers, ele_list)
        gpx_simple = build_gpx_simple(coords, ele_list)
        markers = [(start["lat"], start["lon"])] + [(w["lat"], w["lon"]) for w in auto_wps]
        png_bytes = build_static_map(coords, markers)

        if not check_rate_limit(uid):
            last = LAST_DOWNLOAD.get(uid)
            unlock = last + RATE_LIMIT_DAYS*86400
            send_message(chat_id, f"⏳ Hai già scaricato un percorso di recente.\nPuoi riprovare dopo: *{epoch_to_str(unlock)}*")
            return
        update_rate_limit(uid)

        send_document(chat_id, gpx_turns, "route_turns.gpx", caption="📄 GPX con manovre")
        send_document(chat_id, gpx_simple, "route_track.gpx", caption="📄 GPX semplice (solo traccia)")
        if png_bytes:
            send_photo(chat_id, png_bytes, caption="🗺 Mappa del percorso")
        else:
            send_message(chat_id, "⚠️ Mappa non disponibile al momento.")

        dist_label = f"{trip_km:.1f} km" if isinstance(trip_km, (int, float)) else "n/d"
        time_label = format_time(trip_time)
        dir_label = f" (direzione: {st.get('direction')})" if st.get("direction") and st.get("direction") != "skip" else ""
        elev_line = ""
        if elev_summary:
            elev_line = f"• Dislivello: +{elev_summary['gain']:.0f} m / -{elev_summary['loss']:.0f} m"
            if elev_summary.get("min") is not None and elev_summary.get("max") is not None:
                elev_line += f" (min {elev_summary['min']:.0f} m, max {elev_summary['max']:.0f} m)"
            elev_line += "\n"

        summary = (
            "✅ *Percorso pronto*\n"
            f"• Tipo: Round Trip{dir_label}\n"
            f"• Stile: {style}\n"
            f"• Distanza: ~{dist_label}\n"
            f"• Tempo stimato: {time_label}\n"
            f"{elev_line}"
            f"• Waypoint: {len(auto_wps)}\n"
            f"• Generato: {epoch_to_str(now_epoch())}\n"
            f"Limiti attivi: max {MAX_ROUTE_KM} km, max {MAX_WAYPOINTS_ROUNDTRIP} waypoint manuali (RT)\n"
        )
        send_message(chat_id, summary)
        reset_state(uid)
        return

    # Standard A→B
    if mode == "standard":
        end = st["end"]
        wps = st["waypoints_std"]
        if not precheck_radius_standard(start, end):
            send_message(chat_id, f"🚫 Destinazione troppo lontana dalla partenza (max ~{MAX_RADIUS_KM} km in linea d’aria).")
            return
        locs = build_locations_standard(start, end, wps)
        if not precheck_approx_distance(locs, False):
            send_message(chat_id, LIMITS_EXCEEDED)
            return

        send_message(chat_id, PROCESSING)
        val = route_valhalla(locs, style=style)
        if not val:
            send_message(chat_id, "❌ Errore Valhalla. Riprova più tardi.")
            return
        trip_km = val.get("trip", {}).get("summary", {}).get("length")
        trip_time = val.get("trip", {}).get("summary", {}).get("time")

        if isinstance(trip_km, (int, float)) and trip_km > MAX_ROUTE_KM:
            send_message(
                chat_id,
                f"🚫 Il percorso calcolato è di ~{trip_km:.1f} km e supera il limite di {MAX_ROUTE_KM} km.\n"
                f"Provo a ridurlo automaticamente…"
            )
            val2, coords2, man2, wps2, style2 = try_reduce_standard(start, end, wps, style)
            if not val2 or not coords2:
                send_message(
                    chat_id,
                    "⚠️ Non riesco a rientrare nei limiti senza modifiche ulteriori. "
                    "Riduci i waypoint oppure scegli uno stile più rapido (⚡ Rapido / 🌀 Curvy leggero)."
                )
                reset_state(uid)
                return
            trip_km2 = val2.get("trip", {}).get("summary", {}).get("length")
            trip_time2 = val2.get("trip", {}).get("summary", {}).get("time")
            ele_list, elev_summary = compute_elevation_for_route(coords2) if ELEVATION_ENABLED else (None, None)
            gpx_turns = build_gpx_with_turns(coords2, man2, ele_list)
            gpx_simple = build_gpx_simple(coords2, ele_list)
            markers = [(start["lat"], start["lon"])] + [(w["lat"], w["lon"]) for w in wps2] + [(end["lat"], end["lon"])]
            png_bytes = build_static_map(coords2, markers)
            st["pending_delivery"] = {
                "gpx_turns": gpx_turns,
                "gpx_simple": gpx_simple,
                "png": png_bytes,
                "summary": {
                    "mode": "Standard",
                    "direction": None,
                    "style": style2,
                    "km": trip_km2,
                    "secs": trip_time2,
                    "elev": elev_summary
                }
            }
            msg = (
                f"✅ Riduzione completata: ora ~{trip_km2:.1f} km "
                f"(prima ~{trip_km:.1f} km).\n"
                f"Vuoi procedere con la *versione ridotta*?"
            )
            send_message(chat_id, msg, reply_markup=reduce_confirm_keyboard())
            return

        coords, maneuvers = extract_coords_and_maneuvers(val)
        if not coords:
            send_message(chat_id, "❌ Errore nel percorso.")
            return
        ele_list, elev_summary = compute_elevation_for_route(coords) if ELEVATION_ENABLED else (None, None)
        gpx_turns = build_gpx_with_turns(coords, maneuvers, ele_list)
        gpx_simple = build_gpx_simple(coords, ele_list)
        markers = [(start["lat"], start["lon"])] + [(w["lat"], w["lon"]) for w in wps] + [(end["lat"], end["lon"])]
        png_bytes = build_static_map(coords, markers)

        if not check_rate_limit(uid):
            last = LAST_DOWNLOAD.get(uid)
            unlock = last + RATE_LIMIT_DAYS*86400
            send_message(chat_id, f"⏳ Hai già scaricato un percorso di recente.\nPuoi riprovare dopo: *{epoch_to_str(unlock)}*")
            return
        update_rate_limit(uid)

        send_document(chat_id, gpx_turns, "route_turns.gpx", caption="📄 GPX con manovre")
        send_document(chat_id, gpx_simple, "route_track.gpx", caption="📄 GPX semplice (solo traccia)")
        if png_bytes:
            send_photo(chat_id, png_bytes, caption="🗺 Mappa del percorso")
        else:
            send_message(chat_id, "⚠️ Mappa non disponibile al momento.")

        dist_label = f"{trip_km:.1f} km" if isinstance(trip_km, (int, float)) else "n/d"
        time_label = format_time(trip_time)
        elev_line = ""
        if elev_summary:
            elev_line = f"• Dislivello: +{elev_summary['gain']:.0f} m / -{elev_summary['loss']:.0f} m"
            if elev_summary.get("min") is not None and elev_summary.get("max") is not None:
                elev_line += f" (min {elev_summary['min']:.0f} m, max {elev_summary['max']:.0f} m)"
            elev_line += "\n"

        summary = (
            "✅ *Percorso pronto*\n"
            f"• Tipo: Standard\n"
            f"• Stile: {style}\n"
            f"• Distanza: ~{dist_label}\n"
            f"• Tempo stimato: {time_label}\n"
            f"{elev_line}"
            f"• Waypoint: {len(wps)}\n"
            f"• Generato: {epoch_to_str(now_epoch())}\n"
            f"Limiti attivi: max {MAX_ROUTE_KM} km, max {MAX_WAYPOINTS_STANDARD} waypoint\n"
        )
        send_message(chat_id, summary)
        reset_state(uid)
        return

# ======================================
# CALLBACK QUERY HANDLER
# ======================================

def handle_callback(uid, chat_id, cq_id, data):
    st = USER_STATE.get(uid)
    if st is None:
        reset_state(uid)
        st = USER_STATE[uid]
    st["last_cq_id"] = cq_id  # per i toast

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

    # Common actions
    if data == "action:cancel":
        reset_state(uid)
        send_message(chat_id, CANCELLED)
        return
    if data == "action:restart":
        reset_state(uid)
        send_message(chat_id, RESTARTED, reply_markup=cancel_restart_keyboard())
        return

    # Scelta modalità
    if data.startswith("mode:"):
        mode = data.split(":")[1]
        st["mode"] = mode
        st["roundtrip"] = (mode == "roundtrip")
        st["phase"] = "await_start"
        send_message(chat_id, ASK_START + "\n\n" + HOW_TO_POSITION, reply_markup=cancel_restart_keyboard())
        return

    # Waypoints STD
    if data == "action:add_wp_std":
        st["phase"] = "await_wp_std"
        send_message(chat_id, "📍 Invia il *waypoint* (posizione/indirizzo).\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_std())
        return
    if data == "action:finish_waypoints_std":
        st["phase"] = "choose_style"
        send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
        return

    # Waypoints RT
    if data == "action:add_wp_rt":
        st["phase"] = "await_wp_rt"
        send_message(chat_id, "📍 Invia il *waypoint Round Trip* (posizione/indirizzo).\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_rt())
        return
    if data == "action:finish_waypoints_rt":
        st["phase"] = "choose_style"
        send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
        return

    # Direzione round trip
    if data.startswith("dir:"):
        direction = data.split(":")[1]
        st["direction"] = direction
        st["phase"] = "waypoints_rt"
        send_message(chat_id, ASK_WAYPOINTS_RT + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_rt())
        return

    # Stile percorso
    if data.startswith("style:"):
        style = data.split(":")[1]
        if style in ("super_curvy", "extreme") and uid != OWNER_ID:
            answer_callback_query(cq_id, "Solo utenti premium possono usare Super curvy")
            return
        st["style"] = style
        answer_callback_query(cq_id, "Stile selezionato!")
        compute_and_maybe_reduce(uid, chat_id)
        return

    # Geocoding: scelta di un suggerimento
    if data.startswith("geo_pick:"):
        try:
            idx = int(data.split(":")[1])
        except:
            answer_callback_query(cq_id, "Selezione non valida.")
            return
        candidates = st.get("last_geo_candidates") or []
        if not candidates or idx < 0 or idx >= len(candidates):
            answer_callback_query(cq_id, "Suggerimento non disponibile.")
            return
        lat, lon, name = candidates[idx]
        phase_pick = st.get("geo_pick_phase")

        if phase_pick == "start":
            st["start"] = {"lat": lat, "lon": lon}
            st["last_geo_candidates"] = None
            st["geo_pick_phase"] = None
            if st["roundtrip"]:
                st["phase"] = "choose_direction"
                send_message(chat_id, f"✅ Partenza: {name}")
                send_message(chat_id, ASK_DIRECTION, reply_markup=direction_8_keyboard())
            else:
                st["phase"] = "await_end"
                send_message(chat_id, f"✅ Partenza: {name}")
                send_message(chat_id, ASK_END + "\n\n" + HOW_TO_POSITION, reply_markup=cancel_restart_keyboard())
            return

        if phase_pick == "end":
            st["end"] = {"lat": lat, "lon": lon}
            st["last_geo_candidates"] = None
            st["geo_pick_phase"] = None
            st["phase"] = "waypoints_std"
            send_message(chat_id, f"✅ Destinazione: {name}")
            send_message(chat_id, ASK_WAYPOINTS_STD + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_std())
            return

        if phase_pick == "wp_std":
            if len(st["waypoints_std"]) >= MAX_WAYPOINTS_STANDARD:
                answer_callback_query(cq_id, "Hai raggiunto il numero massimo di waypoint.")
                return
            st["waypoints_std"].append({"lat": lat, "lon": lon})
            st["last_geo_candidates"] = None
            st["geo_pick_phase"] = None
            st["phase"] = "waypoints_std"
            send_message(chat_id, f"✅ Waypoint aggiunto: {name}", reply_markup=waypoints_keyboard_std())
            return

        if phase_pick == "wp_rt":
            start = st["start"]
            d = haversine_km((start["lat"], start["lon"]), (lat, lon))
            if d > MAX_RADIUS_KM:
                answer_callback_query(cq_id, "Waypoint troppo lontano dalla partenza.")
                return
            if len(st["waypoints_rt"]) >= MAX_WAYPOINTS_ROUNDTRIP:
                answer_callback_query(cq_id, "Hai raggiunto il numero massimo di waypoint RT.")
                return
            st["waypoints_rt"].append({"lat": lat, "lon": lon})
            st["last_geo_candidates"] = None
            st["geo_pick_phase"] = None
            st["phase"] = "waypoints_rt"
            send_message(chat_id, f"✅ Waypoint RT aggiunto: {name}", reply_markup=waypoints_keyboard_rt())
            return

        answer_callback_query(cq_id, "Fase non riconosciuta per la scelta.")
        return

    answer_callback_query(cq_id, "Comando non riconosciuto.")

# ======================================
# MESSAGE HANDLER
# ======================================

def handle_message(uid, chat_id, msg):
    # Access control
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

    # /start
    if text == "/start":
        reset_state(uid)
        send_message(chat_id, WELCOME)
        send_message(chat_id, CHOOSE_MODE, reply_markup=main_menu_keyboard())
        return

    # Stato utente
    st = USER_STATE.get(uid)
    if not st:
        reset_state(uid)
        st = USER_STATE[uid]

    phase = st["phase"]

    # Sequenza fasi
    if phase == "choose_mode":
        send_message(chat_id, CHOOSE_MODE, reply_markup=main_menu_keyboard())
        return

    if phase == "await_start":
        parsed = parse_location_from_message(msg)
        if not parsed:
            send_message(chat_id, INVALID_INPUT + "\n\n" + HOW_TO_POSITION, reply_markup=cancel_restart_keyboard())
            return
        if isinstance(parsed, tuple) and len(parsed) == 2 and isinstance(parsed[0], (int, float)):
            lat, lon = parsed
            st["start"] = {"lat": lat, "lon": lon}
            if st["roundtrip"]:
                st["phase"] = "choose_direction"
                send_message(chat_id, ASK_DIRECTION, reply_markup=direction_8_keyboard())
            else:
                st["phase"] = "await_end"
                send_message(chat_id, ASK_END + "\n\n" + HOW_TO_POSITION, reply_markup=cancel_restart_keyboard())
            return
        if isinstance(parsed, tuple) and parsed[0] == "SUGGEST":
            _, candidates = parsed
            st["last_geo_candidates"] = candidates
            st["geo_pick_phase"] = "start"
            send_message(chat_id, CHOOSE_SUGGESTION, reply_markup=geocode_suggestions_keyboard(candidates))
            return
        send_message(chat_id, ASK_REPHRASE_OR_SUGGEST + "\n\n" + HOW_TO_POSITION, reply_markup=cancel_restart_keyboard())
        return

    if phase == "await_end":
        parsed = parse_location_from_message(msg)
        if not parsed:
            send_message(chat_id, INVALID_INPUT + "\n\n" + HOW_TO_POSITION, reply_markup=cancel_restart_keyboard())
            return
        if isinstance(parsed, tuple) and len(parsed) == 2 and isinstance(parsed[0], (int, float)):
            lat, lon = parsed
            st["end"] = {"lat": lat, "lon": lon}
            st["phase"] = "waypoints_std"
            send_message(chat_id, ASK_WAYPOINTS_STD + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_std())
            return
        if isinstance(parsed, tuple) and parsed[0] == "SUGGEST":
            _, candidates = parsed
            st["last_geo_candidates"] = candidates
            st["geo_pick_phase"] = "end"
            send_message(chat_id, CHOOSE_SUGGESTION, reply_markup=geocode_suggestions_keyboard(candidates))
            return
        send_message(chat_id, ASK_REPHRASE_OR_SUGGEST + "\n\n" + HOW_TO_POSITION, reply_markup=cancel_restart_keyboard())
        return

    if phase == "await_wp_std":
        parsed = parse_location_from_message(msg)
        if not parsed:
            send_message(chat_id, INVALID_INPUT + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_std())
            return
        if isinstance(parsed, tuple) and len(parsed) == 2 and isinstance(parsed[0], (int, float)):
            lat, lon = parsed
            if len(st["waypoints_std"]) >= MAX_WAYPOINTS_STANDARD:
                send_message(chat_id, f"⚠️ Puoi aggiungere massimo {MAX_WAYPOINTS_STANDARD} waypoint.")
                return
            st["waypoints_std"].append({"lat": lat, "lon": lon})
            st["phase"] = "waypoints_std"
            send_message(chat_id, ASK_WAYPOINTS_STD + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_std())
            return
        if isinstance(parsed, tuple) and parsed[0] == "SUGGEST":
            _, candidates = parsed
            st["last_geo_candidates"] = candidates
            st["geo_pick_phase"] = "wp_std"
            send_message(chat_id, CHOOSE_SUGGESTION, reply_markup=geocode_suggestions_keyboard(candidates))
            return
        send_message(chat_id, ASK_REPHRASE_OR_SUGGEST + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_std())
        return

    if phase == "choose_direction":
        send_message(chat_id, ASK_DIRECTION, reply_markup=direction_8_keyboard())
        return

    if phase == "waypoints_rt":
        parsed = parse_location_from_message(msg)
        if not parsed:
            send_message(chat_id, ASK_WAYPOINTS_RT + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_rt())
            return
        # coordinata diretta
        if isinstance(parsed, tuple) and len(parsed) == 2 and isinstance(parsed[0], (int, float)):
            lat, lon = parsed
            start = st["start"]
            d = haversine_km((start["lat"], start["lon"]), (lat, lon))
            if d > MAX_RADIUS_KM:
                send_message(chat_id, RT_TOO_FAR_WP, reply_markup=waypoints_keyboard_rt())
                return
            if len(st["waypoints_rt"]) >= MAX_WAYPOINTS_ROUNDTRIP:
                send_message(chat_id, f"⚠️ Puoi aggiungere massimo {MAX_WAYPOINTS_ROUNDTRIP} waypoint per il Round Trip.")
                return
            st["waypoints_rt"].append({"lat": lat, "lon": lon})
            send_message(chat_id, ASK_WAYPOINTS_RT + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_rt())
            return
        # suggerimenti
        if isinstance(parsed, tuple) and parsed[0] == "SUGGEST":
            _, candidates = parsed
            st["last_geo_candidates"] = candidates
            st["geo_pick_phase"] = "wp_rt"
            send_message(chat_id, CHOOSE_SUGGESTION, reply_markup=geocode_suggestions_keyboard(candidates))
            return
        send_message(chat_id, ASK_WAYPOINTS_RT + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_rt())
        return

    if phase == "await_wp_rt":
        parsed = parse_location_from_message(msg)
        if not parsed:
            send_message(chat_id, INVALID_INPUT + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_rt())
            return
        if isinstance(parsed, tuple) and len(parsed) == 2 and isinstance(parsed[0], (int, float)):
            lat, lon = parsed
            start = st["start"]
            d = haversine_km((start["lat"], start["lon"]), (lat, lon))
            if d > MAX_RADIUS_KM:
                send_message(chat_id, RT_TOO_FAR_WP, reply_markup=waypoints_keyboard_rt())
                return
            if len(st["waypoints_rt"]) >= MAX_WAYPOINTS_ROUNDTRIP:
                send_message(chat_id, f"⚠️ Puoi aggiungere massimo {MAX_WAYPOINTS_ROUNDTRIP} waypoint per il Round Trip.")
                return
            st["waypoints_rt"].append({"lat": lat, "lon": lon})
            st["phase"] = "waypoints_rt"
            send_message(chat_id, ASK_WAYPOINTS_RT + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_rt())
            return
        if isinstance(parsed, tuple) and parsed[0] == "SUGGEST":
            _, candidates = parsed
            st["last_geo_candidates"] = candidates
            st["geo_pick_phase"] = "wp_rt"
            send_message(chat_id, CHOOSE_SUGGESTION, reply_markup=geocode_suggestions_keyboard(candidates))
            return
        send_message(chat_id, ASK_REPHRASE_OR_SUGGEST + "\n\n" + HOW_TO_POSITION, reply_markup=waypoints_keyboard_rt())
        return

    if phase == "choose_style":
        send_message(chat_id, ASK_STYLE_TEXT, reply_markup=style_inline_keyboard())
        return

    send_message(chat_id, INVALID_INPUT, reply_markup=cancel_restart_keyboard())

# ======================================
# WEBHOOK
# ======================================

@app.before_request
def verify_telegram_source():
    # Verifica secret token solo sulla rotta webhook
    if request.method == "POST" and request.path.startswith("/webhook"):
        if WEBHOOK_SECRET:
            header_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if header_token != WEBHOOK_SECRET:
                return jsonify({"ok": False, "error": "unauthorized"}), 401

@app.route("/webhook/<path:token>", methods=["POST"])
def webhook(token):
    # (opzionale) puoi verificare che 'token' corrisponda a TOKEN, ma il controllo forte è via header
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({"ok": True})

    if "callback_query" in data:
        cq = data["callback_query"]
        uid = cq["from"]["id"]
        chat_id = cq["message"]["chat"]["id"]
        cq_id = cq["id"]
        handle_callback(uid, chat_id, cq_id, cq.get("data", ""))
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
