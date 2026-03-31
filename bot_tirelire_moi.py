# bot_tirelire_moi.py
# Tirelire Météo Pension - Bot Polymarket météo
# v3.0 — REFONTE COMPLÈTE de la logique de trading :
#
#  DIAGNOSTIC des 146 cycles sans trade :
#  1. MIN_BINS=5 : les marchés météo Polymarket ont 2-10 outcomes (pas des bins continus)
#  2. parse_range() cherchait "22-24°C" → les vrais titres sont "≥75°F", ">80°F", "74°F or higher"
#  3. La stratégie multi-bin était pensée pour des marchés qui n'existent pas sur Polymarket
#
#  SOLUTION v3.0 :
#  • Nouveau moteur : parse_threshold() détecte les seuils (≥, >, or higher, or above, etc.)
#  • Calcul EV binaire correct pour chaque outcome d'un event multi-outcome
#  • MIN_OUTCOMES = 1 (un seul outcome avec EV > seuil suffit pour trader)
#  • Seuil EV adaptatif : 8% (STRONG 3 sources) / 10% (défaut)
#  • Garde toute la stratégie : mise hybride, calibration, redeem, bouclier, rotation

import os, csv, json, time, math, logging, random, re, requests, urllib3
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Configuration ──────────────────────────────────────────────────────────────
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
TRADES_CSV       = DATA_DIR / "trades.csv"
CALIBRATION_JSON = DATA_DIR / "calibration.json"
STATE_JSON       = DATA_DIR / "state.json"
TAG_CACHE_JSON   = DATA_DIR / "tag_cache.json"
ROTATION_JSON    = DATA_DIR / "rotation.json"

PRIVATE_KEY    = os.getenv("PRIVATE_KEY", "")
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN", "")
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

SCAN_INTERVAL       = int(os.getenv("SCAN_INTERVAL",        "900"))
EV_THRESHOLD        = float(os.getenv("EV_THRESHOLD",       "0.08"))  # 8% défaut (était 10%)
EV_THRESHOLD_STRONG = float(os.getenv("EV_THRESHOLD_STRONG","0.06"))  # 6% si 3 sources STRONG
DAILY_LOSS_LIMIT    = float(os.getenv("DAILY_LOSS_LIMIT",   "0.03"))
MIN_OUTCOMES        = int(os.getenv("MIN_OUTCOMES", "1"))   # ← clé : 1 suffit (était MIN_BINS=5)
MAX_OUTCOMES        = int(os.getenv("MAX_OUTCOMES", "5"))
CITIES_PER_CYCLE    = int(os.getenv("CITIES_PER_CYCLE", "9"))
INTER_CITY_DELAY    = float(os.getenv("INTER_CITY_DELAY", "1.5"))

# Polymarket
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"
POLYGON_RPC  = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")
CTF_ADDRESS  = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
HASH_ZERO    = "0x" + "0" * 64

API_CONNECT = 8
API_READ    = 18
OM_CONNECT  = 6
OM_READ     = 14
API_MAX_RETRIES = 3
API_BACKOFF_BASE = 1.0

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.6367.207 Safari/537.36 TirelireMeteoPension/3.0"
)

CITIES = [
    {"name": "New York",    "lat": 40.71, "lon": -74.00, "priority": 10,
     "aliases": ["new york","nyc","ny ","new-york","manhattan"]},
    {"name": "Chicago",     "lat": 41.88, "lon": -87.63, "priority": 9,
     "aliases": ["chicago","chi "]},
    {"name": "Los Angeles", "lat": 34.05, "lon":-118.24, "priority": 8,
     "aliases": ["los angeles","la ","l.a.","los-angeles"]},
    {"name": "Miami",       "lat": 25.76, "lon": -80.19, "priority": 8,
     "aliases": ["miami"]},
    {"name": "Dallas",      "lat": 32.78, "lon": -96.80, "priority": 7,
     "aliases": ["dallas","dfw"]},
    {"name": "Seattle",     "lat": 47.61, "lon":-122.33, "priority": 7,
     "aliases": ["seattle"]},
    {"name": "Boston",      "lat": 42.36, "lon": -71.06, "priority": 7,
     "aliases": ["boston"]},
    {"name": "Washington",  "lat": 38.91, "lon": -77.04, "priority": 7,
     "aliases": ["washington","dc ","d.c."]},
    {"name": "London",      "lat": 51.51, "lon":  -0.13, "priority": 6,
     "aliases": ["london","uk temperature","england weather"]},
    {"name": "Paris",       "lat": 48.85, "lon":   2.35, "priority": 5,
     "aliases": ["paris","france temperature"]},
    {"name": "Berlin",      "lat": 52.52, "lon":  13.41, "priority": 4,
     "aliases": ["berlin","germany temperature"]},
    {"name": "Lyon",        "lat": 45.75, "lon":   4.85, "priority": 3,
     "aliases": ["lyon"]},
    {"name": "Marseille",   "lat": 43.30, "lon":   5.37, "priority": 3,
     "aliases": ["marseille"]},
]

WEATHER_KW = [
    "temperature","temp","weather","degrees","celsius","fahrenheit",
    "°f","°c","precipitation","rainfall","rain","snow","snowfall",
    "humidity","heatwave","heat wave","frost","freeze","cold",
    "warm","hot","record high","record low","high temperature",
    "low temperature","daily high","daily low","forecast","highest","exceed",
]
WEATHER_SLUG_KW = [
    "temperature","weather","rain","snow","precipitation",
    "celsius","fahrenheit","heat","cold","frost","degree",
    "warm","hot","freeze","humidity","highest","exceed",
]

CSV_FIELDS = [
    "timestamp","market","outcome","amount","entry_price",
    "exit_price","pnl","balance_after","ev","model_prob","notes",
]

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(DATA_DIR / "bot.log"),
    ],
)
log = logging.getLogger("TirelireMétéo")
for noisy in ("urllib3.connectionpool","requests.packages.urllib3","urllib3.util.retry"):
    logging.getLogger(noisy).setLevel(logging.ERROR)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Helpers ────────────────────────────────────────────────────────────────────
def _sf(v, d=0.0):
    if v is None: return d
    try: return float(v)
    except: return d

def _s1(lst, d=None):
    if not lst: return d
    v = lst[0] if isinstance(lst,(list,tuple)) else d
    return v if v is not None else d

def _f(v, n=1, fb="N/A"):
    if v is None: return fb
    try: return f"{float(v):.{n}f}"
    except: return fb

def _backoff(attempt: int) -> float:
    return min(API_BACKOFF_BASE * (2 ** (attempt - 1)), 8.0)

def _c2f(c: float) -> float:
    return c * 9/5 + 32

def _f2c(f: float) -> float:
    return (f - 32) * 5/9

# ══════════════════════════════════════════════════════════════════════════════
# ── Rotation équitable des villes ────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class CityRotation:
    def __init__(self, cities, per_cycle):
        self.cities    = cities
        self.per_cycle = per_cycle
        self._credits  = {c["name"]: 0.0 for c in cities}
        self._load()

    def _load(self):
        if ROTATION_JSON.exists():
            try:
                saved = json.load(open(ROTATION_JSON))
                for n in self._credits:
                    if n in saved: self._credits[n] = float(saved[n])
                return
            except Exception: pass
        self._reset()

    def _save(self):
        try: json.dump(self._credits, open(ROTATION_JSON,"w"), indent=2)
        except Exception: pass

    def _reset(self):
        total = sum(c["priority"] for c in self.cities)
        for c in self.cities: self._credits[c["name"]] = c["priority"] / total

    def select(self, available: set) -> list:
        total = sum(c["priority"] for c in self.cities)
        for c in self.cities: self._credits[c["name"]] += c["priority"] / total
        candidates = sorted(
            [c for c in self.cities if c["name"] in available],
            key=lambda c: self._credits[c["name"]] + random.uniform(0, 0.03),
            reverse=True,
        )
        selected = candidates[:self.per_cycle]
        for c in selected: self._credits[c["name"]] -= 1.0
        self._save()
        log.info(f"  🔀 Villes : {', '.join(c['name'] for c in selected)}")
        return selected

# ── Sessions HTTP ──────────────────────────────────────────────────────────────
def _new_session(verify=True, retries=0):
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.headers["Accept"]     = "application/json"
    s.verify = verify
    s.mount("https://", HTTPAdapter(max_retries=Retry(
        total=retries, backoff_factor=1.5,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"], raise_on_status=False,
    )))
    s.mount("http://", HTTPAdapter(max_retries=0))
    return s

_om_ssl   = _new_session(verify=True,  retries=2)
_om_nossl = _new_session(verify=False, retries=2)
_poly     = _new_session(verify=True,  retries=0)

def _api(method, url, label="API", **kwargs):
    kwargs.setdefault("timeout", (API_CONNECT, API_READ))
    last = None
    for attempt in range(1, API_MAX_RETRIES + 2):
        if attempt > 1:
            w = _backoff(attempt-1)
            log.warning(f"  🔄 [{label}] retry {attempt-1}/{API_MAX_RETRIES} dans {w:.0f}s")
            time.sleep(w)
        try:
            r = _poly.request(method, url, **kwargs)
            if r.status_code == 404: return None
            if r.status_code >= 500: last = f"HTTP {r.status_code}"; continue
            raw = r.text.strip()
            if not raw: last = "vide"; continue
            try: return r.json()
            except: last = "JSON invalide"; continue
        except requests.exceptions.ConnectTimeout: last = "ConnectTimeout"
        except requests.exceptions.ReadTimeout:    last = "ReadTimeout"
        except requests.exceptions.ConnectionError as e: last = str(e)
        except Exception as e:
            log.error(f"  [{label}] {e}"); return None
    log.error(f"  ❌ [{label}] abandon — {last}")
    return None

def _om_get(params):
    for attempt in range(1, API_MAX_RETRIES + 2):
        for sess in (_om_ssl, _om_nossl):
            try:
                r = sess.get(OPEN_METEO_URL, params=params,
                             timeout=(OM_CONNECT, OM_READ))
                r.raise_for_status()
                d = r.json()
                temps = (d.get("daily") or {}).get("temperature_2m_max") or []
                if temps and temps[0] is not None: return d
            except requests.exceptions.SSLError: continue
            except: break
        if attempt <= API_MAX_RETRIES: time.sleep(_backoff(attempt))
    return None

# ── CSV ────────────────────────────────────────────────────────────────────────
def init_csv():
    if not TRADES_CSV.exists():
        with open(TRADES_CSV,"w",newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
        log.info("📄 trades.csv créé.")

def append_trade(row: dict):
    with open(TRADES_CSV,"a",newline="") as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(row)

# ── État ───────────────────────────────────────────────────────────────────────
def load_state():
    if STATE_JSON.exists():
        with open(STATE_JSON) as f: return json.load(f)
    return {"balance": float(os.getenv("INITIAL_BALANCE","100.0")),
            "daily_pnl":0.0, "daily_reset":datetime.utcnow().strftime("%Y-%m-%d"),
            "paused_until":None, "milestones_hit":[]}

def save_state(s):
    with open(STATE_JSON,"w") as f: json.dump(s,f,indent=2)

def load_cal():
    if CALIBRATION_JSON.exists():
        with open(CALIBRATION_JSON) as f: return json.load(f)
    return {}

def save_cal(cal):
    with open(CALIBRATION_JSON,"w") as f: json.dump(cal,f,indent=2)

def conf_factor(city, month, cal):
    k = f"{city}_{month:02d}"
    if k not in cal or cal[k]["trades"] < 5: return 1.0
    return round(min(max(0.7 + (cal[k]["wins"]/cal[k]["trades"])*0.6, 0.7), 1.3), 3)

def update_cal(city, month, cal, won):
    k = f"{city}_{month:02d}"
    if k not in cal: cal[k] = {"trades":0,"wins":0,"factor":1.0}
    cal[k]["trades"] += 1
    if won: cal[k]["wins"] += 1
    cal[k]["factor"] = conf_factor(city,month,cal)
    save_cal(cal)

def bet_size(balance):
    if balance < 500:   return 5.0
    if balance <= 1000: return min(balance*0.01, 10.0)
    return                     min(balance*0.0125, 20.0)

# ══════════════════════════════════════════════════════════════════════════════
# ── Météo ─────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _daily(data, src):
    d  = data.get("daily",{})
    tm = _s1(d.get("temperature_2m_max"))
    if tm is None: return None
    tm = _sf(tm, None)
    if tm is None: return None
    return {"source":src,"temp_max":float(tm),
            "temp_min": _sf(_s1(d.get("temperature_2m_min"))) or None,
            "precip_mm": _sf(_s1(d.get("precipitation_sum")), 0.0)}

def _gfs(lat, lon):
    d = _om_get({"latitude":lat,"longitude":lon,
                 "daily":"temperature_2m_max,temperature_2m_min,precipitation_sum",
                 "forecast_days":7,"timezone":"UTC","models":"gfs_seamless"})
    return _daily(d,"GFS") if d else None

def _ecmwf(lat, lon):
    for m in ("ecmwf_ifs04","ecmwf_ifs025"):
        d = _om_get({"latitude":lat,"longitude":lon,
                     "daily":"temperature_2m_max,temperature_2m_min,precipitation_sum",
                     "forecast_days":7,"timezone":"UTC","models":m})
        if d:
            r = _daily(d,"ECMWF")
            if r: return r
    return None

def _noaa(lat, lon):
    if not NOAA_API_TOKEN: return None
    try:
        hdrs = {"User-Agent":UA,"token":NOAA_API_TOKEN}
        r = requests.get(f"https://api.weather.gov/points/{lat},{lon}",
                         headers=hdrs,timeout=(6,12))
        if r.status_code != 200: return None
        fu = r.json().get("properties",{}).get("forecast")
        if not fu: return None
        r2 = requests.get(fu,headers=hdrs,timeout=(6,12))
        r2.raise_for_status()
        periods = r2.json().get("properties",{}).get("periods",[])
        if not periods: return None
        p  = next((x for x in periods[:4] if x.get("isDaytime",True)),periods[0])
        tf = _sf(p.get("temperature"),None)
        if tf is None: return None
        return {"source":"NOAA","temp_max":round(_f2c(tf),1),"temp_min":None,"precip_mm":None}
    except: return None

def build_forecast(city):
    lat, lon = city["lat"], city["lon"]
    sources  = []
    for fn, name in [(_gfs,"GFS"),(_ecmwf,"ECMWF"),(_noaa,"NOAA")]:
        try: r = fn(lat,lon)
        except: r = None
        if r is None: continue
        tm = r.get("temp_max")
        if not isinstance(tm,(int,float)) or math.isnan(tm): continue
        r["temp_max"] = float(tm)
        sources.append(r)
    if len(sources) < 2: return None
    temps = [s["temp_max"] for s in sources]
    avg   = sum(temps)/len(temps)
    sprd  = max(temps)-min(temps)
    cons  = "STRONG" if sprd<1.0 else "MODERATE" if sprd<2.5 else "WEAK"
    ev_th = EV_THRESHOLD_STRONG if cons=="STRONG" and len(sources)>=3 else EV_THRESHOLD
    precips  = [s["precip_mm"] for s in sources if s.get("precip_mm") is not None]
    models   = [s["source"] for s in sources]
    log.info(f"    🌡️  {city['name']} {_f(avg)}°C ±{_f(sprd)}°C [{cons}] {models} EV≥{ev_th*100:.0f}%")
    return {"city":city["name"],"temp_max_c":round(avg,1),"temp_max_f":round(_c2f(avg),1),
            "precip_mm": round(sum(precips)/len(precips),1) if precips else 0.0,
            "spread_c":round(sprd,2),"spread_f":round(sprd*9/5,2),
            "consensus":cons,"sources":len(sources),"models":models,"ev_threshold":ev_th}

# ══════════════════════════════════════════════════════════════════════════════
# ── Nouveau moteur de parsing : seuils réels Polymarket ──────────────────────
#
#  Formats rencontrés (recherche du 31 mars 2026) :
#    "≥75°F", ">80°F", "74°F or higher", "74°F or above",
#    "74°F+", "at least 74°F", "below 70°F", "under 68°F",
#    "between 72°F and 75°F", "22°C or higher", "no (< 68°F)"
# ══════════════════════════════════════════════════════════════════════════════
def parse_threshold(title: str):
    """
    Extrait un seuil de température d'un titre d'outcome Polymarket.
    Retourne (threshold_c: float, direction: str, unit: str) ou None.
    direction : "above" | "below"
    """
    t = title.lower().strip()

    # ── 1. Patterns "or higher / or above / or more / +" ──────────────────
    m = re.search(
        r"(-?\d+(?:\.\d+)?)\s*(°?f|°?c)?\s*"
        r"(?:or higher|or above|or more|\bplus\b|\+|and above|and higher)",
        t, re.IGNORECASE
    )
    if m:
        val  = float(m.group(1))
        unit = (m.group(2) or "f").strip("°").lower()
        val_c = val if unit == "c" else _f2c(val)
        return (round(val_c, 1), "above", unit)

    # ── 2. Patterns "≥ X" / "> X" / "at least X" / "exceed X" ──────────
    m = re.search(
        r"(?:≥|>=|at least|exceed[s]?|above|more than|higher than|over)\s*"
        r"(-?\d+(?:\.\d+)?)\s*(°?f|°?c)?",
        t, re.IGNORECASE
    )
    if m:
        val  = float(m.group(1))
        unit = (m.group(2) or "f").strip("°").lower()
        val_c = val if unit == "c" else _f2c(val)
        return (round(val_c, 1), "above", unit)

    # ── 3. Patterns "below / under / less than / ≤ / <" ─────────────────
    m = re.search(
        r"(?:below|under|less than|≤|<=|<)\s*"
        r"(-?\d+(?:\.\d+)?)\s*(°?f|°?c)?",
        t, re.IGNORECASE
    )
    if m:
        val  = float(m.group(1))
        unit = (m.group(2) or "f").strip("°").lower()
        val_c = val if unit == "c" else _f2c(val)
        return (round(val_c, 1), "below", unit)

    # ── 4. Patterns "or lower / or less" ─────────────────────────────────
    m = re.search(
        r"(-?\d+(?:\.\d+)?)\s*(°?f|°?c)?\s*(?:or lower|or less|and below|and under)",
        t, re.IGNORECASE
    )
    if m:
        val  = float(m.group(1))
        unit = (m.group(2) or "f").strip("°").lower()
        val_c = val if unit == "c" else _f2c(val)
        return (round(val_c, 1), "below", unit)

    # ── 5. Range "between X and Y" → midpoint, direction "range" ─────────
    m = re.search(
        r"between\s*(-?\d+(?:\.\d+)?)\s*(°?f|°?c)?\s*and\s*(-?\d+(?:\.\d+)?)",
        t, re.IGNORECASE
    )
    if m:
        lo   = float(m.group(1))
        hi   = float(m.group(3))
        unit = (m.group(2) or "f").strip("°").lower()
        mid  = (lo + hi) / 2
        mid_c = mid if unit == "c" else _f2c(mid)
        half  = abs(hi - lo) / 2
        half_c = half if unit == "c" else half * 5/9
        return (round(mid_c, 1), "range", unit, round(half_c, 1))

    # ── 6. Numéro seul (ex: "74°F", "23°C") — bucket exact ───────────────
    m = re.search(r"^(-?\d+(?:\.\d+)?)\s*(°?f|°?c)\s*$", t.strip(), re.IGNORECASE)
    if m:
        val  = float(m.group(1))
        unit = m.group(2).strip("°").lower()
        val_c = val if unit == "c" else _f2c(val)
        return (round(val_c, 1), "exact", unit)

    return None

def prob_above(forecast_c: float, threshold_c: float, spread_c: float) -> float:
    """P(temp ≥ threshold) selon distribution normale centrée sur forecast."""
    sigma = max(spread_c, 0.8) * 1.5
    z     = (threshold_c - forecast_c) / (sigma * math.sqrt(2))
    return round(max(0.0, min(1.0, 0.5 * (1 - math.erf(z)))), 4)

def prob_below(forecast_c: float, threshold_c: float, spread_c: float) -> float:
    return round(1.0 - prob_above(forecast_c, threshold_c, spread_c), 4)

def prob_range(forecast_c: float, mid_c: float, half_c: float, spread_c: float) -> float:
    return round(
        prob_above(forecast_c, mid_c - half_c, spread_c) -
        prob_above(forecast_c, mid_c + half_c, spread_c), 4
    )

def compute_ev(prob: float, price: float) -> float:
    if price <= 0 or price >= 1: return -999.0
    return round(prob*(1-price) - (1-prob)*price, 4)

def analyze_outcomes(market: dict, fc: dict) -> list:
    """
    ═══════════════════════════════════════════════════════════════
    Cœur de la nouvelle stratégie v3.0

    Pour chaque outcome d'un marché :
    1. parse_threshold() → seuil réel en °C + direction
    2. Calcul probabilité modèle selon distribution normale
    3. EV = prob_modèle*(1-prix) - (1-prob_modèle)*prix
    4. Ne retient que les outcomes avec EV > ev_threshold

    Compatible avec :
    - Marchés Yes/No sur seuil (≥75°F → Yes ou No)
    - Multi-outcomes (ranges de température)
    - Unités °F et °C
    ═══════════════════════════════════════════════════════════════
    """
    fc_c   = fc["temp_max_c"]
    sprd_c = fc["spread_c"]
    ev_th  = fc.get("ev_threshold", EV_THRESHOLD)

    # Extraire les outcomes (Gamma retourne parfois string JSON)
    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        try: outcomes = json.loads(outcomes)
        except: outcomes = []

    prices_raw = market.get("outcomePrices", "[]")
    if isinstance(prices_raw, str):
        try: prices_raw = json.loads(prices_raw)
        except: prices_raw = []

    clob_ids = market.get("clobTokenIds", "[]")
    if isinstance(clob_ids, str):
        try: clob_ids = json.loads(clob_ids)
        except: clob_ids = []

    # Normalise en liste de dicts
    if outcomes and isinstance(outcomes[0], str):
        outcomes = [
            {"title": outcomes[i],
             "price": prices_raw[i] if i < len(prices_raw) else "0.5",
             "clobTokenIds": [clob_ids[i]] if i < len(clob_ids) else [""]}
            for i in range(len(outcomes))
        ]

    candidates = []
    for o in outcomes:
        title    = (o.get("title","") if isinstance(o,dict) else str(o)).strip()
        price_raw = o.get("price", 0.5) if isinstance(o,dict) else 0.5
        price    = _sf(price_raw, 0.5)
        if price <= 0.01 or price >= 0.99:
            continue  # skip outcomes quasi-résolus

        token_id = ""
        if isinstance(o, dict):
            cl = o.get("clobTokenIds", [""])
            token_id = cl[0] if cl else ""

        # ── Parsing du seuil ─────────────────────────────────────────────
        parsed = parse_threshold(title)
        if parsed is None:
            # Essai sur le titre du marché parent + outcome (ex: "Yes"/"No")
            # On tente d'extraire le seuil du titre du marché
            mq = market.get("question", market.get("groupItemTitle",""))
            combined = f"{mq} {title}"
            parsed = parse_threshold(combined)

        if parsed is None:
            continue

        # ── Calcul probabilité selon le type de seuil ────────────────────
        if parsed[1] == "above":
            threshold_c = parsed[0]
            prob_model  = prob_above(fc_c, threshold_c, sprd_c)
            # Pour un outcome "Yes" sur "≥75°F", on achète Yes
            side = "BUY"

        elif parsed[1] == "below":
            threshold_c = parsed[0]
            prob_model  = prob_below(fc_c, threshold_c, sprd_c)
            side = "BUY"

        elif parsed[1] == "range":
            mid_c, half_c = parsed[0], parsed[3]
            prob_model    = prob_range(fc_c, mid_c, half_c, sprd_c)
            threshold_c   = mid_c
            side = "BUY"

        elif parsed[1] == "exact":
            # bucket exact → range ±0.5°C
            threshold_c = parsed[0]
            prob_model  = prob_range(fc_c, threshold_c, 0.5, sprd_c)
            side = "BUY"

        else:
            continue

        ev = compute_ev(prob_model, price)

        # Log debug compact
        log.debug(
            f"      [{title[:30]}] thresh={_f(threshold_c)}°C "
            f"prob={prob_model:.0%} price={price:.2f} EV={ev:.1%}"
        )

        if ev > ev_th:
            candidates.append({
                "token_id":   token_id,
                "title":      title,
                "threshold_c": threshold_c,
                "direction":  parsed[1],
                "prob":       prob_model,
                "price":      price,
                "ev":         ev,
                "side":       side,
            })

    # Tri par EV décroissant, garde les MAX_OUTCOMES meilleurs
    candidates.sort(key=lambda x: x["ev"], reverse=True)
    return candidates[:MAX_OUTCOMES]

# ══════════════════════════════════════════════════════════════════════════════
# ── Helpers marchés ───────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _mtext(m):
    return " ".join([
        m.get("question",""), m.get("slug",""), m.get("title",""),
        m.get("groupItemTitle",""), m.get("description",""),
        " ".join(t.get("label","")+" "+t.get("slug","") for t in m.get("tags",[])),
    ]).lower()

def _is_weather(m):
    t = _mtext(m); s = m.get("slug","").lower()
    return any(k in t for k in WEATHER_KW) or any(k in s for k in WEATHER_SLUG_KW)

def _mcity(m):
    t = _mtext(m)
    for c in CITIES:
        if any(a in t for a in c["aliases"]): return c
    return None

def _mprio(m):
    c = _mcity(m)
    return (c["priority"] if c else 0,
            _sf(m.get("volume24hr") or m.get("volume_24hr")))

def _unpack(events):
    out = []
    for ev in events:
        tags,title,slug = ev.get("tags",[]),ev.get("title",""),ev.get("slug","")
        for m in ev.get("markets",[]):
            m.setdefault("tags",tags)
            if not m.get("groupItemTitle"): m["groupItemTitle"] = title
            if not m.get("eventSlug"):      m["eventSlug"]      = slug
            out.append(m)
    return out

# ══════════════════════════════════════════════════════════════════════════════
# ── PolymarketClient ─────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class PolymarketClient:
    def __init__(self):
        self._wtag_id  = None
        self._wtag_ttl = 0
        self._signer()
        self._web3()

    def _signer(self):
        try:
            from eth_account import Account
            if PRIVATE_KEY:
                self.account = Account.from_key(PRIVATE_KEY)
                self.address = self.account.address
                log.info(f"🔑 Wallet: {self.address[:10]}…")
            else:
                self.account = self.address = None
                log.warning("⚠️  PRIVATE_KEY absente → simulation")
        except ImportError:
            self.account = self.address = None

    def _web3(self):
        try:
            from web3 import Web3
            self.w3 = Web3(Web3.HTTPProvider(POLYGON_RPC,request_kwargs={"timeout":25}))
            if self.w3.is_connected():
                log.info(f"⛓️  Polygon #{self.w3.eth.block_number}")
            else:
                self.w3 = None
        except ImportError:
            self.w3 = None

    def _weather_tag(self):
        now = time.time()
        if self._wtag_id and now < self._wtag_ttl: return self._wtag_id
        if TAG_CACHE_JSON.exists():
            try:
                c = json.load(open(TAG_CACHE_JSON))
                if now < c.get("expires",0):
                    self._wtag_id = c["tag_id"]; self._wtag_ttl = c["expires"]
                    return self._wtag_id
            except: pass
        log.info("  🔍 Découverte tag weather…")
        for off in [0,100]:
            d = _api("GET",f"{GAMMA_BASE}/tags","tags",params={"limit":100,"offset":off})
            if not d: continue
            for tag in (d if isinstance(d,list) else d.get("tags",[])):
                lbl = (tag.get("label","")+" "+tag.get("slug","")).lower()
                if "weather" in lbl:
                    tid = int(tag.get("id",0))
                    if tid:
                        log.info(f"  ✅ Tag weather id={tid}")
                        self._wtag_id = tid; self._wtag_ttl = now+86400
                        try: json.dump({"tag_id":tid,"expires":self._wtag_ttl},open(TAG_CACHE_JSON,"w"))
                        except: pass
                        return tid
        return None

    def _page_events(self, params, label, cap=300):
        out,off,ps = [], 0, params.get("limit",50)
        while len(out) < cap:
            d = _api("GET",f"{GAMMA_BASE}/events",label,params={**params,"offset":off})
            if not d: break
            ev = d if isinstance(d,list) else d.get("data",[])
            if not ev: break
            out.extend(_unpack(ev))
            more = d.get("has_more",False) if isinstance(d,dict) else len(ev)==ps
            if not more: break
            off += ps
        return out

    def weather_markets(self, target=400):
        log.info(f"🌦️  Marchés météo (target={target})…")
        out, seen = [], set()
        def _add(ms):
            for m in ms:
                mid = m.get("conditionId") or m.get("id") or m.get("slug")
                if mid and mid not in seen: seen.add(mid); out.append(m)

        tid = self._weather_tag()
        if tid:
            _add(self._page_events(
                {"tag_id":tid,"related_tags":"true","active":"true","closed":"false",
                 "order":"volume24hr","ascending":"false","limit":50},
                "L1/tag", cap=target))

        if len(out) < target:
            before = len(out)
            raw = self._page_events(
                {"active":"true","closed":"false","order":"volume24hr",
                 "ascending":"false","limit":100}, "L2/all", cap=2000)
            _add([m for m in raw if _is_weather(m)])
            log.info(f"  L2 +{len(out)-before} → {len(out)}")

        if not out:
            d = _api("GET",f"{GAMMA_BASE}/markets","L3",
                     params={"active":"true","closed":"false","order":"volume24hr",
                             "ascending":"false","limit":100})
            if d:
                ms = d if isinstance(d,list) else d.get("data",[])
                _add([m for m in ms if _is_weather(m)])

        out.sort(key=_mprio,reverse=True)
        log.info(f"✅ {len(out)} marchés météo ({sum(1 for m in out if _mcity(m))} villes)")
        return out[:target]

    def place_order(self, token_id, price, size, side="BUY"):
        if not self.account:
            log.info(f"  [SIM] {side} {size:.2f}$ @ {price:.3f} {token_id[:10]}")
            return {"status":"simulated","token_id":token_id,"price":price,"size":size}
        try:
            order = {"orderType":"GTC","tokenID":token_id,"side":side,
                     "price":str(round(price,4)),"size":str(round(size,2)),
                     "funder":self.address,"maker":self.address,"expiration":"0"}
            log.info(f"  [ORDRE] {side} {size:.2f}$ @ {price:.3f}")
            return order
        except Exception as e:
            log.error(f"  place_order: {e}"); return None

    def redeemable(self):
        if not self.address: return []
        pos = _api("GET",f"{DATA_BASE}/positions","pos",
                   params={"user":self.address,"sizeThreshold":"0"})
        if pos is not None:
            ps = pos if isinstance(pos,list) else pos.get("data",[])
            return [p for p in ps if p.get("redeemable") and _sf(p.get("size"))>0]
        act = _api("GET",f"{DATA_BASE}/activity","activity",
                   params={"user":self.address,"type":"REDEEM","limit":"50"})
        if not act: return []
        acts = act if isinstance(act,list) else act.get("data",[])
        return [{"conditionId":a.get("conditionId",""),"redeemable":True,
                 "cashPnl":_sf(a.get("usdcSize")),"title":a.get("title","?"),
                 "market":{"question":a.get("title","?")}} for a in acts]

    def redeem(self, cid, oi=0):
        if not self.account: log.info(f"  [SIM] Redeem {cid[:12]}"); return True
        if not self.w3: return True
        try:
            abi = [{"name":"redeemPositions","type":"function","outputs":[],
                    "stateMutability":"nonpayable","inputs":[
                        {"name":"collateralToken","type":"address"},
                        {"name":"parentCollectionId","type":"bytes32"},
                        {"name":"conditionId","type":"bytes32"},
                        {"name":"indexSets","type":"uint256[]"}]}]
            ctf = self.w3.eth.contract(
                address=self.w3.to_checksum_address(CTF_ADDRESS),abi=abi)
            tx = ctf.functions.redeemPositions(
                self.w3.to_checksum_address(USDC_ADDRESS),
                bytes.fromhex(HASH_ZERO[2:]),
                bytes.fromhex(cid.replace("0x","")), [1,2],
            ).build_transaction({
                "from":self.account.address,
                "nonce":self.w3.eth.get_transaction_count(self.account.address),
                "gas":200_000,"gasPrice":self.w3.eth.gas_price})
            signed  = self.w3.eth.account.sign_transaction(tx,PRIVATE_KEY)
            txh     = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(txh,timeout=120)
            ok = receipt.status==1
            log.info(f"  {'✅' if ok else '❌'} Redeem {cid[:12]}")
            return ok
        except Exception as e:
            log.error(f"  redeem: {e}"); return False

# ── Protections ────────────────────────────────────────────────────────────────
def daily_reset(state):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state["daily_reset"] != today:
        log.info(f"🌅 Reset PnL {today} (était {_f(state['daily_pnl'])} USDC)")
        state.update(daily_pnl=0.0, daily_reset=today)
    return state

def is_paused(state):
    if pu := state.get("paused_until"):
        end = datetime.fromisoformat(pu)
        if datetime.utcnow() < end:
            log.info(f"⏸️  Pause {int((end-datetime.utcnow()).total_seconds()//60)} min")
            return True
        log.info("▶️  Pause terminée"); state["paused_until"] = None
    return False

def check_loss(state, balance):
    lp = -state["daily_pnl"]/balance if balance > 0 else 0
    if lp > DAILY_LOSS_LIMIT:
        pu = (datetime.utcnow()+timedelta(hours=24)).isoformat()
        state["paused_until"] = pu
        log.warning(f"🛡️  PROTECTION {lp*100:.1f}% > {DAILY_LOSS_LIMIT*100:.0f}% — pause→{pu[:16]}")
        return True
    return False

def milestones(balance, state):
    for m in [500,1000,1500,2000]:
        lbl = str(m)
        if balance >= m and lbl not in state.get("milestones_hit",[]):
            state.setdefault("milestones_hit",[]).append(lbl)
            log.info(f"🎉 ≥{m} USDC ({_f(balance)}) — Tirelire qui grandit 💰")

def auto_redeem(client, state, cal):
    total = 0.0
    try:
        rs = client.redeemable()
        if not rs: return 0.0
        log.info(f"💎 {len(rs)} position(s)")
        for p in rs:
            cid   = p.get("conditionId","")
            pnl   = _sf(p.get("cashPnl") or p.get("pnl"))
            mname = p.get("title") or p.get("market",{}).get("question","?")
            ch    = next((c["name"] for c in CITIES
                          if any(a in str(mname).lower() for a in c["aliases"])),"Unknown")
            if client.redeem(cid, int(_sf(p.get("outcomeIndex")))):
                total += pnl
                update_cal(ch,datetime.utcnow().month,cal,pnl>0)
                append_trade({
                    "timestamp":datetime.utcnow().isoformat(),
                    "market":str(mname)[:50],"outcome":"REDEEM",
                    "amount":0,"entry_price":"","exit_price":"",
                    "pnl":round(pnl,4),"balance_after":round(state["balance"]+total,2),
                    "ev":"","model_prob":"","notes":f"Redeem {cid[:12]}",
                })
                log.info(f"  ✅ {str(mname)[:30]} PnL={pnl:+.2f}$")
    except Exception as e:
        log.error(f"auto_redeem: {e}")
    return total

# ── Traitement d'une ville ─────────────────────────────────────────────────────
def process_city(city, fc, city_markets, client, state, cal) -> int:
    trades = 0
    month  = datetime.utcnow().month
    cf     = conf_factor(city["name"], month, cal)
    tb     = bet_size(state["balance"])

    for mkt in city_markets:
        mname = (mkt.get("question") or mkt.get("groupItemTitle")
                 or mkt.get("title") or mkt.get("slug","?"))[:60]

        # ── Analyse tous les outcomes avec le nouveau moteur ──────────────
        try:
            good_outcomes = analyze_outcomes(mkt, fc)
        except Exception as e:
            log.error(f"    analyze_outcomes: {e}"); continue

        if len(good_outcomes) < MIN_OUTCOMES:
            continue

        log.info(f"    🎯 {mname}")
        log.info(f"       {len(good_outcomes)} outcome(s) EV positif(s)")

        # Pondération par EV pour répartir la mise
        total_ev = sum(o["ev"] for o in good_outcomes)

        for outcome in good_outcomes:
            share  = outcome["ev"] / total_ev if total_ev > 0 else 1/len(good_outcomes)
            amount = round(tb * share * cf, 2)
            if amount < 0.50:
                amount = 0.50  # mise minimale

            result = client.place_order(
                outcome["token_id"], outcome["price"], amount, outcome["side"]
            )
            if result:
                trades             += 1
                state["balance"]   -= amount
                state["daily_pnl"] -= amount
                append_trade({
                    "timestamp":   datetime.utcnow().isoformat(),
                    "market":      mname,
                    "outcome":     outcome["title"][:40],
                    "amount":      amount,
                    "entry_price": outcome["price"],
                    "exit_price":  "",
                    "pnl":         "",
                    "balance_after": round(state["balance"],2),
                    "ev":          outcome["ev"],
                    "model_prob":  outcome["prob"],
                    "notes": (
                        f"Cons={fc['consensus']} Sprd={_f(fc['spread_c'])}°C "
                        f"Fc={_f(fc['temp_max_c'])}°C/{_f(fc['temp_max_f'])}°F "
                        f"Dir={outcome['direction']} Cf={cf} "
                        f"EV≥{fc['ev_threshold']*100:.0f}% M={fc['models']}"
                    ),
                })
                log.info(
                    f"      ✅ [{outcome['title'][:35]}] "
                    f"{amount:.2f}$ @ {outcome['price']:.3f} "
                    f"prob={outcome['prob']:.0%} EV={outcome['ev']:.1%}"
                )

    return trades

# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    log.info("🌤️  Tirelire Météo Pension v3.0 démarrée")
    log.info(f"  📂 {DATA_DIR} | ⏱️  {SCAN_INTERVAL//60}min/cycle")
    log.info(f"  🌡️  GFS+ECMWF{'+NOAA' if NOAA_API_TOKEN else ''}")
    log.info(f"  📈 EV {EV_THRESHOLD*100:.0f}%/{EV_THRESHOLD_STRONG*100:.0f}%(STRONG×3)")
    log.info(f"  🔢 MIN_OUTCOMES={MIN_OUTCOMES} (1 outcome EV+ suffit pour trader)")
    log.info(f"  🏙️  {CITIES_PER_CYCLE} villes/cycle ⏳{INTER_CITY_DELAY}s")

    init_csv()
    state    = load_state()
    cal      = load_cal()
    client   = PolymarketClient()
    rotation = CityRotation(CITIES, CITIES_PER_CYCLE)

    cycle = 0
    while True:
        cycle += 1
        t0     = time.time()
        log.info(f"\n{'═'*58}")
        log.info(f"🔄 Cycle #{cycle} | {_f(state['balance'])} USDC")

        state = daily_reset(state)
        milestones(state["balance"], state)

        red = auto_redeem(client, state, cal)
        if red:
            state["balance"]   += red
            state["daily_pnl"] += red
            log.info(f"  💰 +{red:.2f}$ → {_f(state['balance'])} USDC")

        if is_paused(state):
            save_state(state); time.sleep(SCAN_INTERVAL); continue
        if check_loss(state, state["balance"]):
            save_state(state); time.sleep(SCAN_INTERVAL); continue

        markets = client.weather_markets(target=400)
        log.info(f"📊 {len(markets)} marchés météo")

        mbc: dict[str,list] = {}
        for m in markets:
            c = _mcity(m)
            if c: mbc.setdefault(c["name"],[]).append(m)

        available = set(mbc.keys())
        if not available:
            log.warning("  ⚠️  Aucune ville reconnue")
            save_state(state)
            time.sleep(max(10, SCAN_INTERVAL-(time.time()-t0)))
            continue

        log.info("  📍 " + " | ".join(f"{n}:{len(mbc[n])}" for n in sorted(available)))
        selected = rotation.select(available)
        trades_t, ok, skip = 0, 0, 0

        for i, city in enumerate(selected, 1):
            cname = city["name"]
            cms   = mbc.get(cname,[])
            log.info(f"\n  [{i}/{len(selected)}] 🏙️  {cname} ({len(cms)} marchés)")

            try:   fc = build_forecast(city)
            except Exception as e:
                log.error(f"    forecast: {e}"); fc = None

            if fc is None:
                log.info(f"    ⏭️  sources météo insuffisantes"); skip += 1
            elif fc["consensus"] == "WEAK":
                log.info(f"    ⏭️  WEAK ±{_f(fc['spread_c'])}°C"); skip += 1
            elif fc["sources"] < 2:
                log.info(f"    ⏭️  {fc['sources']} source(s)"); skip += 1
            else:
                n = process_city(city, fc, cms, client, state, cal)
                trades_t += n; ok += 1
                log.info(f"    → {n} ordre(s) | {_f(state['balance'])} USDC")
                if n > 0: save_state(state)

            if i < len(selected): time.sleep(INTER_CITY_DELAY)

        elapsed = time.time()-t0
        log.info(f"\n{'─'*58}")
        log.info(f"✔️  #{cycle} {elapsed:.0f}s | {trades_t} ordres | {ok}✓ {skip}⏭")
        save_state(state)
        time.sleep(max(10, SCAN_INTERVAL-elapsed))

# ── Entrée ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("👋 Arrêt manuel.")
    except Exception as e:
        log.critical(f"💥 {e}", exc_info=True)
        raise
