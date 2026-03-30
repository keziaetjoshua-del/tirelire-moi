# bot_tirelire_moi.py
# Tirelire Météo Pension - Bot Polymarket météo ultra-safe
# v2.0 — Optimisation signaux + timeouts :
#         EV adaptatif 8%/10%, rotation équitable, timeouts 15-20s,
#         backoff intelligent, logs épurés

import os, csv, json, time, math, logging, random, requests, urllib3
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
EV_THRESHOLD        = float(os.getenv("EV_THRESHOLD",       "0.10"))  # 10% défaut
EV_THRESHOLD_STRONG = float(os.getenv("EV_THRESHOLD_STRONG","0.08"))  # 8% si 3 sources STRONG
DAILY_LOSS_LIMIT    = float(os.getenv("DAILY_LOSS_LIMIT",   "0.03"))
MIN_BINS            = int(os.getenv("MIN_BINS",   "5"))
MAX_BINS            = int(os.getenv("MAX_BINS",  "10"))
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

# Timeouts réduits v2.0 (ms → s)
API_CONNECT = 8    # connexion TCP
API_READ    = 18   # lecture réponse Polymarket
OM_CONNECT  = 6    # Open-Meteo connect
OM_READ     = 14   # Open-Meteo read

# Backoff exponentiel plafonné : 1s, 2s, 4s (max 3 retries)
API_BACKOFF_BASE = 1.0
API_MAX_RETRIES  = 3

# User-Agent réaliste — évite les resets CDN
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.6367.207 Safari/537.36 TirelireMeteoPension/2.0"
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
    "low temperature","daily high","daily low","forecast",
]
WEATHER_SLUG_KW = [
    "temperature","weather","rain","snow","precipitation",
    "celsius","fahrenheit","heat","cold","frost","degree",
    "warm","hot","freeze","humidity",
]

CSV_FIELDS = [
    "timestamp","market","bins","amount","entry_price",
    "exit_price","pnl","balance_after","ev","notes",
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
# Supprime les warnings urllib3/SSL dans les logs — les retries sont silencieux
for noisy in ("urllib3.connectionpool","requests.packages.urllib3",
              "urllib3.util.retry","urllib3"):
    logging.getLogger(noisy).setLevel(logging.ERROR)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Helpers ────────────────────────────────────────────────────────────────────
def _sf(v, d=0.0):
    """safe_float"""
    if v is None: return d
    try: return float(v)
    except: return d

def _s1(lst, d=None):
    """safe_first"""
    if not lst: return d
    v = lst[0] if isinstance(lst,(list,tuple)) else d
    return v if v is not None else d

def _f(v, n=1, fb="N/A"):
    """Format float pour logs — jamais NoneType.__format__"""
    if v is None: return fb
    try: return f"{float(v):.{n}f}"
    except: return fb

def _backoff(attempt: int) -> float:
    """Backoff exponentiel plafonné : 1s, 2s, 4s."""
    return min(API_BACKOFF_BASE * (2 ** (attempt - 1)), 8.0)

# ══════════════════════════════════════════════════════════════════════════════
# ── Rotation équitable des villes ────────────────────────────────────────────
# Algorithme credit-scheduling : chaque ville accumule priority/total par
# cycle, les CITIES_PER_CYCLE meilleures crédits sont sélectionnées, puis
# débitées de 1.0.  Garantit que toutes les villes passent proportionnellement.
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
                with open(ROTATION_JSON) as f:
                    saved = json.load(f)
                for n in self._credits:
                    if n in saved:
                        self._credits[n] = float(saved[n])
                return
            except Exception:
                pass
        self._reset()

    def _save(self):
        try:
            with open(ROTATION_JSON,"w") as f:
                json.dump(self._credits, f, indent=2)
        except Exception:
            pass

    def _reset(self):
        total = sum(c["priority"] for c in self.cities)
        for c in self.cities:
            self._credits[c["name"]] = c["priority"] / total

    def select(self, available: set) -> list:
        total = sum(c["priority"] for c in self.cities)
        for c in self.cities:
            self._credits[c["name"]] += c["priority"] / total

        candidates = sorted(
            [c for c in self.cities if c["name"] in available],
            key=lambda c: self._credits[c["name"]] + random.uniform(0, 0.03),
            reverse=True,
        )
        selected = candidates[:self.per_cycle]
        for c in selected:
            self._credits[c["name"]] -= 1.0
        self._save()

        names = [c["name"] for c in selected]
        log.info(f"  🔀 Villes ce cycle : {', '.join(names)}")
        return selected

# ══════════════════════════════════════════════════════════════════════════════
# ── Sessions HTTP ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _new_session(verify=True, retries=0) -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = UA
    s.headers["Accept"]     = "application/json"
    s.verify = verify
    adapter  = HTTPAdapter(max_retries=Retry(
        total=retries, backoff_factor=1.5,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET","POST"],
        raise_on_status=False,
    ))
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

# Sessions Open-Meteo (urllib3 gère les retries réseau)
_om_ssl   = _new_session(verify=True,  retries=2)
_om_nossl = _new_session(verify=False, retries=2)
# Session Polymarket (retries manuels pour log précis)
_poly_session = _new_session(verify=True, retries=0)

# ── safe_api_call — Polymarket ─────────────────────────────────────────────────
def _api(method: str, url: str, label: str = "API", **kwargs):
    """
    Appel Polymarket robuste.
    Retries manuels avec backoff exponentiel.
    Log retry seulement si l'erreur persiste (pas de spam sur succès tardif).
    """
    kwargs.setdefault("timeout", (API_CONNECT, API_READ))
    last_err = None

    for attempt in range(1, API_MAX_RETRIES + 2):   # 4 tentatives max
        if attempt > 1:
            wait = _backoff(attempt - 1)
            log.warning(f"  🔄 [{label}] retry {attempt-1}/{API_MAX_RETRIES} dans {wait:.0f}s…")
            time.sleep(wait)
        try:
            resp = _poly_session.request(method, url, **kwargs)
            if resp.status_code == 404:
                log.warning(f"  🚫 [{label}] 404 {url}")
                return None
            if resp.status_code >= 500:
                last_err = f"HTTP {resp.status_code}"
                continue
            raw = resp.text.strip()
            if not raw:
                last_err = "réponse vide"
                continue
            try:
                return resp.json()
            except ValueError:
                last_err = f"JSON invalide ({raw[:80]})"
                continue
        except requests.exceptions.ConnectTimeout:
            last_err = f"ConnectTimeout >{API_CONNECT}s"
        except requests.exceptions.ReadTimeout:
            last_err = f"ReadTimeout >{API_READ}s"
        except requests.exceptions.ConnectionError as e:
            last_err = f"ConnectionError {e}"
        except Exception as e:
            log.error(f"  💥 [{label}] {e}")
            return None

    log.error(f"  ❌ [{label}] abandon — {last_err}")
    return None

# ── Open-Meteo GET ─────────────────────────────────────────────────────────────
def _om_get(params: dict) -> dict | None:
    """GET Open-Meteo avec fallback no-SSL silencieux."""
    for attempt in range(1, API_MAX_RETRIES + 2):
        for sess in (_om_ssl, _om_nossl):
            try:
                r     = sess.get(OPEN_METEO_URL, params=params,
                                 timeout=(OM_CONNECT, OM_READ))
                r.raise_for_status()
                d     = r.json()
                temps = (d.get("daily") or {}).get("temperature_2m_max") or []
                if temps and temps[0] is not None:
                    return d
            except requests.exceptions.SSLError:
                continue
            except (requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError):
                break
            except Exception:
                return None
        if attempt <= API_MAX_RETRIES:
            time.sleep(_backoff(attempt))
    return None

# ── CSV ────────────────────────────────────────────────────────────────────────
def init_csv():
    if not TRADES_CSV.exists():
        with open(TRADES_CSV,"w",newline="") as f:
            csv.DictWriter(f,fieldnames=CSV_FIELDS).writeheader()
        log.info("📄 trades.csv créé.")

def append_trade(row: dict):
    with open(TRADES_CSV,"a",newline="") as f:
        csv.DictWriter(f,fieldnames=CSV_FIELDS).writerow(row)

# ── État ───────────────────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_JSON.exists():
        with open(STATE_JSON) as f: return json.load(f)
    return {"balance": float(os.getenv("INITIAL_BALANCE","100.0")),
            "daily_pnl": 0.0,
            "daily_reset": datetime.utcnow().strftime("%Y-%m-%d"),
            "paused_until": None, "milestones_hit": []}

def save_state(s: dict):
    with open(STATE_JSON,"w") as f: json.dump(s, f, indent=2)

# ── Calibration ────────────────────────────────────────────────────────────────
def load_cal() -> dict:
    if CALIBRATION_JSON.exists():
        with open(CALIBRATION_JSON) as f: return json.load(f)
    return {}

def save_cal(cal: dict):
    with open(CALIBRATION_JSON,"w") as f: json.dump(cal, f, indent=2)

def conf_factor(city: str, month: int, cal: dict) -> float:
    k = f"{city}_{month:02d}"
    if k not in cal or cal[k]["trades"] < 5: return 1.0
    f = 0.7 + (cal[k]["wins"] / cal[k]["trades"]) * 0.6
    return round(min(max(f, 0.7), 1.3), 3)

def update_cal(city: str, month: int, cal: dict, won: bool):
    k = f"{city}_{month:02d}"
    if k not in cal: cal[k] = {"trades":0,"wins":0,"factor":1.0}
    cal[k]["trades"] += 1
    if won: cal[k]["wins"] += 1
    cal[k]["factor"] = conf_factor(city, month, cal)
    save_cal(cal)

# ── Mise ───────────────────────────────────────────────────────────────────────
def bet_size(balance: float) -> float:
    if balance < 500:    return 5.0
    if balance <= 1000:  return min(balance * 0.01,  10.0)
    return               min(balance * 0.0125, 20.0)

# ══════════════════════════════════════════════════════════════════════════════
# ── Météo : GFS + ECMWF (Open-Meteo) + NOAA API ──────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _daily(data: dict, src: str) -> dict | None:
    d = data.get("daily",{})
    tm = _s1(d.get("temperature_2m_max"))
    tn = _s1(d.get("temperature_2m_min"))
    pr = _s1(d.get("precipitation_sum"), d=0.0)
    if tm is None: return None
    tm = _sf(tm, None)
    if tm is None: return None
    return {"source":src,"temp_max":tm,
            "temp_min": _sf(tn) if tn is not None else None,
            "precip_mm": _sf(pr,0.0)}

def _fetch_gfs(lat, lon):
    d = _om_get({"latitude":lat,"longitude":lon,
                 "daily":"temperature_2m_max,temperature_2m_min,precipitation_sum",
                 "forecast_days":7,"timezone":"UTC","models":"gfs_seamless"})
    return _daily(d,"GFS") if d else None

def _fetch_ecmwf(lat, lon):
    for m in ("ecmwf_ifs04","ecmwf_ifs025"):
        d = _om_get({"latitude":lat,"longitude":lon,
                     "daily":"temperature_2m_max,temperature_2m_min,precipitation_sum",
                     "forecast_days":7,"timezone":"UTC","models":m})
        if d:
            r = _daily(d,"ECMWF")
            if r: return r
    return None

def _fetch_noaa(lat, lon):
    if not NOAA_API_TOKEN: return None
    try:
        hdrs = {"User-Agent": UA, "token": NOAA_API_TOKEN}
        r = requests.get(f"https://api.weather.gov/points/{lat},{lon}",
                         headers=hdrs, timeout=(6,12))
        if r.status_code != 200: return None
        fu = r.json().get("properties",{}).get("forecast")
        if not fu: return None
        r2 = requests.get(fu, headers=hdrs, timeout=(6,12))
        r2.raise_for_status()
        periods = r2.json().get("properties",{}).get("periods",[])
        if not periods: return None
        p  = next((x for x in periods[:4] if x.get("isDaytime",True)), periods[0])
        tf = _sf(p.get("temperature"), None)
        if tf is None: return None
        return {"source":"NOAA_API","temp_max":round((tf-32)*5/9,1),
                "temp_min":None,"precip_mm":None}
    except Exception:
        return None

def forecast(city: dict) -> dict | None:
    """
    Agrège GFS + ECMWF + NOAA.
    Seuil EV adaptatif : 8% si 3 sources STRONG (spread < 1°C), 10% sinon.
    Retourne None si < 2 sources valides.
    """
    lat, lon  = city["lat"], city["lon"]
    sources   = []
    for fn, name in [(_fetch_gfs,"GFS"),(_fetch_ecmwf,"ECMWF"),(_fetch_noaa,"NOAA")]:
        try: r = fn(lat, lon)
        except Exception: r = None
        if r is None: continue
        tm = r.get("temp_max")
        if not isinstance(tm,(int,float)) or math.isnan(tm): continue
        r["temp_max"] = float(tm)
        sources.append(r)

    if len(sources) < 2: return None

    temps = [s["temp_max"] for s in sources]
    avg   = sum(temps) / len(temps)
    sprd  = max(temps) - min(temps)
    cons  = "STRONG" if sprd < 1.0 else "MODERATE" if sprd < 2.5 else "WEAK"
    ev_th = EV_THRESHOLD_STRONG if cons == "STRONG" and len(sources) >= 3 else EV_THRESHOLD
    precips  = [s["precip_mm"] for s in sources if s.get("precip_mm") is not None]
    avg_prec = round(sum(precips)/len(precips),1) if precips else 0.0
    models   = [s["source"] for s in sources]

    log.info(
        f"    🌡️  {city['name']} {_f(avg)}°C ±{_f(sprd)}°C "
        f"[{cons}] {models} EV≥{ev_th*100:.0f}%"
    )
    return {"city":city["name"],"temp_max":round(avg,1),"precip_mm":avg_prec,
            "spread":round(sprd,2),"consensus":cons,"sources":len(sources),
            "models":models,"ev_threshold":ev_th}

# ══════════════════════════════════════════════════════════════════════════════
# ── Helpers marchés ───────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _mtext(m: dict) -> str:
    return " ".join([
        m.get("question",""), m.get("slug",""), m.get("title",""),
        m.get("groupItemTitle",""), m.get("description",""),
        " ".join(t.get("label","")+" "+t.get("slug","") for t in m.get("tags",[])),
    ]).lower()

def _is_weather(m: dict) -> bool:
    t = _mtext(m); s = m.get("slug","").lower()
    return any(k in t for k in WEATHER_KW) or any(k in s for k in WEATHER_SLUG_KW)

def _mcity(m: dict) -> dict | None:
    t = _mtext(m)
    for c in CITIES:
        if any(a in t for a in c["aliases"]): return c
    return None

def _mprio(m: dict) -> tuple:
    c = _mcity(m)
    return (c["priority"] if c else 0,
            _sf(m.get("volume24hr") or m.get("volume_24hr")))

def _unpack_events(events: list) -> list:
    out = []
    for ev in events:
        tags, title, slug = ev.get("tags",[]), ev.get("title",""), ev.get("slug","")
        for m in ev.get("markets",[]):
            m.setdefault("tags", tags)
            if not m.get("groupItemTitle"): m["groupItemTitle"] = title
            if not m.get("eventSlug"):     m["eventSlug"]      = slug
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
            log.warning("eth_account absent → simulation")

    def _web3(self):
        try:
            from web3 import Web3
            self.w3 = Web3(Web3.HTTPProvider(POLYGON_RPC, request_kwargs={"timeout":25}))
            if self.w3.is_connected():
                log.info(f"⛓️  Polygon #{self.w3.eth.block_number}")
            else:
                self.w3 = None
                log.warning("⚠️  Polygon RPC indisponible")
        except ImportError:
            self.w3 = None

    # ── Découverte tag weather ─────────────────────────────────────────────
    def _weather_tag(self) -> int | None:
        now = time.time()
        if self._wtag_id and now < self._wtag_ttl:
            return self._wtag_id
        if TAG_CACHE_JSON.exists():
            try:
                c = json.load(open(TAG_CACHE_JSON))
                if now < c.get("expires",0):
                    self._wtag_id  = c["tag_id"]
                    self._wtag_ttl = c["expires"]
                    return self._wtag_id
            except Exception: pass
        log.info("  🔍 Découverte tag weather…")
        for off in [0, 100]:
            d = _api("GET", f"{GAMMA_BASE}/tags", "gamma/tags",
                     params={"limit":100,"offset":off})
            if not d: continue
            for tag in (d if isinstance(d,list) else d.get("tags",[])):
                lbl = (tag.get("label","") + " " + tag.get("slug","")).lower()
                if "weather" in lbl:
                    tid = int(tag.get("id",0))
                    if tid:
                        log.info(f"  ✅ Tag weather id={tid} '{tag.get('label')}'")
                        self._wtag_id  = tid
                        self._wtag_ttl = now + 86400
                        try:
                            json.dump({"tag_id":tid,"expires":self._wtag_ttl},
                                      open(TAG_CACHE_JSON,"w"))
                        except Exception: pass
                        return tid
        log.warning("  ⚠️  Tag weather introuvable")
        return None

    # ── Pagination events ──────────────────────────────────────────────────
    def _page_events(self, params: dict, label: str, cap=300) -> list:
        out, off, ps = [], 0, params.get("limit",50)
        while len(out) < cap:
            d = _api("GET", f"{GAMMA_BASE}/events", label,
                     params={**params,"offset":off})
            if not d: break
            ev = d if isinstance(d,list) else d.get("data",[])
            if not ev: break
            out.extend(_unpack_events(ev))
            more = (d.get("has_more",False) if isinstance(d,dict) else len(ev)==ps)
            if not more: break
            off += ps
        return out

    # ── Récupération marchés météo ─────────────────────────────────────────
    def weather_markets(self, target=400) -> list:
        log.info(f"🌦️  Marchés météo (target={target})…")
        out, seen = [], set()

        def _add(ms):
            for m in ms:
                mid = m.get("conditionId") or m.get("id") or m.get("slug")
                if mid and mid not in seen:
                    seen.add(mid); out.append(m)

        # L1 : tag_id weather
        tid = self._weather_tag()
        if tid:
            _add(self._page_events(
                {"tag_id":tid,"related_tags":"true","active":"true",
                 "closed":"false","order":"volume24hr","ascending":"false","limit":50},
                "L1/tag", cap=target))

        # L2 : filtre textuel sur tous les events actifs
        if len(out) < target:
            before = len(out)
            raw = self._page_events(
                {"active":"true","closed":"false","order":"volume24hr",
                 "ascending":"false","limit":100},
                "L2/all", cap=2000)
            _add([m for m in raw if _is_weather(m)])
            log.info(f"  L2 +{len(out)-before} → {len(out)} total")

        # L3 : /markets direct (fallback)
        if len(out) == 0:
            d = _api("GET", f"{GAMMA_BASE}/markets", "L3/direct",
                     params={"active":"true","closed":"false",
                             "order":"volume24hr","ascending":"false","limit":100})
            if d:
                ms = d if isinstance(d,list) else d.get("data",[])
                _add([m for m in ms if _is_weather(m)])

        out.sort(key=_mprio, reverse=True)
        nc = sum(1 for m in out if _mcity(m))
        log.info(f"✅ {len(out)} marchés météo ({nc} avec ville)")
        return out[:target]

    # ── Ordre ──────────────────────────────────────────────────────────────
    def place_order(self, token_id, price, size, side="BUY") -> dict | None:
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

    # ── Positions redeemables ──────────────────────────────────────────────
    def redeemable(self) -> list:
        if not self.address: return []
        pos = _api("GET", f"{DATA_BASE}/positions", "data/pos",
                   params={"user":self.address,"sizeThreshold":"0"})
        if pos is not None:
            ps = pos if isinstance(pos,list) else pos.get("data",[])
            return [p for p in ps if p.get("redeemable") and _sf(p.get("size"))>0]
        act = _api("GET", f"{DATA_BASE}/activity", "data/activity",
                   params={"user":self.address,"type":"REDEEM","limit":"50"})
        if not act: return []
        acts = act if isinstance(act,list) else act.get("data",[])
        return [{"conditionId":a.get("conditionId",""),"redeemable":True,
                 "cashPnl":_sf(a.get("usdcSize")),"title":a.get("title","?"),
                 "market":{"question":a.get("title","?")}} for a in acts]

    # ── Redeem on-chain ────────────────────────────────────────────────────
    def redeem(self, cid: str, oi=0) -> bool:
        if not self.account:
            log.info(f"  [SIM] Redeem {cid[:12]}"); return True
        if not self.w3: return True
        try:
            abi = [{"name":"redeemPositions","type":"function","outputs":[],
                    "stateMutability":"nonpayable","inputs":[
                        {"name":"collateralToken","type":"address"},
                        {"name":"parentCollectionId","type":"bytes32"},
                        {"name":"conditionId","type":"bytes32"},
                        {"name":"indexSets","type":"uint256[]"}]}]
            ctf = self.w3.eth.contract(
                address=self.w3.to_checksum_address(CTF_ADDRESS), abi=abi)
            tx = ctf.functions.redeemPositions(
                self.w3.to_checksum_address(USDC_ADDRESS),
                bytes.fromhex(HASH_ZERO[2:]),
                bytes.fromhex(cid.replace("0x","")), [1,2],
            ).build_transaction({
                "from":self.account.address,
                "nonce":self.w3.eth.get_transaction_count(self.account.address),
                "gas":200_000,"gasPrice":self.w3.eth.gas_price})
            signed  = self.w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            txh     = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(txh, timeout=120)
            ok = receipt.status == 1
            log.info(f"  {'✅' if ok else '❌'} Redeem {cid[:12]} tx={txh.hex()[:14]}")
            return ok
        except Exception as e:
            log.error(f"  redeem {cid[:12]}: {e}"); return False

# ── EV & bins ──────────────────────────────────────────────────────────────────
def compute_ev(prob, price):
    if price <= 0 or price >= 1: return -999.0
    return round(prob*(1-price) - (1-prob)*price, 4)

def temp_prob(ft, lo, hi, spread):
    σ = max(spread,1.0)*1.5; μ = ft
    def cdf(x): return 0.5*(1+math.erf((x-μ)/(σ*math.sqrt(2))))
    return round(max(0.0,min(1.0,cdf(hi)-cdf(lo))),4)

def parse_range(title):
    import re
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)", title)
    if m: return float(m.group(1)), float(m.group(2))
    m = re.search(r"[>≥]\s*(-?\d+(?:\.\d+)?)", title)
    if m: return float(m.group(1)), float(m.group(1))+25
    m = re.search(r"[<≤]\s*(-?\d+(?:\.\d+)?)", title)
    if m: return float(m.group(1))-25, float(m.group(1))
    return None, None

def select_bins(market: dict, fc: dict) -> list:
    outcomes = market.get("outcomes",[])
    if isinstance(outcomes, str):
        try: outcomes = json.loads(outcomes)
        except: outcomes = []
    if not outcomes: return []

    if outcomes and isinstance(outcomes[0], str):
        pr = market.get("outcomePrices","[]")
        if isinstance(pr,str):
            try: pr = json.loads(pr)
            except: pr = []
        cl = market.get("clobTokenIds","[]")
        if isinstance(cl,str):
            try: cl = json.loads(cl)
            except: cl = []
        outcomes = [{"title":outcomes[i],
                     "price": pr[i] if i<len(pr) else "0.5",
                     "clobTokenIds":[cl[i]] if i<len(cl) else [""]}
                    for i in range(len(outcomes))]

    t, s, ev_th = fc["temp_max"], fc["spread"], fc.get("ev_threshold", EV_THRESHOLD)
    res = []
    for o in outcomes:
        title  = o.get("title","") if isinstance(o,dict) else str(o)
        lo, hi = parse_range(title)
        if lo is None: continue
        mp = _sf(o.get("price",0.5), 0.5)
        if mp <= 0 or mp >= 1: continue
        prob = temp_prob(t, lo, hi, s)
        ev   = compute_ev(prob, mp)
        clob = o.get("clobTokenIds",[""])
        res.append({"token_id":clob[0] if clob else "","title":title,
                    "bin_low":lo,"bin_high":hi,"prob":prob,
                    "market_price":mp,"ev":ev})

    res.sort(key=lambda x: abs(((x["bin_low"]+x["bin_high"])/2)-t))
    return [b for b in res if b["ev"] > ev_th][:MAX_BINS]

# ── Protections ────────────────────────────────────────────────────────────────
def daily_reset(state):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state["daily_reset"] != today:
        log.info(f"🌅 Reset PnL jour {today} (était {_f(state['daily_pnl'])} USDC)")
        state.update(daily_pnl=0.0, daily_reset=today)
    return state

def is_paused(state):
    if pu := state.get("paused_until"):
        end = datetime.fromisoformat(pu)
        if datetime.utcnow() < end:
            log.info(f"⏸️  Pause {int((end-datetime.utcnow()).total_seconds()//60)} min")
            return True
        log.info("▶️  Pause terminée")
        state["paused_until"] = None
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

# ── Redeem auto ────────────────────────────────────────────────────────────────
def auto_redeem(client, state, cal):
    total = 0.0
    try:
        rs = client.redeemable()
        if not rs: return 0.0
        log.info(f"💎 {len(rs)} position(s) redeemable(s)")
        for p in rs:
            cid   = p.get("conditionId","")
            pnl   = _sf(p.get("cashPnl") or p.get("pnl"))
            mname = p.get("title") or p.get("market",{}).get("question","?")
            ch    = next((c["name"] for c in CITIES
                          if any(a in str(mname).lower() for a in c["aliases"])),
                         "Unknown")
            if client.redeem(cid, int(_sf(p.get("outcomeIndex")))):
                total += pnl
                update_cal(ch, datetime.utcnow().month, cal, pnl > 0)
                append_trade({
                    "timestamp":     datetime.utcnow().isoformat(),
                    "market":        str(mname)[:50], "bins":"REDEEM",
                    "amount":0,"entry_price":"","exit_price":"",
                    "pnl":round(pnl,4),
                    "balance_after": round(state["balance"]+total,2),
                    "ev":"","notes": f"Redeem {cid[:12]}",
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

    for mkt in city_markets:
        try:   bins = select_bins(mkt, fc)
        except Exception as e:
            log.error(f"    select_bins: {e}"); continue
        if len(bins) < MIN_BINS: continue

        tb    = bet_size(state["balance"])
        tev   = sum(b["ev"] for b in bins)
        mname = (mkt.get("question") or mkt.get("title") or mkt.get("slug","?"))[:50]
        log.info(f"    🎯 {mname}")

        for b in bins:
            if tev <= 0: break
            bb = round((b["ev"]/tev)*tb*cf, 2)
            if bb < 0.50: continue
            r = client.place_order(b["token_id"], b["market_price"], bb)
            if r:
                trades            += 1
                state["balance"]  -= bb
                state["daily_pnl"]-= bb
                append_trade({
                    "timestamp":     datetime.utcnow().isoformat(),
                    "market":        mname, "bins": b["title"],
                    "amount":        bb, "entry_price": b["market_price"],
                    "exit_price":"","pnl":"",
                    "balance_after": round(state["balance"],2),
                    "ev":            b["ev"],
                    "notes": (f"Cons={fc['consensus']} Sprd={_f(fc['spread'])} "
                              f"Cf={cf} EV≥{fc['ev_threshold']*100:.0f}% "
                              f"M={fc['models']}"),
                })
                log.info(f"      ✅ [{b['title']}] {bb:.2f}$ @ {b['market_price']:.3f} EV={b['ev']:.1%}")
    return trades

# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    log.info("🌤️  Tirelire Météo Pension v2.0 démarrée")
    log.info(f"  📂 {DATA_DIR} | ⏱️  {SCAN_INTERVAL//60}min/cycle")
    log.info(f"  🌡️  GFS+ECMWF{'+NOAA' if NOAA_API_TOKEN else ''}")
    log.info(f"  📈 EV {EV_THRESHOLD*100:.0f}%/{EV_THRESHOLD_STRONG*100:.0f}%(STRONG×3)")
    log.info(f"  🏙️  {CITIES_PER_CYCLE} villes/cycle ⏳{INTER_CITY_DELAY}s")
    log.info(f"  ⏱️  Timeouts : connect={API_CONNECT}s read={API_READ}s")

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
        log.info(f"🔄 Cycle #{cycle} | Balance {_f(state['balance'])} USDC")

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

        # Regroupement par ville
        mbc: dict[str,list] = {}
        for m in markets:
            c = _mcity(m)
            if c: mbc.setdefault(c["name"],[]).append(m)

        available = set(mbc.keys())
        if not available:
            log.warning("  ⚠️  Aucune ville reconnue — skip")
            save_state(state)
            time.sleep(max(10, SCAN_INTERVAL-(time.time()-t0)))
            continue

        log.info("  📍 " + " | ".join(f"{n}:{len(mbc[n])}" for n in sorted(available)))

        selected = rotation.select(available)
        trades_t, ok, skip = 0, 0, 0

        for i, city in enumerate(selected, 1):
            cname = city["name"]
            cms   = mbc.get(cname, [])
            log.info(f"\n  [{i}/{len(selected)}] 🏙️  {cname} ({len(cms)} marchés)")

            try:   fc = forecast(city)
            except Exception as e:
                log.error(f"    forecast: {e}"); fc = None

            if fc is None:
                log.info(f"    ⏭️  sources insuffisantes"); skip += 1
            elif fc["consensus"] == "WEAK":
                log.info(f"    ⏭️  WEAK spread={_f(fc['spread'])}°C"); skip += 1
            elif fc["sources"] < 2:
                log.info(f"    ⏭️  {fc['sources']} source(s)"); skip += 1
            else:
                n = process_city(city, fc, cms, client, state, cal)
                trades_t += n; ok += 1
                log.info(f"    → {n} ordre(s) | {_f(state['balance'])} USDC")
                if n > 0: save_state(state)

            if i < len(selected):
                time.sleep(INTER_CITY_DELAY)

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
