# bot_tirelire_moi.py
# Tirelire Météo Pension - Bot Polymarket météo ultra-safe
# v1.7 — Open-Meteo robuste (retry + SSL fallback) + détection signaux améliorée
#         EV adaptatif : 8% si consensus STRONG 3 sources, 10% sinon

import os
import csv
import json
import time
import logging
import requests
import urllib3
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

PRIVATE_KEY    = os.getenv("PRIVATE_KEY", "")
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN", "")

# Open-Meteo endpoints (public, no key needed)
OPEN_METEO_PRIMARY = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_BACKUP  = "https://api.open-meteo.com/v1/forecast"   # même host, retry suffit

SCAN_INTERVAL    = int(os.getenv("SCAN_INTERVAL",     "900"))
# EV de base — abaissé dynamiquement à EV_THRESHOLD_STRONG si consensus STRONG 3 sources
EV_THRESHOLD        = float(os.getenv("EV_THRESHOLD",      "0.10"))  # 10% défaut
EV_THRESHOLD_STRONG = float(os.getenv("EV_THRESHOLD_STRONG","0.08"))  # 8% si 3 sources STRONG
DAILY_LOSS_LIMIT    = float(os.getenv("DAILY_LOSS_LIMIT",   "0.03"))
MIN_BINS            = int(os.getenv("MIN_BINS", "5"))
MAX_BINS            = int(os.getenv("MAX_BINS", "10"))

# Polymarket endpoints
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"

POLYGON_RPC  = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")
CTF_ADDRESS  = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
HASH_ZERO    = "0x" + "0" * 64

# Timeouts & retries API Polymarket
API_TIMEOUT_CONNECT = 10
API_TIMEOUT_READ    = 30
API_RETRY_DELAYS    = [1, 3, 8]
API_USER_AGENT      = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36 TirelireMeteoPension/1.7"
)

# Timeouts & retries Open-Meteo (séparés pour ne pas bloquer la boucle)
OM_TIMEOUT_CONNECT = 8
OM_TIMEOUT_READ    = 15
OM_MAX_RETRIES     = 3
OM_RETRY_DELAYS    = [1, 2, 4]   # plus rapide que Polymarket

# Villes surveillées — triées par volume historique Polymarket décroissant
CITIES = [
    {"name": "New York",    "lat": 40.71, "lon": -74.00,
     "aliases": ["new york", "nyc", "ny ", "new-york", "manhattan"],
     "priority": 10},
    {"name": "Chicago",     "lat": 41.88, "lon": -87.63,
     "aliases": ["chicago", "chi "],
     "priority": 9},
    {"name": "Los Angeles", "lat": 34.05, "lon": -118.24,
     "aliases": ["los angeles", "la ", "l.a.", "los-angeles"],
     "priority": 8},
    {"name": "Miami",       "lat": 25.76, "lon":  -80.19,
     "aliases": ["miami"],
     "priority": 8},
    {"name": "Dallas",      "lat": 32.78, "lon":  -96.80,
     "aliases": ["dallas", "dfw"],
     "priority": 7},
    {"name": "Seattle",     "lat": 47.61, "lon": -122.33,
     "aliases": ["seattle"],
     "priority": 7},
    {"name": "Boston",      "lat": 42.36, "lon":  -71.06,
     "aliases": ["boston"],
     "priority": 7},
    {"name": "Washington",  "lat": 38.91, "lon":  -77.04,
     "aliases": ["washington", "dc ", "d.c."],
     "priority": 7},
    {"name": "London",      "lat": 51.51, "lon":  -0.13,
     "aliases": ["london", "uk temperature", "england weather"],
     "priority": 6},
    {"name": "Paris",       "lat": 48.85, "lon":   2.35,
     "aliases": ["paris", "france temperature"],
     "priority": 5},
    {"name": "Berlin",      "lat": 52.52, "lon":  13.41,
     "aliases": ["berlin", "germany temperature"],
     "priority": 4},
    {"name": "Lyon",        "lat": 45.75, "lon":   4.85,
     "aliases": ["lyon"],
     "priority": 3},
    {"name": "Marseille",   "lat": 43.30, "lon":   5.37,
     "aliases": ["marseille"],
     "priority": 3},
]

WEATHER_KEYWORDS = [
    "temperature", "temp", "weather", "degrees", "celsius", "fahrenheit",
    "°f", "°c", "precipitation", "rainfall", "rain", "snow", "snowfall",
    "humidity", "heatwave", "heat wave", "frost", "freeze", "cold",
    "warm", "hot", "record high", "record low", "high temperature",
    "low temperature", "daily high", "daily low", "forecast",
]
WEATHER_SLUG_KEYWORDS = [
    "temperature", "weather", "rain", "snow", "precipitation",
    "celsius", "fahrenheit", "heat", "cold", "frost", "degree",
    "warm", "hot", "freeze", "humidity",
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

# Réduit le bruit des warnings urllib3/SSL dans les logs
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logging.getLogger("requests.packages.urllib3").setLevel(logging.ERROR)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Helpers numériques ─────────────────────────────────────────────────────────
def _safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def _safe_first(lst, default=None):
    if not lst:
        return default
    val = lst[0] if isinstance(lst, (list, tuple)) else default
    return val if val is not None else default

def _fmt(value, digits: int = 1, fallback: str = "N/A") -> str:
    """Format sûr pour les f-strings — jamais de NoneType.__format__."""
    if value is None:
        return fallback
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return fallback

# ══════════════════════════════════════════════════════════════════════════════
# ── Session Open-Meteo dédiée — retry + SSL fallback ─────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _build_openmeteo_session(verify_ssl: bool = True) -> requests.Session:
    """
    Session HTTP dédiée Open-Meteo avec :
    - Retry urllib3 sur erreurs réseau (connection reset, 502/503/504)
    - Backoff exponentiel
    - verify_ssl=False en fallback si SSL échoue
    """
    s = requests.Session()
    retry = Retry(
        total=OM_MAX_RETRIES,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    s.headers.update({"User-Agent": API_USER_AGENT})
    s.verify = verify_ssl
    return s

# Sessions Open-Meteo — créées une seule fois au démarrage
_om_session_ssl    = _build_openmeteo_session(verify_ssl=True)
_om_session_nossl  = _build_openmeteo_session(verify_ssl=False)

def _om_get(params: dict, label: str) -> dict | None:
    """
    GET Open-Meteo avec retry automatique urllib3 + fallback no-SSL.
    Supprime les warnings SSL dans les logs (géré silencieusement).
    Retourne None proprement sur tous les types d'erreur.
    """
    for attempt in range(1, OM_MAX_RETRIES + 1):
        for session, ssl_label in [
            (_om_session_ssl,   "SSL"),
            (_om_session_nossl, "noSSL"),
        ]:
            try:
                r = session.get(
                    OPEN_METEO_PRIMARY,
                    params=params,
                    timeout=(OM_TIMEOUT_CONNECT, OM_TIMEOUT_READ),
                )
                r.raise_for_status()
                data = r.json()

                # Vérifie présence et validité de daily
                daily = data.get("daily", {})
                if not daily:
                    continue

                temps = daily.get("temperature_2m_max") or []
                if not temps or temps[0] is None:
                    continue

                # Succès
                if attempt > 1 or ssl_label == "noSSL":
                    log.debug(f"    {label} OK [{ssl_label}, tentative {attempt}]")
                return data

            except requests.exceptions.SSLError:
                # SSL échoue → on bascule sur noSSL dans la même itération
                continue
            except requests.exceptions.ConnectTimeout:
                break   # timeout → retry dans la boucle extérieure
            except requests.exceptions.ReadTimeout:
                break
            except requests.exceptions.ConnectionError:
                break
            except Exception:
                return None   # erreur inattendue → abandon silencieux

        # Pause avant retry
        if attempt < OM_MAX_RETRIES:
            time.sleep(OM_RETRY_DELAYS[min(attempt - 1, len(OM_RETRY_DELAYS) - 1)])

    return None   # toutes tentatives épuisées

# ══════════════════════════════════════════════════════════════════════════════
# ── safe_api_call — Polymarket ────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def safe_api_call(session: requests.Session, method: str, url: str,
                  label: str = "API", **kwargs):
    kwargs.setdefault("timeout", (API_TIMEOUT_CONNECT, API_TIMEOUT_READ))
    max_attempts = len(API_RETRY_DELAYS) + 1

    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            delay = API_RETRY_DELAYS[attempt - 2]
            log.warning(f"  🔄 [{label}] Retry {attempt-1}/{len(API_RETRY_DELAYS)} dans {delay}s…")
            time.sleep(delay)
        try:
            resp = session.request(method, url, **kwargs)
            if resp.status_code == 404:
                log.warning(f"  🚫 [{label}] HTTP 404 : {url}")
                return None
            if resp.status_code >= 500:
                log.warning(f"  ⚠️  [{label}] HTTP {resp.status_code} — tentative {attempt}/{max_attempts}")
                continue
            raw = resp.text.strip()
            if not raw:
                log.warning(f"  ⚠️  [{label}] Réponse vide — tentative {attempt}/{max_attempts}")
                continue
            try:
                data = resp.json()
                if attempt > 1:
                    log.info(f"  ✅ [{label}] Succès après {attempt} tentatives")
                return data
            except ValueError:
                preview = raw[:120].replace("\n", " ")
                log.warning(f"  ⚠️  [{label}] JSON invalide «{preview}» — tentative {attempt}/{max_attempts}")
                continue
        except requests.exceptions.ConnectTimeout:
            log.warning(f"  ⏱️  [{label}] ConnectTimeout — tentative {attempt}/{max_attempts}")
        except requests.exceptions.ReadTimeout:
            log.warning(f"  ⏱️  [{label}] ReadTimeout ({API_TIMEOUT_READ}s) — tentative {attempt}/{max_attempts}")
        except requests.exceptions.ConnectionError as e:
            log.warning(f"  🔌 [{label}] ConnectionError: {e} — tentative {attempt}/{max_attempts}")
        except Exception as e:
            log.error(f"  💥 [{label}] Erreur inattendue: {e}")
            break

    log.error(f"  ❌ [{label}] Unavailable — abandon après {max_attempts} tentatives.")
    return None

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":   API_USER_AGENT,
        "Accept":       "application/json",
        "Content-Type": "application/json",
    })
    s.mount("https://", HTTPAdapter(max_retries=0))
    s.mount("http://",  HTTPAdapter(max_retries=0))
    return s

# ── CSV ────────────────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "timestamp", "market", "bins", "amount", "entry_price",
    "exit_price", "pnl", "balance_after", "ev", "notes",
]

def init_csv():
    if not TRADES_CSV.exists():
        with open(TRADES_CSV, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
        log.info("📄 trades.csv créé.")

def append_trade(row: dict):
    with open(TRADES_CSV, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(row)

# ── État persistant ────────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_JSON.exists():
        with open(STATE_JSON) as f:
            return json.load(f)
    return {
        "balance":        float(os.getenv("INITIAL_BALANCE", "100.0")),
        "daily_pnl":      0.0,
        "daily_reset":    datetime.utcnow().strftime("%Y-%m-%d"),
        "paused_until":   None,
        "milestones_hit": [],
    }

def save_state(state: dict):
    with open(STATE_JSON, "w") as f:
        json.dump(state, f, indent=2)

# ── Calibration ────────────────────────────────────────────────────────────────
def load_calibration() -> dict:
    if CALIBRATION_JSON.exists():
        with open(CALIBRATION_JSON) as f:
            return json.load(f)
    return {}

def save_calibration(cal: dict):
    with open(CALIBRATION_JSON, "w") as f:
        json.dump(cal, f, indent=2)

def get_confidence_factor(city: str, month: int, cal: dict) -> float:
    key = f"{city}_{month:02d}"
    if key not in cal or cal[key]["trades"] < 5:
        return 1.0
    factor = 0.7 + (cal[key]["wins"] / cal[key]["trades"]) * 0.6
    return round(min(max(factor, 0.7), 1.3), 3)

def update_calibration(city: str, month: int, cal: dict, won: bool):
    key = f"{city}_{month:02d}"
    if key not in cal:
        cal[key] = {"trades": 0, "wins": 0, "factor": 1.0}
    cal[key]["trades"] += 1
    if won:
        cal[key]["wins"] += 1
    cal[key]["factor"] = get_confidence_factor(city, month, cal)
    save_calibration(cal)

# ── Calcul de mise ─────────────────────────────────────────────────────────────
def compute_bet_size(balance: float) -> float:
    if balance < 500:
        return 5.0
    elif balance <= 1000:
        return min(balance * 0.01, 10.0)
    else:
        return min(balance * 0.0125, 20.0)

# ══════════════════════════════════════════════════════════════════════════════
# ── APIs Météo ────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _extract_daily(data: dict, source: str) -> dict | None:
    """
    Extrait temp_max (obligatoire), temp_min, precip depuis une réponse
    Open-Meteo validée. Retourne None si temp_max absent ou non-numérique.
    """
    daily       = data.get("daily", {})
    temp_max_v  = _safe_first(daily.get("temperature_2m_max"))
    temp_min_v  = _safe_first(daily.get("temperature_2m_min"))
    precip_v    = _safe_first(daily.get("precipitation_sum"), default=0.0)

    if temp_max_v is None:
        return None
    temp_max = _safe_float(temp_max_v, default=None)
    if temp_max is None:
        return None

    return {
        "source":    source,
        "temp_max":  temp_max,
        "temp_min":  _safe_float(temp_min_v) if temp_min_v is not None else None,
        "precip_mm": _safe_float(precip_v, 0.0),
    }

def fetch_gfs(lat: float, lon: float) -> dict | None:
    """GFS NOAA via Open-Meteo (gfs_seamless). Gratuit, sans clé."""
    params = {
        "latitude": lat, "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "forecast_days": 7, "timezone": "UTC",
        "models": "gfs_seamless",
    }
    data = _om_get(params, "GFS")
    return _extract_daily(data, "GFS") if data else None

def fetch_ecmwf(lat: float, lon: float) -> dict | None:
    """ECMWF IFS via Open-Meteo. Gratuit, sans clé. Fallback ifs025."""
    for model in ("ecmwf_ifs04", "ecmwf_ifs025"):
        params = {
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "forecast_days": 7, "timezone": "UTC",
            "models": model,
        }
        data = _om_get(params, f"ECMWF/{model}")
        if data:
            result = _extract_daily(data, "ECMWF")
            if result:
                return result
    return None

def fetch_noaa_api(lat: float, lon: float) -> dict | None:
    """NOAA weather.gov (USA uniquement). Requiert NOAA_API_TOKEN."""
    if not NOAA_API_TOKEN:
        return None
    try:
        hdrs = {
            "User-Agent": "TirelireMeteoPension/1.7 contact@example.com",
            "token":      NOAA_API_TOKEN,
        }
        r = requests.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers=hdrs, timeout=(8, 15),
        )
        if r.status_code != 200:
            return None
        forecast_url = r.json().get("properties", {}).get("forecast")
        if not forecast_url:
            return None
        r2 = requests.get(forecast_url, headers=hdrs, timeout=(8, 15))
        r2.raise_for_status()
        periods = r2.json().get("properties", {}).get("periods", [])
        if not periods:
            return None
        day_period = next(
            (p for p in periods[:4] if p.get("isDaytime", True)), periods[0]
        )
        temp_f = _safe_float(day_period.get("temperature"), default=None)
        if temp_f is None:
            return None
        return {
            "source":    "NOAA_API",
            "temp_max":  round((temp_f - 32) * 5 / 9, 1),
            "temp_min":  None,
            "precip_mm": None,
        }
    except Exception:
        return None   # silencieux — NOAA est optionnel

def build_consensus_forecast(city: dict) -> dict | None:
    """
    Agrège GFS + ECMWF + NOAA en consensus.

    Seuil EV adaptatif (v1.7) :
    • 3 sources STRONG (spread < 1°C) → EV_THRESHOLD_STRONG (8%)
    • Sinon                           → EV_THRESHOLD (10%)

    Toutes les valeurs retournées sont des float garantis.
    Retourne None si < 2 sources valides.
    """
    lat, lon  = city["lat"], city["lon"]
    forecasts = []

    for fetch_fn, name in [(fetch_gfs, "GFS"), (fetch_ecmwf, "ECMWF"),
                           (fetch_noaa_api, "NOAA_API")]:
        try:
            result = fetch_fn(lat, lon)
        except Exception:
            result = None

        if result is None:
            continue
        temp_max = result.get("temp_max")
        if not isinstance(temp_max, (int, float)) or temp_max != temp_max:
            # NaN check
            continue
        result["temp_max"] = float(temp_max)
        forecasts.append(result)

    if len(forecasts) < 2:
        return None

    temps    = [f["temp_max"] for f in forecasts]
    avg_temp = sum(temps) / len(temps)
    spread   = max(temps) - min(temps)

    if spread < 1.0:
        consensus = "STRONG"
    elif spread < 2.5:
        consensus = "MODERATE"
    else:
        consensus = "WEAK"

    # Seuil EV adaptatif : plus agressif uniquement si 3 sources STRONG
    if consensus == "STRONG" and len(forecasts) >= 3:
        ev_threshold = EV_THRESHOLD_STRONG
    else:
        ev_threshold = EV_THRESHOLD

    precip_vals = [
        f["precip_mm"] for f in forecasts
        if f.get("precip_mm") is not None
    ]
    avg_precip = round(sum(precip_vals) / len(precip_vals), 1) if precip_vals else 0.0

    sources_names = [f["source"] for f in forecasts]
    log.info(
        f"  🌡️  {city['name']} | {_fmt(avg_temp)}°C ± {_fmt(spread)}°C "
        f"[{consensus}] sources={sources_names} EV_seuil={ev_threshold*100:.0f}%"
    )

    return {
        "city":         city["name"],
        "temp_max":     round(avg_temp, 1),
        "precip_mm":    avg_precip,
        "spread":       round(spread, 2),
        "consensus":    consensus,
        "sources":      len(forecasts),
        "models":       sources_names,
        "ev_threshold": ev_threshold,     # ← seuil adaptatif inclus dans le forecast
    }

# ══════════════════════════════════════════════════════════════════════════════
# ── Helpers filtre marchés météo ──────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _text_is_weather(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in WEATHER_KEYWORDS)

def _slug_is_weather(slug: str) -> bool:
    s = slug.lower()
    return any(k in s for k in WEATHER_SLUG_KEYWORDS)

def _market_search_text(market: dict) -> str:
    return " ".join([
        market.get("question",       ""),
        market.get("slug",           ""),
        market.get("title",          ""),
        market.get("groupItemTitle", ""),
        market.get("description",    ""),
        " ".join(
            t.get("label", "") + " " + t.get("slug", "")
            for t in market.get("tags", [])
        ),
    ]).lower()

def _is_weather_market(market: dict) -> bool:
    return _text_is_weather(_market_search_text(market)) or \
           _slug_is_weather(market.get("slug", ""))

def _market_city(market: dict) -> dict | None:
    text = _market_search_text(market)
    for city in CITIES:
        if any(alias in text for alias in city["aliases"]):
            return city
    return None

def _market_priority(market: dict) -> tuple:
    """
    Tri : (priorité_ville DESC, volume24h DESC).
    Les villes USA à fort volume remontent en tête.
    """
    city  = _market_city(market)
    prio  = city["priority"] if city else 0
    vol24 = _safe_float(market.get("volume24hr") or market.get("volume_24hr"), 0.0)
    return (prio, vol24)

def _extract_markets_from_events(events: list) -> list:
    markets = []
    for event in events:
        event_tags  = event.get("tags",  [])
        event_title = event.get("title", "")
        event_slug  = event.get("slug",  "")
        for m in event.get("markets", []):
            m.setdefault("tags", event_tags)
            if not m.get("groupItemTitle"):
                m["groupItemTitle"] = event_title
            if not m.get("eventSlug"):
                m["eventSlug"] = event_slug
            markets.append(m)
    return markets

# ══════════════════════════════════════════════════════════════════════════════
# ── PolymarketClient ─────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
class PolymarketClient:

    def __init__(self):
        self.session         = build_session()
        self._weather_tag_id = None
        self._tag_cache_ttl  = 0
        self._setup_signer()
        self._setup_web3()

    def _setup_signer(self):
        try:
            from eth_account import Account
            if PRIVATE_KEY:
                self.account = Account.from_key(PRIVATE_KEY)
                self.address = self.account.address
                log.info(f"🔑 Wallet: {self.address[:10]}…")
            else:
                self.account = None
                self.address = None
                log.warning("⚠️  PRIVATE_KEY non définie → mode simulation")
        except ImportError:
            log.warning("eth_account non installé → mode simulation")
            self.account = None
            self.address = None

    def _setup_web3(self):
        try:
            from web3 import Web3
            self.w3 = Web3(Web3.HTTPProvider(POLYGON_RPC, request_kwargs={"timeout": 30}))
            if self.w3.is_connected():
                log.info(f"⛓️  Polygon connecté (block #{self.w3.eth.block_number})")
            else:
                log.warning("⚠️  Polygon RPC non connecté — redeem désactivé")
                self.w3 = None
        except ImportError:
            log.warning("web3 non installé — redeem désactivé")
            self.w3 = None

    def _discover_weather_tag_id(self) -> int | None:
        now = time.time()
        if self._weather_tag_id and now < self._tag_cache_ttl:
            return self._weather_tag_id
        if TAG_CACHE_JSON.exists():
            try:
                with open(TAG_CACHE_JSON) as f:
                    cached = json.load(f)
                if now < cached.get("expires", 0):
                    self._weather_tag_id = cached["tag_id"]
                    self._tag_cache_ttl  = cached["expires"]
                    log.info(f"  📎 Tag weather (cache) : id={self._weather_tag_id}")
                    return self._weather_tag_id
            except Exception:
                pass
        log.info("  🔍 Découverte tag_id 'weather' via /tags…")
        for offset in [0, 100]:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/tags",
                label="gamma/tags_discovery",
                params={"limit": 100, "offset": offset},
            )
            if not data:
                continue
            tags = data if isinstance(data, list) else data.get("tags", [])
            for tag in tags:
                label = (tag.get("label", "") + " " + tag.get("slug", "")).lower()
                if "weather" in label:
                    tag_id = int(tag.get("id", 0))
                    if tag_id:
                        log.info(f"  ✅ Tag weather : id={tag_id} label='{tag.get('label')}'")
                        self._weather_tag_id = tag_id
                        self._tag_cache_ttl  = now + 86400
                        try:
                            with open(TAG_CACHE_JSON, "w") as f:
                                json.dump({"tag_id": tag_id, "expires": self._tag_cache_ttl}, f)
                        except Exception:
                            pass
                        return tag_id
        log.warning("  ⚠️  Tag 'weather' introuvable → filtre textuel uniquement")
        return None

    def _fetch_events_by_weather_tag(self, tag_id: int, limit: int = 200) -> list:
        all_markets = []
        offset      = 0
        page_size   = 50
        log.info(f"  📡 [Couche 1] /events?tag_id={tag_id} (target={limit})")
        while len(all_markets) < limit:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/events",
                label=f"gamma/events_tag_{tag_id}",
                params={
                    "tag_id": tag_id, "related_tags": "true",
                    "active": "true", "closed": "false",
                    "order": "volume24hr", "ascending": "false",
                    "limit": page_size, "offset": offset,
                },
            )
            if not data:
                break
            events = data if isinstance(data, list) else data.get("data", [])
            if not events:
                break
            batch = _extract_markets_from_events(events)
            all_markets.extend(batch)
            log.info(
                f"    offset={offset} → {len(events)} events, "
                f"{len(batch)} marchés | total={len(all_markets)}"
            )
            has_more = (
                data.get("has_more", False) if isinstance(data, dict)
                else len(events) == page_size
            )
            if not has_more:
                break
            offset += page_size
        log.info(f"  ✅ [Couche 1] {len(all_markets)} marchés via tag_id={tag_id}")
        return all_markets

    def _fetch_events_textual_filter(self, limit: int = 500) -> list:
        weather_markets = []
        offset          = 0
        page_size       = 100
        events_scanned  = 0
        max_events      = 1000
        log.info(f"  📡 [Couche 2] /events actifs + filtre textuel (target={limit})")
        while len(weather_markets) < limit and events_scanned < max_events:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/events",
                label="gamma/events_all",
                params={
                    "active": "true", "closed": "false",
                    "order": "volume24hr", "ascending": "false",
                    "limit": page_size, "offset": offset,
                },
            )
            if not data:
                break
            events = data if isinstance(data, list) else data.get("data", [])
            if not events:
                break
            events_scanned += len(events)
            for m in _extract_markets_from_events(events):
                if _is_weather_market(m):
                    weather_markets.append(m)
            log.info(
                f"    offset={offset} → {len(events)} events "
                f"| météo cumulés: {len(weather_markets)}"
            )
            has_more = (
                data.get("has_more", False) if isinstance(data, dict)
                else len(events) == page_size
            )
            if not has_more:
                break
            offset += page_size
        log.info(
            f"  ✅ [Couche 2] {len(weather_markets)} marchés météo "
            f"sur {events_scanned} events"
        )
        return weather_markets

    def _fetch_markets_direct_filter(self, limit: int = 300) -> list:
        weather_markets = []
        offset          = 0
        page_size       = 100
        markets_scanned = 0
        log.info("  📡 [Couche 3/fallback] /markets direct + filtre textuel")
        while len(weather_markets) < limit and markets_scanned < 1000:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/markets",
                label="gamma/markets_direct",
                params={
                    "active": "true", "closed": "false",
                    "order": "volume24hr", "ascending": "false",
                    "limit": page_size, "offset": offset,
                },
            )
            if not data:
                break
            markets = data if isinstance(data, list) else data.get("data", [])
            if not markets:
                break
            markets_scanned += len(markets)
            for m in markets:
                if _is_weather_market(m):
                    weather_markets.append(m)
            has_more = (
                data.get("has_more", False) if isinstance(data, dict)
                else len(markets) == page_size
            )
            if not has_more:
                break
            offset += page_size
        log.info(f"  ✅ [Couche 3] {len(weather_markets)} marchés météo (fallback)")
        return weather_markets

    def get_weather_markets(self, target: int = 500) -> list:
        log.info(f"🌦️  Récupération marchés météo (target={target})…")
        all_markets: list = []
        seen_ids:    set  = set()

        def _add(markets: list):
            for m in markets:
                mid = m.get("conditionId") or m.get("id") or m.get("slug")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(m)

        tag_id = self._discover_weather_tag_id()
        if tag_id:
            _add(self._fetch_events_by_weather_tag(tag_id, limit=target))
            log.info(f"  📊 Après Couche 1 : {len(all_markets)} uniques")

        if len(all_markets) < target:
            before = len(all_markets)
            _add(self._fetch_events_textual_filter(limit=target))
            log.info(
                f"  📊 Après Couche 2 : {len(all_markets)} uniques "
                f"(+{len(all_markets)-before} nouveaux)"
            )

        if len(all_markets) == 0:
            log.warning("  ↩️  Couches 1+2 vides → Couche 3 (fallback)")
            _add(self._fetch_markets_direct_filter(limit=target))
            log.info(f"  📊 Après Couche 3 : {len(all_markets)} uniques")

        all_markets.sort(key=_market_priority, reverse=True)
        city_matches = sum(1 for m in all_markets if _market_city(m))
        log.info(
            f"✅ {len(all_markets)} marchés météo "
            f"({city_matches} avec ville prioritaire)"
        )
        if all_markets:
            top = all_markets[0]
            log.info(
                f"   Top : {top.get('question', top.get('slug', '?'))[:60]} "
                f"| vol24h={_fmt(_safe_float(top.get('volume24hr')), 0)} USDC"
            )
        return all_markets[:target]

    def get_orderbook(self, token_id: str) -> dict | None:
        return safe_api_call(
            self.session, "GET", f"{CLOB_BASE}/book",
            label=f"clob/orderbook/{token_id[:12]}",
            params={"token_id": token_id},
        )

    def place_order(self, token_id: str, price: float,
                    size: float, side: str = "BUY") -> dict | None:
        if not self.account:
            log.info(f"[SIMULATION] {side} {size:.2f} USDC @ {price:.3f} sur {token_id[:12]}…")
            return {"status": "simulated", "token_id": token_id,
                    "price": price, "size": size}
        try:
            order = {
                "orderType": "GTC", "tokenID": token_id, "side": side,
                "price":     str(round(price, 4)), "size": str(round(size, 2)),
                "funder":    self.address, "maker": self.address, "expiration": "0",
            }
            log.info(f"[ORDRE] {side} {size:.2f} USDC @ {price:.3f}")
            return order
        except Exception as e:
            log.error(f"Erreur place_order: {e}")
            return None

    def get_redeemable_positions(self) -> list:
        if not self.address:
            return []
        log.info("  🔍 Recherche positions redeemables…")
        all_pos = safe_api_call(
            self.session, "GET", f"{DATA_BASE}/positions",
            label="data/redeemable_positions",
            params={"user": self.address, "sizeThreshold": "0"},
        )
        if all_pos is not None:
            positions  = all_pos if isinstance(all_pos, list) else all_pos.get("data", [])
            redeemable = [
                p for p in positions
                if p.get("redeemable") is True and _safe_float(p.get("size")) > 0
            ]
            log.info(f"  📊 {len(positions)} positions, {len(redeemable)} redeemables")
            return redeemable
        log.warning("  ↩️  data-api/positions indisponible → fallback /activity")
        activity = safe_api_call(
            self.session, "GET", f"{DATA_BASE}/activity",
            label="data/activity_fallback",
            params={"user": self.address, "type": "REDEEM", "limit": "50"},
        )
        if activity is None:
            log.warning("  ⚠️  Redeem ignoré ce cycle — API indisponible")
            return []
        acts = activity if isinstance(activity, list) else activity.get("data", [])
        return [
            {
                "conditionId": a.get("conditionId", ""),
                "redeemable":  True,
                "cashPnl":     _safe_float(a.get("usdcSize"), 0.0),
                "title":       a.get("title", "?"),
                "market":      {"question": a.get("title", "?")},
            }
            for a in acts
        ]

    def redeem_position(self, condition_id: str, outcome_index: int = 0) -> bool:
        if not self.account:
            log.info(f"[SIMULATION] Redeem {condition_id[:12]}…")
            return True
        if not self.w3:
            log.warning("web3 non disponible → redeem simulé")
            return True
        try:
            ctf_abi = [{
                "name": "redeemPositions", "type": "function",
                "inputs": [
                    {"name": "collateralToken",    "type": "address"},
                    {"name": "parentCollectionId", "type": "bytes32"},
                    {"name": "conditionId",        "type": "bytes32"},
                    {"name": "indexSets",          "type": "uint256[]"},
                ],
                "outputs": [], "stateMutability": "nonpayable",
            }]
            ctf = self.w3.eth.contract(
                address=self.w3.to_checksum_address(CTF_ADDRESS), abi=ctf_abi
            )
            tx = ctf.functions.redeemPositions(
                self.w3.to_checksum_address(USDC_ADDRESS),
                bytes.fromhex(HASH_ZERO[2:]),
                bytes.fromhex(condition_id.replace("0x", "")),
                [1, 2],
            ).build_transaction({
                "from":     self.account.address,
                "nonce":    self.w3.eth.get_transaction_count(self.account.address),
                "gas":      200_000,
                "gasPrice": self.w3.eth.gas_price,
            })
            signed  = self.w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            if receipt.status == 1:
                log.info(f"  ✅ Redeem on-chain OK — tx: {tx_hash.hex()[:20]}…")
                return True
            log.error(f"  ❌ Redeem FAILED — tx: {tx_hash.hex()[:20]}…")
            return False
        except Exception as e:
            log.error(f"Erreur redeem on-chain {condition_id[:12]}: {e}")
            return False

# ── Logique de trading ─────────────────────────────────────────────────────────
def compute_ev(prob_model: float, market_price: float) -> float:
    if market_price <= 0 or market_price >= 1:
        return -999.0
    return round(
        (prob_model * (1.0 - market_price)) - ((1.0 - prob_model) * market_price), 4
    )

def temperature_to_prob(forecast_temp: float, bin_low: float,
                        bin_high: float, spread: float) -> float:
    import math
    sigma = max(spread, 1.0) * 1.5
    mu    = forecast_temp
    def cdf(x):
        return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))
    return round(max(0.0, min(1.0, cdf(bin_high) - cdf(bin_low))), 4)

def parse_temp_range(title: str) -> tuple:
    import re
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)", title)
    if m:
        return float(m.group(1)), float(m.group(2))
    m = re.search(r"[>≥]\s*(-?\d+(?:\.\d+)?)", title)
    if m:
        return float(m.group(1)), float(m.group(1)) + 25
    m = re.search(r"[<≤]\s*(-?\d+(?:\.\d+)?)", title)
    if m:
        return float(m.group(1)) - 25, float(m.group(1))
    return None, None

def select_bins(market: dict, forecast: dict) -> list:
    """
    Sélectionne les bins dont EV > forecast['ev_threshold'] (adaptatif).
    """
    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = []
    if not outcomes:
        return []

    if outcomes and isinstance(outcomes[0], str):
        prices_raw = market.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                prices_raw = json.loads(prices_raw)
            except Exception:
                prices_raw = []
        clob_ids = market.get("clobTokenIds", [])
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = []
        outcomes = [
            {
                "title":        outcomes[i],
                "price":        prices_raw[i] if i < len(prices_raw) else "0.5",
                "clobTokenIds": [clob_ids[i]] if i < len(clob_ids) else [""],
            }
            for i in range(len(outcomes))
        ]

    temp         = forecast["temp_max"]
    spread       = forecast["spread"]
    # Utilise le seuil adaptatif du forecast (8% ou 10%)
    ev_thresh    = forecast.get("ev_threshold", EV_THRESHOLD)
    bins_with_ev = []

    for outcome in outcomes:
        title             = outcome.get("title", "") if isinstance(outcome, dict) else str(outcome)
        bin_low, bin_high = parse_temp_range(title)
        if bin_low is None or bin_high is None:
            continue
        market_price = _safe_float(outcome.get("price", 0.5), 0.5)
        if market_price <= 0 or market_price >= 1:
            continue
        prob = temperature_to_prob(temp, bin_low, bin_high, spread)
        ev   = compute_ev(prob, market_price)
        clob = outcome.get("clobTokenIds", [""])
        bins_with_ev.append({
            "token_id":     clob[0] if clob else "",
            "title":        title,
            "bin_low":      bin_low,
            "bin_high":     bin_high,
            "prob":         prob,
            "market_price": market_price,
            "ev":           ev,
        })

    bins_with_ev.sort(key=lambda x: abs(((x["bin_low"] + x["bin_high"]) / 2) - temp))
    return [b for b in bins_with_ev if b["ev"] > ev_thresh][:MAX_BINS]

# ── Protections ────────────────────────────────────────────────────────────────
def check_daily_reset(state: dict) -> dict:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state["daily_reset"] != today:
        log.info(f"🌅 Nouveau jour {today} — Reset PnL (était {_fmt(state['daily_pnl'])} USDC)")
        state["daily_pnl"]   = 0.0
        state["daily_reset"] = today
    return state

def is_paused(state: dict) -> bool:
    if state.get("paused_until"):
        pause_end = datetime.fromisoformat(state["paused_until"])
        if datetime.utcnow() < pause_end:
            remaining = int((pause_end - datetime.utcnow()).total_seconds() // 60)
            log.info(f"⏸️  Bot en pause — encore {remaining} min")
            return True
        log.info("▶️  Pause terminée")
        state["paused_until"] = None
    return False

def check_daily_loss(state: dict, balance: float) -> bool:
    loss_pct = -state["daily_pnl"] / balance if balance > 0 else 0
    if loss_pct > DAILY_LOSS_LIMIT:
        pause_until = (datetime.utcnow() + timedelta(hours=24)).isoformat()
        state["paused_until"] = pause_until
        log.warning(
            f"🛡️  PROTECTION ACTIVÉE — Perte {loss_pct*100:.1f}% > "
            f"{DAILY_LOSS_LIMIT*100:.0f}% — Pause jusqu'à {pause_until[:16]}"
        )
        return True
    return False

def check_milestones(balance: float, state: dict):
    for m in [500, 1000, 1500, 2000]:
        label = str(m)
        if balance >= m and label not in state.get("milestones_hit", []):
            state.setdefault("milestones_hit", []).append(label)
            log.info(
                f"🎉 FÉLICITATIONS ! Capital ≥ {m} USDC "
                f"({_fmt(balance)} USDC) — La tirelire grossit 💰"
            )

# ── Redeem automatique ─────────────────────────────────────────────────────────
def auto_redeem(client: PolymarketClient, state: dict, cal: dict) -> float:
    total_redeemed = 0.0
    try:
        resolved = client.get_redeemable_positions()
        if not resolved:
            return 0.0
        log.info(f"💎 {len(resolved)} position(s) redeemable(s)")
        for pos in resolved:
            condition_id  = pos.get("conditionId", "")
            pnl           = _safe_float(pos.get("cashPnl") or pos.get("pnl"), 0.0)
            market_name   = pos.get("title") or pos.get("market", {}).get("question", "?")
            city_hint     = next(
                (c["name"] for c in CITIES
                 if any(alias in str(market_name).lower() for alias in c["aliases"])),
                "Unknown",
            )
            outcome_index = int(_safe_float(pos.get("outcomeIndex"), 0))
            if client.redeem_position(condition_id, outcome_index):
                total_redeemed += pnl
                update_calibration(city_hint, datetime.utcnow().month, cal, pnl > 0)
                append_trade({
                    "timestamp":     datetime.utcnow().isoformat(),
                    "market":        str(market_name)[:50],
                    "bins":          "REDEEM",
                    "amount":        0,
                    "entry_price":   "",
                    "exit_price":    "",
                    "pnl":           round(pnl, 4),
                    "balance_after": round(state["balance"] + total_redeemed, 2),
                    "ev":            "",
                    "notes":         f"Auto-redeem conditionId={condition_id[:12]}",
                })
                log.info(f"  ✅ Redeem {str(market_name)[:30]} → PnL {pnl:+.2f} USDC")
    except Exception as e:
        log.error(f"Erreur auto_redeem: {e}")
    return total_redeemed

# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    log.info("🌤️  Tirelire Météo Pension v1.7 démarrée — Ultra-safe mode")
    log.info(f"📂 Données : {DATA_DIR}")
    log.info(f"⏱️  Scan toutes les {SCAN_INTERVAL//60} min")
    log.info(
        f"🌡️  Sources : GFS + ECMWF (Open-Meteo)"
        + (" + NOAA API" if NOAA_API_TOKEN else " [NOAA désactivé]")
    )
    log.info(
        f"📈 EV seuils : {EV_THRESHOLD*100:.0f}% (défaut) "
        f"/ {EV_THRESHOLD_STRONG*100:.0f}% (3 sources STRONG)"
    )
    log.info(f"🏙️  Villes : {', '.join(c['name'] for c in CITIES)}")

    init_csv()
    state  = load_state()
    cal    = load_calibration()
    client = PolymarketClient()

    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n{'─'*60}")
        log.info(f"🔄 Cycle #{cycle} — Balance: {_fmt(state['balance'])} USDC")

        state = check_daily_reset(state)
        check_milestones(state["balance"], state)

        redeemed = auto_redeem(client, state, cal)
        if redeemed != 0:
            state["balance"]   += redeemed
            state["daily_pnl"] += redeemed
            log.info(
                f"💰 Redeemed: {redeemed:+.2f} USDC → "
                f"Balance: {_fmt(state['balance'])} USDC"
            )

        if is_paused(state):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue
        if check_daily_loss(state, state["balance"]):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue

        markets = client.get_weather_markets(target=500)
        log.info(f"📊 {len(markets)} marchés météo à analyser")

        trades_this_cycle = 0
        for market in markets:
            city = _market_city(market)
            if not city:
                continue

            try:
                forecast = build_consensus_forecast(city)
            except Exception as e:
                log.error(f"  💥 build_consensus_forecast({city['name']}): {e}")
                continue

            if not forecast:
                continue
            if forecast["consensus"] == "WEAK" or forecast["sources"] < 2:
                continue

            month         = datetime.utcnow().month
            conf_factor   = get_confidence_factor(city["name"], month, cal)

            try:
                selected_bins = select_bins(market, forecast)
            except Exception as e:
                log.error(f"  💥 select_bins: {e}")
                continue

            if len(selected_bins) < MIN_BINS:
                continue

            total_bet   = compute_bet_size(state["balance"])
            total_ev    = sum(b["ev"] for b in selected_bins)
            market_name = (
                market.get("question") or
                market.get("title") or
                market.get("slug", "?")
            )[:50]

            log.info(f"  🎯 {city['name']} | {market_name}")
            log.info(
                f"     Prévision: {_fmt(forecast['temp_max'])}°C "
                f"± {_fmt(forecast['spread'])}°C "
                f"[{forecast['consensus']}] seuil={forecast['ev_threshold']*100:.0f}%"
            )

            for b in selected_bins:
                if total_ev <= 0:
                    break
                bin_bet = round((b["ev"] / total_ev) * total_bet * conf_factor, 2)
                if bin_bet < 0.50:
                    continue
                result = client.place_order(
                    token_id=b["token_id"],
                    price=b["market_price"],
                    size=bin_bet,
                )
                if result:
                    trades_this_cycle   += 1
                    state["balance"]    -= bin_bet
                    state["daily_pnl"] -= bin_bet
                    append_trade({
                        "timestamp":     datetime.utcnow().isoformat(),
                        "market":        market_name,
                        "bins":          b["title"],
                        "amount":        bin_bet,
                        "entry_price":   b["market_price"],
                        "exit_price":    "",
                        "pnl":           "",
                        "balance_after": round(state["balance"], 2),
                        "ev":            b["ev"],
                        "notes":         (
                            f"Consensus={forecast['consensus']} "
                            f"Spread={_fmt(forecast['spread'])} "
                            f"Conf={conf_factor} "
                            f"EV_seuil={forecast['ev_threshold']*100:.0f}% "
                            f"Models={forecast['models']}"
                        ),
                    })
                    log.info(
                        f"     ✅ [{b['title']}] {bin_bet:.2f} USDC "
                        f"@ {b['market_price']:.3f} | EV={b['ev']:.1%}"
                    )

        log.info(f"✔️  Cycle #{cycle} terminé — {trades_this_cycle} ordres placés")
        save_state(state)
        log.info(f"😴 Prochaine vérification dans {SCAN_INTERVAL//60} min…")
        time.sleep(SCAN_INTERVAL)

# ── Entrée ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("👋 Bot arrêté manuellement.")
    except Exception as e:
        log.critical(f"💥 Erreur fatale : {e}", exc_info=True)
        raise
