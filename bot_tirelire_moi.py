# bot_tirelire_moi.py
# Tirelire Météo Pension - Bot Polymarket météo ultra-safe
# v1.9 — Rotation équitable des villes :
#         • max CITIES_PER_CYCLE villes par cycle
#         • rotation round-robin pondérée (priorité légère NYC/London/Chicago)
#         • 1 seul consensus météo par ville par cycle
#         • délai INTER_CITY_DELAY entre chaque ville

import os
import csv
import json
import time
import math
import logging
import requests
import urllib3
import random
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
ROTATION_JSON    = DATA_DIR / "rotation.json"   # état de la rotation inter-cycles

PRIVATE_KEY    = os.getenv("PRIVATE_KEY", "")
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN", "")
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

SCAN_INTERVAL       = int(os.getenv("SCAN_INTERVAL",        "900"))
EV_THRESHOLD        = float(os.getenv("EV_THRESHOLD",       "0.10"))  # 10% défaut
EV_THRESHOLD_STRONG = float(os.getenv("EV_THRESHOLD_STRONG","0.08"))  # 8% si 3 sources STRONG
DAILY_LOSS_LIMIT    = float(os.getenv("DAILY_LOSS_LIMIT",   "0.03"))
MIN_BINS            = int(os.getenv("MIN_BINS",   "5"))
MAX_BINS            = int(os.getenv("MAX_BINS",  "10"))

# Rotation : combien de villes traiter par cycle (8–10 max pour rester rapide)
CITIES_PER_CYCLE = int(os.getenv("CITIES_PER_CYCLE", "9"))
# Délai entre chaque ville (respecte Open-Meteo + lisibilité des logs)
INTER_CITY_DELAY = float(os.getenv("INTER_CITY_DELAY", "1.5"))

# Polymarket
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"

POLYGON_RPC  = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")
CTF_ADDRESS  = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
HASH_ZERO    = "0x" + "0" * 64

API_TIMEOUT_CONNECT = 10
API_TIMEOUT_READ    = 30
API_RETRY_DELAYS    = [1, 3, 8]
API_USER_AGENT      = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36 TirelireMeteoPension/1.9"
)

OM_TIMEOUT_CONNECT = 8
OM_TIMEOUT_READ    = 15
OM_MAX_RETRIES     = 3
OM_RETRY_DELAYS    = [1, 2, 4]

# ── Catalogue des villes — priorité 1..10 (10 = plus fort) ────────────────────
# La priorité influe sur la FRÉQUENCE de sélection, pas sur l'exclusivité.
CITIES = [
    {"name": "New York",    "lat": 40.71, "lon": -74.00, "priority": 10,
     "aliases": ["new york", "nyc", "ny ", "new-york", "manhattan"]},
    {"name": "Chicago",     "lat": 41.88, "lon": -87.63, "priority": 9,
     "aliases": ["chicago", "chi "]},
    {"name": "Los Angeles", "lat": 34.05, "lon": -118.24, "priority": 8,
     "aliases": ["los angeles", "la ", "l.a.", "los-angeles"]},
    {"name": "Miami",       "lat": 25.76, "lon":  -80.19, "priority": 8,
     "aliases": ["miami"]},
    {"name": "Dallas",      "lat": 32.78, "lon":  -96.80, "priority": 7,
     "aliases": ["dallas", "dfw"]},
    {"name": "Seattle",     "lat": 47.61, "lon": -122.33, "priority": 7,
     "aliases": ["seattle"]},
    {"name": "Boston",      "lat": 42.36, "lon":  -71.06, "priority": 7,
     "aliases": ["boston"]},
    {"name": "Washington",  "lat": 38.91, "lon":  -77.04, "priority": 7,
     "aliases": ["washington", "dc ", "d.c."]},
    {"name": "London",      "lat": 51.51, "lon":  -0.13, "priority": 6,
     "aliases": ["london", "uk temperature", "england weather"]},
    {"name": "Paris",       "lat": 48.85, "lon":   2.35, "priority": 5,
     "aliases": ["paris", "france temperature"]},
    {"name": "Berlin",      "lat": 52.52, "lon":  13.41, "priority": 4,
     "aliases": ["berlin", "germany temperature"]},
    {"name": "Lyon",        "lat": 45.75, "lon":   4.85, "priority": 3,
     "aliases": ["lyon"]},
    {"name": "Marseille",   "lat": 43.30, "lon":   5.37, "priority": 3,
     "aliases": ["marseille"]},
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
    v = lst[0] if isinstance(lst, (list, tuple)) else default
    return v if v is not None else default

def _fmt(value, digits: int = 1, fallback: str = "N/A") -> str:
    if value is None:
        return fallback
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return fallback

# ══════════════════════════════════════════════════════════════════════════════
# ── Rotation équitable des villes ────────────────────────────────────────────
#
# Algorithme :
#   Chaque ville a un "crédit" cumulatif = priority / somme_priorities.
#   À chaque cycle, on sélectionne les CITIES_PER_CYCLE villes dont
#   le crédit est le plus élevé, puis on décrémente leur crédit de 1.
#   Cela garantit que toutes les villes passent, avec une fréquence
#   proportionnelle à leur priorité — sans jamais bloquer les faibles.
#
# Persistance : rotation.json (survit aux redémarrages Railway)
# ══════════════════════════════════════════════════════════════════════════════
class CityRotation:
    """Rotation pondérée équitable des villes."""

    def __init__(self, cities: list[dict], per_cycle: int):
        self.cities    = cities
        self.per_cycle = per_cycle
        self._credits  = {c["name"]: 0.0 for c in cities}
        self._load()

    # ── Persistance ────────────────────────────────────────────────────────
    def _load(self):
        if ROTATION_JSON.exists():
            try:
                with open(ROTATION_JSON) as f:
                    saved = json.load(f)
                for name in self._credits:
                    if name in saved:
                        self._credits[name] = float(saved[name])
                log.info("  🔄 Rotation chargée depuis rotation.json")
                return
            except Exception:
                pass
        self._reset_credits()

    def _save(self):
        try:
            with open(ROTATION_JSON, "w") as f:
                json.dump(self._credits, f, indent=2)
        except Exception:
            pass

    def _reset_credits(self):
        total = sum(c["priority"] for c in self.cities)
        for c in self.cities:
            self._credits[c["name"]] = c["priority"] / total

    # ── Sélection du cycle ──────────────────────────────────────────────────
    def select(self, available_names: set[str]) -> list[dict]:
        """
        Sélectionne jusqu'à `per_cycle` villes parmi `available_names`
        (villes ayant des marchés actifs ce cycle).

        Étapes :
        1. Accumule les crédits (priority / total) pour toutes les villes.
        2. Parmi celles disponibles, prend les per_cycle avec le plus de crédit.
        3. Décrémente leur crédit de 1.0 (dette).
        4. Sauvegarde.
        """
        total = sum(c["priority"] for c in self.cities)

        # Accumulation des crédits
        for c in self.cities:
            self._credits[c["name"]] += c["priority"] / total

        # Candidats : villes disponibles triées par crédit décroissant,
        # avec un léger bruit aléatoire pour éviter les ex-æquo figés
        candidates = sorted(
            [c for c in self.cities if c["name"] in available_names],
            key=lambda c: self._credits[c["name"]] + random.uniform(0, 0.02),
            reverse=True,
        )

        selected = candidates[: self.per_cycle]

        # Débite les villes sélectionnées
        for c in selected:
            self._credits[c["name"]] -= 1.0

        self._save()

        names = [c["name"] for c in selected]
        log.info(
            f"  🔀 Rotation cycle : {', '.join(names)} "
            f"({len(selected)}/{len(available_names)} dispo)"
        )
        return selected

# ══════════════════════════════════════════════════════════════════════════════
# ── Session Open-Meteo — retry + SSL fallback ─────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _build_om_session(verify_ssl: bool = True) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=OM_MAX_RETRIES,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = API_USER_AGENT
    s.verify = verify_ssl
    return s

_om_ssl   = _build_om_session(verify_ssl=True)
_om_nossl = _build_om_session(verify_ssl=False)

def _om_get(params: dict) -> dict | None:
    """GET Open-Meteo avec retry urllib3 + fallback no-SSL silencieux."""
    for attempt in range(1, OM_MAX_RETRIES + 1):
        for session in (_om_ssl, _om_nossl):
            try:
                r     = session.get(OPEN_METEO_URL, params=params,
                                    timeout=(OM_TIMEOUT_CONNECT, OM_TIMEOUT_READ))
                r.raise_for_status()
                data  = r.json()
                daily = data.get("daily", {})
                temps = daily.get("temperature_2m_max") or []
                if temps and temps[0] is not None:
                    return data
            except requests.exceptions.SSLError:
                continue   # bascule sur no-SSL
            except (requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError):
                break      # retry dans la boucle extérieure
            except Exception:
                return None
        if attempt < OM_MAX_RETRIES:
            time.sleep(OM_RETRY_DELAYS[min(attempt - 1, len(OM_RETRY_DELAYS) - 1)])
    return None

# ── safe_api_call — Polymarket ─────────────────────────────────────────────────
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
                log.warning(f"  ⚠️  [{label}] JSON invalide — tentative {attempt}/{max_attempts}")
                continue
        except requests.exceptions.ConnectTimeout:
            log.warning(f"  ⏱️  [{label}] ConnectTimeout — tentative {attempt}/{max_attempts}")
        except requests.exceptions.ReadTimeout:
            log.warning(f"  ⏱️  [{label}] ReadTimeout — tentative {attempt}/{max_attempts}")
        except requests.exceptions.ConnectionError as e:
            log.warning(f"  🔌 [{label}] ConnectionError: {e} — tentative {attempt}/{max_attempts}")
        except Exception as e:
            log.error(f"  💥 [{label}] Erreur: {e}")
            break

    log.error(f"  ❌ [{label}] Abandon après {max_attempts} tentatives.")
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
# ── APIs Météo : GFS + ECMWF (Open-Meteo) + NOAA optionnel ───────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _extract_daily(data: dict, source: str) -> dict | None:
    daily      = data.get("daily", {})
    temp_max_v = _safe_first(daily.get("temperature_2m_max"))
    temp_min_v = _safe_first(daily.get("temperature_2m_min"))
    precip_v   = _safe_first(daily.get("precipitation_sum"), default=0.0)
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
    data = _om_get({
        "latitude": lat, "longitude": lon,
        "daily":    "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "forecast_days": 7, "timezone": "UTC", "models": "gfs_seamless",
    })
    return _extract_daily(data, "GFS") if data else None

def fetch_ecmwf(lat: float, lon: float) -> dict | None:
    for model in ("ecmwf_ifs04", "ecmwf_ifs025"):
        data = _om_get({
            "latitude": lat, "longitude": lon,
            "daily":    "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "forecast_days": 7, "timezone": "UTC", "models": model,
        })
        if data:
            r = _extract_daily(data, "ECMWF")
            if r:
                return r
    return None

def fetch_noaa_api(lat: float, lon: float) -> dict | None:
    if not NOAA_API_TOKEN:
        return None
    try:
        hdrs = {"User-Agent": "TirelireMeteoPension/1.9", "token": NOAA_API_TOKEN}
        r = requests.get(f"https://api.weather.gov/points/{lat},{lon}",
                         headers=hdrs, timeout=(8, 15))
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
        day = next((p for p in periods[:4] if p.get("isDaytime", True)), periods[0])
        tf  = _safe_float(day.get("temperature"), default=None)
        if tf is None:
            return None
        return {"source": "NOAA_API", "temp_max": round((tf - 32) * 5 / 9, 1),
                "temp_min": None, "precip_mm": None}
    except Exception:
        return None

def build_consensus_forecast(city: dict) -> dict | None:
    """
    Agrège GFS + ECMWF + NOAA.
    Seuil EV adaptatif : 8% si 3 sources STRONG, 10% sinon.
    Retourne None si < 2 sources valides.
    """
    lat, lon  = city["lat"], city["lon"]
    forecasts = []

    for fn, name in [(fetch_gfs, "GFS"), (fetch_ecmwf, "ECMWF"),
                     (fetch_noaa_api, "NOAA_API")]:
        try:
            r = fn(lat, lon)
        except Exception:
            r = None
        if r is None:
            continue
        tm = r.get("temp_max")
        if not isinstance(tm, (int, float)) or math.isnan(tm):
            continue
        r["temp_max"] = float(tm)
        forecasts.append(r)

    if len(forecasts) < 2:
        return None

    temps    = [f["temp_max"] for f in forecasts]
    avg_temp = sum(temps) / len(temps)
    spread   = max(temps) - min(temps)

    consensus    = "STRONG" if spread < 1.0 else "MODERATE" if spread < 2.5 else "WEAK"
    ev_threshold = (
        EV_THRESHOLD_STRONG if consensus == "STRONG" and len(forecasts) >= 3
        else EV_THRESHOLD
    )

    precips    = [f["precip_mm"] for f in forecasts if f.get("precip_mm") is not None]
    avg_precip = round(sum(precips) / len(precips), 1) if precips else 0.0
    models     = [f["source"] for f in forecasts]

    log.info(
        f"    🌡️  {city['name']} {_fmt(avg_temp)}°C ± {_fmt(spread)}°C "
        f"[{consensus}] {models} EV≥{ev_threshold*100:.0f}%"
    )

    return {
        "city":         city["name"],
        "temp_max":     round(avg_temp, 1),
        "precip_mm":    avg_precip,
        "spread":       round(spread, 2),
        "consensus":    consensus,
        "sources":      len(forecasts),
        "models":       models,
        "ev_threshold": ev_threshold,
    }

# ══════════════════════════════════════════════════════════════════════════════
# ── Helpers filtre marchés météo ──────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
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
    text = _market_search_text(market)
    slug = market.get("slug", "").lower()
    return (any(k in text for k in WEATHER_KEYWORDS) or
            any(k in slug for k in WEATHER_SLUG_KEYWORDS))

def _market_city(market: dict) -> dict | None:
    text = _market_search_text(market)
    for city in CITIES:
        if any(alias in text for alias in city["aliases"]):
            return city
    return None

def _market_priority(market: dict) -> tuple:
    city  = _market_city(market)
    prio  = city["priority"] if city else 0
    vol24 = _safe_float(market.get("volume24hr") or market.get("volume_24hr"))
    return (prio, vol24)

def _extract_markets_from_events(events: list) -> list:
    markets = []
    for event in events:
        tags  = event.get("tags",  [])
        title = event.get("title", "")
        slug  = event.get("slug",  "")
        for m in event.get("markets", []):
            m.setdefault("tags", tags)
            if not m.get("groupItemTitle"):
                m["groupItemTitle"] = title
            if not m.get("eventSlug"):
                m["eventSlug"] = slug
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
                log.warning("⚠️  Polygon RPC non connecté")
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
                    log.info(f"  📎 Tag weather cache id={self._weather_tag_id}")
                    return self._weather_tag_id
            except Exception:
                pass
        log.info("  🔍 Découverte tag_id 'weather'…")
        for offset in [0, 100]:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/tags",
                label="gamma/tags",
                params={"limit": 100, "offset": offset},
            )
            if not data:
                continue
            tags = data if isinstance(data, list) else data.get("tags", [])
            for tag in tags:
                lbl = (tag.get("label", "") + " " + tag.get("slug", "")).lower()
                if "weather" in lbl:
                    tid = int(tag.get("id", 0))
                    if tid:
                        log.info(f"  ✅ Tag weather id={tid} '{tag.get('label')}'")
                        self._weather_tag_id = tid
                        self._tag_cache_ttl  = now + 86400
                        try:
                            with open(TAG_CACHE_JSON, "w") as f:
                                json.dump({"tag_id": tid, "expires": self._tag_cache_ttl}, f)
                        except Exception:
                            pass
                        return tid
        log.warning("  ⚠️  Tag 'weather' introuvable")
        return None

    def _paginate_events(self, params: dict, label: str,
                         max_markets: int = 300) -> list:
        all_markets = []
        offset      = 0
        page_size   = params.get("limit", 50)
        while len(all_markets) < max_markets:
            p = {**params, "offset": offset}
            data = safe_api_call(self.session, "GET",
                                 f"{GAMMA_BASE}/events", label=label, params=p)
            if not data:
                break
            events = data if isinstance(data, list) else data.get("data", [])
            if not events:
                break
            all_markets.extend(_extract_markets_from_events(events))
            has_more = (data.get("has_more", False) if isinstance(data, dict)
                        else len(events) == page_size)
            if not has_more:
                break
            offset += page_size
        return all_markets

    def get_weather_markets(self, target: int = 400) -> list:
        log.info(f"🌦️  Récupération marchés météo (target={target})…")
        all_markets: list = []
        seen_ids:    set  = set()

        def _add(markets: list):
            for m in markets:
                mid = m.get("conditionId") or m.get("id") or m.get("slug")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(m)

        # Couche 1 : tag_id weather
        tag_id = self._discover_weather_tag_id()
        if tag_id:
            _add(self._paginate_events(
                {"tag_id": tag_id, "related_tags": "true", "active": "true",
                 "closed": "false", "order": "volume24hr", "ascending": "false",
                 "limit": 50},
                label="gamma/L1_tag", max_markets=target,
            ))

        # Couche 2 : filtre textuel sur tous les events actifs
        if len(all_markets) < target:
            batch = self._paginate_events(
                {"active": "true", "closed": "false",
                 "order": "volume24hr", "ascending": "false", "limit": 100},
                label="gamma/L2_all", max_markets=2000,
            )
            before = len(all_markets)
            _add([m for m in batch if _is_weather_market(m)])
            log.info(f"  L2 +{len(all_markets)-before} → {len(all_markets)} total")

        # Couche 3 : /markets direct (fallback)
        if len(all_markets) == 0:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/markets",
                label="gamma/L3_direct",
                params={"active": "true", "closed": "false",
                        "order": "volume24hr", "ascending": "false", "limit": 100},
            )
            if data:
                mkts = data if isinstance(data, list) else data.get("data", [])
                _add([m for m in mkts if _is_weather_market(m)])

        all_markets.sort(key=_market_priority, reverse=True)
        city_matches = sum(1 for m in all_markets if _market_city(m))
        log.info(f"✅ {len(all_markets)} marchés météo ({city_matches} avec ville connue)")
        return all_markets[:target]

    def place_order(self, token_id: str, price: float,
                    size: float, side: str = "BUY") -> dict | None:
        if not self.account:
            log.info(f"  [SIM] {side} {size:.2f}$ @ {price:.3f} {token_id[:10]}…")
            return {"status": "simulated", "token_id": token_id,
                    "price": price, "size": size}
        try:
            order = {
                "orderType": "GTC", "tokenID": token_id, "side": side,
                "price": str(round(price, 4)), "size": str(round(size, 2)),
                "funder": self.address, "maker": self.address, "expiration": "0",
            }
            log.info(f"  [ORDRE] {side} {size:.2f}$ @ {price:.3f}")
            return order
        except Exception as e:
            log.error(f"Erreur place_order: {e}")
            return None

    def get_redeemable_positions(self) -> list:
        if not self.address:
            return []
        all_pos = safe_api_call(
            self.session, "GET", f"{DATA_BASE}/positions",
            label="data/positions",
            params={"user": self.address, "sizeThreshold": "0"},
        )
        if all_pos is not None:
            positions = all_pos if isinstance(all_pos, list) else all_pos.get("data", [])
            return [p for p in positions
                    if p.get("redeemable") is True and _safe_float(p.get("size")) > 0]
        activity = safe_api_call(
            self.session, "GET", f"{DATA_BASE}/activity",
            label="data/activity",
            params={"user": self.address, "type": "REDEEM", "limit": "50"},
        )
        if not activity:
            return []
        acts = activity if isinstance(activity, list) else activity.get("data", [])
        return [{"conditionId": a.get("conditionId", ""), "redeemable": True,
                 "cashPnl": _safe_float(a.get("usdcSize")),
                 "title": a.get("title", "?"),
                 "market": {"question": a.get("title", "?")}} for a in acts]

    def redeem_position(self, condition_id: str, outcome_index: int = 0) -> bool:
        if not self.account:
            log.info(f"  [SIM] Redeem {condition_id[:12]}…")
            return True
        if not self.w3:
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
                address=self.w3.to_checksum_address(CTF_ADDRESS), abi=ctf_abi)
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
            ok = receipt.status == 1
            log.info(f"  {'✅' if ok else '❌'} Redeem {condition_id[:12]} tx={tx_hash.hex()[:16]}…")
            return ok
        except Exception as e:
            log.error(f"Erreur redeem {condition_id[:12]}: {e}")
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
            {"title":        outcomes[i],
             "price":        prices_raw[i] if i < len(prices_raw) else "0.5",
             "clobTokenIds": [clob_ids[i]] if i < len(clob_ids) else [""]}
            for i in range(len(outcomes))
        ]

    temp      = forecast["temp_max"]
    spread    = forecast["spread"]
    ev_thresh = forecast.get("ev_threshold", EV_THRESHOLD)
    result    = []

    for outcome in outcomes:
        title         = outcome.get("title", "") if isinstance(outcome, dict) else str(outcome)
        bl, bh        = parse_temp_range(title)
        if bl is None or bh is None:
            continue
        mp = _safe_float(outcome.get("price", 0.5), 0.5)
        if mp <= 0 or mp >= 1:
            continue
        prob = temperature_to_prob(temp, bl, bh, spread)
        ev   = compute_ev(prob, mp)
        clob = outcome.get("clobTokenIds", [""])
        result.append({
            "token_id":     clob[0] if clob else "",
            "title":        title,
            "bin_low":      bl, "bin_high": bh,
            "prob":         prob, "market_price": mp, "ev": ev,
        })

    result.sort(key=lambda x: abs(((x["bin_low"] + x["bin_high"]) / 2) - temp))
    return [b for b in result if b["ev"] > ev_thresh][:MAX_BINS]

# ── Protections ────────────────────────────────────────────────────────────────
def check_daily_reset(state: dict) -> dict:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state["daily_reset"] != today:
        log.info(f"🌅 Nouveau jour {today} — Reset PnL ({_fmt(state['daily_pnl'])} USDC)")
        state["daily_pnl"]   = 0.0
        state["daily_reset"] = today
    return state

def is_paused(state: dict) -> bool:
    if state.get("paused_until"):
        end = datetime.fromisoformat(state["paused_until"])
        if datetime.utcnow() < end:
            log.info(f"⏸️  Pause — encore {int((end-datetime.utcnow()).total_seconds()//60)} min")
            return True
        log.info("▶️  Pause terminée")
        state["paused_until"] = None
    return False

def check_daily_loss(state: dict, balance: float) -> bool:
    loss_pct = -state["daily_pnl"] / balance if balance > 0 else 0
    if loss_pct > DAILY_LOSS_LIMIT:
        pu = (datetime.utcnow() + timedelta(hours=24)).isoformat()
        state["paused_until"] = pu
        log.warning(
            f"🛡️  PROTECTION ACTIVÉE — Perte {loss_pct*100:.1f}% > "
            f"{DAILY_LOSS_LIMIT*100:.0f}% — Pause jusqu'à {pu[:16]}"
        )
        return True
    return False

def check_milestones(balance: float, state: dict):
    for m in [500, 1000, 1500, 2000]:
        lbl = str(m)
        if balance >= m and lbl not in state.get("milestones_hit", []):
            state.setdefault("milestones_hit", []).append(lbl)
            log.info(f"🎉 Capital ≥ {m} USDC ({_fmt(balance)}) — La tirelire grossit 💰")

# ── Redeem automatique ─────────────────────────────────────────────────────────
def auto_redeem(client: PolymarketClient, state: dict, cal: dict) -> float:
    total = 0.0
    try:
        resolved = client.get_redeemable_positions()
        if not resolved:
            return 0.0
        log.info(f"💎 {len(resolved)} position(s) redeemable(s)")
        for pos in resolved:
            cid   = pos.get("conditionId", "")
            pnl   = _safe_float(pos.get("cashPnl") or pos.get("pnl"))
            mname = pos.get("title") or pos.get("market", {}).get("question", "?")
            city_hint = next(
                (c["name"] for c in CITIES
                 if any(a in str(mname).lower() for a in c["aliases"])),
                "Unknown",
            )
            oi = int(_safe_float(pos.get("outcomeIndex")))
            if client.redeem_position(cid, oi):
                total += pnl
                update_calibration(city_hint, datetime.utcnow().month, cal, pnl > 0)
                append_trade({
                    "timestamp":     datetime.utcnow().isoformat(),
                    "market":        str(mname)[:50],
                    "bins":          "REDEEM",
                    "amount":        0,
                    "entry_price":   "",
                    "exit_price":    "",
                    "pnl":           round(pnl, 4),
                    "balance_after": round(state["balance"] + total, 2),
                    "ev":            "",
                    "notes":         f"Auto-redeem {cid[:12]}",
                })
                log.info(f"  ✅ Redeem {str(mname)[:30]} PnL={pnl:+.2f}$")
    except Exception as e:
        log.error(f"Erreur auto_redeem: {e}")
    return total

# ── Traitement d'une ville ─────────────────────────────────────────────────────
def process_city(city: dict, forecast: dict, city_markets: list,
                 client: PolymarketClient, state: dict, cal: dict) -> int:
    """
    1 forecast pré-calculé × N marchés de la ville.
    Retourne le nombre d'ordres placés.
    """
    trades = 0
    month  = datetime.utcnow().month
    conf   = get_confidence_factor(city["name"], month, cal)

    for market in city_markets:
        try:
            bins = select_bins(market, forecast)
        except Exception as e:
            log.error(f"    select_bins: {e}")
            continue
        if len(bins) < MIN_BINS:
            continue

        total_bet = compute_bet_size(state["balance"])
        total_ev  = sum(b["ev"] for b in bins)
        mname     = (market.get("question") or market.get("title")
                     or market.get("slug", "?"))[:50]
        log.info(f"    🎯 {mname}")

        for b in bins:
            if total_ev <= 0:
                break
            bb = round((b["ev"] / total_ev) * total_bet * conf, 2)
            if bb < 0.50:
                continue
            result = client.place_order(b["token_id"], b["market_price"], bb)
            if result:
                trades             += 1
                state["balance"]   -= bb
                state["daily_pnl"] -= bb
                append_trade({
                    "timestamp":     datetime.utcnow().isoformat(),
                    "market":        mname,
                    "bins":          b["title"],
                    "amount":        bb,
                    "entry_price":   b["market_price"],
                    "exit_price":    "",
                    "pnl":           "",
                    "balance_after": round(state["balance"], 2),
                    "ev":            b["ev"],
                    "notes": (
                        f"Consensus={forecast['consensus']} "
                        f"Spread={_fmt(forecast['spread'])} "
                        f"Conf={conf} EV≥{forecast['ev_threshold']*100:.0f}% "
                        f"Models={forecast['models']}"
                    ),
                })
                log.info(
                    f"      ✅ [{b['title']}] {bb:.2f}$ @ {b['market_price']:.3f} "
                    f"EV={b['ev']:.1%}"
                )
    return trades

# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    log.info("🌤️  Tirelire Météo Pension v1.9 démarrée")
    log.info(f"📂 {DATA_DIR} | ⏱️  {SCAN_INTERVAL//60} min/cycle")
    log.info(
        f"🌡️  GFS + ECMWF"
        + (" + NOAA" if NOAA_API_TOKEN else " [NOAA off]")
    )
    log.info(
        f"📈 EV {EV_THRESHOLD*100:.0f}% / {EV_THRESHOLD_STRONG*100:.0f}% (3src STRONG) | "
        f"🏙️  {CITIES_PER_CYCLE} villes/cycle | ⏳ {INTER_CITY_DELAY}s inter-ville"
    )

    init_csv()
    state    = load_state()
    cal      = load_calibration()
    client   = PolymarketClient()
    rotation = CityRotation(CITIES, per_cycle=CITIES_PER_CYCLE)

    cycle = 0
    while True:
        cycle      += 1
        cycle_start = time.time()
        log.info(f"\n{'═'*60}")
        log.info(f"🔄 Cycle #{cycle} — Balance: {_fmt(state['balance'])} USDC")

        state = check_daily_reset(state)
        check_milestones(state["balance"], state)

        redeemed = auto_redeem(client, state, cal)
        if redeemed:
            state["balance"]   += redeemed
            state["daily_pnl"] += redeemed
            log.info(f"💰 +{redeemed:.2f}$ redeemed → {_fmt(state['balance'])} USDC")

        if is_paused(state):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue
        if check_daily_loss(state, state["balance"]):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue

        # ── Récupération marchés ────────────────────────────────────────────
        markets = client.get_weather_markets(target=400)
        log.info(f"📊 {len(markets)} marchés météo récupérés")

        # ── Regroupement par ville ──────────────────────────────────────────
        markets_by_city: dict[str, list] = {}
        for m in markets:
            city = _market_city(m)
            if city:
                markets_by_city.setdefault(city["name"], []).append(m)

        available = set(markets_by_city.keys())
        if available:
            log.info(
                "📍 Villes disponibles : "
                + ", ".join(f"{n}({len(markets_by_city[n])})" for n in sorted(available))
            )
        else:
            log.warning("  ⚠️  Aucune ville reconnue dans les marchés ce cycle")
            save_state(state)
            time.sleep(max(10, SCAN_INTERVAL - (time.time() - cycle_start)))
            continue

        # ── Sélection des villes par rotation équitable ────────────────────
        selected_cities = rotation.select(available)

        # ── Analyse et trading ville par ville ─────────────────────────────
        trades_total    = 0
        cities_ok       = 0
        cities_skipped  = 0

        for i, city in enumerate(selected_cities, start=1):
            cname        = city["name"]
            city_markets = markets_by_city.get(cname, [])

            log.info(
                f"\n  [{i}/{len(selected_cities)}] 🏙️  {cname} "
                f"— {len(city_markets)} marché(s)"
            )

            # 1 seul appel consensus météo par ville par cycle
            try:
                forecast = build_consensus_forecast(city)
            except Exception as e:
                log.error(f"    forecast error: {e}")
                forecast = None

            if forecast is None:
                log.info(f"    ⏭️  {cname} : sources météo insuffisantes")
                cities_skipped += 1
            elif forecast["consensus"] == "WEAK":
                log.info(
                    f"    ⏭️  {cname} : consensus WEAK "
                    f"(spread={_fmt(forecast['spread'])}°C)"
                )
                cities_skipped += 1
            elif forecast["sources"] < 2:
                log.info(f"    ⏭️  {cname} : {forecast['sources']} source(s) — skip")
                cities_skipped += 1
            else:
                n = process_city(city, forecast, city_markets, client, state, cal)
                trades_total  += n
                cities_ok     += 1
                log.info(f"    → {n} ordre(s) | Balance: {_fmt(state['balance'])} USDC")
                if n > 0:
                    save_state(state)

            # Délai entre villes (sauf après la dernière)
            if i < len(selected_cities):
                time.sleep(INTER_CITY_DELAY)

        # ── Résumé cycle ────────────────────────────────────────────────────
        elapsed = time.time() - cycle_start
        log.info(f"\n{'─'*60}")
        log.info(
            f"✔️  Cycle #{cycle} | {elapsed:.0f}s | "
            f"{trades_total} ordres | "
            f"{cities_ok} villes OK | {cities_skipped} skippées"
        )
        save_state(state)

        next_wait = max(10, SCAN_INTERVAL - elapsed)
        log.info(f"😴 Prochain cycle dans {next_wait:.0f}s…")
        time.sleep(next_wait)

# ── Entrée ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("👋 Bot arrêté manuellement.")
    except Exception as e:
        log.critical(f"💥 Erreur fatale : {e}", exc_info=True)
        raise
