# bot_tirelire_moi.py
# Tirelire Météo Pension - Bot Polymarket météo ultra-safe
# v1.4 — Découverte marchés météo robuste :
#   Couche 1 : /tags → découverte automatique du tag_id "weather"
#   Couche 2 : /events?tag_id=...  (endpoint recommandé, events → markets)
#   Couche 3 : /events?active=true + filtre textuel météo sur slug/title
#   Couche 4 : /markets?active=true + filtre textuel (fallback ultime)
#   Tri final : volume 24h décroissant, villes prioritaires en tête

import os
import csv
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter

# ── Configuration ──────────────────────────────────────────────────────────────
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
TRADES_CSV       = DATA_DIR / "trades.csv"
CALIBRATION_JSON = DATA_DIR / "calibration.json"
STATE_JSON       = DATA_DIR / "state.json"
TAG_CACHE_JSON   = DATA_DIR / "tag_cache.json"   # cache du tag_id weather

PRIVATE_KEY         = os.getenv("PRIVATE_KEY", "")
OPEN_METEO_URL      = "https://api.open-meteo.com/v1/forecast"
NOAA_API_TOKEN      = os.getenv("NOAA_API_TOKEN", "")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")

SCAN_INTERVAL    = int(os.getenv("SCAN_INTERVAL",     "900"))
EV_THRESHOLD     = float(os.getenv("EV_THRESHOLD",    "0.10"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT","0.03"))
MIN_BINS         = int(os.getenv("MIN_BINS", "5"))
MAX_BINS         = int(os.getenv("MAX_BINS", "10"))

# ── URLs Polymarket (mars 2026) ────────────────────────────────────────────────
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"

# Polygon / CTF
POLYGON_RPC  = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")
CTF_ADDRESS  = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
HASH_ZERO    = "0x" + "0" * 64

# ── Robustesse API ─────────────────────────────────────────────────────────────
API_TIMEOUT_CONNECT = 10
API_TIMEOUT_READ    = 30
API_RETRY_DELAYS    = [1, 3, 8]
API_USER_AGENT      = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36 TirelireMeteoPension/1.4"
)

# ── Villes surveillées (lat/lon + variantes de noms pour le filtre textuel) ───
CITIES = [
    {"name": "New York",     "lat": 40.71, "lon": -74.00,
     "aliases": ["new york", "nyc", "ny ", "new-york", "manhattan"]},
    {"name": "London",       "lat": 51.51, "lon":  -0.13,
     "aliases": ["london", "uk temperature", "england weather"]},
    {"name": "Chicago",      "lat": 41.88, "lon": -87.63,
     "aliases": ["chicago", "chi "]},
    {"name": "Paris",        "lat": 48.85, "lon":   2.35,
     "aliases": ["paris", "france temperature"]},
    {"name": "Los Angeles",  "lat": 34.05, "lon": -118.24,
     "aliases": ["los angeles", "la ", "l.a.", "los-angeles"]},
    {"name": "Miami",        "lat": 25.76, "lon":  -80.19,
     "aliases": ["miami"]},
    {"name": "Dallas",       "lat": 32.78, "lon":  -96.80,
     "aliases": ["dallas", "dfw"]},
    {"name": "Seattle",      "lat": 47.61, "lon": -122.33,
     "aliases": ["seattle"]},
    {"name": "Boston",       "lat": 42.36, "lon":  -71.06,
     "aliases": ["boston"]},
    {"name": "Washington",   "lat": 38.91, "lon":  -77.04,
     "aliases": ["washington", "dc ", "d.c."]},
    {"name": "Lyon",         "lat": 45.75, "lon":   4.85,
     "aliases": ["lyon"]},
    {"name": "Marseille",    "lat": 43.30, "lon":   5.37,
     "aliases": ["marseille"]},
    {"name": "Berlin",       "lat": 52.52, "lon":  13.41,
     "aliases": ["berlin", "germany temperature"]},
]

# Mots-clés météo pour le filtre textuel (slug + question)
WEATHER_KEYWORDS = [
    "temperature", "temp", "weather", "degrees", "celsius", "fahrenheit",
    "°f", "°c", "precipitation", "rainfall", "rain", "snow", "snowfall",
    "humidity", "heatwave", "heat wave", "frost", "freeze", "cold",
    "warm", "hot", "record high", "record low", "high temperature",
    "low temperature", "daily high", "daily low", "forecast",
]

# Mots-clés dans les slugs (format kebab-case)
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

# ══════════════════════════════════════════════════════════════════════════════
# ── safe_api_call ─────────────────────────────────────────────────────────────
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
                log.warning(f"  🚫 [{label}] HTTP 404 — endpoint introuvable : {url}")
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
                log.warning(f"  ⚠️  [{label}] JSON invalide — «{preview}» — tentative {attempt}/{max_attempts}")
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

    log.error(f"  ❌ [{label}] API Polymarket unavailable — abandon après {max_attempts} tentatives.")
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

# ── APIs Météo ─────────────────────────────────────────────────────────────────
def fetch_open_meteo(lat: float, lon: float, days: int = 7) -> dict | None:
    try:
        r = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode",
                "forecast_days": days, "timezone": "UTC",
            },
            timeout=(10, 20),
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Open-Meteo erreur: {e}")
        return None

def fetch_noaa_forecast(lat: float, lon: float) -> dict | None:
    if not NOAA_API_TOKEN:
        return None
    try:
        hdrs = {"User-Agent": "TirelireMeteoPension/1.4", "token": NOAA_API_TOKEN}
        r = requests.get(f"https://api.weather.gov/points/{lat},{lon}",
                         headers=hdrs, timeout=(10, 20))
        if r.status_code != 200:
            return None
        r2 = requests.get(r.json()["properties"]["forecast"],
                          headers=hdrs, timeout=(10, 20))
        r2.raise_for_status()
        return r2.json()
    except Exception as e:
        log.warning(f"NOAA erreur: {e}")
        return None

def fetch_openweather(lat: float, lon: float) -> dict | None:
    if not OPENWEATHER_API_KEY:
        return None
    try:
        r = requests.get(
            "https://api.openweathermap.org/data/2.5/forecast",
            params={"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY,
                    "units": "metric", "cnt": 40},
            timeout=(10, 20),
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"OpenWeather erreur: {e}")
        return None

def build_consensus_forecast(city: dict) -> dict | None:
    lat, lon  = city["lat"], city["lon"]
    forecasts = []
    om = fetch_open_meteo(lat, lon)
    if om and "daily" in om:
        temps = om["daily"].get("temperature_2m_max", [])
        if temps:
            forecasts.append({
                "source": "OpenMeteo", "temp_max": temps[0],
                "precip": om["daily"].get("precipitation_sum", [0])[0],
            })
    noaa = fetch_noaa_forecast(lat, lon)
    if noaa and "properties" in noaa:
        periods = noaa["properties"].get("periods", [])
        if periods and periods[0].get("temperature"):
            forecasts.append({
                "source": "NOAA",
                "temp_max": (periods[0]["temperature"] - 32) * 5 / 9,
                "precip": None,
            })
    ow = fetch_openweather(lat, lon)
    if ow and "list" in ow:
        tomorrow = [x for x in ow["list"] if "12:00" in x.get("dt_txt", "")]
        if tomorrow:
            forecasts.append({
                "source": "OpenWeather",
                "temp_max": tomorrow[0]["main"]["temp_max"],
                "precip": tomorrow[0].get("rain", {}).get("3h", 0),
            })
    if not forecasts:
        return None
    temps     = [f["temp_max"] for f in forecasts]
    avg_temp  = sum(temps) / len(temps)
    spread    = max(temps) - min(temps)
    consensus = "STRONG" if spread < 1.0 else ("MODERATE" if spread < 2.5 else "WEAK")
    precips   = [f["precip"] for f in forecasts if f["precip"] is not None]
    return {
        "city":      city["name"],
        "temp_max":  round(avg_temp, 1),
        "precip_mm": round(sum(precips) / len(precips), 1) if precips else 0.0,
        "spread":    round(spread, 2),
        "consensus": consensus,
        "sources":   len(forecasts),
    }

# ══════════════════════════════════════════════════════════════════════════════
# ── Helpers filtre météo ──────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
def _text_is_weather(text: str) -> bool:
    """Retourne True si le texte contient un mot-clé météo."""
    t = text.lower()
    return any(k in t for k in WEATHER_KEYWORDS)

def _slug_is_weather(slug: str) -> bool:
    """Retourne True si le slug contient un mot-clé météo (format kebab)."""
    s = slug.lower()
    return any(k in s for k in WEATHER_SLUG_KEYWORDS)

def _market_search_text(market: dict) -> str:
    """Agrège tous les champs textuels d'un market pour le filtrage."""
    return " ".join([
        market.get("question", ""),
        market.get("slug", ""),
        market.get("title", ""),
        market.get("groupItemTitle", ""),
        market.get("description", ""),
        # Tags sous forme de labels
        " ".join(
            t.get("label", "") + " " + t.get("slug", "")
            for t in market.get("tags", [])
        ),
    ]).lower()

def _is_weather_market(market: dict) -> bool:
    """Filtre complet : mot-clé météo dans texte OU slug."""
    text = _market_search_text(market)
    slug = market.get("slug", "")
    return _text_is_weather(text) or _slug_is_weather(slug)

def _market_city(market: dict) -> dict | None:
    """Associe un market à une ville surveillée (alias inclus)."""
    text = _market_search_text(market)
    for city in CITIES:
        if any(alias in text for alias in city["aliases"]):
            return city
    return None

def _market_priority(market: dict) -> tuple:
    """
    Score de priorité pour le tri final.
    Retourne (ville_connue: 0/1, volume_24h: float) → tri décroissant.
    """
    city_match   = 1 if _market_city(market) else 0
    vol24        = float(market.get("volume24hr", market.get("volume_24hr", 0)) or 0)
    return (city_match, vol24)

def _extract_markets_from_events(events: list) -> list:
    """
    Extrait les markets depuis une liste d'events Gamma.
    Chaque event contient un champ "markets" (liste).
    Injecte les tags de l'event dans chaque market pour le filtrage.
    """
    markets = []
    for event in events:
        event_tags  = event.get("tags", [])
        event_title = event.get("title", "")
        event_slug  = event.get("slug", "")
        for m in event.get("markets", []):
            # Enrichit le market avec les métadonnées de l'event parent
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
        self.session      = build_session()
        self._weather_tag_id = None      # découvert dynamiquement
        self._tag_cache_ttl = 0          # timestamp d'expiration cache
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
                log.warning("⚠️  Polygon RPC non connecté — redeem on-chain désactivé")
                self.w3 = None
        except ImportError:
            log.warning("web3 non installé — redeem on-chain désactivé")
            self.w3 = None

    # ── Découverte du tag_id weather ─────────────────────────────────────────
    def _discover_weather_tag_id(self) -> int | None:
        """
        Interroge /tags pour trouver le tag_id correspondant à "weather".
        Résultat mis en cache 24h dans TAG_CACHE_JSON (économie d'appels API).
        """
        now = time.time()

        # Cache mémoire valide ?
        if self._weather_tag_id and now < self._tag_cache_ttl:
            return self._weather_tag_id

        # Cache fichier valide ?
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

        # Cherche dans les 200 premiers tags
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
                        log.info(
                            f"  ✅ Tag weather trouvé : id={tag_id} "
                            f"label='{tag.get('label')}' slug='{tag.get('slug')}'"
                        )
                        self._weather_tag_id = tag_id
                        self._tag_cache_ttl  = now + 86400  # 24h
                        try:
                            with open(TAG_CACHE_JSON, "w") as f:
                                json.dump({"tag_id": tag_id, "expires": self._tag_cache_ttl}, f)
                        except Exception:
                            pass
                        return tag_id

        log.warning("  ⚠️  Tag 'weather' introuvable dans /tags → filtre textuel uniquement")
        return None

    # ── Couche 1 : events par tag_id weather ─────────────────────────────────
    def _fetch_events_by_weather_tag(self, tag_id: int, limit: int = 200) -> list:
        """
        GET /events?tag_id=<weather>&related_tags=true&active=true&closed=false
        Triés par volume 24h décroissant — les marchés météo les plus liquides.
        """
        all_markets = []
        offset      = 0
        page_size   = 50       # Gamma recommande ≤100 par page

        log.info(f"  📡 [Couche 1] /events?tag_id={tag_id} (target={limit} marchés)")

        while len(all_markets) < limit:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/events",
                label=f"gamma/events_tag_{tag_id}",
                params={
                    "tag_id":       tag_id,
                    "related_tags": "true",
                    "active":       "true",
                    "closed":       "false",
                    "order":        "volume24hr",
                    "ascending":    "false",
                    "limit":        page_size,
                    "offset":       offset,
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
                f"    page offset={offset} → {len(events)} events, "
                f"{len(batch)} marchés | total={len(all_markets)}"
            )

            # Pagination : has_more ou liste pleine
            has_more = (
                data.get("has_more", False) if isinstance(data, dict)
                else len(events) == page_size
            )
            if not has_more:
                break
            offset += page_size

        log.info(f"  ✅ [Couche 1] {len(all_markets)} marchés récupérés via tag_id={tag_id}")
        return all_markets

    # ── Couche 2 : events actifs + filtre textuel météo ───────────────────────
    def _fetch_events_textual_filter(self, limit: int = 500) -> list:
        """
        GET /events?active=true&closed=false trié par volume.
        Filtre côté client sur mots-clés météo dans slug/question/tags.
        Pagine jusqu'à `limit` marchés météo trouvés ou 1000 events parcourus.
        """
        weather_markets = []
        offset          = 0
        page_size       = 100
        events_scanned  = 0
        max_events      = 1000   # limite raisonnable

        log.info(f"  📡 [Couche 2] /events actifs + filtre textuel (target={limit})")

        while len(weather_markets) < limit and events_scanned < max_events:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/events",
                label="gamma/events_all",
                params={
                    "active":    "true",
                    "closed":    "false",
                    "order":     "volume24hr",
                    "ascending": "false",
                    "limit":     page_size,
                    "offset":    offset,
                },
            )
            if not data:
                break

            events = data if isinstance(data, list) else data.get("data", [])
            if not events:
                break

            events_scanned += len(events)
            batch = _extract_markets_from_events(events)

            # Filtre météo + villes prioritaires
            for m in batch:
                if _is_weather_market(m):
                    weather_markets.append(m)

            log.info(
                f"    offset={offset} → {len(events)} events scannés "
                f"| météo trouvés: {len(weather_markets)}"
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
            f"sur {events_scanned} events scannés"
        )
        return weather_markets

    # ── Couche 3 : /markets direct + filtre textuel (fallback) ───────────────
    def _fetch_markets_direct_filter(self, limit: int = 300) -> list:
        """
        GET /markets?active=true&closed=false — fallback si /events échoue.
        Filtre côté client.
        """
        weather_markets = []
        offset          = 0
        page_size       = 100
        markets_scanned = 0
        max_scan        = 1000

        log.info(f"  📡 [Couche 3/fallback] /markets direct + filtre textuel")

        while len(weather_markets) < limit and markets_scanned < max_scan:
            data = safe_api_call(
                self.session, "GET", f"{GAMMA_BASE}/markets",
                label="gamma/markets_direct",
                params={
                    "active":    "true",
                    "closed":    "false",
                    "order":     "volume24hr",
                    "ascending": "false",
                    "limit":     page_size,
                    "offset":    offset,
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

            log.info(
                f"    offset={offset} → {len(markets)} marchés scannés "
                f"| météo: {len(weather_markets)}"
            )

            has_more = (
                data.get("has_more", False) if isinstance(data, dict)
                else len(markets) == page_size
            )
            if not has_more:
                break
            offset += page_size

        log.info(f"  ✅ [Couche 3] {len(weather_markets)} marchés météo (fallback)")
        return weather_markets

    # ── get_weather_markets — méthode principale ──────────────────────────────
    def get_weather_markets(self, target: int = 500) -> list:
        """
        Récupère les marchés météo Polymarket en 3 couches successives :

        Couche 1 : /events?tag_id=<weather> — précis, faible volume d'appels
                   Nécessite de connaître le tag_id weather (découverte auto).

        Couche 2 : /events?active=true + filtre textuel — large, robuste,
                   scan jusqu'à 1000 events, arrêt dès target atteint.

        Couche 3 : /markets direct + filtre textuel — fallback si /events échoue.

        Tri final : villes prioritaires en tête, puis volume 24h décroissant.
        Déduplication par conditionId.

        Retourne jusqu'à `target` marchés météo.
        """
        log.info(f"🌦️  Récupération marchés météo (target={target})…")
        all_markets: list = []
        seen_ids:    set  = set()

        def _add(markets: list):
            for m in markets:
                mid = m.get("conditionId") or m.get("id") or m.get("slug")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(m)

        # ── Couche 1 : tag_id weather ──────────────────────────────────────
        tag_id = self._discover_weather_tag_id()
        if tag_id:
            layer1 = self._fetch_events_by_weather_tag(tag_id, limit=target)
            _add(layer1)
            log.info(f"  📊 Après Couche 1 : {len(all_markets)} uniques")

        # ── Couche 2 : filtre textuel events ──────────────────────────────
        # Toujours exécutée pour compléter/vérifier (évite faux négatifs du tag)
        if len(all_markets) < target:
            layer2 = self._fetch_events_textual_filter(limit=target)
            before = len(all_markets)
            _add(layer2)
            log.info(
                f"  📊 Après Couche 2 : {len(all_markets)} uniques "
                f"(+{len(all_markets)-before} nouveaux)"
            )

        # ── Couche 3 : fallback /markets direct ───────────────────────────
        if len(all_markets) == 0:
            log.warning("  ↩️  Couches 1+2 vides → Couche 3 (fallback /markets)")
            layer3 = self._fetch_markets_direct_filter(limit=target)
            _add(layer3)
            log.info(f"  📊 Après Couche 3 : {len(all_markets)} uniques")

        # ── Tri final ──────────────────────────────────────────────────────
        # Priorité : (ville connue DESC, volume24h DESC)
        all_markets.sort(key=_market_priority, reverse=True)

        # Résumé
        city_matches = sum(1 for m in all_markets if _market_city(m))
        log.info(
            f"✅ {len(all_markets)} marchés météo trouvés "
            f"({city_matches} avec ville prioritaire)"
        )
        if all_markets:
            top = all_markets[0]
            log.info(
                f"   Top marché : {top.get('question', top.get('slug', '?'))[:60]} "
                f"| vol24h={float(top.get('volume24hr', 0) or 0):.0f} USDC"
            )

        return all_markets[:target]

    # ── get_orderbook ────────────────────────────────────────────────────────
    def get_orderbook(self, token_id: str) -> dict | None:
        return safe_api_call(
            self.session, "GET", f"{CLOB_BASE}/book",
            label=f"clob/orderbook/{token_id[:12]}",
            params={"token_id": token_id},
        )

    # ── place_order ──────────────────────────────────────────────────────────
    def place_order(self, token_id: str, price: float,
                    size: float, side: str = "BUY") -> dict | None:
        if not self.account:
            log.info(f"[SIMULATION] Ordre {side} {size:.2f} USDC @ {price:.3f} sur {token_id[:12]}…")
            return {"status": "simulated", "token_id": token_id,
                    "price": price, "size": size}
        try:
            order = {
                "orderType": "GTC", "tokenID": token_id, "side": side,
                "price": str(round(price, 4)), "size": str(round(size, 2)),
                "funder": self.address, "maker": self.address, "expiration": "0",
            }
            log.info(f"[ORDRE] {side} {size:.2f} USDC @ {price:.3f}")
            return order
        except Exception as e:
            log.error(f"Erreur place_order: {e}")
            return None

    # ── get_redeemable_positions ─────────────────────────────────────────────
    def get_redeemable_positions(self) -> list:
        if not self.address:
            return []
        log.info("  🔍 Recherche positions redeemables (data-api)…")
        all_pos = safe_api_call(
            self.session, "GET", f"{DATA_BASE}/positions",
            label="data/redeemable_positions",
            params={"user": self.address, "sizeThreshold": "0"},
        )
        if all_pos is not None:
            positions  = all_pos if isinstance(all_pos, list) else all_pos.get("data", [])
            redeemable = [
                p for p in positions
                if p.get("redeemable") is True and float(p.get("size", 0)) > 0
            ]
            log.info(f"  📊 {len(positions)} positions totales, {len(redeemable)} redeemables")
            return redeemable
        # Fallback activity
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
                "cashPnl":     float(a.get("usdcSize", 0)),
                "title":       a.get("title", "?"),
                "market":      {"question": a.get("title", "?")},
            }
            for a in acts
        ]

    # ── redeem_position ──────────────────────────────────────────────────────
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
            signed   = self.w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            tx_hash  = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt  = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            if receipt.status == 1:
                log.info(f"  ✅ Redeem on-chain OK — tx: {tx_hash.hex()[:20]}…")
                return True
            log.error(f"  ❌ Redeem on-chain FAILED — tx: {tx_hash.hex()[:20]}…")
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
    outcomes = market.get("outcomes", [])
    # Gamma retourne parfois outcomes comme string JSON
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = []
    if not outcomes:
        return []
    # Gamma peut retourner outcomes comme liste de strings plutôt que dicts
    if outcomes and isinstance(outcomes[0], str):
        prices_raw = market.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                prices_raw = json.loads(prices_raw)
            except Exception:
                prices_raw = []
        outcomes = [
            {"title": outcomes[i], "price": prices_raw[i] if i < len(prices_raw) else "0.5",
             "clobTokenIds": []}
            for i in range(len(outcomes))
        ]
        # Injecte les clobTokenIds depuis le marché
        clob_ids = market.get("clobTokenIds", [])
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = []
        for i, o in enumerate(outcomes):
            o["clobTokenIds"] = [clob_ids[i]] if i < len(clob_ids) else [""]

    temp = forecast["temp_max"]
    bins_with_ev = []
    for outcome in outcomes:
        title         = outcome.get("title", "") if isinstance(outcome, dict) else str(outcome)
        bin_low, bin_high = parse_temp_range(title)
        if bin_low is None:
            continue
        market_price = float(outcome.get("price", 0.5))
        prob  = temperature_to_prob(temp, bin_low, bin_high, forecast["spread"])
        ev    = compute_ev(prob, market_price)
        clob  = outcome.get("clobTokenIds", [""])
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
    return [b for b in bins_with_ev if b["ev"] > EV_THRESHOLD][:MAX_BINS]

# ── Protections ────────────────────────────────────────────────────────────────
def check_daily_reset(state: dict) -> dict:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state["daily_reset"] != today:
        log.info(f"🌅 Nouveau jour {today} — Reset PnL journalier (était {state['daily_pnl']:.2f} USDC)")
        state["daily_pnl"] = 0.0
        state["daily_reset"] = today
    return state

def is_paused(state: dict) -> bool:
    if state.get("paused_until"):
        pause_end = datetime.fromisoformat(state["paused_until"])
        if datetime.utcnow() < pause_end:
            remaining = int((pause_end - datetime.utcnow()).total_seconds() // 60)
            log.info(f"⏸️  Bot en pause — encore {remaining} min")
            return True
        log.info("▶️  Pause terminée, reprise des trades")
        state["paused_until"] = None
    return False

def check_daily_loss(state: dict, balance: float) -> bool:
    loss_pct = -state["daily_pnl"] / balance if balance > 0 else 0
    if loss_pct > DAILY_LOSS_LIMIT:
        pause_until = (datetime.utcnow() + timedelta(hours=24)).isoformat()
        state["paused_until"] = pause_until
        log.warning(
            f"🛡️  PROTECTION ACTIVÉE — Perte journalière {loss_pct*100:.1f}% > "
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
                f"🎉 FÉLICITATIONS ! Capital ≥ {m} USDC ({balance:.2f} USDC) — "
                f"Nouveau palier atteint ! La tirelire grossit 💰"
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
            pnl           = float(pos.get("cashPnl", pos.get("pnl", 0)))
            market_name   = pos.get("title") or pos.get("market", {}).get("question", "?")
            city_hint     = next(
                (c["name"] for c in CITIES
                 if any(alias in market_name.lower() for alias in c["aliases"])),
                "Unknown",
            )
            outcome_index = int(pos.get("outcomeIndex", 0))
            if client.redeem_position(condition_id, outcome_index):
                total_redeemed += pnl
                update_calibration(city_hint, datetime.utcnow().month, cal, pnl > 0)
                append_trade({
                    "timestamp":     datetime.utcnow().isoformat(),
                    "market":        market_name[:50],
                    "bins":          "REDEEM",
                    "amount":        0,
                    "entry_price":   "",
                    "exit_price":    "",
                    "pnl":           round(pnl, 4),
                    "balance_after": round(state["balance"] + total_redeemed, 2),
                    "ev":            "",
                    "notes":         f"Auto-redeem conditionId={condition_id[:12]}",
                })
                log.info(f"  ✅ Redeem {market_name[:30]} → PnL {pnl:+.2f} USDC")
    except Exception as e:
        log.error(f"Erreur auto_redeem: {e}")
    return total_redeemed

# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    log.info("🌤️  Tirelire Météo Pension v1.4 démarrée — Ultra-safe mode")
    log.info(f"📂 Données dans : {DATA_DIR}")
    log.info(f"⏱️  Scan toutes les {SCAN_INTERVAL//60} minutes")
    log.info(f"🗺️  APIs : GAMMA={GAMMA_BASE} | DATA={DATA_BASE} | CLOB={CLOB_BASE}")
    log.info(f"🏙️  Villes : {', '.join(c['name'] for c in CITIES)}")

    init_csv()
    state  = load_state()
    cal    = load_calibration()
    client = PolymarketClient()

    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n{'─'*60}")
        log.info(f"🔄 Cycle #{cycle} — Balance: {state['balance']:.2f} USDC")

        state = check_daily_reset(state)
        check_milestones(state["balance"], state)

        redeemed = auto_redeem(client, state, cal)
        if redeemed != 0:
            state["balance"]   += redeemed
            state["daily_pnl"] += redeemed
            log.info(f"💰 Redeemed: {redeemed:+.2f} USDC → Balance: {state['balance']:.2f}")

        if is_paused(state):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue
        if check_daily_loss(state, state["balance"]):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue

        # Récupération marchés météo — 3 couches
        markets = client.get_weather_markets(target=500)
        log.info(f"📊 {len(markets)} marchés météo à analyser")

        trades_this_cycle = 0
        for market in markets:
            # Ville associée (obligatoire pour la prévision météo)
            city = _market_city(market)
            if not city:
                continue

            forecast = build_consensus_forecast(city)
            if not forecast:
                continue

            if forecast["consensus"] == "WEAK" or forecast["sources"] < 2:
                continue

            month         = datetime.utcnow().month
            conf_factor   = get_confidence_factor(city["name"], month, cal)
            selected_bins = select_bins(market, forecast)

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
                f"     Prévision: {forecast['temp_max']}°C | "
                f"Spread: {forecast['spread']}°C | Consensus: {forecast['consensus']}"
            )

            for b in selected_bins:
                if total_ev <= 0:
                    break
                bin_bet = round((b["ev"] / total_ev) * total_bet * conf_factor, 2)
                if bin_bet < 0.50:
                    continue

                result = client.place_order(
                    token_id=b["token_id"], price=b["market_price"], size=bin_bet
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
                            f"Spread={forecast['spread']} Conf={conf_factor}"
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
