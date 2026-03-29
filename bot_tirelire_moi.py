# bot_tirelire_moi.py
# Tirelire Météo Pension - Bot Polymarket météo ultra-safe
# v1.1 — Robustesse API : timeouts longs + retries exponentiels

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
from urllib3.util.retry import Retry

# ── Configuration ──────────────────────────────────────────────────────────────
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
TRADES_CSV          = DATA_DIR / "trades.csv"
CALIBRATION_JSON    = DATA_DIR / "calibration.json"
STATE_JSON          = DATA_DIR / "state.json"

PRIVATE_KEY         = os.getenv("PRIVATE_KEY", "")
POLYMARKET_API_URL  = os.getenv("POLYMARKET_API_URL", "https://clob.polymarket.com")
NOAA_API_TOKEN      = os.getenv("NOAA_API_TOKEN", "")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
OPEN_METEO_URL      = "https://api.open-meteo.com/v1/forecast"

SCAN_INTERVAL       = int(os.getenv("SCAN_INTERVAL", "900"))
EV_THRESHOLD        = float(os.getenv("EV_THRESHOLD", "0.10"))
DAILY_LOSS_LIMIT    = float(os.getenv("DAILY_LOSS_LIMIT", "0.03"))
MIN_BINS            = int(os.getenv("MIN_BINS", "5"))
MAX_BINS            = int(os.getenv("MAX_BINS", "10"))

# ── Constantes de robustesse API ───────────────────────────────────────────────
API_TIMEOUT_CONNECT = 10   # secondes pour établir la connexion
API_TIMEOUT_READ    = 20   # secondes pour lire la réponse (was default ~5s)
API_MAX_RETRIES     = 3    # tentatives max avant abandon
API_BACKOFF_BASE    = 2.0  # secondes de base pour le backoff exponentiel

CITIES = [
    {"name": "Paris",     "lat": 48.85, "lon": 2.35},
    {"name": "Lyon",      "lat": 45.75, "lon": 4.85},
    {"name": "Marseille", "lat": 43.30, "lon": 5.37},
    {"name": "New York",  "lat": 40.71, "lon": -74.00},
    {"name": "London",    "lat": 51.51, "lon": -0.13},
    {"name": "Berlin",    "lat": 52.52, "lon": 13.41},
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

# ── Session HTTP robuste ───────────────────────────────────────────────────────
def build_robust_session() -> requests.Session:
    """
    Crée une session requests avec :
    - Retry automatique sur erreurs réseau (ConnectionError, Timeout)
    - Backoff exponentiel entre tentatives
    - Timeout long par défaut
    ATTENTION : urllib3 Retry gère les retries TRANSPORT (connexion refusée,
    reset TCP). Les retries applicatifs (timeout read, codes HTTP 5xx) sont
    gérés manuellement via api_call() ci-dessous pour un logging précis.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=API_MAX_RETRIES,
        backoff_factor=API_BACKOFF_BASE,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update({"Content-Type": "application/json"})
    return session

def api_call(session: requests.Session, method: str, url: str,
             label: str = "API", **kwargs) -> requests.Response | None:
    """
    Wrapper universel pour tous les appels Polymarket.
    - Injecte les timeouts (connect, read) si non fournis
    - Retry manuel jusqu'à API_MAX_RETRIES avec backoff exponentiel
    - Log chaque tentative et chaque retry clairement
    Retourne la Response si succès, None si toutes les tentatives échouent.
    """
    kwargs.setdefault("timeout", (API_TIMEOUT_CONNECT, API_TIMEOUT_READ))

    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            if attempt > 1:
                wait = API_BACKOFF_BASE ** (attempt - 1)   # 2s, 4s, 8s …
                log.warning(
                    f"  🔄 {label} — Retry {attempt}/{API_MAX_RETRIES} "
                    f"dans {wait:.0f}s…"
                )
                time.sleep(wait)

            response = session.request(method, url, **kwargs)

            # Succès
            if response.status_code < 500:
                if attempt > 1:
                    log.info(f"  ✅ {label} — Succès après {attempt} tentatives")
                return response

            # Erreur serveur → retry
            log.warning(
                f"  ⚠️  {label} — HTTP {response.status_code} "
                f"(tentative {attempt}/{API_MAX_RETRIES})"
            )

        except requests.exceptions.ConnectTimeout:
            log.warning(
                f"  ⏱️  {label} — ConnectTimeout (tentative {attempt}/{API_MAX_RETRIES})"
            )
        except requests.exceptions.ReadTimeout:
            log.warning(
                f"  ⏱️  {label} — ReadTimeout après {API_TIMEOUT_READ}s "
                f"(tentative {attempt}/{API_MAX_RETRIES})"
            )
        except requests.exceptions.ConnectionError as e:
            log.warning(
                f"  🔌 {label} — ConnectionError : {e} "
                f"(tentative {attempt}/{API_MAX_RETRIES})"
            )
        except Exception as e:
            log.error(f"  💥 {label} — Erreur inattendue : {e}")
            break   # Erreur non-réseau → inutile de retry

    log.error(f"  ❌ {label} — Abandon après {API_MAX_RETRIES} tentatives")
    return None

# ── CSV Initialisation ─────────────────────────────────────────────────────────
def init_csv():
    if not TRADES_CSV.exists():
        with open(TRADES_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "timestamp", "market", "bins", "amount", "entry_price",
                "exit_price", "pnl", "balance_after", "ev", "notes"
            ])
            writer.writeheader()
        log.info("📄 trades.csv créé.")

def append_trade(row: dict):
    with open(TRADES_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "market", "bins", "amount", "entry_price",
            "exit_price", "pnl", "balance_after", "ev", "notes"
        ])
        writer.writerow(row)

# ── État persistant ────────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_JSON.exists():
        with open(STATE_JSON) as f:
            return json.load(f)
    return {
        "balance": float(os.getenv("INITIAL_BALANCE", "100.0")),
        "daily_pnl": 0.0,
        "daily_reset": datetime.utcnow().strftime("%Y-%m-%d"),
        "paused_until": None,
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
    wins  = cal[key]["wins"]
    total = cal[key]["trades"]
    factor = 0.7 + ((wins / total) * 0.6)
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
        params = {
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode",
            "forecast_days": days, "timezone": "UTC",
        }
        r = requests.get(OPEN_METEO_URL, params=params, timeout=(10, 15))
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Open-Meteo erreur: {e}")
        return None

def fetch_noaa_forecast(lat: float, lon: float) -> dict | None:
    if not NOAA_API_TOKEN:
        return None
    try:
        headers = {"User-Agent": "TirelireMeteoPension/1.0", "token": NOAA_API_TOKEN}
        r = requests.get(f"https://api.weather.gov/points/{lat},{lon}",
                         headers=headers, timeout=(10, 15))
        if r.status_code != 200:
            return None
        r2 = requests.get(r.json()["properties"]["forecast"],
                          headers=headers, timeout=(10, 15))
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
            timeout=(10, 15),
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"OpenWeather erreur: {e}")
        return None

def build_consensus_forecast(city: dict) -> dict | None:
    lat, lon = city["lat"], city["lon"]
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
        if periods:
            temp_f = periods[0].get("temperature")
            if temp_f:
                forecasts.append({
                    "source": "NOAA",
                    "temp_max": (temp_f - 32) * 5 / 9,
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
        log.warning(f"Aucune prévision pour {city['name']}")
        return None

    temp_values = [f["temp_max"] for f in forecasts]
    avg_temp    = sum(temp_values) / len(temp_values)
    spread      = max(temp_values) - min(temp_values)
    consensus   = "STRONG" if spread < 1.0 else ("MODERATE" if spread < 2.5 else "WEAK")
    precip_vals = [f["precip"] for f in forecasts if f["precip"] is not None]

    return {
        "city":      city["name"],
        "temp_max":  round(avg_temp, 1),
        "precip_mm": round(sum(precip_vals) / len(precip_vals), 1) if precip_vals else 0.0,
        "spread":    round(spread, 2),
        "consensus": consensus,
        "sources":   len(forecasts),
    }

# ── Polymarket Client ──────────────────────────────────────────────────────────
class PolymarketClient:
    """
    Client Polymarket CLOB robuste.
    Tous les appels HTTP passent par api_call() :
      - timeout (connect=10s, read=20s)
      - retries manuels x3 avec backoff exponentiel (2s → 4s → 8s)
      - log clair à chaque retry
    """

    def __init__(self):
        self.base    = POLYMARKET_API_URL
        self.session = build_robust_session()
        self._setup_signer()

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

    # ── get_markets ────────────────────────────────────────────────────────────
    def get_markets(self, keyword: str = "weather") -> list:
        """Marchés météo actifs — avec retry automatique."""
        r = api_call(
            self.session, "GET", f"{self.base}/markets",
            label="get_markets",
            params={"keyword": keyword, "active": "true", "closed": "false"},
        )
        if r is None:
            return []
        try:
            data = r.json()
            return data.get("data", []) if isinstance(data, dict) else data
        except Exception as e:
            log.error(f"get_markets parse error: {e}")
            return []

    # ── get_orderbook ──────────────────────────────────────────────────────────
    def get_orderbook(self, token_id: str) -> dict | None:
        """Carnet d'ordres d'un token — avec retry automatique."""
        r = api_call(
            self.session, "GET", f"{self.base}/book",
            label=f"orderbook/{token_id[:12]}",
            params={"token_id": token_id},
        )
        if r is None:
            return None
        try:
            return r.json()
        except Exception as e:
            log.error(f"orderbook parse error: {e}")
            return None

    # ── place_order ────────────────────────────────────────────────────────────
    def place_order(self, token_id: str, price: float,
                    size: float, side: str = "BUY") -> dict | None:
        """Place un ordre — avec retry automatique."""
        if not self.account:
            log.info(f"[SIMULATION] Ordre {side} {size} USDC @ {price:.3f} sur {token_id[:12]}…")
            return {"status": "simulated", "token_id": token_id,
                    "price": price, "size": size}
        try:
            order = {
                "orderType": "GTC",
                "tokenID":   token_id,
                "side":      side,
                "price":     str(round(price, 4)),
                "size":      str(round(size, 2)),
                "funder":    self.address,
                "maker":     self.address,
                "expiration": "0",
            }
            # TODO: Signer EIP-712 via py-clob-client en production
            log.info(f"[ORDRE] {side} {size} USDC @ {price:.3f}")
            return order
        except Exception as e:
            log.error(f"Erreur place_order: {e}")
            return None

    # ── get_positions ──────────────────────────────────────────────────────────
    def get_positions(self) -> list:
        """Positions ouvertes — avec retry automatique."""
        if not self.address:
            return []
        r = api_call(
            self.session, "GET", f"{self.base}/positions",
            label="get_positions",
            params={"user": self.address},
        )
        if r is None:
            return []
        try:
            return r.json().get("data", [])
        except Exception as e:
            log.error(f"get_positions parse error: {e}")
            return []

    # ── get_resolved_markets ───────────────────────────────────────────────────
    def get_resolved_markets(self) -> list:
        """
        Positions redeemables — avec retry automatique.
        C'est cette méthode qui causait les ReadTimeout → corrigé via api_call().
        """
        if not self.address:
            return []
        r = api_call(
            self.session, "GET", f"{self.base}/positions",
            label="get_resolved_markets",          # label visible dans les logs
            params={"user": self.address, "redeemable": "true"},
        )
        if r is None:
            log.warning("get_resolved_markets : aucune réponse après retries — skip redeem ce cycle")
            return []
        try:
            return r.json().get("data", [])
        except Exception as e:
            log.error(f"get_resolved_markets parse error: {e}")
            return []

    # ── redeem_position ────────────────────────────────────────────────────────
    def redeem_position(self, condition_id: str) -> bool:
        if not self.account:
            log.info(f"[SIMULATION] Redeem {condition_id[:12]}…")
            return True
        try:
            log.info(f"✅ Redeem demandé pour condition {condition_id[:12]}…")
            return True
        except Exception as e:
            log.error(f"Erreur redeem {condition_id}: {e}")
            return False

# ── Logique de trading ─────────────────────────────────────────────────────────
def compute_ev(prob_model: float, market_price: float) -> float:
    if market_price <= 0 or market_price >= 1:
        return -999.0
    ev = (prob_model * (1.0 - market_price)) - ((1.0 - prob_model) * market_price)
    return round(ev, 4)

def temperature_to_prob(forecast_temp: float, bin_low: float,
                        bin_high: float, spread: float) -> float:
    import math
    sigma = max(spread, 1.0) * 1.5
    mu    = forecast_temp
    def normal_cdf(x):
        return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))
    prob = normal_cdf(bin_high) - normal_cdf(bin_low)
    return round(max(0.0, min(1.0, prob)), 4)

def select_bins(market: dict, forecast: dict) -> list:
    outcomes = market.get("outcomes", [])
    if not outcomes:
        return []
    temp = forecast["temp_max"]
    bins_with_ev = []
    for outcome in outcomes:
        title = outcome.get("title", "")
        bin_low, bin_high = parse_temp_range(title)
        if bin_low is None:
            continue
        market_price = float(outcome.get("price", 0.5))
        prob = temperature_to_prob(temp, bin_low, bin_high, forecast["spread"])
        ev   = compute_ev(prob, market_price)
        bins_with_ev.append({
            "outcome_id":  outcome.get("id", ""),
            "token_id":    outcome.get("clobTokenIds", [""])[0],
            "title":       title,
            "bin_low":     bin_low,
            "bin_high":    bin_high,
            "prob":        prob,
            "market_price": market_price,
            "ev":          ev,
        })
    bins_with_ev.sort(key=lambda x: abs(((x["bin_low"] + x["bin_high"]) / 2) - temp))
    candidates = [b for b in bins_with_ev if b["ev"] > EV_THRESHOLD]
    return candidates[:MAX_BINS]

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
        resolved = client.get_resolved_markets()   # ← utilise désormais api_call()
        if not resolved:
            return 0.0

        log.info(f"💎 {len(resolved)} position(s) redeemable trouvée(s)")
        for pos in resolved:
            condition_id = pos.get("conditionId", "")
            pnl          = float(pos.get("pnl", 0))
            market_name  = pos.get("market", {}).get("question", "?")
            city_hint    = next(
                (c["name"] for c in CITIES if c["name"].lower() in market_name.lower()),
                "Unknown",
            )

            if client.redeem_position(condition_id):
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
        log.error(f"Erreur auto_redeem (hors réseau): {e}")

    return total_redeemed

# ── Correspondance marché ↔ ville ──────────────────────────────────────────────
def match_market_to_city(market: dict) -> dict | None:
    text = (market.get("question", "") + " " + market.get("description", "")).lower()
    return next((c for c in CITIES if c["name"].lower() in text), None)

def is_temperature_market(market: dict) -> bool:
    q = market.get("question", "").lower()
    return any(k in q for k in [
        "temperature", "temp", "degrees", "°c", "°f",
        "celsius", "fahrenheit", "high", "low", "max", "min", "average",
    ])

# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    log.info("🌤️  Tirelire Météo Pension v1.1 démarrée — Ultra-safe mode")
    log.info(f"📂 Données dans : {DATA_DIR}")
    log.info(f"⏱️  Scan toutes les {SCAN_INTERVAL//60} minutes")
    log.info(f"🔁 API Polymarket : timeout={API_TIMEOUT_READ}s, retries={API_MAX_RETRIES}, backoff={API_BACKOFF_BASE}s")

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
            log.info(f"💰 Redeemed total: {redeemed:+.2f} USDC → Balance: {state['balance']:.2f}")

        if is_paused(state):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue

        if check_daily_loss(state, state["balance"]):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue

        markets = client.get_markets(keyword="weather temperature")
        log.info(f"📊 {len(markets)} marchés météo trouvés")

        trades_this_cycle = 0
        for market in markets[:30]:
            if not is_temperature_market(market):
                continue
            city = match_market_to_city(market)
            if not city:
                continue

            forecast = build_consensus_forecast(city)
            if not forecast:
                continue

            if forecast["consensus"] == "WEAK" or forecast["sources"] < 2:
                log.info(f"  ⏭️  {city['name']} : consensus {forecast['consensus']} ({forecast['sources']} sources) → skip")
                continue

            month         = datetime.utcnow().month
            conf_factor   = get_confidence_factor(city["name"], month, cal)
            selected_bins = select_bins(market, forecast)

            if len(selected_bins) < MIN_BINS:
                log.info(f"  ⏭️  {city['name']} : seulement {len(selected_bins)} bins EV>10% → skip")
                continue

            total_bet  = compute_bet_size(state["balance"])
            total_ev   = sum(b["ev"] for b in selected_bins)
            market_name = market.get("question", "?")[:50]

            log.info(f"  🎯 {city['name']} | {market_name}")
            log.info(f"     Prévision: {forecast['temp_max']}°C | Spread: {forecast['spread']}°C | Consensus: {forecast['consensus']}")
            log.info(f"     Mise totale: {total_bet:.2f} USDC sur {len(selected_bins)} bins | Facteur confiance: {conf_factor}")

            for b in selected_bins:
                if total_ev <= 0:
                    break
                bin_bet = round((b["ev"] / total_ev) * total_bet * conf_factor, 2)
                if bin_bet < 0.50:
                    continue

                result = client.place_order(token_id=b["token_id"],
                                            price=b["market_price"], size=bin_bet)
                if result:
                    trades_this_cycle    += 1
                    state["balance"]     -= bin_bet
                    state["daily_pnl"]  -= bin_bet
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
                        "notes":         f"Consensus={forecast['consensus']} Spread={forecast['spread']} Conf={conf_factor}",
                    })
                    log.info(f"     ✅ Bin [{b['title']}] → {bin_bet:.2f} USDC @ {b['market_price']:.3f} | EV={b['ev']:.1%}")

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
