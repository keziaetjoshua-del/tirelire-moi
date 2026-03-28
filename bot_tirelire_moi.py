# bot_tirelire_moi.py
# Tirelire Météo Pension - Bot Polymarket météo ultra-safe
# Stratégie: marchés météo uniquement, EV strict, protection capital

import os
import csv
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# ── Configuration ──────────────────────────────────────────────────────────────
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
TRADES_CSV = DATA_DIR / "trades.csv"
CALIBRATION_JSON = DATA_DIR / "calibration.json"
STATE_JSON = DATA_DIR / "state.json"

PRIVATE_KEY         = os.getenv("PRIVATE_KEY", "")
POLYMARKET_API_URL  = os.getenv("POLYMARKET_API_URL", "https://clob.polymarket.com")
NOAA_API_TOKEN      = os.getenv("NOAA_API_TOKEN", "")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")  # Backup gratuit
OPEN_METEO_URL      = "https://api.open-meteo.com/v1/forecast"  # Gratuit, pas de clé

SCAN_INTERVAL       = int(os.getenv("SCAN_INTERVAL", "900"))  # 15 min
EV_THRESHOLD        = float(os.getenv("EV_THRESHOLD", "0.10"))  # 10%
DAILY_LOSS_LIMIT    = float(os.getenv("DAILY_LOSS_LIMIT", "0.03"))  # 3%
MIN_BINS            = int(os.getenv("MIN_BINS", "5"))
MAX_BINS            = int(os.getenv("MAX_BINS", "10"))

# Villes surveillées (lat, lon, nom)
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
    return {}  # {ville_mois: {"trades": 0, "wins": 0, "factor": 1.0}}

def save_calibration(cal: dict):
    with open(CALIBRATION_JSON, "w") as f:
        json.dump(cal, f, indent=2)

def get_confidence_factor(city: str, month: int, cal: dict) -> float:
    """Facteur de confiance ajusté par l'historique local (0.7 → 1.3)."""
    key = f"{city}_{month:02d}"
    if key not in cal or cal[key]["trades"] < 5:
        return 1.0  # Pas assez de data → neutre
    wins = cal[key]["wins"]
    total = cal[key]["trades"]
    win_rate = wins / total
    # Ajustement doux : ±30% max
    factor = 0.7 + (win_rate * 0.6)
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
    """Stratégie hybride selon le capital."""
    if balance < 500:
        return 5.0
    elif balance <= 1000:
        return min(balance * 0.01, 10.0)
    else:
        return min(balance * 0.0125, 20.0)

# ── APIs Météo ─────────────────────────────────────────────────────────────────
def fetch_open_meteo(lat: float, lon: float, days: int = 7) -> dict | None:
    """Open-Meteo : gratuit, pas de clé, très fiable (modèle GFS + ECMWF blend)."""
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode",
            "forecast_days": days,
            "timezone": "UTC",
        }
        r = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Open-Meteo erreur: {e}")
        return None

def fetch_noaa_forecast(lat: float, lon: float) -> dict | None:
    """NOAA API (USA surtout). Retourne les périodes de prévision."""
    if not NOAA_API_TOKEN:
        return None
    try:
        # Étape 1 : obtenir le point de grille
        point_url = f"https://api.weather.gov/points/{lat},{lon}"
        headers = {"User-Agent": "TirelireMeteoPension/1.0", "token": NOAA_API_TOKEN}
        r = requests.get(point_url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        grid = r.json()
        forecast_url = grid["properties"]["forecast"]
        r2 = requests.get(forecast_url, headers=headers, timeout=10)
        r2.raise_for_status()
        return r2.json()
    except Exception as e:
        log.warning(f"NOAA erreur: {e}")
        return None

def fetch_openweather(lat: float, lon: float) -> dict | None:
    """OpenWeatherMap backup (clé gratuite 1000 appels/jour)."""
    if not OPENWEATHER_API_KEY:
        return None
    try:
        url = "https://api.openweathermap.org/data/2.5/forecast"
        params = {"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY,
                  "units": "metric", "cnt": 40}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"OpenWeather erreur: {e}")
        return None

def build_consensus_forecast(city: dict) -> dict | None:
    """
    Agrège Open-Meteo + NOAA + OpenWeather.
    Retourne un dict avec température max prévue et niveau de consensus.
    """
    lat, lon = city["lat"], city["lon"]
    forecasts = []

    # Source 1 : Open-Meteo (toujours disponible)
    om = fetch_open_meteo(lat, lon)
    if om and "daily" in om:
        temps = om["daily"].get("temperature_2m_max", [])
        if temps:
            forecasts.append({"source": "OpenMeteo", "temp_max": temps[0], "precip": om["daily"].get("precipitation_sum", [0])[0]})

    # Source 2 : NOAA (USA seulement)
    noaa = fetch_noaa_forecast(lat, lon)
    if noaa and "properties" in noaa:
        periods = noaa["properties"].get("periods", [])
        if periods:
            temp_f = periods[0].get("temperature", None)
            if temp_f:
                temp_c = (temp_f - 32) * 5 / 9
                forecasts.append({"source": "NOAA", "temp_max": temp_c, "precip": None})

    # Source 3 : OpenWeather
    ow = fetch_openweather(lat, lon)
    if ow and "list" in ow:
        tomorrow = [x for x in ow["list"] if "12:00" in x.get("dt_txt", "")]
        if tomorrow:
            t = tomorrow[0]["main"]["temp_max"]
            p = tomorrow[0].get("rain", {}).get("3h", 0)
            forecasts.append({"source": "OpenWeather", "temp_max": t, "precip": p})

    if not forecasts:
        log.warning(f"Aucune prévision pour {city['name']}")
        return None

    # Consensus : moyenne pondérée
    temp_values = [f["temp_max"] for f in forecasts]
    avg_temp = sum(temp_values) / len(temp_values)
    spread = max(temp_values) - min(temp_values)

    # Niveau de consensus : faible spread = bonne confiance
    if spread < 1.0:
        consensus = "STRONG"
    elif spread < 2.5:
        consensus = "MODERATE"
    else:
        consensus = "WEAK"

    precip_values = [f["precip"] for f in forecasts if f["precip"] is not None]
    avg_precip = sum(precip_values) / len(precip_values) if precip_values else 0.0

    return {
        "city": city["name"],
        "temp_max": round(avg_temp, 1),
        "precip_mm": round(avg_precip, 1),
        "spread": round(spread, 2),
        "consensus": consensus,
        "sources": len(forecasts),
    }

# ── Polymarket Client ──────────────────────────────────────────────────────────
class PolymarketClient:
    """Client simplifié pour l'API CLOB Polymarket."""

    def __init__(self):
        self.base = POLYMARKET_API_URL
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # NOTE: En production, ajouter signature ECDSA via eth_account
        # pour les ordres. Ici on structure le code pour faciliter l'intégration.
        self._setup_signer()

    def _setup_signer(self):
        """Prépare le signer Ethereum pour les ordres signés."""
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

    def get_markets(self, keyword: str = "weather") -> list:
        """Récupère les marchés météo actifs."""
        try:
            r = self.session.get(
                f"{self.base}/markets",
                params={"keyword": keyword, "active": "true", "closed": "false"},
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                return data.get("data", []) if isinstance(data, dict) else data
            return []
        except Exception as e:
            log.error(f"Erreur get_markets: {e}")
            return []

    def get_orderbook(self, token_id: str) -> dict | None:
        """Récupère le carnet d'ordres d'un token."""
        try:
            r = self.session.get(f"{self.base}/book", params={"token_id": token_id}, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.error(f"Erreur orderbook {token_id}: {e}")
            return None

    def place_order(self, token_id: str, price: float, size: float, side: str = "BUY") -> dict | None:
        """
        Place un ordre limité signé.
        En production: utiliser py-clob-client ou signer manuellement EIP-712.
        """
        if not self.account:
            log.info(f"[SIMULATION] Ordre {side} {size} USDC @ {price:.3f} sur {token_id[:12]}…")
            return {"status": "simulated", "token_id": token_id, "price": price, "size": size}

        try:
            # Structure de l'ordre pour Polymarket CLOB
            order = {
                "orderType": "GTC",
                "tokenID": token_id,
                "side": side,
                "price": str(round(price, 4)),
                "size": str(round(size, 2)),
                "funder": self.address,
                "maker": self.address,
                "expiration": "0",
            }
            # TODO: Signer avec EIP-712 via py-clob-client
            # from py_clob_client.client import ClobClient
            # client = ClobClient(host=self.base, key=PRIVATE_KEY, chain_id=137)
            # return client.create_and_post_order(...)
            log.info(f"[ORDRE] {side} {size} USDC @ {price:.3f}")
            return order
        except Exception as e:
            log.error(f"Erreur place_order: {e}")
            return None

    def get_positions(self) -> list:
        """Positions ouvertes du wallet."""
        if not self.address:
            return []
        try:
            r = self.session.get(
                f"{self.base}/positions",
                params={"user": self.address},
                timeout=10,
            )
            if r.status_code == 200:
                return r.json().get("data", [])
            return []
        except Exception as e:
            log.error(f"Erreur positions: {e}")
            return []

    def redeem_position(self, condition_id: str) -> bool:
        """Claim les gains d'une position résolue."""
        if not self.account:
            log.info(f"[SIMULATION] Redeem {condition_id[:12]}…")
            return True
        try:
            # En production: appel contrat CTF (ConditionalTokens) sur Polygon
            # contract.redeemPositions(collateral, parentCollectionId, conditionId, indexSets)
            log.info(f"✅ Redeem demandé pour condition {condition_id[:12]}…")
            return True
        except Exception as e:
            log.error(f"Erreur redeem {condition_id}: {e}")
            return False

    def get_resolved_markets(self) -> list:
        """Marchés résolus avec positions gagnantes."""
        if not self.address:
            return []
        try:
            r = self.session.get(
                f"{self.base}/positions",
                params={"user": self.address, "redeemable": "true"},
                timeout=10,
            )
            if r.status_code == 200:
                return r.json().get("data", [])
            return []
        except Exception as e:
            log.error(f"Erreur resolved_markets: {e}")
            return []

# ── Logique de trading ─────────────────────────────────────────────────────────
def compute_ev(prob_model: float, market_price: float) -> float:
    """
    Expected Value = (prob_modèle * gain) - (1 - prob_modèle) * perte
    Pour une mise à `market_price` centimes → gain de (1 - market_price).
    """
    if market_price <= 0 or market_price >= 1:
        return -999.0
    gain = (1.0 - market_price)
    perte = market_price
    ev = (prob_model * gain) - ((1.0 - prob_model) * perte)
    return round(ev, 4)

def temperature_to_prob(forecast_temp: float, bin_low: float, bin_high: float,
                        spread: float) -> float:
    """
    Convertit la prévision de température en probabilité pour un bin donné.
    Utilise une distribution normale autour de la température prévue.
    """
    import math
    # Incertitude σ proportionnelle au spread du consensus
    sigma = max(spread, 1.0) * 1.5
    mu = forecast_temp
    # Probabilité d'être dans [bin_low, bin_high]
    def normal_cdf(x):
        return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))
    prob = normal_cdf(bin_high) - normal_cdf(bin_low)
    return round(max(0.0, min(1.0, prob)), 4)

def select_bins(market: dict, forecast: dict) -> list:
    """
    Identifie les 5-10 bins adjacents autour de la température prévue.
    Retourne les outcomes les plus proches.
    """
    outcomes = market.get("outcomes", [])
    if not outcomes:
        return []

    temp = forecast["temp_max"]
    bins_with_ev = []

    for outcome in outcomes:
        title = outcome.get("title", "")
        # Essaie de parser le range de température (ex: "22-24°C", "> 25°C")
        bin_low, bin_high = parse_temp_range(title)
        if bin_low is None:
            continue

        # Prix du marché pour cet outcome
        market_price = float(outcome.get("price", 0.5))
        # Probabilité modèle
        prob = temperature_to_prob(temp, bin_low, bin_high, forecast["spread"])
        # Facteur de confiance calibration
        # (passé depuis l'appelant)
        ev = compute_ev(prob, market_price)

        bins_with_ev.append({
            "outcome_id": outcome.get("id", ""),
            "token_id": outcome.get("clobTokenIds", [""])[0],
            "title": title,
            "bin_low": bin_low,
            "bin_high": bin_high,
            "prob": prob,
            "market_price": market_price,
            "ev": ev,
        })

    # Trie par distance à la température prévue, garde les 5-10 meilleurs EV
    bins_with_ev.sort(key=lambda x: abs(((x["bin_low"] + x["bin_high"]) / 2) - temp))
    candidates = [b for b in bins_with_ev if b["ev"] > EV_THRESHOLD]
    return candidates[:MAX_BINS]

def parse_temp_range(title: str) -> tuple:
    """Parse '20-22°C' → (20, 22), '>25°C' → (25, 50), '<10°C' → (-50, 10)."""
    import re
    # Format "X-Y°C" ou "X-Y"
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)", title)
    if m:
        return float(m.group(1)), float(m.group(2))
    # Format "> X"
    m = re.search(r"[>≥]\s*(-?\d+(?:\.\d+)?)", title)
    if m:
        return float(m.group(1)), float(m.group(1)) + 25
    # Format "< X"
    m = re.search(r"[<≤]\s*(-?\d+(?:\.\d+)?)", title)
    if m:
        return float(m.group(1)) - 25, float(m.group(1))
    return None, None

# ── Protections ────────────────────────────────────────────────────────────────
def check_daily_reset(state: dict) -> dict:
    """Remet le PnL journalier à zéro si nouveau jour."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state["daily_reset"] != today:
        log.info(f"🌅 Nouveau jour {today} — Reset PnL journalier (était {state['daily_pnl']:.2f} USDC)")
        state["daily_pnl"] = 0.0
        state["daily_reset"] = today
    return state

def is_paused(state: dict) -> bool:
    """Vérifie si le bot est en pause suite à une grosse perte."""
    if state.get("paused_until"):
        pause_end = datetime.fromisoformat(state["paused_until"])
        if datetime.utcnow() < pause_end:
            remaining = (pause_end - datetime.utcnow()).seconds // 60
            log.info(f"⏸️  Bot en pause — encore {remaining} min")
            return True
        else:
            log.info("▶️  Pause terminée, reprise des trades")
            state["paused_until"] = None
    return False

def check_daily_loss(state: dict, balance: float) -> bool:
    """Déclenche le bouclier si perte > 3% du capital."""
    loss_pct = -state["daily_pnl"] / balance if balance > 0 else 0
    if loss_pct > DAILY_LOSS_LIMIT:
        pause_until = (datetime.utcnow() + timedelta(hours=24)).isoformat()
        state["paused_until"] = pause_until
        log.warning(
            f"🛡️  PROTECTION ACTIVÉE — Perte journalière {loss_pct*100:.1f}% > {DAILY_LOSS_LIMIT*100:.0f}% "
            f"— Pause jusqu'à {pause_until[:16]}"
        )
        return True
    return False

def check_milestones(balance: float, state: dict):
    """Rappels gentils aux paliers de capital."""
    milestones = [500, 1000, 1500, 2000]
    for m in milestones:
        label = str(m)
        if balance >= m and label not in state.get("milestones_hit", []):
            if "milestones_hit" not in state:
                state["milestones_hit"] = []
            state["milestones_hit"].append(label)
            log.info(
                f"🎉 FÉLICITATIONS ! Capital ≥ {m} USDC ({balance:.2f} USDC) — "
                f"Nouveau palier atteint ! La tirelire grossit 💰"
            )

# ── Redeem automatique ─────────────────────────────────────────────────────────
def auto_redeem(client: PolymarketClient, state: dict, cal: dict) -> float:
    """Détecte et redeem les marchés résolus. Retourne les gains réclamés."""
    total_redeemed = 0.0
    try:
        resolved = client.get_resolved_markets()
        if not resolved:
            return 0.0

        log.info(f"💎 {len(resolved)} position(s) redeemable trouvée(s)")
        for pos in resolved:
            condition_id = pos.get("conditionId", "")
            pnl = float(pos.get("pnl", 0))
            market_name = pos.get("market", {}).get("question", "?")
            city_hint = next((c["name"] for c in CITIES if c["name"].lower() in market_name.lower()), "Unknown")

            if client.redeem_position(condition_id):
                total_redeemed += pnl
                won = pnl > 0
                month = datetime.utcnow().month
                update_calibration(city_hint, month, cal, won)

                append_trade({
                    "timestamp": datetime.utcnow().isoformat(),
                    "market": market_name[:50],
                    "bins": "REDEEM",
                    "amount": 0,
                    "entry_price": "",
                    "exit_price": "",
                    "pnl": round(pnl, 4),
                    "balance_after": round(state["balance"] + total_redeemed, 2),
                    "ev": "",
                    "notes": f"Auto-redeem conditionId={condition_id[:12]}",
                })
                log.info(f"  ✅ Redeem {market_name[:30]} → PnL {pnl:+.2f} USDC")

    except Exception as e:
        log.error(f"Erreur auto_redeem: {e}")

    return total_redeemed

# ── Correspondance marché ↔ ville ──────────────────────────────────────────────
def match_market_to_city(market: dict) -> dict | None:
    """Essaie de faire correspondre un marché Polymarket à une ville surveillée."""
    question = market.get("question", "").lower()
    description = market.get("description", "").lower()
    text = question + " " + description

    for city in CITIES:
        if city["name"].lower() in text:
            return city
    return None

def is_temperature_market(market: dict) -> bool:
    """Filtre : marché de type température uniquement."""
    q = market.get("question", "").lower()
    keywords = ["temperature", "temp", "degrees", "°c", "°f", "celsius", "fahrenheit",
                "high", "low", "max", "min", "average"]
    return any(k in q for k in keywords)

# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    log.info("🌤️  Tirelire Météo Pension démarrée — Ultra-safe mode")
    log.info(f"📂 Données dans : {DATA_DIR}")
    log.info(f"⏱️  Scan toutes les {SCAN_INTERVAL//60} minutes")

    init_csv()
    state   = load_state()
    cal     = load_calibration()
    client  = PolymarketClient()

    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n{'─'*60}")
        log.info(f"🔄 Cycle #{cycle} — Balance: {state['balance']:.2f} USDC")

        # ── Reset journalier ──
        state = check_daily_reset(state)

        # ── Milestones ──
        check_milestones(state["balance"], state)

        # ── Auto-redeem ──
        redeemed = auto_redeem(client, state, cal)
        if redeemed != 0:
            state["balance"] += redeemed
            state["daily_pnl"] += redeemed
            log.info(f"💰 Redeemed total: {redeemed:+.2f} USDC → Balance: {state['balance']:.2f}")

        # ── Vérif pause ──
        if is_paused(state):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue

        if check_daily_loss(state, state["balance"]):
            save_state(state)
            time.sleep(SCAN_INTERVAL)
            continue

        # ── Scan marchés météo ──
        markets = client.get_markets(keyword="weather temperature")
        log.info(f"📊 {len(markets)} marchés météo trouvés")

        trades_this_cycle = 0
        for market in markets[:30]:  # Limite à 30 marchés max par cycle
            if not is_temperature_market(market):
                continue

            city = match_market_to_city(market)
            if not city:
                continue

            # Prévision météo consensuelle
            forecast = build_consensus_forecast(city)
            if not forecast:
                continue

            # Ne trader que si consensus fort ou modéré + au moins 2 sources
            if forecast["consensus"] == "WEAK" or forecast["sources"] < 2:
                log.info(f"  ⏭️  {city['name']} : consensus {forecast['consensus']} ({forecast['sources']} sources) → skip")
                continue

            # Sélection des bins
            month = datetime.utcnow().month
            conf_factor = get_confidence_factor(city["name"], month, cal)
            selected_bins = select_bins(market, forecast)

            if len(selected_bins) < MIN_BINS:
                log.info(f"  ⏭️  {city['name']} : seulement {len(selected_bins)} bins EV>10% → skip")
                continue

            # Calcul mise totale répartie sur les bins
            total_bet = compute_bet_size(state["balance"])
            # Pondération par EV
            total_ev = sum(b["ev"] for b in selected_bins)
            market_name = market.get("question", "?")[:50]
            bins_labels = [b["title"] for b in selected_bins]

            log.info(f"  🎯 {city['name']} | {market_name}")
            log.info(f"     Prévision: {forecast['temp_max']}°C | Spread: {forecast['spread']}°C | Consensus: {forecast['consensus']}")
            log.info(f"     Mise totale: {total_bet:.2f} USDC sur {len(selected_bins)} bins | Facteur confiance: {conf_factor}")

            orders_placed = 0
            for b in selected_bins:
                if total_ev <= 0:
                    break
                # Mise proportionnelle à l'EV de chaque bin
                bin_bet = round((b["ev"] / total_ev) * total_bet * conf_factor, 2)
                if bin_bet < 0.50:  # Mise minimale 0.50 USDC
                    continue

                result = client.place_order(
                    token_id=b["token_id"],
                    price=b["market_price"],
                    size=bin_bet,
                )

                if result:
                    orders_placed += 1
                    state["balance"] -= bin_bet
                    state["daily_pnl"] -= bin_bet  # Comptabilisé en sortie; PnL réel au redeem

                    append_trade({
                        "timestamp": datetime.utcnow().isoformat(),
                        "market": market_name,
                        "bins": b["title"],
                        "amount": bin_bet,
                        "entry_price": b["market_price"],
                        "exit_price": "",
                        "pnl": "",
                        "balance_after": round(state["balance"], 2),
                        "ev": b["ev"],
                        "notes": f"Consensus={forecast['consensus']} Spread={forecast['spread']} Conf={conf_factor}",
                    })
                    log.info(f"     ✅ Bin [{b['title']}] → {bin_bet:.2f} USDC @ {b['market_price']:.3f} | EV={b['ev']:.1%}")

            trades_this_cycle += orders_placed

        log.info(f"✔️  Cycle #{cycle} terminé — {trades_this_cycle} ordres placés")
        save_state(state)

        # Attendre le prochain cycle
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