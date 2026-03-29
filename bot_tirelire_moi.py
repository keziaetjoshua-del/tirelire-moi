# bot_tirelire_moi.py
# Tirelire Météo Pension - Bot Polymarket météo ultra-safe
# v1.3 — Endpoints corrigés (mars 2026) :
#   Marchés  → gamma-api.polymarket.com/markets
#   Positions → data-api.polymarket.com/positions   (redeemable dans la réponse)
#   Redeem   → contrat CTF Polygon via web3 (pas d'endpoint REST, cf. issue #139)
#   Fallback → gamma-api si clob indisponible

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
TRADES_CSV       = DATA_DIR / "trades.csv"
CALIBRATION_JSON = DATA_DIR / "calibration.json"
STATE_JSON       = DATA_DIR / "state.json"

PRIVATE_KEY         = os.getenv("PRIVATE_KEY", "")
OPEN_METEO_URL      = "https://api.open-meteo.com/v1/forecast"
NOAA_API_TOKEN      = os.getenv("NOAA_API_TOKEN", "")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")

SCAN_INTERVAL    = int(os.getenv("SCAN_INTERVAL",    "900"))
EV_THRESHOLD     = float(os.getenv("EV_THRESHOLD",   "0.10"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT","0.03"))
MIN_BINS         = int(os.getenv("MIN_BINS", "5"))
MAX_BINS         = int(os.getenv("MAX_BINS", "10"))

# ── URLs Polymarket officielles (mars 2026) ────────────────────────────────────
# Séparation stricte des trois APIs Polymarket :
#   GAMMA : découverte marchés, métadonnées            (public, pas d'auth)
#   CLOB  : orderbook, prix, placement d'ordres        (auth pour trading)
#   DATA  : positions user, historique, PnL            (public avec ?user=)
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"

# Polygon / CTF pour redeem on-chain
POLYGON_RPC    = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")
CTF_ADDRESS    = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # ConditionalTokens Polygon
USDC_ADDRESS   = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC Polygon
HASH_ZERO      = "0x" + "0" * 64

# ── Robustesse API ─────────────────────────────────────────────────────────────
API_TIMEOUT_CONNECT = 10
API_TIMEOUT_READ    = 30
API_RETRY_DELAYS    = [1, 3, 8]
API_USER_AGENT      = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36 TirelireMeteoPension/1.3"
)

CITIES = [
    {"name": "Paris",     "lat": 48.85, "lon":  2.35},
    {"name": "Lyon",      "lat": 45.75, "lon":  4.85},
    {"name": "Marseille", "lat": 43.30, "lon":  5.37},
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

# ══════════════════════════════════════════════════════════════════════════════
# ── safe_api_call ─────────────────────────────────────────────────────────────
# Helper central pour TOUS les appels REST Polymarket.
# Garanties : timeout 30s · User-Agent · retry 1s/3s/8s · JSON-guard
# Retourne le dict/list parsé, ou None si échec total.
# ══════════════════════════════════════════════════════════════════════════════
def safe_api_call(session: requests.Session, method: str, url: str,
                  label: str = "API", **kwargs):
    kwargs.setdefault("timeout", (API_TIMEOUT_CONNECT, API_TIMEOUT_READ))
    max_attempts = len(API_RETRY_DELAYS) + 1

    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            delay = API_RETRY_DELAYS[attempt - 2]
            log.warning(
                f"  🔄 [{label}] Retry {attempt-1}/{len(API_RETRY_DELAYS)} "
                f"dans {delay}s…"
            )
            time.sleep(delay)

        try:
            resp = session.request(method, url, **kwargs)

            # 404 → endpoint inexistant, inutile de retry
            if resp.status_code == 404:
                log.warning(f"  🚫 [{label}] HTTP 404 — endpoint introuvable : {url}")
                return None

            # 5xx → retry
            if resp.status_code >= 500:
                log.warning(
                    f"  ⚠️  [{label}] HTTP {resp.status_code} — "
                    f"tentative {attempt}/{max_attempts}"
                )
                continue

            # Corps vide → retry
            raw = resp.text.strip()
            if not raw:
                log.warning(
                    f"  ⚠️  [{label}] Réponse vide (HTTP {resp.status_code}) — "
                    f"tentative {attempt}/{max_attempts}"
                )
                continue

            # JSON invalide → retry
            try:
                data = resp.json()
                if attempt > 1:
                    log.info(f"  ✅ [{label}] Succès après {attempt} tentatives")
                return data
            except ValueError:
                preview = raw[:120].replace("\n", " ")
                log.warning(
                    f"  ⚠️  [{label}] JSON invalide — «{preview}» — "
                    f"tentative {attempt}/{max_attempts}"
                )
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

    log.error(
        f"  ❌ [{label}] API Polymarket unavailable — "
        f"abandon après {max_attempts} tentatives."
    )
    return None

# ── Session HTTP ───────────────────────────────────────────────────────────────
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
        hdrs = {"User-Agent": "TirelireMeteoPension/1.3", "token": NOAA_API_TOKEN}
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
        log.warning(f"Aucune prévision pour {city['name']}")
        return None

    temps     = [f["temp_max"] for f in forecasts]
    avg_temp  = sum(temps) / len(temps)
    spread    = max(temps) - min(temps)
    consensus = "STRONG" if spread < 1.0 else ("MODERATE" if spread < 2.5 else "WEAK")
    precips   = [f["precip"] for f in forecasts if f["precip"] is not None]

    return {
        "city":      city["name"],
        "temp_max":  round(avg_temp, 1),
        "precip_mm": round(sum(precips)/len(precips), 1) if precips else 0.0,
        "spread":    round(spread, 2),
        "consensus": consensus,
        "sources":   len(forecasts),
    }

# ══════════════════════════════════════════════════════════════════════════════
# ── PolymarketClient ─────────────────────────────────────────────────────────
# Architecture 3-API officielle (mars 2026) :
#
#   GAMMA  gamma-api.polymarket.com   → get_markets()
#   CLOB   clob.polymarket.com        → get_orderbook(), place_order()
#   DATA   data-api.polymarket.com    → get_positions(), get_redeemable_positions()
#
# Redeem → on-chain CTF contract via web3.py  (pas d'endpoint REST dédié)
# ══════════════════════════════════════════════════════════════════════════════
class PolymarketClient:

    def __init__(self):
        self.session = build_session()
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
        """Web3 pour le redeem on-chain via contrat CTF Polygon."""
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

    # ── get_markets ─────────────────────────────────────────────────────────
    # Source : GAMMA API (découverte publique, pas d'auth)
    # Fallback : CLOB /markets si GAMMA indisponible
    def get_markets(self, keyword: str = "weather") -> list:
        """
        Récupère les marchés météo actifs.
        Endpoint principal : gamma-api.polymarket.com/markets
        Fallback           : clob.polymarket.com/markets
        """
        # Tentative 1 : GAMMA (source officielle pour la découverte)
        data = safe_api_call(
            self.session, "GET", f"{GAMMA_BASE}/markets",
            label="gamma/get_markets",
            params={
                "tag": "weather",         # tag Polymarket officiel
                "active": "true",
                "closed": "false",
                "limit": "100",
            },
        )
        if data is not None:
            markets = data if isinstance(data, list) else data.get("markets", [])
            # Filtre textuel si tag insuffisant
            filtered = [
                m for m in markets
                if keyword.lower() in (m.get("question", "") + m.get("groupItemTitle", "")).lower()
            ]
            log.info(f"  📡 [gamma/get_markets] {len(filtered)} marchés météo")
            return filtered

        # Fallback : CLOB /markets
        log.warning("  ↩️  Fallback → clob/get_markets")
        data2 = safe_api_call(
            self.session, "GET", f"{CLOB_BASE}/markets",
            label="clob/get_markets(fallback)",
            params={"keyword": keyword, "active": "true", "closed": "false"},
        )
        if data2 is None:
            return []
        return data2.get("data", []) if isinstance(data2, dict) else data2

    # ── get_orderbook ────────────────────────────────────────────────────────
    # Source : CLOB API uniquement (orderbook n'existe que là)
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
                "orderType":  "GTC",
                "tokenID":    token_id,
                "side":       side,
                "price":      str(round(price, 4)),
                "size":       str(round(size, 2)),
                "funder":     self.address,
                "maker":      self.address,
                "expiration": "0",
            }
            # TODO production : signer EIP-712 via py-clob-client
            log.info(f"[ORDRE] {side} {size:.2f} USDC @ {price:.3f}")
            return order
        except Exception as e:
            log.error(f"Erreur place_order: {e}")
            return None

    # ── get_positions ────────────────────────────────────────────────────────
    # Source : DATA API (data-api.polymarket.com/positions?user=...)
    # Fallback : CLOB /positions si DATA indisponible
    def get_positions(self) -> list:
        """
        Positions ouvertes du wallet.
        Endpoint principal : data-api.polymarket.com/positions
        Fallback           : clob.polymarket.com/positions
        """
        if not self.address:
            return []

        # Tentative 1 : DATA API (source correcte selon docs officielles)
        data = safe_api_call(
            self.session, "GET", f"{DATA_BASE}/positions",
            label="data/get_positions",
            params={"user": self.address, "sizeThreshold": "0"},
        )
        if data is not None:
            positions = data if isinstance(data, list) else data.get("data", [])
            log.info(f"  📡 [data/get_positions] {len(positions)} positions")
            return positions

        # Fallback : CLOB (peut être 404 mais on tente)
        log.warning("  ↩️  Fallback → clob/get_positions")
        data2 = safe_api_call(
            self.session, "GET", f"{CLOB_BASE}/positions",
            label="clob/get_positions(fallback)",
            params={"user": self.address},
        )
        if data2 is None:
            return []
        return data2.get("data", []) if isinstance(data2, dict) else []

    # ── get_redeemable_positions ─────────────────────────────────────────────
    # Source : DATA API — le champ `redeemable: true` est dans la réponse
    # Pas de param ?redeemable= : on filtre côté client sur le champ booléen
    def get_redeemable_positions(self) -> list:
        """
        Positions redeemables (marchés résolus avec gains).

        Historique des erreurs corrigées ici :
          v1.0 → clob.polymarket.com/positions?redeemable=true  → 404
          v1.1 → clob.polymarket.com/positions?redeemable=true  → ReadTimeout
          v1.2 → idem + JSON vide                               → JSONDecodeError
          v1.3 → data-api.polymarket.com/positions + filtre     → ✅

        Architecture :
          1. data-api /positions?user=...  (source officielle)
          2. Filtre Python sur `pos["redeemable"] == True`
          3. Fallback : data-api /activity?type=REDEEM (vérification croisée)
        """
        if not self.address:
            return []

        log.info("  🔍 Recherche positions redeemables (data-api)…")

        # Étape 1 : toutes les positions via DATA API
        all_positions = safe_api_call(
            self.session, "GET", f"{DATA_BASE}/positions",
            label="data/redeemable_positions",
            params={"user": self.address, "sizeThreshold": "0"},
        )

        if all_positions is not None:
            positions = all_positions if isinstance(all_positions, list) \
                        else all_positions.get("data", [])
            # Filtre : redeemable = True ET size > 0
            redeemable = [
                p for p in positions
                if p.get("redeemable") is True and float(p.get("size", 0)) > 0
            ]
            log.info(
                f"  📊 [data/positions] {len(positions)} totales, "
                f"{len(redeemable)} redeemables"
            )
            return redeemable

        # Fallback : activity endpoint pour croiser avec les REDEEM passés
        log.warning("  ↩️  data-api/positions indisponible — fallback activity")
        activity = safe_api_call(
            self.session, "GET", f"{DATA_BASE}/activity",
            label="data/activity(fallback_redeem)",
            params={
                "user": self.address,
                "type": "REDEEM",
                "limit": "50",
            },
        )
        if activity is None:
            log.warning(
                "  ⚠️  get_redeemable_positions : API Polymarket unavailable — "
                "redeem ignoré ce cycle, sera retenté au prochain."
            )
            return []

        # Convertit les activités REDEEM en format position compatible
        acts = activity if isinstance(activity, list) else activity.get("data", [])
        log.info(f"  📡 [data/activity] {len(acts)} REDEEM récents trouvés")
        return [
            {
                "conditionId": a.get("conditionId", ""),
                "redeemable": True,
                "cashPnl": float(a.get("usdcSize", 0)),
                "title": a.get("title", "?"),
                "market": {"question": a.get("title", "?")},
            }
            for a in acts
        ]

    # ── redeem_position ──────────────────────────────────────────────────────
    # Redeem on-chain via contrat CTF Polygon (web3.py)
    # Note : pas d'endpoint REST dédié (cf. GitHub issue #139, juillet 2025)
    def redeem_position(self, condition_id: str, outcome_index: int = 0) -> bool:
        """
        Redeem on-chain via le contrat ConditionalTokens (CTF) sur Polygon.
        index_sets : [1] = outcome 0 (Yes), [2] = outcome 1 (No), [3] = les deux
        """
        if not self.account:
            log.info(f"[SIMULATION] Redeem {condition_id[:12]}…")
            return True

        if not self.w3:
            log.warning("web3 non disponible → redeem simulé")
            return True

        try:
            # ABI minimal pour redeemPositions
            ctf_abi = [{
                "name": "redeemPositions",
                "type": "function",
                "inputs": [
                    {"name": "collateralToken",    "type": "address"},
                    {"name": "parentCollectionId", "type": "bytes32"},
                    {"name": "conditionId",        "type": "bytes32"},
                    {"name": "indexSets",          "type": "uint256[]"},
                ],
                "outputs": [],
                "stateMutability": "nonpayable",
            }]
            ctf = self.w3.eth.contract(
                address=self.w3.to_checksum_address(CTF_ADDRESS),
                abi=ctf_abi,
            )
            # index_sets : 1 = Yes (bit 0), 2 = No (bit 1)
            index_sets = [1, 2]
            tx = ctf.functions.redeemPositions(
                self.w3.to_checksum_address(USDC_ADDRESS),
                bytes.fromhex(HASH_ZERO[2:]),
                bytes.fromhex(condition_id.replace("0x", "")),
                index_sets,
            ).build_transaction({
                "from":  self.account.address,
                "nonce": self.w3.eth.get_transaction_count(self.account.address),
                "gas":   200_000,
                "gasPrice": self.w3.eth.gas_price,
            })
            signed = self.w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            if receipt.status == 1:
                log.info(f"  ✅ Redeem on-chain OK — tx: {tx_hash.hex()[:20]}…")
                return True
            else:
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
    if not outcomes:
        return []
    temp = forecast["temp_max"]
    bins_with_ev = []
    for outcome in outcomes:
        title          = outcome.get("title", "")
        bin_low, bin_high = parse_temp_range(title)
        if bin_low is None:
            continue
        market_price = float(outcome.get("price", 0.5))
        prob  = temperature_to_prob(temp, bin_low, bin_high, forecast["spread"])
        ev    = compute_ev(prob, market_price)
        bins_with_ev.append({
            "outcome_id":   outcome.get("id", ""),
            "token_id":     outcome.get("clobTokenIds", [""])[0],
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
        # Utilise get_redeemable_positions() — DATA API + filtre Python
        resolved = client.get_redeemable_positions()
        if not resolved:
            return 0.0

        log.info(f"💎 {len(resolved)} position(s) redeemable(s) détectée(s)")
        for pos in resolved:
            condition_id = pos.get("conditionId", "")
            # cashPnl = PnL en USDC selon DATA API
            pnl          = float(pos.get("cashPnl", pos.get("pnl", 0)))
            market_name  = (
                pos.get("title")
                or pos.get("market", {}).get("question", "?")
            )
            city_hint = next(
                (c["name"] for c in CITIES if c["name"].lower() in market_name.lower()),
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
        log.error(f"Erreur auto_redeem (hors réseau): {e}")

    return total_redeemed

# ── Utilitaires marchés ────────────────────────────────────────────────────────
def match_market_to_city(market: dict) -> dict | None:
    text = (
        market.get("question", "") + " " +
        market.get("groupItemTitle", "") + " " +
        market.get("description", "")
    ).lower()
    return next((c for c in CITIES if c["name"].lower() in text), None)

def is_temperature_market(market: dict) -> bool:
    q = (market.get("question", "") + " " + market.get("groupItemTitle", "")).lower()
    return any(k in q for k in [
        "temperature", "temp", "degrees", "°c", "°f",
        "celsius", "fahrenheit", "high", "low", "max", "min", "average",
    ])

# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    log.info("🌤️  Tirelire Météo Pension v1.3 démarrée — Ultra-safe mode")
    log.info(f"📂 Données dans : {DATA_DIR}")
    log.info(f"⏱️  Scan toutes les {SCAN_INTERVAL//60} minutes")
    log.info(f"🔁 API config : timeout={API_TIMEOUT_READ}s, retries={len(API_RETRY_DELAYS)}, backoff={API_RETRY_DELAYS}")
    log.info(f"🗺️  Endpoints : GAMMA={GAMMA_BASE} | DATA={DATA_BASE} | CLOB={CLOB_BASE}")

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
                log.info(
                    f"  ⏭️  {city['name']} : consensus {forecast['consensus']} "
                    f"({forecast['sources']} sources) → skip"
                )
                continue

            month         = datetime.utcnow().month
            conf_factor   = get_confidence_factor(city["name"], month, cal)
            selected_bins = select_bins(market, forecast)

            if len(selected_bins) < MIN_BINS:
                log.info(
                    f"  ⏭️  {city['name']} : {len(selected_bins)} bins EV>10% "
                    f"(besoin de {MIN_BINS}) → skip"
                )
                continue

            total_bet   = compute_bet_size(state["balance"])
            total_ev    = sum(b["ev"] for b in selected_bins)
            market_name = market.get("question", "?")[:50]

            log.info(f"  🎯 {city['name']} | {market_name}")
            log.info(
                f"     Prévision: {forecast['temp_max']}°C | "
                f"Spread: {forecast['spread']}°C | Consensus: {forecast['consensus']}"
            )
            log.info(
                f"     Mise totale: {total_bet:.2f} USDC sur "
                f"{len(selected_bins)} bins | Facteur confiance: {conf_factor}"
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
                        f"     ✅ Bin [{b['title']}] → {bin_bet:.2f} USDC "
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
