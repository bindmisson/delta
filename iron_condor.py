# INSTITUTIONAL IRON CONDOR ENGINE — DELTA EXCHANGE
# Port of angel000/bot/iron_condor.py for BTC / ETH options
# ============================================================
# FEATURES
# ------------------------------------------------------------
# ✅ BTC + ETH options (Delta Exchange India)
# ✅ Daily expiry @ 17:30 IST
# ✅ Net Delta Risk Engine
# ✅ Expiry Day Mode
# ✅ CE/PE Side Rolling
# ✅ Bias Based Entry
# ✅ Breakeven Management
# ✅ Gamma Panic Exit
# ✅ JSON Persistence
# ✅ Expiry Cleanup
# ✅ Structured Logging
# ✅ FastAPI Webhook
# ============================================================

import os
import json
import time
import pytz
import logging
import requests

from dotenv import load_dotenv
from fastapi import FastAPI
from threading import Thread, Semaphore
from datetime import datetime, timedelta, date, time as dtime
from logging.handlers import RotatingFileHandler

import uvicorn

try:
    from delta_rest_client import DeltaRestClient, OrderType, TimeInForce
except ImportError:  # pragma: no cover
    DeltaRestClient = None
    OrderType = None
    TimeInForce = None

load_dotenv()

# ============================================================
# APP
# ============================================================

app = FastAPI()

# ============================================================
# TZ
# ============================================================

IST = pytz.timezone("Asia/Kolkata")

# ============================================================
# CONFIG
# ============================================================

CONFIG = {

    # Delta Exchange India (override via .env)
    "DELTA_BASE_URL": os.getenv(
        "DELTA_BASE_URL",
        "https://api.india.delta.exchange",
    ),
    "DELTA_API_KEY": os.getenv("DELTA_API_KEY", ""),
    "DELTA_API_SECRET": os.getenv("DELTA_API_SECRET", ""),
    # When true (or keys missing), orders are logged but not sent
    "DRY_RUN": os.getenv("DRY_RUN", "true").lower()
        in ("1", "true", "yes"),

    # API LIMIT
    "API_DELAY": 1,
    "API_RETRY": 3,
    "API_RETRY_DELAY": 1,

    # MONITOR
    "SAFE_INTERVAL": 60,
    "DANGER_INTERVAL": 30,
    "EXPIRY_INTERVAL": 15,

    "DELTA_PREPARE": 0.20,
    "DELTA_ADJUST": 0.33,
    "DELTA_AGGRESSIVE": 0.50,
    "DELTA_PANIC": 0.75,

    # ========================================================
    # EXPIRY DAY (same-day daily options)
    # ========================================================
    "EXPIRY_DELTA_PREPARE": 0.30,
    "EXPIRY_DELTA_ADJUST": 0.5,
    "EXPIRY_DELTA_AGGRESSIVE": 0.65,
    "EXPIRY_DELTA_PANIC": 0.80,

    # GAMMA
    "PANIC_GAMMA": 0.008,
    "EXPIRY_PANIC_GAMMA": 0.005,

    # BREAKEVEN (USD points)
    "BREAKEVEN_BUFFER": 150,
    "EXPIRY_BE_BUFFER": 250,

    # PROFIT BOOKING
    "PROFIT_TARGET_PERCENT": 85,
    "EXPIRY_PROFIT_TARGET_PERCENT": 80,
    # On expiry day after this IST time → trade next daily expiry
    "EXPIRY_NEXT_WEEK_TIME": "12:55",
    # Hard settlement clock for Delta daily options
    "EXPIRY_SETTLEMENT_TIME": "17:30",

    # ADJUSTMENT
    "MAX_ADJUSTMENTS": 6,
    "EXPIRY_MAX_ADJUSTMENTS": 10,
    "ADJUSTMENT_COOLDOWN": 150,

    # STEPPED TRAILING STOP
    "TSL_START": 70,
    "TSL_LOCK": 50,
    "TSL_STEP": 5,

    # Day-based PnL target (USD credit * contracts)
    # Day N → target = DAY_PNL_BASE_USD * N
    "DAY_PNL_ENABLED": True,
    "DAY_PNL_BASE_RUPEE": float(os.getenv("DAY_PNL_BASE_USD", "15")),
    "DAY_PNL_INCLUDE_ENTRY_DAY": True,
    "DAY_PNL_REENTER": True,

    # Crypto options nearly 24x7; cut off just before settlement
    "MARKET_START": "00:05",
    "MARKET_END": "17:25",

    "WEBHOOK_PORT": int(os.getenv("WEBHOOK_PORT", "9000")),
}

# ============================================================
# INDEX CONFIG
# ============================================================

INDEX_CONFIG = {

    "BTC": {
        "exchange": "DELTA",
        "underlying_symbol": "BTCUSD",
        "strike_step": 200,
        "lot_size": int(os.getenv("BTC_LOT_SIZE", "1")),
        # Daily expiry
        "expiry_day": "daily",
        "ce_distance": 400,
        "pe_distance": 400,
        "CE_BIAS_CE_DISTANCE": 600,
        "CE_BIAS_PE_DISTANCE": 200,
        "PE_BIAS_CE_DISTANCE": 200,
        "PE_BIAS_PE_DISTANCE": 600,
        "hedge_distance": 2000,
        "be_buffer": 150,
        "expiry_be_buffer": 250,
    },

    "ETH": {
        "exchange": "DELTA",
        "underlying_symbol": "ETHUSD",
        "strike_step": 10,
        "lot_size": int(os.getenv("ETH_LOT_SIZE", "1")),
        "expiry_day": "daily",
        "ce_distance": 20,
        "pe_distance": 20,
        "CE_BIAS_CE_DISTANCE": 30,
        "CE_BIAS_PE_DISTANCE": 10,
        "PE_BIAS_CE_DISTANCE": 10,
        "PE_BIAS_PE_DISTANCE": 30,
        "hedge_distance": 100,
        "be_buffer": 8,
        "expiry_be_buffer": 12,
    },
}

# ============================================================
# HOLIDAYS
# ============================================================

# Delta crypto options trade daily; no NSE holiday calendar
HOLIDAYS = set()
# ============================================================
# GLOBALS
# ============================================================

POSITIONS = {}
PENDING_ROLLS = {}
# Latest bias from webhook — used on every create_condor / re-entry
CURRENT_BIAS = {}
API_SEMAPHORE = Semaphore(1)
DELTA_CLIENT = None
# Cache: (underlying, expiry_ddmmyy) -> sorted strike floats
STRIKE_CACHE = {}
# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger("IC")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    "ic_engine.log",
    maxBytes=20 * 1024 * 1024,
    backupCount=5
)

logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())

# ============================================================
# HELPERS
# ============================================================
def build_option_symbol(
    symbol,
    expiry,
    strike,
    option_type
):
    """
    Delta option symbol:
      C-BTC-64800-310726 / P-ETH-1920-310726
    expiry is DDMMYY; option_type is CE/PE or C/P.
    """
    right = str(option_type).upper()
    if right in ("CE", "C", "CALL"):
        prefix = "C"
    elif right in ("PE", "P", "PUT"):
        prefix = "P"
    else:
        raise ValueError(f"bad option_type: {option_type}")

    strike_i = int(round(float(strike)))
    return f"{prefix}-{symbol}-{strike_i}-{expiry}"


def now_ist():
    return datetime.now(IST)


def format_expiry(day):
    """Delta daily expiry date tag: DDMMYY."""
    return day.strftime("%d%m%y")


def parse_expiry(expiry_str):
    return datetime.strptime(expiry_str, "%d%m%y").date()


def settlement_time():
    return datetime.strptime(
        CONFIG["EXPIRY_SETTLEMENT_TIME"],
        "%H:%M",
    ).time()


def round_to_step(price, step):
    return int(round(float(price) / step) * step)


def expiry_to_api_date(expiry_str):
    """DDMMYY → DD-MM-YYYY for Delta ticker filters."""
    day = parse_expiry(expiry_str)
    return day.strftime("%d-%m-%Y")


def get_available_strikes(symbol, expiry):
    """Listed call/put strikes for underlying + daily expiry."""
    key = (symbol, expiry)
    cached = STRIKE_CACHE.get(key)
    if cached:
        return cached

    api_date = expiry_to_api_date(expiry)
    path = (
        "/v2/tickers"
        f"?contract_types=call_options,put_options"
        f"&underlying_asset_symbols={symbol}"
        f"&expiry_date={api_date}"
    )
    data = _public_get(path)
    strikes = set()
    if data:
        for row in data:
            sp = row.get("strike_price")
            if sp is None:
                continue
            try:
                strikes.add(float(sp))
            except (TypeError, ValueError):
                continue

    ordered = sorted(strikes)
    if ordered:
        STRIKE_CACHE[key] = ordered
    return ordered


def snap_strike(symbol, expiry, strike, prefer="nearest"):
    """
    Snap a theoretical strike onto a listed Delta strike.
    prefer: nearest | up | down
    """
    target = float(strike)
    strikes = get_available_strikes(symbol, expiry)
    if not strikes:
        return int(round(target))

    if prefer == "up":
        for s in strikes:
            if s >= target:
                return int(round(s))
        return int(round(strikes[-1]))

    if prefer == "down":
        for s in reversed(strikes):
            if s <= target:
                return int(round(s))
        return int(round(strikes[0]))

    best = min(strikes, key=lambda s: (abs(s - target), s))
    return int(round(best))



def snap_candidate_list(symbol, expiry, candidates, prefer="nearest"):
    snapped = []
    seen = set()
    for c in candidates:
        s = snap_strike(symbol, expiry, c, prefer=prefer)
        if s in seen:
            continue
        seen.add(s)
        snapped.append(s)
    return snapped


def get_delta_client():
    global DELTA_CLIENT
    if DELTA_CLIENT is not None:
        return DELTA_CLIENT

    if DeltaRestClient is None:
        raise RuntimeError(
            "delta-rest-client not installed"
        )

    key = CONFIG.get("DELTA_API_KEY") or ""
    secret = CONFIG.get("DELTA_API_SECRET") or ""
    DELTA_CLIENT = DeltaRestClient(
        base_url=CONFIG["DELTA_BASE_URL"],
        api_key=key or None,
        api_secret=secret or None,
        raise_for_status=True,
    )
    return DELTA_CLIENT
def calculate_net_delta(position):

    return (

        # SHORT CE
        - position["ce_short"]["delta"]

        # SHORT PE
        - position["pe_short"]["delta"]

        # LONG CE
        + position["ce_buy"]["delta"]

        # LONG PE
        + position["pe_buy"]["delta"]

    )


def get_tsl_tier_for_peak(peak):
    start = CONFIG.get("TSL_START")
    lock_base = CONFIG.get("TSL_LOCK")
    step_inc = CONFIG.get("TSL_STEP")
    if start is None or lock_base is None or step_inc is None:
        return None, None
    if step_inc <= 0:
        return None, None

    profit_cap = CONFIG.get(
        "PROFIT_TARGET_PERCENT", 100
    )
    active_step = None
    lock = None
    tier = 0
    while True:
        step_threshold = start + tier * step_inc
        lock_floor = lock_base + tier * step_inc
        if step_threshold >= profit_cap:
            break
        if peak >= step_threshold:
            active_step = step_threshold
            lock = lock_floor
            tier += 1
        else:
            break

    return active_step, lock


def update_trailing_stop_from_steps(position):
    peak = position.get("max_capture_percent", 0) or 0
    active_step, lock = get_tsl_tier_for_peak(peak)
    if lock is None:
        return

    current_lock = position.get("tsl_lock")
    if current_lock is None:
        current_lock = position.get("trailing_stop_percent")
    if current_lock is None or lock > current_lock:
        position["tsl_lock"] = lock
        position["tsl_step"] = active_step
        position.pop("trailing_stop_percent", None)


def is_exchange_trading_day(day):
    """Crypto daily options — every calendar day counts for day-PNL."""
    return day not in HOLIDAYS


def count_held_trading_days(entry_date, asof_date=None):
    """Trading days held inclusive of entry_date through asof_date."""
    if asof_date is None:
        asof_date = now_ist().date()

    if isinstance(entry_date, datetime):
        entry_date = entry_date.date()

    if entry_date > asof_date:
        return 0

    days = 0
    cursor = entry_date
    while cursor <= asof_date:
        if is_exchange_trading_day(cursor):
            days += 1
        cursor += timedelta(days=1)

    return days


def get_day_pnl_day_number(position):
    """
    Day N for day-PNL ladder.
    INCLUDE_ENTRY_DAY=True  → entry day = 1, next = 2, …
    INCLUDE_ENTRY_DAY=False → entry day = 0 (no exit), next = 1, …
    """
    include_entry = CONFIG.get("DAY_PNL_INCLUDE_ENTRY_DAY", True)
    raw = position.get("entry_time")
    if not raw:
        return 1 if include_entry else 0

    try:
        entry_dt = datetime.fromisoformat(raw)
        if entry_dt.tzinfo is None:
            entry_dt = IST.localize(entry_dt)
        else:
            entry_dt = entry_dt.astimezone(IST)
        entry_date = entry_dt.date()
    except Exception:
        return 1 if include_entry else 0

    held = count_held_trading_days(entry_date)
    if include_entry:
        return max(held, 1)

    return max(held - 1, 0)


def get_day_pnl_target_rupee(position):
    """
    Day N target ₹ = DAY_PNL_BASE_RUPEE * N.
    Returns (target_rupee, day_n) or None if exit should not fire.
    """
    day_n = get_day_pnl_day_number(position)
    if day_n <= 0:
        return None

    base = float(CONFIG.get("DAY_PNL_BASE_RUPEE", 1500) or 0)
    if base <= 0:
        return None

    return base * day_n, day_n


def reenter_full_condor(symbol, reason):
    """
    Close full 4-leg condor and open a fresh one via normal create_condor
    (current webhook bias + live spot) — no /iron entry required.
    """
    position = POSITIONS.get(symbol) or {}
    if not position.get("active"):
        log_event(
            "REENTER",
            "SKIP — NO ACTIVE POSITION",
            {"symbol": symbol, "reason": reason},
        )
        return False

    bias = get_current_bias(symbol)
    old_expiry = position.get("expiry")

    log_event(
        "REENTER",
        "DAY PNL CLOSE — STARTING NORMAL RE-ENTRY",
        {
            "symbol": symbol,
            "reason": reason,
            "bias": bias,
            "old_expiry": old_expiry,
        },
    )

    close_position(symbol)
    time.sleep(2)

    if not CONFIG.get("DAY_PNL_REENTER", True):
        log_event(
            "REENTER",
            "RE-ENTRY DISABLED — WAITING FOR NORMAL SIGNAL",
            {"symbol": symbol, "reason": reason},
        )
        return True

    if not is_market_open():
        log_event(
            "REENTER",
            "MARKET CLOSED — RE-ENTRY SKIPPED",
            {"symbol": symbol, "reason": reason},
        )
        return False

    try:
        spot = get_live_spot(symbol)
        create_condor(symbol, bias, spot)

        log_event(
            "REENTER",
            "CONDOR RE-ENTERED (NORMAL PATH)",
            {
                "symbol": symbol,
                "reason": reason,
                "bias": bias,
                "spot": spot,
                "new_expiry": POSITIONS.get(
                    symbol, {}
                ).get("expiry"),
                "entry_credit": POSITIONS.get(
                    symbol, {}
                ).get("entry_credit"),
            },
        )
        return True

    except Exception as e:
        log_event(
            "ERROR",
            "DAY PNL RE-ENTRY FAILED",
            {
                "symbol": symbol,
                "reason": reason,
                "bias": bias,
                "error": str(e),
            },
        )
        return False


def log_event(category, message, extra=None):

    payload = {
        "time": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        "category": category,
        "message": message,
        "extra": extra
    }

    logger.info(json.dumps(payload))



def ensure_position_dir():
    os.makedirs("positions", exist_ok=True)



def get_position_file(symbol):
    return f"positions/{symbol}.json"



def save_position(symbol):

    ensure_position_dir()

    with open(get_position_file(symbol), "w") as f:
        json.dump(POSITIONS[symbol], f, indent=2)

def load_positions():

    ensure_position_dir()

    for symbol in INDEX_CONFIG.keys():

        try:

            file_path = get_position_file(symbol)

            if not os.path.exists(file_path):

                POSITIONS[symbol] = {
                    "active": False
                }

                continue

            with open(file_path, "r") as f:

                position = json.load(f)

            POSITIONS[symbol] = position

            log_event(
                "SYSTEM",
                "POSITION LOADED",
                {
                    "symbol": symbol,
                    "active": position.get(
                        "active"
                    )
                }
            )

        except Exception as e:

            POSITIONS[symbol] = {
                "active": False
            }

            log_event(
                "ERROR",
                "LOAD POSITION ERROR",
                {
                    "symbol": symbol,
                    "error": str(e)
                }
            )

def delete_position_file(symbol):

    try:

        path = get_position_file(symbol)

        if os.path.exists(path):
            os.remove(path)

    except Exception as e:

        log_event(
            "ERROR",
            "DELETE POSITION FILE ERROR",
            {"error": str(e)}
        )


# ============================================================
# MARKET
# ============================================================



def ensure_state_dir():
    os.makedirs("state", exist_ok=True)


def ensure_signals_dir():
    os.makedirs("signals", exist_ok=True)


def get_bias_file(symbol):
    return f"signals/{symbol}.json"


def normalize_bias(bias):
    value = str(bias or "NONE").strip().upper()
    if value in ("CE", "PE", "NONE"):
        return value
    return "NONE"


def get_current_bias(symbol):
    """Latest bot-managed bias (webhook). Defaults to NONE."""
    return normalize_bias(
        CURRENT_BIAS.get(symbol)
        or "NONE"
    )


def set_current_bias(symbol, bias, source="webhook"):
    """Persist bias so auto entries always use the latest signal."""
    value = normalize_bias(bias)
    CURRENT_BIAS[symbol] = value

    ensure_signals_dir()
    path = get_bias_file(symbol)
    state = {}
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                state = json.load(f) or {}
        except Exception:
            state = {}

    state["last_webhook_bias"] = value
    state["bias"] = value
    state["last_signal_at"] = now_ist().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    state["bias_source"] = source

    with open(path, "w") as f:
        json.dump(state, f, indent=2)

    # Keep armed next-week roll in sync with latest bias
    if symbol in PENDING_ROLLS:
        PENDING_ROLLS[symbol]["bias"] = value
        save_pending_roll(symbol)

    log_event(
        "BIAS",
        "CURRENT BIAS UPDATED",
        {
            "symbol": symbol,
            "bias": value,
            "source": source,
        },
    )
    return value


def load_biases():
    """Load saved bias; seed from open position if missing."""
    ensure_signals_dir()
    for symbol in INDEX_CONFIG.keys():
        path = get_bias_file(symbol)
        bias = None

        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    state = json.load(f) or {}
                bias = (
                    state.get("last_webhook_bias")
                    or state.get("bias")
                )
            except Exception as e:
                log_event(
                    "ERROR",
                    "LOAD BIAS ERROR",
                    {
                        "symbol": symbol,
                        "error": str(e),
                    },
                )

        if not bias:
            pos = POSITIONS.get(symbol) or {}
            if pos.get("active"):
                bias = pos.get("bias")

        CURRENT_BIAS[symbol] = normalize_bias(
            bias or "NONE"
        )
        log_event(
            "SYSTEM",
            "BIAS LOADED",
            {
                "symbol": symbol,
                "bias": CURRENT_BIAS[symbol],
            },
        )


def get_pending_roll_file(symbol):
    return f"state/{symbol}_pending_roll.json"


def save_pending_roll(symbol):
    ensure_state_dir()
    pending = PENDING_ROLLS.get(symbol)
    path = get_pending_roll_file(symbol)
    if not pending:
        if os.path.exists(path):
            os.remove(path)
        return
    with open(path, "w") as f:
        json.dump(pending, f, indent=2)


def clear_pending_roll(symbol):
    PENDING_ROLLS.pop(symbol, None)
    save_pending_roll(symbol)


def load_pending_rolls():
    ensure_state_dir()
    for symbol in INDEX_CONFIG.keys():
        path = get_pending_roll_file(symbol)
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r") as f:
                pending = json.load(f)
            PENDING_ROLLS[symbol] = pending
            log_event(
                "SYSTEM",
                "PENDING ROLL LOADED",
                {"symbol": symbol, **pending},
            )
        except Exception as e:
            log_event(
                "ERROR",
                "LOAD PENDING ROLL ERROR",
                {"symbol": symbol, "error": str(e)},
            )


def get_switch_time():
    return datetime.strptime(
        CONFIG["EXPIRY_NEXT_WEEK_TIME"],
        "%H:%M",
    ).time()


def arm_pending_next_week_roll(symbol, bias, closed_expiry, reason="EXPIRY_TARGET_BEFORE_ROLL"):
    """Arm next-week entry when expiry-day book-out happens before EXPIRY_NEXT_WEEK_TIME."""
    pending = {
        "bias": bias or "NONE",
        "closed_expiry": closed_expiry,
        "armed_date": now_ist().date().isoformat(),
        "armed_at": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        "reason": reason,
    }
    PENDING_ROLLS[symbol] = pending
    save_pending_roll(symbol)
    log_event(
        "EXPIRY_SHIFT",
        "PENDING NEXT WEEK ROLL ARMED",
        {"symbol": symbol, **pending},
    )
    return maybe_execute_pending_next_week(symbol)


def maybe_execute_pending_next_week(symbol):
    """At/after EXPIRY_NEXT_WEEK_TIME on the armed expiry day, open next-week condor."""
    pending = PENDING_ROLLS.get(symbol)
    if not pending:
        return False

    now = now_ist()
    armed_date = pending.get("armed_date")
    if armed_date and now.date().isoformat() != armed_date:
        log_event(
            "EXPIRY_SHIFT",
            "PENDING NEXT WEEK ROLL EXPIRED",
            {"symbol": symbol, **pending},
        )
        clear_pending_roll(symbol)
        return False

    if now.time() < get_switch_time():
        return False

    position = POSITIONS.get(symbol) or {}
    if position.get("active"):
        clear_pending_roll(symbol)
        return False

    closed_expiry = pending.get("closed_expiry")
    if not closed_expiry:
        clear_pending_roll(symbol)
        return False

    target_expiry = get_next_expiry_after(closed_expiry)
    # Always prefer latest webhook bias at entry time
    bias = get_current_bias(symbol)
    spot = get_live_spot(symbol)

    log_event(
        "EXPIRY_SHIFT",
        "PENDING NEXT WEEK ROLL EXECUTED",
        {
            "symbol": symbol,
            "old_expiry": closed_expiry,
            "new_expiry": target_expiry,
            "bias": bias,
            "spot": spot,
            "reason": pending.get("reason"),
        },
    )

    clear_pending_roll(symbol)
    create_condor(
        symbol,
        bias,
        spot,
        expiry=target_expiry,
    )
    return True


def is_market_open():
    """Open until MARKET_END IST (before 17:30 settlement)."""
    now = now_ist()
    current = now.strftime("%H:%M")
    return (
        CONFIG["MARKET_START"]
        <= current
        <= CONFIG["MARKET_END"]
    )


# ============================================================
# EXPIRY
# ============================================================


def is_trading_day(day):
    return day not in HOLIDAYS


def previous_trading_day(day):
    while not is_trading_day(day):
        day -= timedelta(days=1)
    return day


def get_nearest_expiry(symbol):
    """
    Nearest daily expiry tag (DDMMYY).
    After 17:30 IST settlement → tomorrow.
    """
    _ = symbol
    now = now_ist()
    today = now.date()

    if now.time() >= settlement_time():
        day = previous_trading_day(
            today + timedelta(days=1)
        )
    else:
        day = previous_trading_day(today)

    return format_expiry(day)


def get_next_expiry(symbol):
    """Next daily expiry after the nearest one."""
    nearest = parse_expiry(get_nearest_expiry(symbol))
    nxt = previous_trading_day(
        nearest + timedelta(days=1)
    )
    return format_expiry(nxt)


def get_next_expiry_after(expiry_str):
    expiry_date = parse_expiry(expiry_str)
    nxt = previous_trading_day(
        expiry_date + timedelta(days=1)
    )
    return format_expiry(nxt)


def shift_expiry_if_needed(symbol):

    position = POSITIONS.get(symbol)

    if not position or not position.get("active"):
        return False

    pos_expiry = position.get("expiry")

    if not pos_expiry:
        return False

    active_expiry = get_active_expiry(symbol)

    if pos_expiry == active_expiry:
        return False

    target_expiry = get_next_expiry_after(
        pos_expiry
    )

    log_event(
        "EXPIRY_SHIFT",
        "NEXT EXPIRY SHIFT",
        {
            "symbol": symbol,
            "old_expiry": pos_expiry,
            "new_expiry": target_expiry,
            "active_expiry": active_expiry,
        },
    )

    spot = get_live_spot(symbol)
    bias = get_current_bias(symbol)

    close_position(symbol)

    time.sleep(2)

    create_condor(
        symbol,
        bias,
        spot,
        expiry=target_expiry,
    )

    return True


def get_active_expiry(symbol):

    now = now_ist()

    current_expiry = get_nearest_expiry(symbol)

    expiry_date = parse_expiry(current_expiry)

    switch_time = datetime.strptime(
        CONFIG["EXPIRY_NEXT_WEEK_TIME"],
        "%H:%M"
    ).time()

    # ====================================================
    # EXPIRY DAY AFTER 1PM
    # ====================================================

    if (
        now.date() == expiry_date
        and now.time() >= switch_time
    ):

        return get_next_expiry(symbol)

    return current_expiry


def get_dte(symbol):

    expiry = get_active_expiry(symbol)

    expiry_date = parse_expiry(expiry)

    return (
        expiry_date - now_ist().date()
    ).days


# ============================================================
# API / BROKER (Delta Exchange)
# ============================================================


def _public_get(path):
    url = CONFIG["DELTA_BASE_URL"].rstrip("/") + path
    for attempt in range(CONFIG["API_RETRY"]):
        try:
            time.sleep(CONFIG["API_DELAY"] * 0.25)
            response = requests.get(
                url,
                timeout=15,
                headers={"Accept": "application/json"},
            )
            if response.status_code != 200:
                log_event(
                    "ERROR",
                    "API STATUS ERROR",
                    {
                        "status": response.status_code,
                        "path": path,
                    },
                )
                time.sleep(CONFIG["API_RETRY_DELAY"])
                continue
            data = response.json()
            if not data.get("success"):
                log_event(
                    "ERROR",
                    "API RESULT ERROR",
                    {"path": path, "data": data},
                )
                time.sleep(CONFIG["API_RETRY_DELAY"])
                continue
            return data.get("result")
        except Exception as e:
            log_event(
                "ERROR",
                "API ERROR",
                {
                    "attempt": attempt + 1,
                    "path": path,
                    "error": str(e),
                },
            )
            time.sleep(CONFIG["API_RETRY_DELAY"])
    return None


def place_order(exchange, action, symbol, quantity):
    """
    Market order on Delta.
    exchange kept for signature parity with angel bot.
    """
    _ = exchange
    side = action.lower()
    if side not in ("buy", "sell"):
        side = "buy" if action.upper() == "BUY" else "sell"

    dry = CONFIG.get("DRY_RUN", True) or not (
        CONFIG.get("DELTA_API_KEY")
        and CONFIG.get("DELTA_API_SECRET")
    )

    with API_SEMAPHORE:
        for attempt in range(CONFIG["API_RETRY"]):
            try:
                time.sleep(CONFIG["API_DELAY"])

                if dry:
                    log_event(
                        "ORDER",
                        "DRY RUN",
                        {
                            "side": side,
                            "symbol": symbol,
                            "size": quantity,
                        },
                    )
                    return {
                        "dry_run": True,
                        "side": side,
                        "symbol": symbol,
                        "size": quantity,
                    }

                client = get_delta_client()
                order = {
                    "product_symbol": symbol,
                    "size": int(quantity),
                    "side": side,
                    "order_type": OrderType.MARKET.value,
                    "time_in_force": TimeInForce.IOC.value,
                }
                result = client.create_order(order)
                log_event(
                    "ORDER",
                    "FILLED/ACCEPTED",
                    {
                        "side": side,
                        "symbol": symbol,
                        "size": quantity,
                        "result": {
                            "id": (result or {}).get("id"),
                            "state": (result or {}).get("state"),
                            "average_fill_price": (
                                result or {}
                            ).get("average_fill_price"),
                        },
                    },
                )
                return result or True

            except Exception as e:
                log_event(
                    "ERROR",
                    "ORDER ERROR",
                    {
                        "attempt": attempt + 1,
                        "side": side,
                        "symbol": symbol,
                        "size": quantity,
                        "error": str(e),
                    },
                )
                time.sleep(CONFIG["API_RETRY_DELAY"])

    return None


def buy(exchange, symbol, quantity):
    return place_order(exchange, "BUY", symbol, quantity)


def sell(exchange, symbol, quantity):
    return place_order(exchange, "SELL", symbol, quantity)


# ============================================================
# QUOTES
# ============================================================


def get_live_spot(symbol):
    cfg = INDEX_CONFIG[symbol]
    ticker_symbol = cfg["underlying_symbol"]
    data = _public_get(f"/v2/tickers/{ticker_symbol}")
    if not data:
        return 0
    for key in ("spot_price", "mark_price", "close"):
        val = data.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return 0


# ============================================================
# GREEKS
# ============================================================


def fetch_option_data(option_symbol, index_symbol):
    _ = index_symbol
    data = _public_get(f"/v2/tickers/{option_symbol}")
    if not data:
        return None

    greeks = data.get("greeks") or {}
    quotes = data.get("quotes") or {}

    premium = data.get("mark_price")
    if premium is None:
        bid = quotes.get("best_bid")
        ask = quotes.get("best_ask")
        try:
            if bid is not None and ask is not None:
                premium = (float(bid) + float(ask)) / 2.0
            else:
                premium = bid or ask or data.get("close") or 0
        except (TypeError, ValueError):
            premium = 0

    def g(name):
        try:
            return float(greeks.get(name) or 0)
        except (TypeError, ValueError):
            return 0.0

    try:
        premium_f = float(premium or 0)
    except (TypeError, ValueError):
        premium_f = 0.0

    return {
        "delta": g("delta"),
        "gamma": g("gamma"),
        "theta": g("theta"),
        "vega": g("vega"),
        "premium": premium_f,
    }


# ============================================================
# BREAKEVEN
# ============================================================
# ============================================================
# CURRENT CONDOR VALUE
# ============================================================

def get_current_condor_value(
    ce_short,
    pe_short,
    ce_buy,
    pe_buy
):

    return (
        ce_short["premium"]
        + pe_short["premium"]
        - ce_buy["premium"]
        - pe_buy["premium"]
    )

def calculate_breakeven(position):

    credit = (
        position["ce_short"]["premium"]
        + position["pe_short"]["premium"]
        - position["ce_buy"]["premium"]
        - position["pe_buy"]["premium"]
    )

    upper_be = (
        position["ce_short"]["strike"]
        + credit
    )

    lower_be = (
        position["pe_short"]["strike"]
        - credit
    )

    position["upper_be"] = upper_be
    position["lower_be"] = lower_be
    position["entry_credit"] = credit



def get_adjustment_zone(
    net_delta,
    dte
):

    abs_delta = abs(net_delta)

    if dte < 1:

        if abs_delta >= CONFIG[
            "EXPIRY_DELTA_PANIC"
        ]:
            return "PANIC"

        elif abs_delta >= CONFIG[
            "EXPIRY_DELTA_AGGRESSIVE"
        ]:
            return "AGGRESSIVE"

        elif abs_delta >= CONFIG[
            "EXPIRY_DELTA_ADJUST"
        ]:
            return "ADJUST"

        elif abs_delta >= CONFIG[
            "EXPIRY_DELTA_PREPARE"
        ]:
            return "PREPARE"

        return "SAFE"

    else:

        if abs_delta >= CONFIG[
            "DELTA_PANIC"
        ]:
            return "PANIC"

        elif abs_delta >= CONFIG[
            "DELTA_AGGRESSIVE"
        ]:
            return "AGGRESSIVE"

        elif abs_delta >= CONFIG[
            "DELTA_ADJUST"
        ]:
            return "ADJUST"

        elif abs_delta >= CONFIG[
            "DELTA_PREPARE"
        ]:
            return "PREPARE"

        return "SAFE"

# ============================================================
# ENTRY
# ============================================================


def create_condor(symbol, bias, spot, expiry=None):

    cfg = INDEX_CONFIG[symbol]

    if expiry is None:
        expiry = get_active_expiry(symbol)

    atm = round_to_step(
        spot,
        cfg["strike_step"]
    )

    ce_distance = cfg["ce_distance"]
    pe_distance = cfg["pe_distance"]

    # ========================================================
    # NORMAL DISTANCE
    # ========================================================

    ce_distance = cfg["ce_distance"]
    pe_distance = cfg["pe_distance"]

    # ========================================================
    # BIAS ENTRY
    # ========================================================

    if bias == "CE":

        ce_distance = cfg[
            "CE_BIAS_CE_DISTANCE"
        ]

        pe_distance = cfg[
            "CE_BIAS_PE_DISTANCE"
        ]

    elif bias == "PE":

        ce_distance = cfg[
            "PE_BIAS_CE_DISTANCE"
        ]

        pe_distance = cfg[
            "PE_BIAS_PE_DISTANCE"
        ]

    ce_short_strike = snap_strike(
        symbol,
        expiry,
        atm + ce_distance,
        prefer="up",
    )
    pe_short_strike = snap_strike(
        symbol,
        expiry,
        atm - pe_distance,
        prefer="down",
    )

    ce_buy_strike = snap_strike(
        symbol,
        expiry,
        ce_short_strike + cfg["hedge_distance"],
        prefer="up",
    )
    pe_buy_strike = snap_strike(
        symbol,
        expiry,
        pe_short_strike - cfg["hedge_distance"],
        prefer="down",
    )

    ce_short_symbol = build_option_symbol(
        symbol, expiry, ce_short_strike, "CE"
    )
    pe_short_symbol = build_option_symbol(
        symbol, expiry, pe_short_strike, "PE"
    )
    ce_buy_symbol = build_option_symbol(
        symbol, expiry, ce_buy_strike, "CE"
    )
    pe_buy_symbol = build_option_symbol(
        symbol, expiry, pe_buy_strike, "PE"
    )

    qty = cfg["lot_size"]
    exchange = cfg["exchange"]

    buy(exchange, ce_buy_symbol, qty)
    time.sleep(CONFIG["API_DELAY"])
    buy(exchange, pe_buy_symbol, qty)
    time.sleep(CONFIG["API_DELAY"])
    sell(exchange, ce_short_symbol, qty)
    time.sleep(CONFIG["API_DELAY"])
    sell(exchange, pe_short_symbol, qty)
    time.sleep(CONFIG["API_DELAY"])

    ce_short = fetch_option_data(
        ce_short_symbol,
        symbol
    )
    time.sleep(0.25)
    pe_short = fetch_option_data(
        pe_short_symbol,
        symbol
    )
    time.sleep(0.25)
    ce_buy = fetch_option_data(
        ce_buy_symbol,
        symbol
    )
    time.sleep(0.25)
    pe_buy = fetch_option_data(
        pe_buy_symbol,
        symbol
    )
    time.sleep(0.25)

    if not all([ce_short, pe_short, ce_buy, pe_buy]):
        log_event(
            "ERROR",
            "ENTRY GREEKS UNAVAILABLE",
            {
                "symbol": symbol,
                "expiry": expiry,
                "legs": {
                    "ce_short": ce_short_symbol,
                    "pe_short": pe_short_symbol,
                    "ce_buy": ce_buy_symbol,
                    "pe_buy": pe_buy_symbol,
                },
            },
        )
        raise RuntimeError(
            "option greeks unavailable for entry legs"
        )

    POSITIONS[symbol] = {

        "active": True,
        "symbol": symbol,
        "exchange": exchange,
        "expiry": expiry,
        "bias": bias,
        "spot": spot,
        "adjustments": 0,
        "last_adjustment": 0,
        # For day-based PNL ladder (₹1500 × day_n)
        "entry_time": now_ist().isoformat(),

        "ce_short": {
            "symbol": ce_short_symbol,
            "strike": ce_short_strike,
            **ce_short
        },

        "pe_short": {
            "symbol": pe_short_symbol,
            "strike": pe_short_strike,
            **pe_short
        },

        "ce_buy": {
            "symbol": ce_buy_symbol,
            "strike": ce_buy_strike,
            **ce_buy
        },

        "pe_buy": {
            "symbol": pe_buy_symbol,
            "strike": pe_buy_strike,
            **pe_buy
        },

        # TSL (step = armed tier peak %, lock = protected floor %)
        "max_capture_percent": 0,
        "tsl_step": None,
        "tsl_lock": None,
    }

    calculate_breakeven(
        POSITIONS[symbol]
    )

    save_position(symbol)

    log_event(
        "ENTRY",
        "CONDOR CREATED",
        POSITIONS[symbol]
    )


# ============================================================
# CLOSE
# ============================================================


def close_position(symbol):

    position = POSITIONS[symbol]

    if not position.get("active"):
        return

    exchange = position["exchange"]

    qty = INDEX_CONFIG[symbol][
        "lot_size"
    ]

    buy(
        exchange,
        position["ce_short"]["symbol"],
        qty
    )
    time.sleep(CONFIG["API_DELAY"])
    buy(
        exchange,
        position["pe_short"]["symbol"],
        qty
    )
    time.sleep(CONFIG["API_DELAY"])
    sell(
        exchange,
        position["ce_buy"]["symbol"],
        qty
    )
    time.sleep(CONFIG["API_DELAY"])
    sell(
        exchange,
        position["pe_buy"]["symbol"],
        qty
    )

    POSITIONS[symbol] = {
        "active": False
    }

    delete_position_file(symbol)


# ============================================================
# CE SIDE ROLL
# ============================================================
# PROFESSIONAL SMART CE ROLL
# ============================================================

# ============================================================
# PROFESSIONAL SMART CE ROLL
# ============================================================

def roll_ce_side(symbol):

    try:

        position = POSITIONS[symbol]

        if not position["active"]:
            return

        if shift_expiry_if_needed(symbol):
            return

        cooldown = (
            int(time.time())
            - position["last_adjustment"]
        )

        if cooldown < CONFIG[
            "ADJUSTMENT_COOLDOWN"
        ]:
            return

        cfg = INDEX_CONFIG[symbol]

        step = cfg["strike_step"]

        spot = get_live_spot(symbol)

        atm = round_to_step(
            spot,
            step
        )

        new_expiry = get_active_expiry(symbol)

        dte = get_dte(symbol)

        # ====================================================
        # REFRESH LIVE GREEKS
        # ====================================================

        ce_short_live = fetch_option_data(
            position["ce_short"]["symbol"],
            symbol
        )

        time.sleep(CONFIG["API_DELAY"])

        pe_short_live = fetch_option_data(
            position["pe_short"]["symbol"],
            symbol
        )

        time.sleep(CONFIG["API_DELAY"])

        ce_buy_live = fetch_option_data(
            position["ce_buy"]["symbol"],
            symbol
        )

        time.sleep(CONFIG["API_DELAY"])

        pe_buy_live = fetch_option_data(
            position["pe_buy"]["symbol"],
            symbol
        )

        if (
            not ce_short_live
            or not pe_short_live
            or not ce_buy_live
            or not pe_buy_live
        ):
            return

        position["ce_short"].update(
            ce_short_live
        )

        position["pe_short"].update(
            pe_short_live
        )

        position["ce_buy"].update(
            ce_buy_live
        )

        position["pe_buy"].update(
            pe_buy_live
        )

        net_delta = calculate_net_delta(
            position
        )

        # ====================================================
        # DELTA CHECK
        # ====================================================

        if abs(net_delta) < CONFIG[
            "DELTA_ADJUST"
        ]:
            return

        if net_delta > 0:
            return

        old_net_delta = net_delta

        # ====================================================
        # BASE DISTANCE
        # ====================================================

        ce_distance = cfg[
            "ce_distance"
        ]

        pe_distance = cfg[
            "pe_distance"
        ]

        # ====================================================
        # ONLY ADAPTIVE SHIFT
        # ====================================================

        adaptive_shift = (

            max(
                1,
                int(
                    (
                        abs(net_delta)
                        - CONFIG[
                            "DELTA_ADJUST"
                        ]
                    ) / 0.10
                )
            )

            * step
        )

        ce_distance += adaptive_shift
        pe_distance -= adaptive_shift

        ce_distance = max(
            step,
            ce_distance
        )

        pe_distance = max(
            step,
            pe_distance
        )

        # ====================================================
        # NEW STRIKES
        # ====================================================

        new_ce_strike = (
            atm + ce_distance
        )

        new_pe_strike = (
            atm - pe_distance
        )

        # ====================================================
        # SMART RECENTER
        # ====================================================

        candidate_ce_list = snap_candidate_list(
            symbol,
            new_expiry,
            [
                new_ce_strike - step,
                new_ce_strike,
                new_ce_strike + step,
                new_ce_strike + (2 * step),
            ],
            prefer="up",
        )

        candidate_pe_list = snap_candidate_list(
            symbol,
            new_expiry,
            [
                new_pe_strike - step,
                new_pe_strike,
                new_pe_strike + step,
                new_pe_strike + (2 * step),
            ],
            prefer="down",
        )

        target_delta = 0

        best_delta_distance = 999

        best_ce = None
        best_pe = None
        best_estimated_delta = None

        optimizer_start = time.time()

        log_event(
            "OPTIMIZER",
            "STARTING CE PAIR SEARCH",
            {
                "symbol": symbol,
                "target_delta": target_delta,
                "candidate_ce_list": (
                    candidate_ce_list
                ),
                "candidate_pe_list": (
                    candidate_pe_list
                )
            }
        )

        # ====================================================
        # PREFETCH CACHE
        # ====================================================

        ce_cache = {}

        for ce_candidate in (
            candidate_ce_list
        ):

            time.sleep(0.10)

            ce_symbol = (
                build_option_symbol(
                    symbol,
                    new_expiry,
                    ce_candidate,
                    "CE"
                )
            )

            ce_cache[
                ce_candidate
            ] = fetch_option_data(
                ce_symbol,
                symbol
            )

        pe_cache = {}

        for pe_candidate in (
            candidate_pe_list
        ):

            time.sleep(0.10)

            pe_symbol = (
                build_option_symbol(
                    symbol,
                    new_expiry,
                    pe_candidate,
                    "PE"
                )
            )

            pe_cache[
                pe_candidate
            ] = fetch_option_data(
                pe_symbol,
                symbol
            )

        # ====================================================
        # OPTIMIZER
        # ====================================================

        for ce_candidate in (
            candidate_ce_list
        ):

            for pe_candidate in (
                candidate_pe_list
            ):

                ce_data = ce_cache.get(
                    ce_candidate
                )

                pe_data = pe_cache.get(
                    pe_candidate
                )

                if (
                    not ce_data
                    or not pe_data
                ):
                    continue

                estimated_delta = (

                    (-1 * ce_data["delta"])
                    + (-1 * pe_data["delta"])

                )

                delta_distance = abs(
                    estimated_delta
                    - target_delta
                )

                log_event(
                    "PAIR_CHECK",
                    "CE DELTA ESTIMATION",
                    {

                        "symbol": symbol,

                        "ce_candidate": (
                            ce_candidate
                        ),

                        "pe_candidate": (
                            pe_candidate
                        ),

                        "ce_delta": round(
                            ce_data["delta"],
                            4
                        ),

                        "pe_delta": round(
                            pe_data["delta"],
                            4
                        ),

                        "estimated_delta": round(
                            estimated_delta,
                            4
                        ),

                        "delta_distance": round(
                            delta_distance,
                            4
                        )
                    }
                )

                if (
                    delta_distance
                    < best_delta_distance
                ):

                    best_delta_distance = (
                        delta_distance
                    )

                    best_ce = ce_candidate
                    best_pe = pe_candidate

                    best_estimated_delta = (
                        estimated_delta
                    )

                    log_event(
                        "BEST_PAIR",
                        "NEW BEST CE PAIR",
                        {

                            "symbol": symbol,

                            "best_ce": (
                                best_ce
                            ),

                            "best_pe": (
                                best_pe
                            ),

                            "best_estimated_delta": round(
                                best_estimated_delta,
                                4
                            ),

                            "delta_distance": round(
                                best_delta_distance,
                                4
                            )
                        }
                    )

        optimizer_time = round(
            time.time()
            - optimizer_start,
            2
        )

        log_event(
            "OPTIMIZER",
            "FINAL CE BEST COMBINATION",
            {

                "symbol": symbol,

                "best_ce": best_ce,

                "best_pe": best_pe,

                "best_estimated_delta": round(
                    best_estimated_delta,
                    4
                ) if best_estimated_delta else None,

                "best_delta_distance": round(
                    best_delta_distance,
                    4
                ),

                "optimizer_time": (
                    optimizer_time
                )
            }
        )

        if (
            best_estimated_delta
            is None
        ):
            return

        # ====================================================
        # HARD PRE CHECK
        # ====================================================

        if abs(
            best_estimated_delta
        ) > 0.10:

            log_event(
                "ROLL_ABORT",
                "NO SAFE DELTA FOUND",
                {
                    "symbol": symbol,
                    "best_delta": round(
                        best_estimated_delta,
                        4
                    )
                }
            )

            return

        new_ce_strike = best_ce
        new_pe_strike = best_pe

        # ====================================================
        # SYMBOLS
        # ====================================================

        new_ce_symbol = (
            build_option_symbol(
                symbol,
                new_expiry,
                new_ce_strike,
                "CE"
            )
        )

        new_pe_symbol = (
            build_option_symbol(
                symbol,
                new_expiry,
                new_pe_strike,
                "PE"
            )
        )

        old_ce = position[
            "ce_short"
        ]

        old_pe = position[
            "pe_short"
        ]

        # ====================================================
        # DUPLICATE CHECK
        # ====================================================

        if (
            old_ce["symbol"]
            == new_ce_symbol

            and

            old_pe["symbol"]
            == new_pe_symbol
        ):
            return

        # ====================================================
        # CLOSE OLD SHORTS
        # ====================================================

        if not buy(
            position["exchange"],
            old_ce["symbol"],
            cfg["lot_size"]
        ):
            return

        time.sleep(CONFIG["API_DELAY"])

        if not buy(
            position["exchange"],
            old_pe["symbol"],
            cfg["lot_size"]
        ):
            return

        time.sleep(CONFIG["API_DELAY"])

        # ====================================================
        # OPEN NEW SHORTS
        # ====================================================

        if not sell(
            position["exchange"],
            new_ce_symbol,
            cfg["lot_size"]
        ):
            return

        time.sleep(CONFIG["API_DELAY"])

        if not sell(
            position["exchange"],
            new_pe_symbol,
            cfg["lot_size"]
        ):
            return

        time.sleep(CONFIG["API_DELAY"])

        # ====================================================
        # REFRESH NEW SHORTS
        # ====================================================

        new_ce_data = fetch_option_data(
            new_ce_symbol,
            symbol
        )

        time.sleep(CONFIG["API_DELAY"])

        new_pe_data = fetch_option_data(
            new_pe_symbol,
            symbol
        )

        if (
            not new_ce_data
            or not new_pe_data
        ):
            return

        # ====================================================
        # UPDATE POSITION
        # ====================================================

        position["ce_short"] = {

            "symbol": (
                new_ce_symbol
            ),

            "strike": (
                new_ce_strike
            ),

            **new_ce_data
        }

        position["pe_short"] = {

            "symbol": (
                new_pe_symbol
            ),

            "strike": (
                new_pe_strike
            ),

            **new_pe_data
        }

        save_position(symbol)

        # ====================================================
        # REFRESH HEDGES
        # ====================================================

        ce_buy_live = fetch_option_data(
            position["ce_buy"]["symbol"],
            symbol
        )

        if ce_buy_live:

            position["ce_buy"].update(
                ce_buy_live
            )

        time.sleep(CONFIG["API_DELAY"])

        pe_buy_live = fetch_option_data(
            position["pe_buy"]["symbol"],
            symbol
        )

        if pe_buy_live:

            position["pe_buy"].update(
                pe_buy_live
            )

        new_net_delta = (
            calculate_net_delta(
                position
            )
        )

        # ====================================================
        # UPDATE STATE
        # ====================================================

        position[
            "last_adjustment"
        ] = int(time.time())

        position[
            "adjustments"
        ] += 1

        position["spot"] = spot

        calculate_breakeven(
            position
        )

        save_position(symbol)

        # ====================================================
        # LOG
        # ====================================================

        log_event(
            "ROLL",
            "PROFESSIONAL CE ROLL",
            {

                "symbol": symbol,

                "dte": dte,

                "old_delta": round(
                    old_net_delta,
                    4
                ),

                "new_delta": round(
                    new_net_delta,
                    4
                ),

                "adaptive_shift": (
                    adaptive_shift
                ),

                "new_ce": (
                    new_ce_strike
                ),

                "new_pe": (
                    new_pe_strike
                )
            }
        )

    except Exception as e:

        log_event(
            "ERROR",
            "CE ROLL ERROR",
            {
                "symbol": symbol,
                "error": str(e)
            }
        )

def roll_pe_side(symbol):

    try:

        position = POSITIONS[symbol]

        if not position["active"]:
            return

        if shift_expiry_if_needed(symbol):
            return

        cooldown = (
            int(time.time())
            - position["last_adjustment"]
        )

        if cooldown < CONFIG[
            "ADJUSTMENT_COOLDOWN"
        ]:
            return

        cfg = INDEX_CONFIG[symbol]

        step = cfg["strike_step"]

        spot = get_live_spot(symbol)

        atm = round_to_step(
            spot,
            step
        )

        new_expiry = get_active_expiry(symbol)

        dte = get_dte(symbol)

        ce_short_live = fetch_option_data(
            position["ce_short"]["symbol"],
            symbol
        )

        time.sleep(CONFIG["API_DELAY"])

        pe_short_live = fetch_option_data(
            position["pe_short"]["symbol"],
            symbol
        )

        time.sleep(CONFIG["API_DELAY"])

        ce_buy_live = fetch_option_data(
            position["ce_buy"]["symbol"],
            symbol
        )

        time.sleep(CONFIG["API_DELAY"])

        pe_buy_live = fetch_option_data(
            position["pe_buy"]["symbol"],
            symbol
        )

        if (
            not ce_short_live
            or not pe_short_live
            or not ce_buy_live
            or not pe_buy_live
        ):
            return

        position["ce_short"].update(
            ce_short_live
        )

        position["pe_short"].update(
            pe_short_live
        )

        position["ce_buy"].update(
            ce_buy_live
        )

        position["pe_buy"].update(
            pe_buy_live
        )

        net_delta = calculate_net_delta(
            position
        )

        if abs(net_delta) < CONFIG[
            "DELTA_ADJUST"
        ]:
            return

        if net_delta < 0:
            return

        old_net_delta = net_delta

        ce_distance = cfg[
            "ce_distance"
        ]

        pe_distance = cfg[
            "pe_distance"
        ]

        adaptive_shift = (

            max(
                1,
                int(
                    (
                        abs(net_delta)
                        - CONFIG[
                            "DELTA_ADJUST"
                        ]
                    ) / 0.10
                )
            )

            * step
        )

        pe_distance += adaptive_shift
        ce_distance -= adaptive_shift

        ce_distance = max(
            step,
            ce_distance
        )

        pe_distance = max(
            step,
            pe_distance
        )

        new_pe_strike = (
            atm - pe_distance
        )

        new_ce_strike = (
            atm + ce_distance
        )

        candidate_ce_list = snap_candidate_list(
            symbol,
            new_expiry,
            [
                new_ce_strike - step,
                new_ce_strike,
                new_ce_strike + step,
                new_ce_strike + (2 * step),
            ],
            prefer="up",
        )

        candidate_pe_list = snap_candidate_list(
            symbol,
            new_expiry,
            [
                new_pe_strike - step,
                new_pe_strike,
                new_pe_strike + step,
                new_pe_strike + (2 * step),
            ],
            prefer="down",
        )

        target_delta = 0

        best_delta_distance = 999

        best_ce = None
        best_pe = None
        best_estimated_delta = None

        optimizer_start = time.time()

        log_event(
            "OPTIMIZER",
            "STARTING PE PAIR SEARCH",
            {
                "symbol": symbol,
                "target_delta": target_delta,
                "candidate_ce_list": (
                    candidate_ce_list
                ),
                "candidate_pe_list": (
                    candidate_pe_list
                )
            }
        )

        ce_cache = {}

        for ce_candidate in (
            candidate_ce_list
        ):

            time.sleep(0.10)

            ce_symbol = (
                build_option_symbol(
                    symbol,
                    new_expiry,
                    ce_candidate,
                    "CE"
                )
            )

            ce_cache[
                ce_candidate
            ] = fetch_option_data(
                ce_symbol,
                symbol
            )

        pe_cache = {}

        for pe_candidate in (
            candidate_pe_list
        ):

            time.sleep(0.10)

            pe_symbol = (
                build_option_symbol(
                    symbol,
                    new_expiry,
                    pe_candidate,
                    "PE"
                )
            )

            pe_cache[
                pe_candidate
            ] = fetch_option_data(
                pe_symbol,
                symbol
            )

        for ce_candidate in (
            candidate_ce_list
        ):

            for pe_candidate in (
                candidate_pe_list
            ):

                ce_data = ce_cache.get(
                    ce_candidate
                )

                pe_data = pe_cache.get(
                    pe_candidate
                )

                if (
                    not ce_data
                    or not pe_data
                ):
                    continue

                estimated_delta = (

                    (-1 * ce_data["delta"])
                    + (-1 * pe_data["delta"])

                    + position[
                        "ce_buy"
                    ]["delta"]

                    + position[
                        "pe_buy"
                    ]["delta"]

                )

                delta_distance = abs(
                    estimated_delta
                    - target_delta
                )

                log_event(
                    "PAIR_CHECK",
                    "PE DELTA ESTIMATION",
                    {

                        "symbol": symbol,

                        "ce_candidate": (
                            ce_candidate
                        ),

                        "pe_candidate": (
                            pe_candidate
                        ),

                        "ce_delta": round(
                            ce_data["delta"],
                            4
                        ),

                        "pe_delta": round(
                            pe_data["delta"],
                            4
                        ),

                        "estimated_delta": round(
                            estimated_delta,
                            4
                        ),

                        "delta_distance": round(
                            delta_distance,
                            4
                        )
                    }
                )

                if (
                    delta_distance
                    < best_delta_distance
                ):

                    best_delta_distance = (
                        delta_distance
                    )

                    best_ce = ce_candidate
                    best_pe = pe_candidate

                    best_estimated_delta = (
                        estimated_delta
                    )

                    log_event(
                        "BEST_PAIR",
                        "NEW BEST PE PAIR",
                        {

                            "symbol": symbol,

                            "best_ce": (
                                best_ce
                            ),

                            "best_pe": (
                                best_pe
                            ),

                            "best_estimated_delta": round(
                                best_estimated_delta,
                                4
                            ),

                            "delta_distance": round(
                                best_delta_distance,
                                4
                            )
                        }
                    )

        optimizer_time = round(
            time.time()
            - optimizer_start,
            2
        )

        log_event(
            "OPTIMIZER",
            "FINAL PE BEST COMBINATION",
            {

                "symbol": symbol,

                "best_ce": best_ce,

                "best_pe": best_pe,

                "best_estimated_delta": round(
                    best_estimated_delta,
                    4
                ) if best_estimated_delta else None,

                "best_delta_distance": round(
                    best_delta_distance,
                    4
                ),

                "optimizer_time": (
                    optimizer_time
                )
            }
        )

        if (
            best_estimated_delta
            is None
        ):
            return

        if abs(
            best_estimated_delta
        ) > 0.10:

            log_event(
                "ROLL_ABORT",
                "NO SAFE DELTA FOUND",
                {
                    "symbol": symbol,
                    "best_delta": round(
                        best_estimated_delta,
                        4
                    )
                }
            )

            return

        new_ce_strike = best_ce
        new_pe_strike = best_pe

        new_ce_symbol = (
            build_option_symbol(
                symbol,
                new_expiry,
                new_ce_strike,
                "CE"
            )
        )

        new_pe_symbol = (
            build_option_symbol(
                symbol,
                new_expiry,
                new_pe_strike,
                "PE"
            )
        )

        old_ce = position[
            "ce_short"
        ]

        old_pe = position[
            "pe_short"
        ]

        if (
            old_ce["symbol"]
            == new_ce_symbol

            and

            old_pe["symbol"]
            == new_pe_symbol
        ):
            return

        if not buy(
            position["exchange"],
            old_pe["symbol"],
            cfg["lot_size"]
        ):
            return

        time.sleep(CONFIG["API_DELAY"])

        if not buy(
            position["exchange"],
            old_ce["symbol"],
            cfg["lot_size"]
        ):
            return

        time.sleep(CONFIG["API_DELAY"])

        if not sell(
            position["exchange"],
            new_pe_symbol,
            cfg["lot_size"]
        ):
            return

        time.sleep(CONFIG["API_DELAY"])

        if not sell(
            position["exchange"],
            new_ce_symbol,
            cfg["lot_size"]
        ):
            return

        time.sleep(CONFIG["API_DELAY"])

        new_pe_data = fetch_option_data(
            new_pe_symbol,
            symbol
        )

        time.sleep(CONFIG["API_DELAY"])

        new_ce_data = fetch_option_data(
            new_ce_symbol,
            symbol
        )

        if (
            not new_pe_data
            or not new_ce_data
        ):
            return

        position["pe_short"] = {

            "symbol": (
                new_pe_symbol
            ),

            "strike": (
                new_pe_strike
            ),

            **new_pe_data
        }

        position["ce_short"] = {

            "symbol": (
                new_ce_symbol
            ),

            "strike": (
                new_ce_strike
            ),

            **new_ce_data
        }

        save_position(symbol)

        ce_buy_live = fetch_option_data(
            position["ce_buy"]["symbol"],
            symbol
        )

        if ce_buy_live:

            position["ce_buy"].update(
                ce_buy_live
            )

        time.sleep(CONFIG["API_DELAY"])

        pe_buy_live = fetch_option_data(
            position["pe_buy"]["symbol"],
            symbol
        )

        if pe_buy_live:

            position["pe_buy"].update(
                pe_buy_live
            )

        new_net_delta = (
            calculate_net_delta(
                position
            )
        )

        position[
            "last_adjustment"
        ] = int(time.time())

        position[
            "adjustments"
        ] += 1

        position["spot"] = spot

        calculate_breakeven(
            position
        )

        save_position(symbol)

        log_event(
            "ROLL",
            "PROFESSIONAL PE ROLL",
            {

                "symbol": symbol,

                "dte": dte,

                "old_delta": round(
                    old_net_delta,
                    4
                ),

                "new_delta": round(
                    new_net_delta,
                    4
                ),

                "adaptive_shift": (
                    adaptive_shift
                ),

                "new_ce": (
                    new_ce_strike
                ),

                "new_pe": (
                    new_pe_strike
                )
            }
        )

    except Exception as e:

        log_event(
            "ERROR",
            "PE ROLL ERROR",
            {
                "symbol": symbol,
                "error": str(e)
            }
        )


def recenter_condor(symbol, reason):

    position = POSITIONS[symbol]

    cooldown = (
        int(time.time())
        - position[
            "last_adjustment"
        ]
    )

    if cooldown < CONFIG[
        "ADJUSTMENT_COOLDOWN"
    ]:
        return

    spot = get_live_spot(symbol)
    bias = get_current_bias(symbol)

    close_position(symbol)

    time.sleep(2)

    create_condor(
        symbol,
        bias,
        spot
    )

    POSITIONS[symbol][
        "adjustments"
    ] += 1

    POSITIONS[symbol][
        "last_adjustment"
    ] = int(time.time())

    save_position(symbol)

    log_event(
        "ADJUSTMENT",
        reason,
        {
            "symbol": symbol,
            "bias": bias,
        }
    )


# ============================================================
# MONITOR
# ============================================================


def monitor_symbol(symbol):



    startup_delay = {
        "BTC": 0,
        "ETH": 5,
    }.get(symbol, 0)

    time.sleep(startup_delay)
    while True:

        try:

            if not is_market_open():
                time.sleep(10)
                continue

            position = POSITIONS.get(symbol)

            if not position:
                time.sleep(5)
                continue

            if not position.get("active"):
                if maybe_execute_pending_next_week(symbol):
                    time.sleep(
                        CONFIG["EXPIRY_INTERVAL"]
                    )
                    continue
                time.sleep(5)
                continue

            if shift_expiry_if_needed(symbol):
                time.sleep(
                    CONFIG["EXPIRY_INTERVAL"]
                )
                continue

            dte = get_dte(symbol)

            # ====================================================
            # EXPIRY MODE
            # ====================================================

            if dte < 1:

                delta_warning = CONFIG[
                    "EXPIRY_DELTA_PREPARE"
                ]

                delta_adjust = CONFIG[
                    "EXPIRY_DELTA_ADJUST"
                ]

                panic_delta = CONFIG[
                    "EXPIRY_DELTA_PANIC"
                ]

                panic_gamma = CONFIG[
                    "EXPIRY_PANIC_GAMMA"
                ]

                be_buffer = INDEX_CONFIG[symbol].get(
                    "expiry_be_buffer",
                    CONFIG["EXPIRY_BE_BUFFER"],
                )

                interval = CONFIG[
                    "EXPIRY_INTERVAL"
                ]

            else:

                delta_warning = CONFIG[
                    "DELTA_PREPARE"
                ]

                delta_adjust = CONFIG[
                    "DELTA_ADJUST"
                ]

                panic_delta = CONFIG[
                    "DELTA_PANIC"
                ]

                panic_gamma = CONFIG[
                    "PANIC_GAMMA"
                ]

                be_buffer = INDEX_CONFIG[symbol].get(
                    "be_buffer",
                    CONFIG["BREAKEVEN_BUFFER"],
                )

                interval = CONFIG[
                    "SAFE_INTERVAL"
                ]

            ce_short = fetch_option_data(
                position[
                    "ce_short"
                ]["symbol"],
                symbol
            )
            time.sleep(0.25)
            pe_short = fetch_option_data(
                position[
                    "pe_short"
                ]["symbol"],
                symbol
            )
            time.sleep(0.25)
            ce_buy = fetch_option_data(
                position[
                    "ce_buy"
                ]["symbol"],
                symbol
            )
            time.sleep(0.25)
            pe_buy = fetch_option_data(
                position[
                    "pe_buy"
                ]["symbol"],
                symbol
            )
            time.sleep(0.25)
            if (
                not ce_short
                or not pe_short
                or not ce_buy
                or not pe_buy
            ):

                log_event(
                    "ERROR",
                    "OPTION DATA FETCH FAILED",
                    {
                        "symbol": symbol
                    }
                )

                time.sleep(10)
                continue
            # ====================================================
            # REFRESH LIVE GREEKS
            # ====================================================

            position["ce_short"].update(
                ce_short
            )

            position["pe_short"].update(
                pe_short
            )

            position["ce_buy"].update(
                ce_buy
            )

            position["pe_buy"].update(
                pe_buy
            )


            # ====================================================
            # NET DELTA
            # ====================================================

            net_delta = (
                - ce_short["delta"]
                - pe_short["delta"]
                + ce_buy["delta"]
                + pe_buy["delta"]
            )

            abs_net_delta = abs(
                net_delta
            )


            # ====================================================
            # PROFIT TARGET
            # ====================================================

            current_value = get_current_condor_value(
                ce_short,
                pe_short,
                ce_buy,
                pe_buy
            )

            entry_credit = position[
                "entry_credit"
            ]

            captured = (
                entry_credit
                - current_value
            )

            capture_percent = 0

            if entry_credit > 0:

                capture_percent = (
                    captured
                    / entry_credit
                ) * 100

            if (
                capture_percent
                > position.get(
                    "max_capture_percent",
                    0,
                )
            ):
                position[
                    "max_capture_percent"
                ] = capture_percent

            update_trailing_stop_from_steps(position)

            tsl_lock = position.get("tsl_lock")
            if tsl_lock is None:
                tsl_lock = position.get(
                    "trailing_stop_percent"
                )

            if (
                tsl_lock is not None
                and capture_percent <= tsl_lock
            ):

                log_event(
                    "TSL",
                    "TRAILING STOP HIT",
                    {
                        "symbol": symbol,
                        "capture_percent": round(
                            capture_percent,
                            2
                        ),
                        "peak_capture_percent": round(
                            position.get(
                                "max_capture_percent",
                                0,
                            ),
                            2,
                        ),
                        "tsl_lock": tsl_lock,
                        "tsl_step": position.get(
                            "tsl_step"
                        ),
                    }
                )

                close_position(symbol)

                continue

            # Stamp entry_time on legacy open positions (prefer file mtime)
            if not position.get("entry_time"):
                try:
                    mtime = os.path.getmtime(
                        get_position_file(symbol)
                    )
                    position["entry_time"] = (
                        datetime.fromtimestamp(
                            mtime, IST
                        ).isoformat()
                    )
                except Exception:
                    position["entry_time"] = (
                        now_ist().isoformat()
                    )

            save_position(symbol)

            # ====================================================
            # DAY-BASED PNL TARGET (₹1500 × day_n)
            # On hit → close full + normal re-entry
            # ====================================================

            if CONFIG.get("DAY_PNL_ENABLED"):

                lot = INDEX_CONFIG[symbol][
                    "lot_size"
                ]
                pnl_rupee = captured * lot
                day_target = get_day_pnl_target_rupee(
                    position
                )

                if day_target is not None:

                    target_rupee, day_n = day_target

                    if (
                        target_rupee > 0
                        and pnl_rupee >= target_rupee
                    ):

                        log_event(
                            "PROFIT",
                            "DAY PNL TARGET REACHED",
                            {
                                "symbol": symbol,
                                "day_n": day_n,
                                "pnl_rupee": round(
                                    pnl_rupee,
                                    2
                                ),
                                "target_rupee": round(
                                    target_rupee,
                                    2
                                ),
                                "base_rupee": CONFIG.get(
                                    "DAY_PNL_BASE_RUPEE",
                                    1500,
                                ),
                                "capture_percent": round(
                                    capture_percent,
                                    2
                                ),
                                "entry_credit": round(
                                    entry_credit,
                                    2
                                ),
                            }
                        )

                        reenter_full_condor(
                            symbol,
                            "DAY_PNL_REENTER",
                        )

                        continue

            # ====================================================
            # EXPIRY TARGET
            # ====================================================

            profit_target = (
                CONFIG[
                    "EXPIRY_PROFIT_TARGET_PERCENT"
                ]
                if dte < 1
                else CONFIG[
                    "PROFIT_TARGET_PERCENT"
                ]
            )

            # ====================================================
            # PROFIT EXIT
            # ====================================================

            if capture_percent >= profit_target:

                log_event(
                    "PROFIT",
                    "TARGET REACHED",
                    {
                        "symbol": symbol,
                        "capture_percent": round(
                            capture_percent,
                            2
                        ),
                        "target": profit_target,
                        "entry_credit": round(
                            entry_credit,
                            2
                        ),
                        "current_value": round(
                            current_value,
                            2
                        ),
                        "captured": round(
                            captured,
                            2
                        )
                    }
                )

                closed_bias = get_current_bias(symbol)
                closed_expiry = position.get(
                    "expiry"
                )
                on_expiry_day = dte < 1

                close_position(symbol)

                # Expiry-day target before 12:55 used to skip the next-week
                # roll. Arm it so EXPIRY_NEXT_WEEK_TIME still opens next week.
                if on_expiry_day and closed_expiry:
                    arm_pending_next_week_roll(
                        symbol,
                        closed_bias,
                        closed_expiry,
                        reason="EXPIRY_TARGET_BEFORE_ROLL",
                    )

                continue
            # ====================================================
            # NET GAMMA
            # ====================================================

            net_gamma = (

                - ce_short["gamma"]
                - pe_short["gamma"]

                + ce_buy["gamma"]
                + pe_buy["gamma"]

            )
            # ====================================================

            # ====================================================
            # THREAT SIDE
            # ====================================================

            if abs(net_delta) < 0.1:

                threatened_side = "NONE"

            elif net_delta > 0:

                # Bullish exposure
                # Downside danger

                threatened_side = "PE"

            else:

                # Bearish exposure
                # Upside danger

                threatened_side = "CE"

            # ====================================================
            # PANIC EXIT
            # ====================================================

            if abs_net_delta >= panic_delta:

                log_event(
                    "PANIC",
                    "NET DELTA EXIT",
                    {"symbol": symbol}
                )

                close_position(symbol)

                continue

            # ====================================================
            # GAMMA EXIT
            # ====================================================

            #if abs(net_gamma) >= panic_gamma:
            #
            #    log_event(
            #        "PANIC",
            #        "NET GAMMA EXIT",
            #        {"symbol": symbol}
            #    )
            #
            #    close_position(symbol)
             
            #    continue

            # ====================================================
            # DELTA ADJUST
            # ====================================================

            if abs_net_delta >= delta_adjust:

                if threatened_side == "CE":
                    roll_ce_side(symbol)
                else:
                    roll_pe_side(symbol)
                continue

            # ====================================================
            # BREAKEVEN
            # ====================================================

            spot = get_live_spot(symbol)

            upper_trigger = (
                position["upper_be"]
                - be_buffer
            )

            lower_trigger = (
                position["lower_be"]
                + be_buffer
            )

            if spot >= upper_trigger:

                roll_ce_side(symbol)
                continue

            if spot <= lower_trigger:

                roll_pe_side(symbol)
                continue

            # ====================================================
            # LOGGING
            # ====================================================
            upper_trigger = (
                position["upper_be"]
                - be_buffer
            )

            lower_trigger = (
                position["lower_be"]
                + be_buffer
            )

            log_event(
                "GREEKS",
                "LIVE",
                {
                    "symbol": symbol,

                    "spot": round(
                        spot,
                        2
                    ),

                    "dte": dte,

                    "net_delta": round(
                        net_delta,
                        4
                    ),

                    "net_gamma": round(
                        net_gamma,
                        6
                    ),

                    "side": threatened_side,

                    # ====================================
                    # CREDIT
                    # ====================================

                    "entry_credit": round(
                        entry_credit,
                        2
                    ),

                    "current_value": round(
                        current_value,
                        2
                    ),

                    "captured": round(
                        captured,
                        2
                    ),

                    "capture_percent": round(
                        capture_percent,
                        2
                    ),

                    # ====================================
                    # BREAKEVEN
                    # ====================================

                    "upper_be": round(
                        position["upper_be"],
                        2
                    ),

                    "lower_be": round(
                        position["lower_be"],
                        2
                    ),

                    "upper_trigger": round(
                        upper_trigger,
                        2
                    ),

                    "lower_trigger": round(
                        lower_trigger,
                        2
                    ),

                    # ====================================
                    # DISTANCE
                    # ====================================

                    "distance_to_upper_be": round(
                        position["upper_be"]
                        - spot,
                        2
                    ),

                    "distance_to_lower_be": round(
                        spot
                        - position["lower_be"],
                        2
                    )
                }
            )

            time.sleep(interval)

        except Exception as e:

            log_event(
                "ERROR",
                "MONITOR ERROR",
                {"error": str(e)}
            )

            time.sleep(5)


# ============================================================
# CLEANUP
# ============================================================
def cleanup_expired_positions():

    for symbol in INDEX_CONFIG.keys():

        try:

            file_path = get_position_file(symbol)

            if not os.path.exists(file_path):
                continue

            with open(file_path, "r") as f:
                position = json.load(f)

            saved_expiry = position.get("expiry")

            if not saved_expiry:
                continue

            # ============================================
            # CONVERT EXPIRY STRING TO DATE
            # ============================================

            expiry_date = parse_expiry(saved_expiry)

            today = now_ist().date()

            # ============================================
            # DELETE ONLY IF ACTUALLY EXPIRED
            # ============================================

            if expiry_date < today:

                delete_position_file(symbol)

                POSITIONS[symbol] = {
                    "active": False
                }

                log_event(
                    "CLEANUP",
                    "EXPIRED FILE REMOVED",
                    {
                        "symbol": symbol,
                        "expiry": saved_expiry
                    }
                )

        except Exception as e:

            log_event(
                "ERROR",
                "CLEANUP ERROR",
                {
                    "symbol": symbol,
                    "error": str(e)
                }
            )


# ============================================================
# WEBHOOK
# ============================================================


@app.post("/iron")
def iron(payload: dict):
    """
    Bias webhook (preferred):
      {"symbol":"BTC","bias":"PE"}
    → always updates bot current bias; does NOT require price.

    Optional force entry (bot fetches live spot):
      {"symbol":"BTC","bias":"PE","enter":true}
      {"symbol":"ETH","bias":"CE","enter":true}

    Legacy: if price/close is present, also enters (still uses live spot).
    """

    try:

        symbol = payload.get("symbol")
        if symbol not in INDEX_CONFIG:
            return {
                "status": "error",
                "message": "INVALID SYMBOL"
            }

        bias = set_current_bias(
            symbol,
            payload.get("bias", "NONE"),
            source="webhook",
        )

        # Bias-only by default. Force entry only with enter/action.
        # price/close in payload are ignored for entry — bot uses live LTP.
        enter_flag = payload.get("enter")
        action = str(
            payload.get("action") or ""
        ).lower()
        do_enter = (
            enter_flag in (True, "true", 1, "1", "yes")
            or action == "enter"
        )

        if not do_enter:
            return {
                "status": "success",
                "entered": False,
                "bias": bias,
                "message": "BIAS UPDATED",
                "spot": None,
            }

        # Bot always detects live index price itself
        spot = get_live_spot(symbol)
        if not spot:
            return {
                "status": "error",
                "entered": False,
                "bias": bias,
                "message": "LIVE SPOT UNAVAILABLE",
            }

        if POSITIONS.get(symbol):
            if POSITIONS[symbol].get("active"):
                close_position(symbol)

        create_condor(
            symbol,
            bias,
            float(spot)
        )

        return {
            "status": "success",
            "entered": True,
            "bias": bias,
            "spot": spot,
            "position": POSITIONS[symbol],
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }


# ============================================================
# POSITIONS / BIAS
# ============================================================


@app.get("/positions")
def positions():
    return POSITIONS


@app.get("/bias")
def bias_status():
    return {
        symbol: get_current_bias(symbol)
        for symbol in INDEX_CONFIG.keys()
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "dry_run": CONFIG.get("DRY_RUN"),
        "symbols": list(INDEX_CONFIG.keys()),
        "active_expiry": {
            symbol: get_active_expiry(symbol)
            for symbol in INDEX_CONFIG.keys()
        },
        "settlement_ist": CONFIG["EXPIRY_SETTLEMENT_TIME"],
    }


# ============================================================
# STARTUP
# ============================================================


if __name__ == "__main__":

    cleanup_expired_positions()

    load_positions()
    load_pending_rolls()
    load_biases()


    for idx, symbol in enumerate(INDEX_CONFIG.keys()):

        Thread(
            target=monitor_symbol,
            args=(symbol,),
            daemon=True
        ).start()

    time.sleep(8)

    log_event(
        "SYSTEM",
        "ENGINE STARTED"
    )

    log_event(
        "SYSTEM",
        "DELTA CONFIG",
        {
            "base_url": CONFIG["DELTA_BASE_URL"],
            "dry_run": CONFIG.get("DRY_RUN"),
            "has_keys": bool(
                CONFIG.get("DELTA_API_KEY")
                and CONFIG.get("DELTA_API_SECRET")
            ),
            "symbols": list(INDEX_CONFIG.keys()),
            "settlement": CONFIG["EXPIRY_SETTLEMENT_TIME"],
        },
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=CONFIG.get("WEBHOOK_PORT", 9000),
    )
