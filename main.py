import asyncio
import aiohttp
import time
from datetime import datetime
import sys
import importlib
import json
import os
import html
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

# =====================================================================
# --- Dynamic Configuration Loader ---
# =====================================================================
config_name = sys.argv[1] if len(sys.argv) > 1 else "config"
if config_name.endswith(".py"):
    config_name = config_name[:-3]

try:
    config = importlib.import_module(config_name)
    print(f"⚙️ Loaded configuration: {config_name}.py")
except ImportError:
    print(f"\n⚠️ Error: '{config_name}.py' not found!")
    sys.exit(1)

# Wallet Configuration
TARGET_WALLETS = config.TARGET_WALLETS
POLL_INTERVAL = getattr(config, 'POLL_INTERVAL', 2)
TELEGRAM_BOT_TOKEN = config.TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID = config.TELEGRAM_CHAT_ID

# Official Phoenix Program IDs on Solana Mainnet
PHOENIX_PROGRAMS = {
    "PhoeNiX2EyyJBKw5EaWZbg8hkfz3DcjMBNfmsyJR8qQ": "Phoenix Spot 🦅",
    "EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih": "Phoenix Eternal (Perps) ⚡"
}

# Helius + Backup RPC Setup (Auto-failover enabled)
HELIUS_API_KEYS = getattr(config, 'HELIUS_API_KEYS', [])
if not HELIUS_API_KEYS and hasattr(config, 'HELIUS_API_KEY') and config.HELIUS_API_KEY:
    HELIUS_API_KEYS = [config.HELIUS_API_KEY]
elif isinstance(HELIUS_API_KEYS, str):
    HELIUS_API_KEYS = [HELIUS_API_KEYS]

helius_urls = [
    f"https://mainnet.helius-rpc.com/?api-key={quote(str(key).strip(), safe='')}"
    for key in HELIUS_API_KEYS
    if key
    and "your-" not in str(key).lower()
    and "your_" not in str(key).lower()
    and "placeholder" not in str(key).lower()
    and "کلید api خودت" not in str(key).casefold()
]

configured_rpcs = getattr(config, 'RPC_URLS', [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
    "https://rpc.ankr.com/solana"
])
if isinstance(configured_rpcs, str):
    configured_rpcs = [configured_rpcs]

# Helius endpoints are built separately for each key above. Ignore the common
# malformed config entry that interpolates the whole key list into one URL.
backup_rpcs = []
for configured_rpc in configured_rpcs:
    rpc_url = str(configured_rpc).strip()
    if not rpc_url or rpc_url in backup_rpcs:
        continue
    if "mainnet.helius-rpc.com" in rpc_url.lower():
        if helius_urls:
            continue
        if "api-key=" not in rpc_url.lower() or "[" in rpc_url or "]" in rpc_url:
            continue
    backup_rpcs.append(rpc_url)

# Combine Primary Helius with Backup RPCs
RPC_URLS = list(dict.fromkeys(helius_urls + backup_rpcs))
rpc_round_robin_index = 0

last_429_warn_time = 0
last_rpc_error_warn_time = 0
SEEN_SIGNATURES_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "seen_signatures.json"
)
RPC_CONCURRENCY = max(1, int(getattr(config, "RPC_CONCURRENCY", 10)))
RPC_TIMEOUT = max(0.5, float(getattr(config, "RPC_TIMEOUT", 1.8)))
RPC_MAX_ATTEMPTS = max(1, int(getattr(config, "RPC_MAX_ATTEMPTS", 4)))
RPC_429_COOLDOWN = max(2.0, float(getattr(config, "RPC_429_COOLDOWN", 10)))
RPC_ERROR_COOLDOWN = max(1.0, float(getattr(config, "RPC_ERROR_COOLDOWN", 4)))
rpc_semaphore = asyncio.Semaphore(RPC_CONCURRENCY)
rpc_cooldowns = [0.0 for _ in RPC_URLS]
last_signature_gap_warn = {}
inflight_signatures = set()
background_tasks = set()
NO_SIGNATURE_BASELINE = "__NO_SIGNATURE_BASELINE__"

LINKED_SIGNERS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "linked_signers.json"
)

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def is_valid_solana_address(address):
    """Validate a base58-encoded 32-byte Solana public key without extra packages."""
    if not isinstance(address, str) or not address:
        return False
    number = 0
    try:
        for char in address:
            number = number * 58 + BASE58_ALPHABET.index(char)
    except ValueError:
        return False
    decoded = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    leading_zeroes = len(address) - len(address.lstrip("1"))
    return len((b"\x00" * leading_zeroes) + decoded) == 32


def load_linked_signers():
    try:
        with open(LINKED_SIGNERS_FILE, "r", encoding="utf-8") as file:
            saved = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(saved, dict):
        return {}

    linked = {}
    for owner, signers in saved.items():
        if not isinstance(owner, str) or not isinstance(signers, list):
            continue
        valid_signers = sorted({
            signer.strip() for signer in signers
            if isinstance(signer, str)
            and signer.strip() != owner
            and is_valid_solana_address(signer.strip())
        })
        if valid_signers:
            linked[owner] = valid_signers
    return linked


def save_linked_signers(linked_signers):
    temp_path = LINKED_SIGNERS_FILE + ".tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(linked_signers, file, ensure_ascii=False, indent=2)
        os.replace(temp_path, LINKED_SIGNERS_FILE)
    except OSError as exc:
        print(f"⚠️ Could not save linked signer history: {exc}")


def load_seen_signatures():
    try:
        with open(SEEN_SIGNATURES_FILE, "r", encoding="utf-8") as file:
            saved = json.load(file)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        return {}

    if not isinstance(saved, dict):
        return {}

    return {
        wallet: {signature: None for signature in signatures[-1000:] if isinstance(signature, str)}
        for wallet, signatures in saved.items()
        if isinstance(wallet, str) and isinstance(signatures, list)
    }


def save_seen_signatures(seen_signatures):
    temp_path = SEEN_SIGNATURES_FILE + ".tmp"
    saved = {
        wallet: list(signatures.keys())[-1000:]
        for wallet, signatures in seen_signatures.items()
    }

    try:
        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(saved, file)
        os.replace(temp_path, SEEN_SIGNATURES_FILE)
    except OSError as exc:
        print(f"⚠️ Could not save signature history: {exc}")


def get_next_rpc_index():
    """Choose a different first provider for each request without interleaving retries."""
    global rpc_round_robin_index
    if not RPC_URLS:
        return None
    index = rpc_round_robin_index
    rpc_round_robin_index = (rpc_round_robin_index + 1) % len(RPC_URLS)
    return index


async def send_telegram_alert(session: aiohttp.ClientSession, message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram bot token or chat ID is missing from the active config.")
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        async with session.post(url, json=payload, timeout=5) as resp:
            data = await resp.json(content_type=None)
            if resp.status == 200 and data.get("ok"):
                return data.get("result", {}).get("message_id")
            print(f"⚠️ Telegram send failed (HTTP {resp.status}): {str(data)[:250]}")
    except Exception as exc:
        print(f"⚠️ Telegram send failed: {type(exc).__name__}")
    return None


async def edit_telegram_alert(session: aiohttp.ClientSession, message_id: int, message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not message_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "message_id": message_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with session.post(url, json=payload, timeout=5) as resp:
            if resp.status != 200:
                error_text = (await resp.text())[:250]
                print(f"⚠️ Telegram could not add trade details ({resp.status}): {error_text}")
    except Exception as exc:
        print(f"⚠️ Telegram detail update failed: {type(exc).__name__}")


async def fetch_rpc(session: aiohttp.ClientSession, method: str, params: list, result_validator=None):
    global last_429_warn_time, last_rpc_error_warn_time
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    attempts = len(RPC_URLS)
    if not attempts:
        print("⚠️ No valid RPC endpoint is configured.")
        return None

    first_index = get_next_rpc_index()
    if first_index is None:
        return None
    now_mono = time.monotonic()
    provider_order = [(first_index + offset) % attempts for offset in range(attempts)]
    ready_providers = [index for index in provider_order if rpc_cooldowns[index] <= now_mono]
    if not ready_providers:
        # All providers recently failed. Probe the one whose cooldown expires first
        # instead of waiting through every dead endpoint on every wallet poll.
        ready_providers = [min(provider_order, key=lambda index: rpc_cooldowns[index])]
    else:
        ready_providers = ready_providers[:min(attempts, RPC_MAX_ATTEMPTS)]

    invalid_param_seen = False
    for provider_index in ready_providers:
        rpc_url = RPC_URLS[provider_index]
        provider_label = f"RPC #{provider_index + 1}/{attempts}"
        try:
            async with rpc_semaphore:
                async with session.post(rpc_url, json=payload, timeout=RPC_TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if data.get("result") is not None:
                            result = data["result"]
                            if result_validator is not None and not result_validator(result):
                                # A lagging RPC can return an older page that does not
                                # contain our saved cursor. Try another provider now.
                                rpc_cooldowns[provider_index] = time.monotonic() + min(RPC_ERROR_COOLDOWN, 2.0)
                                now = time.time()
                                if now - last_rpc_error_warn_time > 15:
                                    last_rpc_error_warn_time = now
                                    print(f"⏳ {provider_label} returned stale {method} history; trying the next endpoint.")
                                continue
                            rpc_cooldowns[provider_index] = 0.0
                            return result
                        if data.get("error"):
                            now = time.time()
                            error = data["error"]
                            if isinstance(error, dict):
                                error_code = error.get("code")
                                error_text = str(error.get("message", ""))
                            else:
                                error_code = None
                                error_text = str(error)
                            if now - last_rpc_error_warn_time > 15:
                                last_rpc_error_warn_time = now
                                detail = f"{error_code or ''} {error_text}".strip()
                                print(f"⚠️ {provider_label} JSON error in {method} ({detail[:180]}).")

                            if error_code == -32602:
                                # Invalid request parameters are normally provider-independent.
                                # Check one alternate provider; if it agrees, stop the futile loop.
                                if invalid_param_seen:
                                    if method == "getSignaturesForAddress" and params:
                                        address = str(params[0]).strip()
                                        masked = f"{address[:6]}...{address[-4:]} (chars={len(address)}, key32={is_valid_solana_address(address)})"
                                        print(f"❌ RPC rejected getSignaturesForAddress for {masked}; same -32602 came from two providers.")
                                    return None
                                invalid_param_seen = True
                                continue

                            # A provider-side JSON error gets a brief cooldown so the
                            # next wallet request can use a different endpoint first.
                            rpc_cooldowns[provider_index] = time.monotonic() + RPC_ERROR_COOLDOWN
                            print(f"↪️ Retrying {method} with the next RPC/key...")
                            continue
                        rpc_cooldowns[provider_index] = time.monotonic() + RPC_ERROR_COOLDOWN
                        continue
                    elif resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        try:
                            cooldown = max(RPC_429_COOLDOWN, float(retry_after)) if retry_after else RPC_429_COOLDOWN
                        except (TypeError, ValueError):
                            cooldown = RPC_429_COOLDOWN
                        rpc_cooldowns[provider_index] = time.monotonic() + cooldown
                        now = time.time()
                        if now - last_429_warn_time > 15:
                            last_429_warn_time = now
                            print(f"⏳ {provider_label} rate limit (429) in {method}; trying the next key/endpoint...")
                        continue
                    else:
                        rpc_cooldowns[provider_index] = time.monotonic() + RPC_ERROR_COOLDOWN
                        now = time.time()
                        if now - last_rpc_error_warn_time > 15:
                            last_rpc_error_warn_time = now
                            print(f"⚠️ {provider_label} returned HTTP {resp.status} for {method}; trying the next key/endpoint...")
        except Exception as exc:
            rpc_cooldowns[provider_index] = time.monotonic() + RPC_ERROR_COOLDOWN
            now = time.time()
            if now - last_rpc_error_warn_time > 15:
                last_rpc_error_warn_time = now
                print(f"⚠️ RPC request failed ({type(exc).__name__}); trying the next key/endpoint...")
            continue
    return None


def get_phoenix_type(tx_info: dict) -> str:
    """Detects whether the transaction interacted with Phoenix Spot or Eternal."""
    if not tx_info:
        return None
    try:
        message = tx_info.get("transaction", {}).get("message", {})
        account_keys = message.get("accountKeys", [])
        for acc in account_keys:
            pubkey = acc.get("pubkey") if isinstance(acc, dict) else acc
            if pubkey in PHOENIX_PROGRAMS:
                return PHOENIX_PROGRAMS[pubkey]
    except Exception:
        pass
    return None


def quick_detect_action(logs: list) -> str:
    """Parses transaction logs to determine the action type."""
    if not logs:
        return "⚡ معامله / مدیریت سفارش"
    
    logs_str = " ".join(logs).lower()
    if "placelimit" in logs_str or "place_limit" in logs_str:
        return "📥 ثبت سفارش لیمیت (Limit Order)"
    elif "placemarket" in logs_str or "place_market" in logs_str:
        return "⚡ معامله مارکت (Market Order)"
    elif "cancel" in logs_str:
        return "❌ لغو سفارش (Cancel Order)"
    
    return "⚡ معامله / مدیریت سفارش"


# Public Phoenix endpoints used for exact fill size, price, fees, and realized PnL.
# This is data formatting only; no AI request or generated analysis is used.
configured_phoenix_urls = getattr(config, "PHOENIX_API_URLS", None)
if isinstance(configured_phoenix_urls, str):
    configured_phoenix_urls = [configured_phoenix_urls]
if not configured_phoenix_urls:
    configured_phoenix_urls = [getattr(config, "PHOENIX_API_URL", "https://perp-api.phoenix.trade")]
PHOENIX_API_URLS = list(dict.fromkeys(
    str(url).strip().rstrip("/") for url in configured_phoenix_urls if str(url).strip()
))
PHOENIX_API_URL = PHOENIX_API_URLS[0] if PHOENIX_API_URLS else ""
PHOENIX_API_TIMEOUT = max(0.5, float(getattr(config, "PHOENIX_API_TIMEOUT", 2.0)))
PHOENIX_API_COOLDOWN = max(1.0, float(getattr(config, "PHOENIX_API_COOLDOWN", 4)))
phoenix_api_cooldowns = {url: 0.0 for url in PHOENIX_API_URLS}
phoenix_api_round_robin_index = 0
PHOENIX_API_PAGES = max(1, int(getattr(config, "PHOENIX_API_PAGES", 3)))
last_phoenix_api_warn_time = 0


def as_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def timestamp_ms(value):
    if value is None:
        return None
    try:
        if isinstance(value, (int, float, Decimal)):
            number = Decimal(str(value))
            return int(number if number > Decimal("100000000000") else number * 1000)
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            from datetime import timezone
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return None


def format_number(value, decimals=8):
    if value is None:
        return "نامشخص"
    text = f"{value:,.{decimals}f}".rstrip("0").rstrip(".")
    return text or "0"


async def phoenix_api_get(session, path, params=None):
    global last_phoenix_api_warn_time, phoenix_api_round_robin_index
    if not PHOENIX_API_URLS:
        return None

    count = len(PHOENIX_API_URLS)
    first = phoenix_api_round_robin_index % count
    phoenix_api_round_robin_index = (first + 1) % count
    order = [(first + offset) % count for offset in range(count)]
    now_mono = time.monotonic()
    ready = [PHOENIX_API_URLS[i] for i in order if phoenix_api_cooldowns[PHOENIX_API_URLS[i]] <= now_mono]
    if not ready:
        now = time.time()
        if now - last_phoenix_api_warn_time > 30:
            last_phoenix_api_warn_time = now
            print("⏳ Phoenix trade-data API endpoints are cooling down; the basic alert was kept.")
        return None

    for api_url in ready:
        try:
            async with session.get(
                f"{api_url}{path}", params=params, timeout=PHOENIX_API_TIMEOUT
            ) as resp:
                if resp.status == 200:
                    payload = await resp.json(content_type=None)
                    if isinstance(payload, dict):
                        phoenix_api_cooldowns[api_url] = 0.0
                        return payload
                    phoenix_api_cooldowns[api_url] = time.monotonic() + PHOENIX_API_COOLDOWN
                elif resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        cooldown = max(PHOENIX_API_COOLDOWN, float(retry_after)) if retry_after else PHOENIX_API_COOLDOWN
                    except (TypeError, ValueError):
                        cooldown = PHOENIX_API_COOLDOWN
                    phoenix_api_cooldowns[api_url] = time.monotonic() + cooldown
                    now = time.time()
                    if now - last_phoenix_api_warn_time > 30:
                        last_phoenix_api_warn_time = now
                        print("⏳ Phoenix trade-data API rate limit (429); trying the next configured API.")
                else:
                    phoenix_api_cooldowns[api_url] = time.monotonic() + PHOENIX_API_COOLDOWN
                    now = time.time()
                    if now - last_phoenix_api_warn_time > 30:
                        last_phoenix_api_warn_time = now
                        print(f"⚠️ Phoenix trade-data API returned HTTP {resp.status}; trying the next configured API.")
        except Exception as exc:
            phoenix_api_cooldowns[api_url] = time.monotonic() + PHOENIX_API_COOLDOWN
            now = time.time()
            if now - last_phoenix_api_warn_time > 30:
                last_phoenix_api_warn_time = now
                print(f"⚠️ Phoenix trade-data API failed ({type(exc).__name__}); trying the next configured API.")
    return None


async def fetch_rows_for_signature(session, path, signature):
    cursor = None
    for _ in range(PHOENIX_API_PAGES):
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        payload = await phoenix_api_get(session, path, params)
        if not payload:
            return []
        rows = payload.get("data", [])
        if not isinstance(rows, list):
            return []
        matched = [
            row for row in rows
            if isinstance(row, dict) and row.get("signature") == signature
        ]
        if matched:
            return matched
        cursor = payload.get("nextCursor")
        if not cursor or not payload.get("hasMore", bool(cursor)):
            break
    return []


async def fetch_market_fills(session, symbol, signature, history_rows):
    path = f"/v1/trades/{quote(symbol, safe='')}/fills"
    stamps = [timestamp_ms(row.get("timestamp")) for row in history_rows]
    stamps = [stamp for stamp in stamps if stamp is not None]
    time_filter = {}
    if stamps:
        time_filter = {
            "startTime": min(stamps) - 60_000,
            "endTime": max(stamps) + 60_000,
        }

    cursor = None
    for _ in range(PHOENIX_API_PAGES):
        params = {"limit": 100, **time_filter}
        if cursor:
            params["cursor"] = cursor
        payload = await phoenix_api_get(session, path, params)
        if not payload:
            return []
        rows = payload.get("data", [])
        if not isinstance(rows, list):
            return []
        matched = [
            row for row in rows
            if isinstance(row, dict)
            and row.get("transactionSignature") == signature
            and row.get("marketSymbol") == symbol
        ]
        if matched:
            return matched
        cursor = payload.get("nextCursor")
        if not cursor or not payload.get("hasMore", bool(cursor)):
            break
    return []


async def fetch_phoenix_trade_details(session, wallet_addr, signature):
    path = f"/v1/trader/{quote(wallet_addr, safe='')}/trades-history"
    history_rows = []
    for attempt in range(3):
        history_rows = await fetch_rows_for_signature(session, path, signature)
        if history_rows:
            break
        if attempt < 2:
            await asyncio.sleep(0.7)
    if not history_rows:
        print(f"ℹ️ Phoenix has not indexed trade details yet for {signature[:8]}.")
        return []

    grouped = {}
    for row in history_rows:
        symbol = row.get("marketSymbol")
        if symbol:
            grouped.setdefault(symbol, {"symbol": symbol, "history": [], "fills": []})["history"].append(row)
    for symbol, market in grouped.items():
        market["fills"] = await fetch_market_fills(session, symbol, signature, market["history"])
    return list(grouped.values())


def market_metrics(market):
    history = market.get("history", [])
    fills = market.get("fills", [])
    valid_fills = []
    for row in fills:
        base = as_decimal(row.get("baseQty"))
        quote_value = as_decimal(row.get("quoteQty"))
        if base is not None and quote_value is not None:
            valid_fills.append((abs(base), abs(quote_value)))

    exact = bool(valid_fills)
    if exact:
        base_qty = sum((base for base, _ in valid_fills), Decimal("0"))
        quote_qty = sum((quote_value for _, quote_value in valid_fills), Decimal("0"))
        price = quote_qty / base_qty if base_qty else None
        fill_count = len(valid_fills)
    else:
        fallback = []
        for row in history:
            delta = as_decimal(row.get("baseLotsDelta"))
            price_value = as_decimal(row.get("price"))
            if delta is not None and price_value is not None and delta != 0:
                fallback.append((abs(delta), price_value))
        base_qty = sum((base for base, _ in fallback), Decimal("0")) if fallback else None
        quote_qty = sum((base * price for base, price in fallback), Decimal("0")) if fallback else None
        price = quote_qty / base_qty if base_qty and quote_qty is not None else None
        fill_count = len(fallback)

    deltas = [as_decimal(row.get("baseLotsDelta")) for row in history]
    sides = {"خرید" if delta > 0 else "فروش" for delta in deltas if delta not in (None, 0)}
    side = next(iter(sides)) if len(sides) == 1 else ("ترکیبی" if sides else "نامشخص")
    fees = [as_decimal(row.get("fees")) for row in history]
    pnls = [as_decimal(row.get("realizedPnl")) for row in history]
    return {
        "symbol": market.get("symbol"),
        "side": side,
        "base_qty": base_qty,
        "quote_qty": quote_qty,
        "price": price,
        "fees": sum((value for value in fees if value is not None), Decimal("0")) if any(value is not None for value in fees) else None,
        "pnl": sum((value for value in pnls if value is not None), Decimal("0")) if any(value is not None for value in pnls) else None,
        "trade_types": sorted({str(row.get("tradeType")) for row in history if row.get("tradeType")}),
        "instructions": sorted({str(row.get("instructionType")) for row in history if row.get("instructionType")}),
        "liquidity": sorted({str(row.get("liquidity")) for row in history if row.get("liquidity")}),
        "fill_count": fill_count,
        "exact": exact,
    }


def build_trade_details(markets):
    if not markets:
        return ""
    sections = ["\n📈 <b>جزئیات معامله</b>"]
    for market in markets[:5]:
        metrics = market_metrics(market)
        symbol = html.escape(str(metrics["symbol"] or "نامشخص"))
        lines = [f"🪙 <b>{symbol}</b> · {metrics['side']}"]
        if metrics["base_qty"] is not None:
            lines.append(f"📦 حجم اجراشده: <b>{format_number(metrics['base_qty'])} {symbol}</b>")
        else:
            lines.append("📦 حجم اجراشده: نامشخص")
        if metrics["price"] is not None:
            lines.append(f"💹 میانگین قیمت اجرا: <b>{format_number(metrics['price'])} PhUSD</b>")
        if metrics["quote_qty"] is not None:
            label = "ارزش معامله" if metrics["exact"] else "ارزش برآوردی معامله"
            lines.append(f"💰 {label}: <b>{format_number(metrics['quote_qty'])} PhUSD</b>")
        if metrics["fees"] is not None:
            lines.append(f"🧾 مجموع کارمزد: <b>{format_number(metrics['fees'])} PhUSD</b>")
        if metrics["pnl"] is not None:
            lines.append(f"📊 PnL تحقق‌یافته: <b>{format_number(metrics['pnl'])} PhUSD</b>")
        if metrics["fill_count"]:
            lines.append(f"🔢 تعداد اجراها: {metrics['fill_count']}")
        fills = market.get("fills", [])
        if fills:
            lines.append("🧾 <b>جزئیات هر اجرا:</b>")
            for index, fill in enumerate(fills[:5], start=1):
                fill_base = as_decimal(fill.get("baseQty"))
                fill_quote = as_decimal(fill.get("quoteQty"))
                fill_price = as_decimal(fill.get("price"))
                fill_side = metrics["side"]
                if fill_base is not None and fill_base != 0:
                    fill_side = "خرید" if fill_base > 0 else "فروش"
                details = [
                    f"{index}) {fill_side} {format_number(abs(fill_base)) if fill_base is not None else 'حجم نامشخص'} {symbol}"
                ]
                if fill_price is not None:
                    details.append(f"@ {format_number(fill_price)} PhUSD")
                if fill_quote is not None:
                    details.append(f"= {format_number(abs(fill_quote))} PhUSD")
                fill_instruction = fill.get("instructionType")
                if fill_instruction:
                    details.append(f"· {html.escape(str(fill_instruction))}")
                lines.append("🔹 " + " ".join(details))
            if len(fills) > 5:
                lines.append(f"… و {len(fills) - 5} اجرای دیگر")
        if metrics["trade_types"]:
            lines.append(f"⚡ نوع معامله: {html.escape(', '.join(metrics['trade_types']))}")
        if metrics["instructions"]:
            lines.append(f"🛠 دستور: {html.escape(', '.join(metrics['instructions']))}")
        if metrics["liquidity"]:
            lines.append(f"💧 نقدشوندگی: {html.escape(', '.join(metrics['liquidity']))}")
        sections.append("\n".join(lines))
    if len(markets) > 5:
        sections.append(f"… و {len(markets) - 5} بازار دیگر")
    return "\n\n".join(sections)


def transaction_account_key_rows(tx_info):
    message = tx_info.get("transaction", {}).get("message", {}) if isinstance(tx_info, dict) else {}
    keys = message.get("accountKeys", [])
    header = message.get("header", {})
    signer_count = header.get("numRequiredSignatures", 0) if isinstance(header, dict) else 0
    rows = []
    for index, item in enumerate(keys if isinstance(keys, list) else []):
        if isinstance(item, dict):
            pubkey = item.get("pubkey")
            signer = item.get("signer")
            if signer is None:
                signer = index < signer_count
        else:
            pubkey = item
            signer = index < signer_count
        if isinstance(pubkey, str):
            rows.append({"pubkey": pubkey, "signer": bool(signer)})
    return rows


def discover_session_signers(owner_addr, tx_info, signature, linked_signers, last_signatures):
    """Link non-fee-payer Phoenix signers only when the watched wallet is in that tx."""
    rows = transaction_account_key_rows(tx_info)
    pubkeys = [row["pubkey"] for row in rows]
    if owner_addr not in pubkeys:
        return []

    fee_payer = pubkeys[0] if pubkeys else None
    candidates = {
        row["pubkey"] for row in rows
        if row["signer"]
        and row["pubkey"] not in (owner_addr, fee_payer)
        and is_valid_solana_address(row["pubkey"])
    }
    if not candidates:
        return []

    saved = set(linked_signers.setdefault(owner_addr, []))
    new_signers = sorted(candidates - saved)
    if new_signers:
        saved.update(new_signers)
        linked_signers[owner_addr] = sorted(saved)
        save_linked_signers(linked_signers)
        for signer in new_signers:
            # This transaction is the safe starting cursor for the newly linked signer.
            last_signatures.setdefault(signer, signature)
            print(f"🔗 Linked Phoenix signer {signer[:6]}...{signer[-4:]} to {owner_addr[:6]}...{owner_addr[-4:]}")
    return new_signers


async def append_trade_details(session, wallet_addr, signature, message_id, alert_text):
    try:
        markets = await fetch_phoenix_trade_details(session, wallet_addr, signature)
        details_text = build_trade_details(markets)
        if details_text:
            await edit_telegram_alert(session, message_id, alert_text + details_text)
    except Exception as exc:
        print(f"⚠️ Trade details failed for {signature[:8]}: {type(exc).__name__}")


async def monitor_wallet(session: aiohttp.ClientSession, owner_addr: str, wallet_name: str,
                        source_addr: str, last_signatures: dict, seen_signatures: dict,
                        linked_signers: dict):
    owner_addr = str(owner_addr).strip()
    source_addr = str(source_addr).strip()
    wallet_name = str(wallet_name)
    if not is_valid_solana_address(source_addr):
        print(f"❌ Invalid runtime address for [{wallet_name}]: {source_addr[:6]}...{source_addr[-4:]} (chars={len(source_addr)})")
        return

    last_sig = last_signatures.get(source_addr)
    cursor_validator = None
    if last_sig is not None and last_sig != NO_SIGNATURE_BASELINE:
        cursor_validator = lambda rows: isinstance(rows, list) and any(
            isinstance(item, dict) and item.get("signature") == last_sig for item in rows
        )
    sigs = await fetch_rpc(
        session,
        "getSignaturesForAddress",
        [source_addr, {"limit": 100}],
        result_validator=cursor_validator,
    )
    if sigs is None:
        return

    if last_sig is None and not sigs:
        last_signatures[source_addr] = NO_SIGNATURE_BASELINE
        print(f"✅ Monitoring active for [{wallet_name}] (waiting for first transaction)")
        return
    if last_sig == NO_SIGNATURE_BASELINE and not sigs:
        return

    seen = seen_signatures.setdefault(owner_addr, {})
    if last_sig is None:
        last_signatures[source_addr] = sigs[0]["signature"]
        for sig_info in sigs:
            seen[sig_info["signature"]] = None
        save_seen_signatures(seen_signatures)
        if source_addr == owner_addr:
            print(f"✅ Monitoring active for [{wallet_name}]")
        else:
            print(f"✅ Monitoring linked Phoenix signer for [{wallet_name}]")
        return

    new_sigs = []
    baseline_found = last_sig == NO_SIGNATURE_BASELINE
    if baseline_found:
        new_sigs = list(sigs)
    else:
        for sig_info in sigs:
            sig = sig_info["signature"]
            if sig == last_sig:
                baseline_found = True
                break
            new_sigs.append(sig_info)

    if not baseline_found:
        # Multiple RPCs may have different history heads. Do not advance to an
        # unverified page and accidentally replay it; the round-robin will try
        # a different key/provider on the next request.
        now = time.time()
        if now - last_signature_gap_warn.get(source_addr, 0) > 15:
            last_signature_gap_warn[source_addr] = now
            print(f"⏳ RPC history for [{wallet_name}] missed its cursor; waiting for another provider.")
        return

    for sig_info in reversed(new_sigs):
        sig = sig_info["signature"]
        if sig in seen:
            last_signatures[source_addr] = sig
            continue
        if sig in inflight_signatures:
            continue

        err = sig_info.get("err")
        block_time = sig_info.get("blockTime")
        inflight_signatures.add(sig)
        tx_info = await fetch_rpc(
            session,
            "getTransaction",
            [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
        )
        if not tx_info:
            # Leave this signature pending. The next polling cycle can retry it
            # through another RPC key instead of silently losing the event.
            print(f"⏳ Could not fetch transaction {sig[:8]}; keeping it pending for RPC failover.")
            inflight_signatures.discard(sig)
            break

        phoenix_market_type = get_phoenix_type(tx_info)
        if not phoenix_market_type:
            seen[sig] = None
            last_signatures[source_addr] = sig
            if len(seen) > 1000:
                seen.pop(next(iter(seen)))
            save_seen_signatures(seen_signatures)
            inflight_signatures.discard(sig)
            continue

        discover_session_signers(owner_addr, tx_info, sig, linked_signers, last_signatures)

        time_str = datetime.fromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S') if block_time else "Unknown"
        status = "❌ Failed" if err else "✅ Success"
        raw_logs = tx_info.get("meta", {}).get("logMessages", [])
        action_type = quick_detect_action(raw_logs)
        phoenix_portfolio_url = f"https://www.phoenix.trade/portfolio?ghost={owner_addr}"
        alert_text = (
            f"🦅 <b>تراکنش جدید Phoenix ثبت شد!</b>\n\n"
            f"🏷 <b>نام ولت:</b> {html.escape(wallet_name)}\n"
            f"🏢 <b>بخش:</b> {html.escape(phoenix_market_type)}\n"
            f"👤 <b>آدرس:</b> <code>{owner_addr[:6]}...{owner_addr[-4:]}</code>\n"
            f"📌 <b>نوع دستور:</b> {action_type}\n"
            f"📊 <b>وضعیت:</b> {status}\n"
            f"⏰ <b>زمان:</b> {time_str}\n\n"
            f"💡 <i>جهت مشاهده جزئیات پوزیشن و پورتفولیو:</i>\n"
            f"🔗 <a href='https://solscan.io/tx/{sig}'>مشاهده تراکنش در Solscan</a>\n"
            f"🦅 <a href='{phoenix_portfolio_url}'>مشاهده پورتفولیو زنده در Phoenix</a>"
        )

        message_id = await send_telegram_alert(session, alert_text)
        if not message_id:
            # Don't mark the event as delivered when Telegram rejected the send.
            print(f"⚠️ Telegram did not confirm alert {sig[:8]}; keeping it pending for retry.")
            inflight_signatures.discard(sig)
            break

        seen[sig] = None
        last_signatures[source_addr] = sig
        if len(seen) > 1000:
            seen.pop(next(iter(seen)))
        save_seen_signatures(seen_signatures)
        print(f"⚡ Alert sent [{phoenix_market_type}] for {wallet_name}: {sig[:8]}")

        if not err and phoenix_market_type == "Phoenix Eternal (Perps) ⚡":
            task = asyncio.create_task(append_trade_details(session, owner_addr, sig, message_id, alert_text))
            background_tasks.add(task)
            task.add_done_callback(background_tasks.discard)
        inflight_signatures.discard(sig)


async def main():
    print(f"🚀 Phoenix Tracker Active using [{config_name}.py]...")
    print(
        f"🔌 RPC providers ready: {len(RPC_URLS)} "
        f"(Helius keys: {len(helius_urls)}, backups: {len(backup_rpcs)}, "
        f"timeout: {RPC_TIMEOUT}s, max attempts/request: {RPC_MAX_ATTEMPTS})."
    )
    print(f"📊 Phoenix trade-data API endpoints configured: {len(PHOENIX_API_URLS)}.")
    last_signatures = {}
    seen_signatures = load_seen_signatures()
    linked_signers = load_linked_signers()
    configured_linked_signers = getattr(config, "LINKED_SIGNERS", {})
    if isinstance(configured_linked_signers, dict):
        for owner_addr, signers in configured_linked_signers.items():
            if isinstance(signers, str):
                signers = [signers]
            if owner_addr not in TARGET_WALLETS or not isinstance(signers, (list, tuple, set)):
                continue
            current = set(linked_signers.get(owner_addr, []))
            current.update(
                signer.strip() for signer in signers
                if isinstance(signer, str)
                and signer.strip() != owner_addr
                and is_valid_solana_address(signer.strip())
            )
            if current:
                linked_signers[owner_addr] = sorted(current)
    if linked_signers:
        count = sum(len(signers) for signers in linked_signers.values())
        print(f"🔗 Loaded {count} linked Phoenix signer(s) for {len(linked_signers)} wallet(s).")

    async with aiohttp.ClientSession() as session:
        await send_telegram_alert(session, f"🚀 <b>Phoenix Multi-Wallet Tracker Active ({config_name}.py).</b>")

        while True:
            start_time = time.time()

            owners_by_signer = {}
            for owner_addr, signers in linked_signers.items():
                if owner_addr not in TARGET_WALLETS:
                    continue
                for signer in signers:
                    if signer in TARGET_WALLETS:
                        continue
                    owners_by_signer.setdefault(signer, set()).add(owner_addr)

            tasks = []
            for owner_addr, name in TARGET_WALLETS.items():
                tasks.append(monitor_wallet(
                    session, owner_addr, name, owner_addr,
                    last_signatures, seen_signatures, linked_signers
                ))

            for signer, owners in owners_by_signer.items():
                # A shared signer cannot be attributed to one wallet safely.
                if len(owners) != 1:
                    continue
                owner_addr = next(iter(owners))
                tasks.append(monitor_wallet(
                    session, owner_addr, TARGET_WALLETS[owner_addr], signer,
                    last_signatures, seen_signatures, linked_signers
                ))

            await asyncio.gather(*tasks)

            elapsed = time.time() - start_time
            sleep_time = max(0.1, POLL_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot Stopped.")
