import asyncio
import aiohttp
import time
from datetime import datetime, timezone
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
POLL_INTERVAL = getattr(config, 'POLL_INTERVAL', 1.5)
TELEGRAM_BOT_TOKEN = config.TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID = config.TELEGRAM_CHAT_ID

# Official Phoenix Program IDs on Solana Mainnet
PHOENIX_PROGRAMS = {
    "PhoeNiX2EyyJBKw5EaWZbg8hkfz3DcjMBNfmsyJR8qQ": "Phoenix Spot 🦅",
    "EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih": "Phoenix Eternal (Perps) ⚡"
}

# Helius + Backup RPC Setup
HELIUS_API_KEYS = getattr(config, 'HELIUS_API_KEYS', [])
if not HELIUS_API_KEYS and hasattr(config, 'HELIUS_API_KEY') and config.HELIUS_API_KEY:
    HELIUS_API_KEYS = [config.HELIUS_API_KEY]
elif isinstance(HELIUS_API_KEYS, str):
    HELIUS_API_KEYS = [HELIUS_API_KEYS]

helius_urls = [
    f"https://mainnet.helius-rpc.com/?api-key={quote(str(key).strip(), safe='')}"
    for key in HELIUS_API_KEYS
    if key and "your-" not in str(key).lower() and "your_" not in str(key).lower()
]

configured_rpcs = getattr(config, 'RPC_URLS', [
    "https://solana-rpc.publicnode.com",
    "https://api.mainnet-beta.solana.com",
])
if isinstance(configured_rpcs, str):
    configured_rpcs = [configured_rpcs]

backup_rpcs = [
    str(r).strip() for r in configured_rpcs
    if str(r).strip() and "mainnet.helius-rpc.com" not in str(r).lower()
]

RPC_URLS = list(dict.fromkeys(helius_urls + backup_rpcs))
rpc_round_robin_index = 0

last_429_warn_time = 0
last_rpc_error_warn_time = 0
SEEN_SIGNATURES_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "seen_signatures.json"
)
# افزایش هم‌زمانی برای جلوگیری از ایجاد صف طولانی
RPC_CONCURRENCY = max(1, int(getattr(config, "RPC_CONCURRENCY", 10)))
rpc_semaphore = asyncio.Semaphore(RPC_CONCURRENCY)
last_signature_gap_warn = {}


def load_seen_signatures():
    try:
        with open(SEEN_SIGNATURES_FILE, "r", encoding="utf-8") as file:
            saved = json.load(file)
    except (FileNotFoundError, OSError, json.JSONDecodeError):
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
    global rpc_round_robin_index
    if not RPC_URLS:
        return None
    index = rpc_round_robin_index
    rpc_round_robin_index = (rpc_round_robin_index + 1) % len(RPC_URLS)
    return index


async def send_telegram_alert(session: aiohttp.ClientSession, message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
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
            print(f"⚠️ Telegram send failed (HTTP {resp.status}): {str(data)[:200]}")
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
                error_text = (await resp.text())[:200]
                print(f"⚠️ Telegram could not add trade details ({resp.status}): {error_text}")
    except Exception:
        pass


async def fetch_rpc(session: aiohttp.ClientSession, method: str, params: list):
    global last_429_warn_time, last_rpc_error_warn_time
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    total_urls = len(RPC_URLS)
    if not total_urls:
        return None

    # حداکثر ۳ تلاش سریع برای جلوگیری از فریز شدن صف روی RPCهای کند
    max_attempts = min(3, total_urls)
    first_index = get_next_rpc_index()
    if first_index is None:
        return None

    for offset in range(max_attempts):
        provider_index = (first_index + offset) % total_urls
        rpc_url = RPC_URLS[provider_index]
        try:
            async with rpc_semaphore:
                # تایم‌اوت کوتاه ۲.۵ ثانیه‌ای برای جلوگیری از ایجاد تأخیر انباشته
                async with session.post(rpc_url, json=payload, timeout=2.5) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if data.get("result") is not None:
                            return data["result"]
                    elif resp.status == 429:
                        now = time.time()
                        if now - last_429_warn_time > 15:
                            last_429_warn_time = now
                            print(f"⏳ RPC #{provider_index + 1} rate limit (429); switching key...")
                        continue
        except Exception:
            continue
    return None


def get_phoenix_type(tx_info: dict) -> str:
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
    if not logs:
        return "⚡ معامله / مدیریت سفارش"
    
    logs_str = " ".join(logs).lower()
    if "placelimit" in logs_str or "place_limit" in logs_str or "limitorder" in logs_str:
        return "📥 ثبت سفارش لیمیت (Limit Order)"
    elif "placemarket" in logs_str or "place_market" in logs_str or "marketorder" in logs_str:
        return "⚡ معامله مارکت (Market Order)"
    elif "cancel" in logs_str:
        return "❌ لغو سفارش (Cancel Order)"
    elif "deposit" in logs_str or "withdraw" in logs_str:
        return "💳 واریز / برداشت مارجین"
    
    return "⚡ معامله / مدیریت سفارش"


# =====================================================================
# --- Phoenix Eternal Trade Details Enrichment ---
# =====================================================================
PHOENIX_API_URL = getattr(config, "PHOENIX_API_URL", "https://perp-api.phoenix.trade").rstrip("/")
PHOENIX_API_TIMEOUT = getattr(config, "PHOENIX_API_TIMEOUT", 5)
PHOENIX_API_PAGES = max(1, int(getattr(config, "PHOENIX_API_PAGES", 2)))


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
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def format_number(value, decimals=8):
    if value is None:
        return "نامشخص"
    text = f"{value:,.{decimals}f}".rstrip("0").rstrip(".")
    return text or "0"


async def phoenix_api_get(session, path, params=None):
    try:
        async with session.get(
            f"{PHOENIX_API_URL}{path}", params=params, timeout=PHOENIX_API_TIMEOUT
        ) as resp:
            if resp.status != 200:
                return None
            payload = await resp.json(content_type=None)
            return payload if isinstance(payload, dict) else None
    except Exception:
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
    for attempt in range(4):
        history_rows = await fetch_rows_for_signature(session, path, signature)
        if history_rows:
            break
        if attempt < 3:
            await asyncio.sleep(1.0)
    if not history_rows:
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
    return "\n\n".join(sections)


async def dispatch_alert_background(session, alert_text, wallet_addr, sig, phoenix_market_type, is_failed):
    """ارسال فوری پیام و تکمیل جزئیات در پس‌زمینه بدون مسدود کردن حلقه اصلی"""
    message_id = await send_telegram_alert(session, alert_text)
    if not message_id:
        return

    if not is_failed and phoenix_market_type == "Phoenix Eternal (Perps) ⚡":
        try:
            markets = await fetch_phoenix_trade_details(session, wallet_addr, sig)
            details_text = build_trade_details(markets)
            if details_text:
                await edit_telegram_alert(session, message_id, alert_text + details_text)
        except Exception as exc:
            print(f"⚠️ Trade details failed for {sig[:8]}: {type(exc).__name__}")


async def process_single_signature(session, wallet_addr, wallet_name, sig_info, seen, seen_signatures):
    """بررسی موازی یک تراکنش جهت جلوگیری از گیر کردن صف"""
    sig = sig_info["signature"]
    if sig in seen:
        return

    err = sig_info.get("err")
    block_time = sig_info.get("blockTime")
    tx_info = await fetch_rpc(
        session,
        "getTransaction",
        [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
    )
    if not tx_info:
        return

    seen[sig] = None
    if len(seen) > 1000:
        seen.pop(next(iter(seen)))

    phoenix_market_type = get_phoenix_type(tx_info)
    if not phoenix_market_type:
        return

    time_str = datetime.fromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S') if block_time else "Unknown"
    status = "❌ Failed" if err else "✅ Success"
    raw_logs = tx_info.get("meta", {}).get("logMessages", [])
    action_type = quick_detect_action(raw_logs)
    phoenix_portfolio_url = f"https://www.phoenix.trade/portfolio?ghost={wallet_addr}"

    alert_text = (
        f"🦅 <b>تراکنش جدید Phoenix ثبت شد!</b>\n\n"
        f"🏷 <b>نام ولت:</b> {html.escape(wallet_name)}\n"
        f"🏢 <b>بخش:</b> {html.escape(phoenix_market_type)}\n"
        f"👤 <b>آدرس:</b> <code>{wallet_addr[:6]}...{wallet_addr[-4:]}</code>\n"
        f"📌 <b>نوع دستور:</b> {action_type}\n"
        f"📊 <b>وضعیت:</b> {status}\n"
        f"⏰ <b>زمان:</b> {time_str}\n\n"
        f"💡 <i>جهت مشاهده جزئیات پوزیشن و پورتفولیو:</i>\n"
        f"🔗 <a href='https://solscan.io/tx/{sig}'>مشاهده تراکنش در Solscan</a>\n"
        f"🦅 <a href='{phoenix_portfolio_url}'>مشاهده پورتفولیو زنده در Phoenix</a>"
    )

    asyncio.create_task(
        dispatch_alert_background(session, alert_text, wallet_addr, sig, phoenix_market_type, bool(err))
    )
    print(f"⚡ Alert triggered [{phoenix_market_type}] for {wallet_name}: {sig[:8]}")


async def monitor_wallet(session: aiohttp.ClientSession, wallet_addr: str, wallet_name: str, last_signatures: dict, seen_signatures: dict):
    wallet_addr = str(wallet_addr).strip()
    wallet_name = str(wallet_name)
    sigs = await fetch_rpc(session, "getSignaturesForAddress", [wallet_addr, {"limit": 15}])
    if not sigs:
        return

    seen = seen_signatures.setdefault(wallet_addr, {})
    last_sig = last_signatures.get(wallet_addr)
    if last_sig is None:
        last_signatures[wallet_addr] = sigs[0]["signature"]
        for sig_info in sigs:
            seen[sig_info["signature"]] = None
        save_seen_signatures(seen_signatures)
        print(f"✅ Monitoring active for [{wallet_name}]")
        return

    new_sigs = []
    for sig_info in sigs:
        sig = sig_info["signature"]
        if sig == last_sig or sig in seen:
            break
        new_sigs.append(sig_info)

    if new_sigs:
        last_signatures[wallet_addr] = new_sigs[0]["signature"]
        # بررسی هم‌زمان (موازی) تمام امضاهای جدید بدون قفل کردن صف
        tx_tasks = [
            process_single_signature(session, wallet_addr, wallet_name, sig_info, seen, seen_signatures)
            for sig_info in reversed(new_sigs)
        ]
        await asyncio.gather(*tx_tasks)
        save_seen_signatures(seen_signatures)


async def main():
    print(f"🚀 High-Speed Phoenix Tracker Active using [{config_name}.py]...")
    print(f"🔌 RPC providers ready: {len(RPC_URLS)} (Helius keys: {len(helius_urls)}, backups: {len(backup_rpcs)}).")
    last_signatures = {}
    seen_signatures = load_seen_signatures()

    async with aiohttp.ClientSession() as session:
        await send_telegram_alert(session, f"🚀 <b>High-Speed Phoenix Tracker Active ({config_name}.py).</b>")

        while True:
            start_time = time.time()
            
            tasks = [
                monitor_wallet(session, addr, name, last_signatures, seen_signatures)
                for addr, name in TARGET_WALLETS.items()
            ]
            await asyncio.gather(*tasks)

            elapsed = time.time() - start_time
            sleep_time = max(0.1, POLL_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot Stopped.")
