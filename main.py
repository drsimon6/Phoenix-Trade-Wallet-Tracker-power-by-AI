import asyncio
import aiohttp
import time
from datetime import datetime, timezone
import sys
import importlib
import json
import html
import os
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

helius_urls = [
    f"https://mainnet.helius-rpc.com/?api-key={k}"
    for k in HELIUS_API_KEYS
    if k and "your-" not in str(k).lower() and "your_" not in str(k).lower()
]

backup_rpcs = getattr(config, 'RPC_URLS', [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
    "https://rpc.ankr.com/solana"
])

# Combine Primary Helius with Backup RPCs
RPC_URLS = helius_urls + [url for url in backup_rpcs if url not in helius_urls]
active_rpc_index = 0
RPC_CONCURRENCY = max(1, int(getattr(config, "RPC_CONCURRENCY", 3)))
rpc_semaphore = asyncio.Semaphore(RPC_CONCURRENCY)

last_429_warn_time = 0
last_phoenix_api_warn_time = 0
last_rpc_stale_warn_time = 0


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
            if resp.status == 200:
                result = await resp.json(content_type=None)
                return result.get("result", {}).get("message_id")
    except Exception:
        pass
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
                error_data = await resp.text()
                print(f"⚠️ Could not update Telegram alert ({resp.status}): {error_data[:200]}")
    except Exception:
        pass


async def fetch_rpc(session: aiohttp.ClientSession, method: str, params: list):
    global last_429_warn_time, last_rpc_stale_warn_time, active_rpc_index
    min_context_slot = None
    request_params = list(params)
    if method == "getSignaturesForAddress" and len(request_params) > 1:
        rpc_config = dict(request_params[1]) if isinstance(request_params[1], dict) else {}
        min_context_slot = rpc_config.get("minContextSlot")
        request_params[1] = rpc_config

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": request_params}
    attempts = len(RPC_URLS) if RPC_URLS else 1

    if not RPC_URLS:
        return None

    # Stay on one RPC after it succeeds. The old code rotated providers on every
    # request, so getSignaturesForAddress could return different/stale histories.
    stale_endpoints = 0
    for offset in range(attempts):
        rpc_index = (active_rpc_index + offset) % len(RPC_URLS)
        rpc_url = RPC_URLS[rpc_index]
        try:
            async with rpc_semaphore:
                async with session.post(rpc_url, json=payload, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("result") is not None:
                            result = data["result"]
                            if method == "getSignaturesForAddress" and min_context_slot is not None:
                                latest_slot = result[0].get("slot") if isinstance(result, list) and result else None
                                # A successful HTTP response can still come from an RPC whose
                                # address-history index is behind the last signature we saw.
                                # Reject it here and try the next configured provider.
                                if latest_slot is None or latest_slot < min_context_slot:
                                    stale_endpoints += 1
                                    continue
                            active_rpc_index = rpc_index
                            return result
                        rpc_error = str(data.get("error", "")).casefold()
                        if min_context_slot is not None and (
                            "minimum context slot" in rpc_error or "mincontextslot" in rpc_error
                        ):
                            stale_endpoints += 1
                            continue
                    elif resp.status == 429:
                        # Prevent log spam: Print at most once every 15 seconds
                        now = time.time()
                        if now - last_429_warn_time > 15:
                            last_429_warn_time = now
                            print("⏳ RPC rate limit (429). Switching to the next backup RPC...")
                        continue
        except Exception:
            continue

    if stale_endpoints:
        now = time.time()
        if now - last_rpc_stale_warn_time > 15:
            last_rpc_stale_warn_time = now
            print("⏳ RPC backups are behind the last seen slot; waiting for a caught-up provider.")
    active_rpc_index = (active_rpc_index + 1) % len(RPC_URLS)
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


# Phoenix Eternal exposes decoded public trade history through its official Rise API.
# Keep these lookups optional: if the API is temporarily unavailable, the original
# transaction alert still goes out unchanged.
PHOENIX_API_URL = getattr(config, "PHOENIX_API_URL", "https://perp-api.phoenix.trade").rstrip("/")
PHOENIX_API_TIMEOUT = getattr(config, "PHOENIX_API_TIMEOUT", 5)
PHOENIX_API_PAGES = getattr(config, "PHOENIX_API_PAGES", 3)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or getattr(config, "OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL") or getattr(config, "OPENAI_MODEL", "gpt-6-luna")


def clean_api_key(value):
    key = str(value or "").strip()
    lowered = key.casefold()
    placeholders = (
        "your-",
        "your_",
        "placeholder",
        "replace_me",
        "replace-me",
        "کلید api خودت",
        "api key here",
    )
    return "" if not key or any(marker in lowered for marker in placeholders) else key


OPENAI_API_KEY = clean_api_key(OPENAI_API_KEY)


def _as_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _timestamp_ms(value):
    """Phoenix trade-history timestamps are ISO strings; accept numeric forms too."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float, Decimal)):
            number = Decimal(str(value))
            return int(number if number > Decimal("100000000000") else number * 1000)
        text = str(value).strip()
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return None


def _fmt_number(value, max_decimals=8):
    if value is None:
        return "نامشخص"
    text = f"{value:,.{max_decimals}f}".rstrip("0").rstrip(".")
    return text if text else "0"


async def phoenix_api_get(session: aiohttp.ClientSession, path: str, params: dict = None):
    """Fetches public Phoenix API data; failures must not stop transaction alerts."""
    global last_phoenix_api_warn_time
    try:
        async with session.get(
            f"{PHOENIX_API_URL}{path}",
            params=params,
            timeout=PHOENIX_API_TIMEOUT,
        ) as resp:
            if resp.status != 200:
                now = time.time()
                if now - last_phoenix_api_warn_time > 30:
                    last_phoenix_api_warn_time = now
                    print(f"⚠️ Phoenix trade-data API returned HTTP {resp.status}; details may be delayed.")
                return None
            payload = await resp.json(content_type=None)
            return payload if isinstance(payload, dict) else None
    except Exception:
        return None


async def fetch_phoenix_api_rows(session, path, signature):
    """Read a few newest pages and return rows belonging to this exact signature."""
    cursor = None
    for _ in range(max(1, int(PHOENIX_API_PAGES))):
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        payload = await phoenix_api_get(session, path, params)
        if not payload:
            return []

        rows = payload.get("data", [])
        if not isinstance(rows, list):
            return []
        matched = [row for row in rows if isinstance(row, dict) and row.get("signature") == signature]
        if matched:
            return matched

        cursor = payload.get("nextCursor")
        if not cursor or not payload.get("hasMore", bool(cursor)):
            break
    return []


async def fetch_market_fills_for_signature(session, symbol, signature, history_rows):
    """Fetch the fill page around this transaction, then match its exact signature."""
    cursor = None
    path = f"/v1/trades/{quote(symbol, safe='')}/fills"
    timestamps = [_timestamp_ms(row.get("timestamp")) for row in history_rows]
    timestamps = [value for value in timestamps if value is not None]
    time_params = {}
    if timestamps:
        # A narrow time window avoids paging through hundreds of newer market fills.
        time_params = {
            "startTime": min(timestamps) - 60_000,
            "endTime": max(timestamps) + 60_000,
        }

    for _ in range(max(1, int(PHOENIX_API_PAGES))):
        params = {"limit": 100, **time_params}
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
    """Enrich an Eternal transaction using Phoenix's decoded trader/fill APIs."""
    history_path = f"/v1/trader/{quote(wallet_addr, safe='')}/trades-history"

    # The public API can index a confirmed transaction a moment after the RPC.
    history_rows = []
    for attempt in range(3):
        history_rows = await fetch_phoenix_api_rows(session, history_path, signature)
        if history_rows:
            break
        if attempt < 2:
            await asyncio.sleep(0.7)

    if not history_rows:
        print(f"ℹ️ No decoded Phoenix fill found yet for {signature[:8]}; AI/details skipped for this alert.")
        return []

    by_market = {}
    for row in history_rows:
        symbol = row.get("marketSymbol")
        if not symbol:
            continue
        item = by_market.setdefault(symbol, {
            "history": [],
            "fills": [],
        })
        item["history"].append(row)

    # Market fill records supply exact base and quote quantities. History records
    # supply fees, realized PnL, side direction, and order/trade type.
    for symbol, item in by_market.items():
        item["fills"] = await fetch_market_fills_for_signature(
            session, symbol, signature, item["history"]
        )

    return [
        {"symbol": symbol, **item}
        for symbol, item in by_market.items()
    ]


def get_market_metrics(market):
    history = market.get("history", [])
    fills = market.get("fills", [])

    parsed_base = [_as_decimal(row.get("baseQty")) for row in fills]
    parsed_quote = [_as_decimal(row.get("quoteQty")) for row in fills]
    valid_fill_rows = [
        (abs(base), quote)
        for base, quote in zip(parsed_base, parsed_quote)
        if base is not None and quote is not None
    ]

    exact_fill_data = bool(valid_fill_rows)
    if exact_fill_data:
        base_qty = sum((base for base, _ in valid_fill_rows), Decimal("0"))
        quote_qty = sum((quote for _, quote in valid_fill_rows), Decimal("0"))
        price = quote_qty / base_qty if base_qty else None
        fill_count = len(valid_fill_rows)
    else:
        # Rise trade history already provides signed position-size deltas and
        # execution prices, so use those as a fallback if a fill page is delayed.
        fallback_rows = []
        for row in history:
            delta = _as_decimal(row.get("baseLotsDelta"))
            price_value = _as_decimal(row.get("price"))
            if delta is not None and price_value is not None and delta != 0:
                fallback_rows.append((abs(delta), price_value))
        base_qty = sum((base for base, _ in fallback_rows), Decimal("0")) if fallback_rows else None
        quote_qty = sum((base * price_value for base, price_value in fallback_rows), Decimal("0")) if fallback_rows else None
        price = quote_qty / base_qty if base_qty and quote_qty is not None else None
        fill_count = len(fallback_rows) if fallback_rows else len(history)

    deltas = [_as_decimal(row.get("baseLotsDelta")) for row in history]
    sides = {"خرید" if value > 0 else "فروش" for value in deltas if value not in (None, 0)}
    side_text = next(iter(sides)) if len(sides) == 1 else ("ترکیبی" if sides else "نامشخص")

    fees = [_as_decimal(row.get("fees")) for row in history]
    pnls = [_as_decimal(row.get("realizedPnl")) for row in history]
    fee_total = sum((v for v in fees if v is not None), Decimal("0")) if any(v is not None for v in fees) else None
    pnl_total = sum((v for v in pnls if v is not None), Decimal("0")) if any(v is not None for v in pnls) else None

    return {
        "symbol": market.get("symbol"),
        "side": side_text,
        "base_qty": base_qty,
        "quote_qty": quote_qty,
        "price": price,
        "fee_total": fee_total,
        "pnl_total": pnl_total,
        "trade_types": sorted({str(row.get("tradeType")) for row in history if row.get("tradeType")}),
        "instruction_types": sorted({str(row.get("instructionType")) for row in history if row.get("instructionType")}),
        "liquidity_types": sorted({str(row.get("liquidity")) for row in history if row.get("liquidity")}),
        "fill_count": fill_count,
        "exact_fill_data": exact_fill_data,
    }


def build_trade_details_text(markets):
    if not markets:
        return ""

    sections = ["\n📈 <b>جزئیات اجرا</b>"]
    for market in markets[:5]:
        metrics = get_market_metrics(market)
        symbol = html.escape(str(metrics["symbol"] or "نامشخص"))
        lines = [f"🪙 <b>{symbol}</b> · {metrics['side']}"]

        if metrics["base_qty"] is not None:
            lines.append(f"📦 حجم اجراشده: <b>{_fmt_number(metrics['base_qty'])} {symbol}</b>")
        else:
            lines.append("📦 حجم اجراشده: نامشخص")

        if metrics["price"] is not None:
            lines.append(f"💹 میانگین قیمت اجرا: <b>{_fmt_number(metrics['price'])} PhUSD</b>")
        if metrics["quote_qty"] is not None:
            quote_label = "ارزش معامله" if metrics["exact_fill_data"] else "ارزش برآوردی معامله"
            lines.append(f"💰 {quote_label}: <b>{_fmt_number(metrics['quote_qty'])} PhUSD</b>")
        if metrics["fee_total"] is not None:
            lines.append(f"🧾 کارمزد: <b>{_fmt_number(metrics['fee_total'])} PhUSD</b>")
        if metrics["pnl_total"] is not None:
            lines.append(f"📊 PnL تحقق‌یافته: <b>{_fmt_number(metrics['pnl_total'])} PhUSD</b>")
        if metrics["fill_count"]:
            lines.append(f"🔢 تعداد اجراها: {metrics['fill_count']}")
        if metrics["trade_types"]:
            lines.append(f"⚡ نوع معامله: {', '.join(metrics['trade_types'])}")
        if metrics["instruction_types"]:
            lines.append(f"🛠 دستور: {', '.join(metrics['instruction_types'])}")
        if metrics["liquidity_types"]:
            lines.append(f"💧 نقدشوندگی: {', '.join(metrics['liquidity_types'])}")
        sections.append("\n".join(lines))

    if len(markets) > 5:
        sections.append(f"… و {len(markets) - 5} بازار دیگر")
    return "\n\n".join(sections)


async def analyze_trade_with_ai(session, markets):
    """Optional Persian explanation based only on verified structured fields."""
    if not OPENAI_API_KEY or not markets:
        return None

    facts = []
    for market in markets:
        metrics = get_market_metrics(market)
        facts.append({
            "symbol": metrics["symbol"],
            "side": metrics["side"],
            "base_volume": str(metrics["base_qty"]) if metrics["base_qty"] is not None else None,
            "quote_volume_PhUSD": str(metrics["quote_qty"]) if metrics["quote_qty"] is not None else None,
            "execution_price_PhUSD": str(metrics["price"]) if metrics["price"] is not None else None,
            "trade_types": metrics["trade_types"],
            "fees_PhUSD": str(metrics["fee_total"]) if metrics["fee_total"] is not None else None,
            "realized_pnl_PhUSD": str(metrics["pnl_total"]) if metrics["pnl_total"] is not None else None,
        })

    payload = {
        "model": OPENAI_MODEL,
        "store": False,
        "reasoning": {"effort": "low"},
        "max_output_tokens": 180,
        "instructions": (
            "به فارسی و حداکثر در دو جمله، فقط همین داده‌های تأییدشده را توضیح بده. "
            "سمت را فقط از فیلد side بخوان؛ قیمت، حجم، سود، علت یا سیگنال را حدس نزن. "
            "اگر داده‌ای null است، درباره‌اش ادعایی نکن. توصیه خرید یا فروش نده."
        ),
        "input": json.dumps(facts, ensure_ascii=False),
    }
    try:
        async with session.post(
            "https://api.openai.com/v1/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json=payload,
            timeout=8,
        ) as resp:
            if resp.status != 200:
                error_body = (await resp.text()).replace("\n", " ")[:300]
                print(
                    f"⚠️ AI analysis failed (OpenAI HTTP {resp.status}): {error_body}"
                )
                return None
            response = await resp.json(content_type=None)
        texts = []
        for output_item in response.get("output", []):
            for content in output_item.get("content", []):
                if content.get("type") == "output_text" and content.get("text"):
                    texts.append(content["text"].strip())
        if not texts:
            print("⚠️ OpenAI returned no text for the trade analysis.")
            return None
        return " ".join(texts)[:700]
    except Exception as exc:
        print(f"⚠️ AI analysis request failed: {type(exc).__name__}")
        return None


async def send_and_enrich_alert(session, alert_text, wallet_addr, signature, phoenix_market_type, failed):
    """Send the original alert immediately, then add decoded details if available."""
    message_id = await send_telegram_alert(session, alert_text)
    if (
        not message_id
        or failed
        or phoenix_market_type != "Phoenix Eternal (Perps) ⚡"
    ):
        return

    try:
        markets = await fetch_phoenix_trade_details(session, wallet_addr, signature)
        if not markets:
            return

        enriched_text = alert_text + build_trade_details_text(markets)
        analysis = await analyze_trade_with_ai(session, markets)
        if analysis:
            enriched_text += f"\n\n🤖 <b>تحلیل AI:</b>\n<i>{html.escape(analysis)}</i>"
        await edit_telegram_alert(session, message_id, enriched_text)
    except Exception as exc:
        # Enrichment is best-effort and must never affect the original alert.
        print(f"⚠️ Phoenix details enrichment failed for {signature[:8]}: {exc}")


async def monitor_wallet(
    session: aiohttp.ClientSession,
    wallet_addr: str,
    wallet_name: str,
    last_signatures: dict,
    last_signature_slots: dict,
):
    previous_slot = last_signature_slots.get(wallet_addr)
    signature_config = {"limit": 20}
    if previous_slot is not None:
        # Ask each provider to evaluate the request at least at the cursor slot.
        # Older backups return an RPC error and fetch_rpc tries another endpoint.
        signature_config["minContextSlot"] = previous_slot
    sigs = await fetch_rpc(
        session,
        "getSignaturesForAddress",
        [wallet_addr, signature_config],
    )
    if not sigs:
        return

    latest = sigs[0]
    latest_signature = latest.get("signature")
    latest_slot = latest.get("slot")
    last_sig = last_signatures.get(wallet_addr)
    if last_sig is None:
        # Establish a fresh baseline on every process start; never replay history.
        last_signatures[wallet_addr] = latest_signature
        last_signature_slots[wallet_addr] = latest_slot
        print(f"✅ Monitoring active for [{wallet_name}]")
        return

    if previous_slot is not None and latest_slot is not None and latest_slot < previous_slot:
        # Some public RPCs can lag behind the last response. Treat that response
        # as stale and do not mistake its old signatures for newly seen trades.
        print(f"⏳ Ignoring stale RPC history for [{wallet_name}] (slot moved backwards).")
        return

    new_sigs = []
    baseline_found = False
    for sig_info in sigs:
        sig = sig_info["signature"]
        if sig == last_sig:
            baseline_found = True
            break
        new_sigs.append(sig_info)

    if not baseline_found:
        # The cursor falling out of the 20-signature window is ambiguous: it may
        # be an RPC history gap. Re-sync without sending that batch as old alerts.
        if latest_signature:
            last_signatures[wallet_addr] = latest_signature
            last_signature_slots[wallet_addr] = latest_slot
        print(f"⚠️ Signature gap for [{wallet_name}]; re-synced without replaying older transactions.")
        return

    if new_sigs:
        last_signatures[wallet_addr] = new_sigs[0]["signature"]
        last_signature_slots[wallet_addr] = new_sigs[0].get("slot")

        for sig_info in reversed(new_sigs):
            sig = sig_info["signature"]
            err = sig_info.get("err")
            block_time = sig_info.get("blockTime")
            
            tx_info = await fetch_rpc(session, "getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
            
            # Filter non-Phoenix transactions
            phoenix_market_type = get_phoenix_type(tx_info)
            if not phoenix_market_type:
                continue

            time_str = datetime.fromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S') if block_time else "Unknown"
            status = "❌ Failed" if err else "✅ Success"

            raw_logs = tx_info.get("meta", {}).get("logMessages", []) if tx_info else []
            action_type = quick_detect_action(raw_logs)
            phoenix_portfolio_url = f"https://www.phoenix.trade/portfolio?ghost={wallet_addr}"

            alert_text = (
                f"🦅 <b>تراکنش جدید Phoenix ثبت شد!</b>\n\n"
                f"🏷 <b>نام ولت:</b> {wallet_name}\n"
                f"🏢 <b>بخش:</b> {phoenix_market_type}\n"
                f"👤 <b>آدرس:</b> <code>{wallet_addr[:6]}...{wallet_addr[-4:]}</code>\n"
                f"📌 <b>نوع دستور:</b> {action_type}\n"
                f"📊 <b>وضعیت:</b> {status}\n"
                f"⏰ <b>زمان:</b> {time_str}\n\n"
                f"💡 <i>جهت مشاهده جزئیات پوزیشن و پورتفولیو:</i>\n"
                f"🔗 <a href='https://solscan.io/tx/{sig}'>مشاهده تراکنش در Solscan</a>\n"
                f"🦅 <a href='{phoenix_portfolio_url}'>مشاهده پورتفولیو زنده در Phoenix</a>"
            )

            asyncio.create_task(
                send_and_enrich_alert(
                    session,
                    alert_text,
                    wallet_addr,
                    sig,
                    phoenix_market_type,
                    bool(err),
                )
            )
            print(f"⚡ Alert sent [{phoenix_market_type}] for {wallet_name}: {sig[:8]}")


async def main():
    print(f"🚀 Phoenix Tracker Active using [{config_name}.py]...")
    if not OPENAI_API_KEY:
        print("⚠️ AI analysis is disabled: OPENAI_API_KEY is empty or still a placeholder in this config.")
    last_signatures = {}
    last_signature_slots = {}

    async with aiohttp.ClientSession() as session:
        await send_telegram_alert(session, f"🚀 <b>Phoenix Multi-Wallet Tracker Active ({config_name}.py).</b>")

        while True:
            start_time = time.time()
            
            tasks = [
                monitor_wallet(session, addr, name, last_signatures, last_signature_slots)
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
