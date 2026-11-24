import asyncio
import aiohttp
import time
from datetime import datetime
import sys
import itertools
import importlib
import json
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

helius_urls = [f"https://mainnet.helius-rpc.com/?api-key={k}" for k in HELIUS_API_KEYS if k and "your-" not in str(k)]

backup_rpcs = getattr(config, 'RPC_URLS', [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
    "https://rpc.ankr.com/solana"
])

# Combine Primary Helius with Backup RPCs
RPC_URLS = helius_urls + [url for url in backup_rpcs if url not in helius_urls]
helius_cycle = itertools.cycle(RPC_URLS) if RPC_URLS else None

last_429_warn_time = 0


def get_next_rpc_url():
    if helius_cycle:
        return next(helius_cycle)
    return RPC_URLS[0]


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
    global last_429_warn_time
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    attempts = len(RPC_URLS) if RPC_URLS else 1

    for _ in range(attempts):
        rpc_url = get_next_rpc_url()
        try:
            async with session.post(rpc_url, json=payload, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "result" in data:
                        return data["result"]
                elif resp.status == 429:
                    # Prevent log spam: Print at most once every 15 seconds
                    now = time.time()
                    if now - last_429_warn_time > 15:
                        last_429_warn_time = now
                        print("⏳ Helius Rate Limit (429) hit. Automatically switching to backup RPC...")
                    continue
        except Exception:
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


# Phoenix Eternal exposes decoded public trade history through its official Rise API.
# Keep these lookups optional: if the API is temporarily unavailable, the original
# transaction alert still goes out unchanged.
PHOENIX_API_URL = getattr(config, "PHOENIX_API_URL", "https://perp-api.phoenix.trade").rstrip("/")
PHOENIX_API_TIMEOUT = getattr(config, "PHOENIX_API_TIMEOUT", 5)
PHOENIX_API_PAGES = getattr(config, "PHOENIX_API_PAGES", 3)

OPENAI_API_KEY = getattr(config, "OPENAI_API_KEY", "")
OPENAI_MODEL = getattr(config, "OPENAI_MODEL", "gpt-6-luna")


def _as_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _fmt_number(value, max_decimals=8):
    if value is None:
        return "نامشخص"
    text = f"{value:,.{max_decimals}f}".rstrip("0").rstrip(".")
    return text if text else "0"


async def phoenix_api_get(session: aiohttp.ClientSession, path: str, params: dict = None):
    """Fetches public Phoenix API data; failures must not stop transaction alerts."""
    try:
        async with session.get(
            f"{PHOENIX_API_URL}{path}",
            params=params,
            timeout=PHOENIX_API_TIMEOUT,
        ) as resp:
            if resp.status != 200:
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


async def fetch_market_fills_for_signature(session, symbol, signature):
    """Fetch recent market fills and match by transaction signature."""
    cursor = None
    path = f"/v1/trades/{quote(symbol, safe='')}/fills"
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
        item["fills"] = await fetch_market_fills_for_signature(session, symbol, signature)

    return [
        {"symbol": symbol, **item}
        for symbol, item in by_market.items()
    ]


def build_trade_details_text(markets):
    if not markets:
        return ""

    sections = ["\n📈 <b>جزئیات معامله:</b>"]
    for market in markets[:5]:
        symbol = market["symbol"]
        history = market.get("history", [])
        fills = market.get("fills", [])

        base_qtys = [_as_decimal(row.get("baseQty")) for row in fills]
        quote_qtys = [_as_decimal(row.get("quoteQty")) for row in fills]
        base_qty = sum((v for v in base_qtys if v is not None), Decimal("0")) if fills else None
        quote_qty = sum((v for v in quote_qtys if v is not None), Decimal("0")) if fills else None
        price = (quote_qty / base_qty) if base_qty and quote_qty is not None else None

        signed_deltas = [
            _as_decimal(row.get("baseLotsDelta"))
            for row in history
        ]
        sides = {"خرید" if value > 0 else "فروش" for value in signed_deltas if value not in (None, 0)}
        side_text = next(iter(sides)) if len(sides) == 1 else ("خرید و فروش" if sides else "نامشخص")

        fees = [_as_decimal(row.get("fees")) for row in history]
        pnl = [_as_decimal(row.get("realizedPnl")) for row in history]
        fee_total = sum((v for v in fees if v is not None), Decimal("0")) if any(v is not None for v in fees) else None
        pnl_total = sum((v for v in pnl if v is not None), Decimal("0")) if any(v is not None for v in pnl) else None

        trade_types = sorted({str(row.get("tradeType")) for row in history if row.get("tradeType")})
        instruction_types = sorted({str(row.get("instructionType")) for row in history if row.get("instructionType")})
        liquidity_types = sorted({str(row.get("liquidity")) for row in history if row.get("liquidity")})

        lines = [f"<b>{symbol}</b> · {side_text}"]
        lines.append(f"  حجم: {_fmt_number(base_qty)} {symbol}" if base_qty is not None else "  حجم: در منبع معامله موجود نبود")
        if quote_qty is not None:
            lines.append(f"  ارزش معامله: {_fmt_number(quote_qty)} PhUSD")
        if price is not None:
            lines.append(f"  میانگین قیمت اجرا: {_fmt_number(price)} PhUSD")
        if fee_total is not None:
            lines.append(f"  کارمزد: {_fmt_number(fee_total)} PhUSD")
        if pnl_total is not None:
            lines.append(f"  PnL تحقق‌یافته در همین اجرا: {_fmt_number(pnl_total)} PhUSD")
        if trade_types:
            lines.append(f"  نوع معامله: {', '.join(trade_types)}")
        if instruction_types:
            lines.append(f"  دستور: {', '.join(instruction_types)}")
        if liquidity_types:
            lines.append(f"  نقدشوندگی: {', '.join(liquidity_types)}")
        sections.append("\n".join(lines))

    if len(markets) > 5:
        sections.append(f"… و {len(markets) - 5} بازار دیگر")
    return "\n\n" + "\n\n".join(sections)


async def analyze_trade_with_ai(session, markets):
    """Optional Persian explanation based only on verified structured fields."""
    if not OPENAI_API_KEY or not markets:
        return None

    facts = []
    for market in markets:
        fills = market.get("fills", [])
        history = market.get("history", [])
        base_qty = sum((_as_decimal(row.get("baseQty")) or Decimal("0")) for row in fills) if fills else None
        quote_qty = sum((_as_decimal(row.get("quoteQty")) or Decimal("0")) for row in fills) if fills else None
        signed_deltas = [_as_decimal(row.get("baseLotsDelta")) for row in history]
        sides = {"buy" if value > 0 else "sell" for value in signed_deltas if value not in (None, 0)}
        side = next(iter(sides)) if len(sides) == 1 else ("mixed" if sides else None)
        facts.append({
            "symbol": market.get("symbol"),
            "side": side,
            "base_volume": str(base_qty) if base_qty is not None else None,
            "quote_volume_PhUSD": str(quote_qty) if quote_qty is not None else None,
            "price_PhUSD": str(quote_qty / base_qty) if base_qty and quote_qty is not None else None,
            "trade_types": sorted({str(row.get("tradeType")) for row in history if row.get("tradeType")}),
            "fees_PhUSD": str(sum((_as_decimal(row.get("fees")) or Decimal("0")) for row in history)) if history else None,
            "realized_pnl_PhUSD": str(sum((_as_decimal(row.get("realizedPnl")) or Decimal("0")) for row in history)) if history else None,
        })

    payload = {
        "model": OPENAI_MODEL,
        "store": False,
        "max_output_tokens": 100,
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
                return None
            response = await resp.json(content_type=None)
        texts = []
        for output_item in response.get("output", []):
            for content in output_item.get("content", []):
                if content.get("type") == "output_text" and content.get("text"):
                    texts.append(content["text"].strip())
        return " ".join(texts)[:700] or None
    except Exception:
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


async def monitor_wallet(session: aiohttp.ClientSession, wallet_addr: str, wallet_name: str, last_signatures: dict):
    sigs = await fetch_rpc(session, "getSignaturesForAddress", [wallet_addr, {"limit": 20}])
    if not sigs:
        return

    last_sig = last_signatures.get(wallet_addr)
    if last_sig is None:
        last_signatures[wallet_addr] = sigs[0]["signature"]
        print(f"✅ Monitoring active for [{wallet_name}]")
        return

    new_sigs = []
    for sig_info in sigs:
        sig = sig_info["signature"]
        if sig == last_sig:
            break
        new_sigs.append(sig_info)

    if new_sigs:
        last_signatures[wallet_addr] = new_sigs[0]["signature"]

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
    last_signatures = {}

    async with aiohttp.ClientSession() as session:
        await send_telegram_alert(session, f"🚀 <b>Phoenix Multi-Wallet Tracker Active ({config_name}.py).</b>")

        while True:
            start_time = time.time()
            
            tasks = [
                monitor_wallet(session, addr, name, last_signatures)
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
