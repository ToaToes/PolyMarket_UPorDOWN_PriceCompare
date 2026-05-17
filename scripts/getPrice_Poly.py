'''

Author: ToaToes - https://github.com/ToaToes
Description:
    This script connects to Polymarket's WebSocket for real-time BTC price updates,
    fetches the current "price to beat" for the active 5-minute BTC event, and displays
    a live dashboard in the terminal showing the current price, price to beat, and time remaining
    in the current window. It automatically detects new windows and updates accordingly.

    Time zone is set to Eastern Time (ET) for display as its what Polymarket's frontend uses.
    
    There might be a 0.002% error in the price to beat due to how Polymarket calculates it internally, 
    but it should be close enough for comparison purposes.

'''

import re, json, requests, time, threading
from datetime import datetime
from zoneinfo import ZoneInfo
import websocket

RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
WINDOW_SECS = 300
UTC = ZoneInfo("UTC")
ET  = ZoneInfo("America/New_York")

btc_live      = None
price_to_beat = None
price_lock    = threading.Lock()
ws_connected  = False
ptb_message   = None

def slug_from_url(url_or_slug: str) -> str:
    s = url_or_slug.strip()
    match = re.search(r'polymarket\.com/event/([^/?#]+)', s)
    return match.group(1) if match else s

def current_window_slug():
    now = int(time.time())
    w   = now - (now % WINDOW_SECS)
    return f"btc-updown-5m-{w}", w, w + WINDOW_SECS

def get_market(slug: str) -> dict:
    r = requests.get("https://gamma-api.polymarket.com/markets",
                     params={"slug": slug}, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data[0] if isinstance(data, list) and data else data

def parse_tokens(market: dict):
    outcomes  = market.get("outcomes", "[]")
    token_ids = (market.get("clobTokenIds") or market.get("clobTokensIds") or "[]")
    if isinstance(outcomes,  str): outcomes  = json.loads(outcomes)
    if isinstance(token_ids, str): token_ids = json.loads(token_ids)
    tid_up = tid_dn = None
    for name, tid in zip(outcomes, token_ids):
        if   name.strip().lower() == "up":   tid_up = tid
        elif name.strip().lower() == "down":  tid_dn = tid
    return tid_up, tid_dn

def get_clob_midprice(token_id: str) -> float | None:
    try:
        r = requests.get("https://clob.polymarket.com/midpoint",
                         params={"token_id": token_id}, timeout=4)
        r.raise_for_status()
        mid = r.json().get("mid")
        return float(mid) if mid is not None else None
    except:
        return None

# ── RTDS WebSocket ────────────────────────────────────────────────────
def start_rtds_ws():
    global btc_live, ws_connected

    sub = json.dumps({
        "action": "subscribe",
        "subscriptions": [{
            "topic":   "crypto_prices_chainlink",
            "type":    "*",
            "filters": ""
        }]
    })
    ping = json.dumps({"type": "PING"})

    def on_open(ws):
        global ws_connected
        ws_connected = True
        ws.send(sub)
        def hb():
            while ws_connected:
                try:    ws.send(ping)
                except: break
                time.sleep(5)
        threading.Thread(target=hb, daemon=True).start()

    def on_message(ws, raw):
        global btc_live
        if not raw or not raw.strip():
            return
        try:
            msg = json.loads(raw)
            if msg.get("topic") != "crypto_prices_chainlink":
                return
            payload = msg.get("payload", {})
            if "btc" not in str(payload.get("symbol", "")).lower():
                return
            val = float(payload.get("value", 0) or 0)
            if val < 1000:
                return

            with price_lock:
                btc_live = val
        except:
            pass

    def on_error(ws, err): print(f"  [WS error] {err}")
    def on_close(ws, *_):
        global ws_connected
        ws_connected = False

    ws = websocket.WebSocketApp(RTDS_WS_URL,
        on_open=on_open, on_message=on_message,
        on_error=on_error, on_close=on_close)
    threading.Thread(target=ws.run_forever,
                     kwargs={"ping_interval": 0}, daemon=True).start()
    return ws

# ── Market setup ──────────────────────────────────────────────────────
url_input = input("Enter Polymarket URL or slug (Enter = current window): ").strip()

if url_input:
    slug = slug_from_url(url_input)
    m_ts = re.search(r'btc-updown-5m-(\d+)', slug)
    window_start_ts = int(m_ts.group(1)) if m_ts else int(time.time()) - (int(time.time()) % WINDOW_SECS)
    window_end_ts   = window_start_ts + WINDOW_SECS
else:
    slug, window_start_ts, window_end_ts = current_window_slug()

# 获取第一个窗口的ptb需要从Chainlink历史获取，后续窗口会用前一个窗口的收盘价
# 对于第一个窗口，尝试从WebSocket获取第一个tick作为ptb
first_window = True

market = get_market(slug)
title  = market.get("question") or market.get("title") or slug
tid_up, tid_dn = parse_tokens(market)

open_et  = datetime.fromtimestamp(window_start_ts, tz=UTC).astimezone(ET)
close_et = datetime.fromtimestamp(window_end_ts,   tz=UTC).astimezone(ET)

print(f"\n{'='*75}")
print(f"  {title}")
print(f"  Window: {open_et.strftime('%Y-%m-%d %H:%M:%S %Z')} → {close_et.strftime('%H:%M:%S %Z')}")
print(f"{'='*75}")

print("  Connecting to Chainlink WebSocket...")
ws_conn = start_rtds_ws()
time.sleep(4)

# 表头
HDR = (f"{'Time (ET)':<22} {'BTC (Chainlink)':>16} {'Price to Beat':>14} "
       f"{'Δ':>9} {'UP%':>8} {'DOWN%':>8} {'Time Left':>12}")
SEP = "-" * len(HDR)

last_up = last_dn = None
tick = 0
window_end_price = None  # 保存当前窗口结束时的价格

try:
    while True:
        now       = int(time.time())
        remaining = max(0, window_end_ts - now)
        
        # 检查窗口是否即将结束，保存窗口结束时的价格
        if remaining == 0:
            # 保存当前窗口的最终价格作为下一个窗口的ptb
            with price_lock:
                if btc_live:
                    window_end_price = btc_live
                    print(f"\n  📌 Window closed. Final price: ${window_end_price:,.2f} (will be next window's Price to Beat)")
            
            print("\n  ⏱  Window closed — advancing to next window...")
            time.sleep(3)
            
            # 切换到下一个窗口
            slug, window_start_ts, window_end_ts = current_window_slug()
            market  = get_market(slug)
            title   = market.get("question") or market.get("title") or slug
            tid_up, tid_dn = parse_tokens(market)
            last_up = last_dn = None
            
            # 将上一个窗口的收盘价设置为新窗口的ptb
            with price_lock:
                if window_end_price:
                    price_to_beat = window_end_price
                    ptb_message = f"★ Price to Beat (from previous window close): ${price_to_beat:,.2f}"
                    print(f"\n{ptb_message}")
                    window_end_price = None  # 重置
                else:
                    price_to_beat = None
                    ptb_message = None
            
            open_et  = datetime.fromtimestamp(window_start_ts, tz=UTC).astimezone(ET)
            close_et = datetime.fromtimestamp(window_end_ts,   tz=UTC).astimezone(ET)
            print(f"\n  New window: {title}")
            print(f"  {open_et.strftime('%Y-%m-%d %H:%M:%S %Z')} → {close_et.strftime('%H:%M:%S %Z')}")
            print(f"\n{HDR}")
            print(f"{SEP}")
            time.sleep(3)
            continue
        
        # 对于第一个窗口，如果没有ptb，尝试从WebSocket获取第一个tick作为ptb
        if first_window and price_to_beat is None:
            with price_lock:
                if btc_live:
                    price_to_beat = btc_live
                    ptb_message = f"★ Price to Beat (first Chainlink tick of window): ${price_to_beat:,.2f}"
                    print(f"\n{ptb_message}\n")
                    print(f"{HDR}")
                    print(f"{SEP}")
                    first_window = False
        
        tick += 1
        if tick % 2 == 0 or last_up is None:
            up_mid = get_clob_midprice(tid_up)
            dn_mid = get_clob_midprice(tid_dn)
            if up_mid is not None: last_up = up_mid * 100
            if dn_mid is not None: last_dn = dn_mid * 100

        with price_lock:
            live = btc_live
            ptb  = price_to_beat

        dt_str    = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S")
        mins, scs = divmod(remaining, 60)
        time_left = f"{mins}m {scs:02d}s"
        live_str  = f"${live:>14,.2f}" if live else "             N/A"
        
        # 显示ptb
        if ptb:
            ptb_str = f"${ptb:>12,.2f}"
            delta_str = f"{'UP' if (live - ptb) >= 0 else 'DOWN'}${abs(live - ptb):>7,.2f}" if live else ""
        else:
            ptb_str = "      pending..."
            delta_str = ""
        
        up_str = f"{last_up:>7.2f}%" if last_up is not None else "     N/A"
        dn_str = f"{last_dn:>7.2f}%" if last_dn is not None else "     N/A"

        print(f"{dt_str}  {live_str}  {ptb_str}  {delta_str:>11}  {up_str}  {dn_str}  {time_left:>10}")
        
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopped.")
    ws_conn.close()
