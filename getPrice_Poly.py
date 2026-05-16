import re, json, requests, time, threading
from datetime import datetime
from zoneinfo import ZoneInfo
import websocket
import numpy as np
from collections import deque

RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
WINDOW_SECS = 300
UTC = ZoneInfo("UTC")
ET  = ZoneInfo("America/New_York")

btc_live      = None
price_to_beat = None
price_lock    = threading.Lock()
ws_connected  = False

# 添加PTB估算器类
class PTBEstimator:
    def __init__(self):
        self.price_history = []  # (btc_price, up_pct, timestamp)
        self.sigma = 105  # 初始波动率
        self.ptb_estimates = deque(maxlen=10)  # 保存最近10个估算值
        self.last_btc = None
        self.last_up = None
        
    def estimate_ptb_simple(self, current_btc, up_pct):
        """简化版本，无需scipy"""
        p = up_pct / 100.0
        p = max(0.01, min(0.99, p))
        
        # 近似反函数
        if p < 0.5:
            z = -0.5 * ((1-p)/p)**0.6
        else:
            z = 0.5 * ((p)/(1-p))**0.6
        
        ptb = current_btc - (z * self.sigma)
        return ptb, z
    
    def estimate_with_both(self, current_btc, up_pct, down_pct):
        """使用UP和DOWN价格取平均"""
        # 从UP反推
        p_up = up_pct / 100.0
        p_up = max(0.01, min(0.99, p_up))
        if p_up < 0.5:
            z_up = -0.5 * ((1-p_up)/p_up)**0.6
        else:
            z_up = 0.5 * ((p_up)/(1-p_up))**0.6
        
        # 从DOWN反推真实UP概率
        p_from_down = 1 - down_pct / 100.0
        p_from_down = max(0.01, min(0.99, p_from_down))
        if p_from_down < 0.5:
            z_down = -0.5 * ((1-p_from_down)/p_from_down)**0.6
        else:
            z_down = 0.5 * ((p_from_down)/(1-p_from_down))**0.6
        
        # 平均z值
        z_avg = (z_up + z_down) / 2
        
        ptb = current_btc - (z_avg * self.sigma)
        return ptb, z_avg
    
    def update_sigma(self, current_btc, current_up_pct):
        """动态更新隐含波动率"""
        if len(self.price_history) < 2:
            self.price_history.append((current_btc, current_up_pct, time.time()))
            return
        
        # 限制历史长度
        self.price_history.append((current_btc, current_up_pct, time.time()))
        if len(self.price_history) > 10:
            self.price_history.pop(0)
        
        if len(self.price_history) < 3:
            return
        
        sigmas = []
        for i in range(len(self.price_history)-1):
            btc1, up1, _ = self.price_history[i]
            btc2, up2, t2 = self.price_history[i+1]
            
            # 时间差太小的跳过（避免噪音）
            if i < len(self.price_history)-2:
                continue
                
            # 计算z值
            p1 = up1 / 100.0
            p2 = up2 / 100.0
            p1 = max(0.01, min(0.99, p1))
            p2 = max(0.01, min(0.99, p2))
            
            # 近似z值
            if p1 < 0.5:
                z1 = -0.5 * ((1-p1)/p1)**0.6
            else:
                z1 = 0.5 * ((p1)/(1-p1))**0.6
                
            if p2 < 0.5:
                z2 = -0.5 * ((1-p2)/p2)**0.6
            else:
                z2 = 0.5 * ((p2)/(1-p2))**0.6
            
            dz = abs(z2 - z1)
            dprice = abs(btc2 - btc1)
            
            if dz > 0.01 and dprice > 0.5:
                sigma_est = dprice / dz
                if 50 < sigma_est < 300:
                    sigmas.append(sigma_est)
        
        if sigmas:
            new_sigma = np.mean(sigmas)
            self.sigma = 0.7 * self.sigma + 0.3 * new_sigma
    
    def estimate(self, current_btc, up_pct, down_pct=None, use_smoothing=True):
        """主估算函数"""
        if current_btc is None or up_pct is None:
            return None, None
        
        # 更新波动率
        self.update_sigma(current_btc, up_pct)
        
        # 估算ptb
        if down_pct is not None:
            ptb, z = self.estimate_with_both(current_btc, up_pct, down_pct)
        else:
            ptb, z = self.estimate_ptb_simple(current_btc, up_pct)
        
        # 合理性检查
        if ptb < 50000 or ptb > 150000:
            if len(self.ptb_estimates) > 0:
                ptb = self.ptb_estimates[-1]  # 使用上一个有效值
        
        # 平滑处理
        if use_smoothing:
            self.ptb_estimates.append(ptb)
            if len(self.ptb_estimates) >= 3:
                # 使用中位数过滤异常值
                ptb = np.median(list(self.ptb_estimates))
        
        return ptb, self.sigma

# 创建全局估算器实例
estimator = PTBEstimator()

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

def fetch_ptb_from_clob_history(token_id_up: str, window_start_ts: int) -> float | None:
    try:
        r = requests.get(
            "https://clob.polymarket.com/prices-history",
            params={
                "market":   token_id_up,
                "interval": "1h",
                "fidelity": 1,
            },
            timeout=10
        )
        r.raise_for_status()
        pts = r.json().get("history", [])
        if not pts:
            return None
        closest = min(pts, key=lambda x: abs(int(x.get("t", 0)) - window_start_ts))
        t = int(closest.get("t", 0))
        p = float(closest.get("p", 0))
        print(f"  [debug] CLOB history closest point: t={t}, p={p} (window_start={window_start_ts})")
        return p
    except Exception as e:
        print(f"  [ptb error] {e}")
        return None

def fetch_ptb_from_chainlink_history(window_start_ts: int) -> float | None:
    CHAINLINK_BTC_USD = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
    RPC_URLS = [
        "https://polygon-rpc.com",
        "https://rpc-mainnet.matic.network",
    ]
    for rpc in RPC_URLS:
        try:
            payload = {
                "jsonrpc": "2.0",
                "method":  "eth_call",
                "params": [{
                    "to":   CHAINLINK_BTC_USD,
                    "data": "0xfeaf968c"
                }, "latest"],
                "id": 1
            }
            r = requests.post(rpc, json=payload, timeout=8)
            result = r.json().get("result", "")
            if result and len(result) >= 194:
                answer_hex = result[66:130]
                price_raw  = int(answer_hex, 16)
                price = price_raw / 1e8
                if price > 1000:
                    return price
        except:
            continue
    return None

# ── RTDS WebSocket ────────────────────────────────────────────────────
def start_rtds_ws(window_start_ts: int):
    global btc_live, price_to_beat, ws_connected

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
        global btc_live, price_to_beat
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
            oracle_ts = int(payload.get("timestamp", 0)) // 1000

            with price_lock:
                btc_live = val
                if price_to_beat is None and oracle_ts >= window_start_ts:
                    price_to_beat = val
                    print(f"\n  ★ Price to Beat (Chainlink tick @{oracle_ts}): ${price_to_beat:,.2f}\n")
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

market = get_market(slug)
title  = market.get("question") or market.get("title") or slug
tid_up, tid_dn = parse_tokens(market)

open_et  = datetime.fromtimestamp(window_start_ts, tz=UTC).astimezone(ET)
close_et = datetime.fromtimestamp(window_end_ts,   tz=UTC).astimezone(ET)

print(f"\n{'='*75}")
print(f"  {title}")
print(f"  Window: {open_et.strftime('%Y-%m-%d %H:%M:%S %Z')} → {close_et.strftime('%H:%M:%S %Z')}")
print(f"{'='*75}")

# Try to get PTB from Chainlink on-chain (most accurate)
print("  Fetching Price to Beat from Chainlink on-chain...")
price_to_beat = fetch_ptb_from_chainlink_history(window_start_ts)
if price_to_beat:
    print(f"  ★ Price to Beat (on-chain): ${price_to_beat:,.2f}")
else:
    print("  On-chain fetch failed — will use estimation from UP/DOWN prices")

print("  Connecting to Chainlink WebSocket...")
ws_conn = start_rtds_ws(window_start_ts)
time.sleep(4)

# 修改表头，增加估算标记
HDR = (f"{'Time (ET)':<22} {'BTC (Chainlink)':>16} {'Price to Beat':>14} "
       f"{'Δ':>11} {'UP%':>8} {'DOWN%':>8} {'Time Left':>10}")
print(f"\n{HDR}")
print("-" * len(HDR))

last_up = last_dn = None
tick = 0
estimated_ptb = None  # 用于存储估算的ptb

try:
    while True:
        now       = int(time.time())
        remaining = max(0, window_end_ts - now)

        if remaining == 0:
            print("\n  ⏱  Window closed — advancing...")
            time.sleep(3)
            slug, window_start_ts, window_end_ts = current_window_slug()
            market  = get_market(slug)
            title   = market.get("question") or market.get("title") or slug
            tid_up, tid_dn = parse_tokens(market)
            last_up = last_dn = None
            with price_lock:
                price_to_beat = None
            estimated_ptb = None
            # 重置估算器
            estimator = PTBEstimator()
            ws_conn.close()
            time.sleep(1)
            ws_conn = start_rtds_ws(window_start_ts)
            open_et  = datetime.fromtimestamp(window_start_ts, tz=UTC).astimezone(ET)
            close_et = datetime.fromtimestamp(window_end_ts,   tz=UTC).astimezone(ET)
            print(f"\n  New window: {title}")
            print(f"  {open_et.strftime('%H:%M:%S %Z')} → {close_et.strftime('%H:%M:%S %Z')}")
            print(f"\n{HDR}\n{'-'*len(HDR)}")
            time.sleep(3)
            continue

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
        
        # 如果没有真实的ptb，但有live和up/down价格，则估算
        if ptb is None and live is not None and last_up is not None:
            estimated_ptb, current_sigma = estimator.estimate(live, last_up, last_dn, use_smoothing=True)
            if estimated_ptb:
                # 显示估算的ptb（带标记）
                ptb_str = f"≈${estimated_ptb:>11,.2f}"
                # 使用估算的ptb计算delta
                d = live - estimated_ptb
                delta_str = f"{'UP' if d >= 0 else 'DOWN'}${abs(d):>7,.2f}"
                # 每30秒显示一次当前的sigma值
                if tick % 30 == 0:
                    print(f"  [σ={current_sigma:.1f}]", end=" ")
            else:
                ptb_str = "      pending..."
                delta_str = ""
        else:
            ptb_str   = f"${ptb:>12,.2f}"  if ptb  else "      pending..."
            delta_str = ""
            if live and ptb:
                d = live - ptb
                delta_str = f"{'UP' if d >= 0 else 'DOWN'}${abs(d):>7,.2f}"
        
        up_str = f"{last_up:>7.2f}%" if last_up is not None else "     N/A"
        dn_str = f"{last_dn:>7.2f}%" if last_dn is not None else "     N/A"

        print(f"{dt_str}  {live_str}  {ptb_str}  {delta_str:>11}  {up_str}  {dn_str}  {time_left:>10}")
        
        # 每30秒额外显示一次统计信息（可选）
        if tick % 30 == 0 and estimated_ptb and live:
            accuracy = abs(live - estimated_ptb) if ptb is None else "N/A"
            if ptb is None and len(estimator.ptb_estimates) > 0:
                variance = np.std(list(estimator.ptb_estimates))
                print(f"\n  [估算稳定性: ±${variance:.2f}]")
        
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopped.")
    ws_conn.close()