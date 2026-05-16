import requests
import time
from datetime import datetime, timezone

SERIES = "btc-up-or-down-5m"
session = requests.Session()


# -------------------------
# POLYMARKET EVENT
# -------------------------
def get_latest_market():
    url = f"https://gamma-api.polymarket.com/events?series_slug={SERIES}"
    data = session.get(url).json()

    now = datetime.now(timezone.utc)

    valid = []
    for e in data:
        try:
            t = e.get("startTime")
            if not t:
                continue

            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            if dt <= now:
                valid.append(e)

        except:
            continue

    return max(valid or data, key=lambda x: x["startTime"])


# -------------------------
# BTC PRICES
# -------------------------
def get_binance_price():
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    return float(session.get(url).json()["price"])


def get_okx_price():
    url = "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT"
    return float(session.get(url).json()["data"][0]["last"])


# -------------------------
# POLYMARKET TOKEN PRICE
# -------------------------
def get_polymarket_prices(condition_id):
    try:
        if not condition_id:
            return None

        url = f"https://clob.polymarket.com/markets/{condition_id}"
        r = session.get(url, timeout=5)

        if r.status_code != 200:
            return None

        data = r.json()

        if not data:
            return None

        prices = data.get("outcomePrices")
        if not prices or len(prices) < 2:
            return None

        return {
            "yes": float(prices[0]),
            "no": float(prices[1])
        }

    except Exception as e:
        return None


# -------------------------
# MAIN LOOP
# -------------------------
current_market = None

while True:
    try:
        latest = get_latest_market()

        # New market detection
        if current_market is None or latest["slug"] != current_market["slug"]:
            current_market = latest
            
            markets = current_market.get("markets") or []

            if not markets:
                print("No markets in event")
                continue

            market = markets[0]
            condition_id = market.get("conditionId")
            
            # 获取问题文本 - 从 market 对象中获取
            question = (
                market.get("question") or      # 优先使用 market 的 question
                market.get("title") or         # 其次使用 market 的 title
                current_market.get("title") or # 最后使用 event 的 title
                "UNKNOWN"
            )

            if not condition_id:
                print("No conditionId")
                continue

            print("\n==========================")
            print("NEW MARKET")
            print("Question:", question)
            print("Slug:", current_market["slug"])
            print("Start Time:", current_market.get("startTime"))
            print("==========================\n")

        # BTC prices
        binance_price = get_binance_price()
        okx_price = get_okx_price()

        # Polymarket price
        polymarket = get_polymarket_prices(condition_id)

        now = datetime.now().strftime("%H:%M:%S")

        print(
            f"[{now}] "
            f"Binance: {binance_price:.2f} | "
            f"OKX: {okx_price:.2f} | "
            f"Polymarket YES: {polymarket['yes'] if polymarket else 'N/A'} | "
            f"Polymarket NO: {polymarket['no'] if polymarket else 'N/A'}"
        )

    except Exception as e:
        print("ERROR:", e)

    time.sleep(1)