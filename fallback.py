def fetch_polymarket_price_to_beat(slug: str) -> float | None:
    """
    Polymarket's frontend fetches the price-to-beat from their internal
    series/events API. Try the events endpoint with the ticker.
    """
    # Try 1: events endpoint (has more metadata than markets)
    try:
        r = requests.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": slug},
            timeout=8
        )
        r.raise_for_status()
        data = r.json()
        event = data[0] if isinstance(data, list) and data else data
        # Check all nested fields
        for key in ("startPrice", "openPrice", "priceToBeat", "strikePrice",
                    "referencePrice", "underlyingPrice"):
            v = event.get(key)
            if v is not None:
                try: return float(v)
                except: pass
        # Also check nested markets list
        for m in event.get("markets", []):
            for key in ("startPrice", "openPrice", "priceToBeat"):
                v = m.get(key)
                if v is not None:
                    try: return float(v)
                    except: pass
    except:
        pass

    # Try 2: series endpoint
    try:
        r = requests.get(
            "https://gamma-api.polymarket.com/series",
            params={"ticker": "btc-updown-5m"},
            timeout=8
        )
        r.raise_for_status()
        data = r.json()
        series = data[0] if isinstance(data, list) and data else data
        for key in ("startPrice", "currentPrice", "priceToBeat"):
            v = series.get(key)
            if v is not None:
                try: return float(v)
                except: pass
    except:
        pass

    return None



import requests, json

slug = "btc-updown-5m-1778911200"  # replace with any active slug

# Check events endpoint
r = requests.get("https://gamma-api.polymarket.com/events", params={"slug": slug})
data = r.json()
event = data[0] if isinstance(data, list) else data
print("=== EVENT FIELDS ===")
for k, v in event.items():
    if k != "markets":
        print(f"  {k}: {repr(v)[:120]}")

print("\n=== FIRST MARKET FIELDS ===")
if event.get("markets"):
    for k, v in event["markets"][0].items():
        print(f"  {k}: {repr(v)[:120]}")