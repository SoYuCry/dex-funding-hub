import streamlit as st
import asyncio
import pandas as pd
import requests
import streamlit as st
from exchanges.aster import Aster
from exchanges.edgex import EdgeX
from exchanges.lighter import Lighter
from exchanges.hyperliquid import Hyperliquid
from exchanges.binance import Binance
from exchanges.backpack import Backpack
import time
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Funding Fee Monitor", layout="wide")
st.title("Crypto Funding Fee Monitor")

# Auto-refresh every 60s without resetting widgets
st_autorefresh(interval=60_000, key="data_refresh")

# Funding interval assumptions (hours). Override per symbol if needed.
# Toggle interval auto-inference
ENABLE_INTERVAL_INFERENCE = True
# No caps: infer for all symbols
MAX_INTERVAL_FETCH = None
MAX_BINANCE_INTERVAL_FETCH = None

DEFAULT_INTERVAL_HOURS = {
    "Aster": 4,         # adjust to 8 if you confirm default is 8h
    "EdgeX": 4,
    "Lighter": 1,
    "Hyperliquid": 1,
    "Binance": 8,
    "Backpack": 8, # fundingInterval usually provided; default to 8h
}
# Example overrides: {"Aster": {"BTCUSDT": 1, "ETHUSDT": 1}}
SYMBOL_INTERVAL_OVERRIDES = {
    "Aster": {},  # fill 1h symbols here if needed
    "EdgeX": {},
    "Lighter": {},
    "Hyperliquid": {},
    "Binance": {},
    "Backpack": {},
}

# Caches for inferred intervals
ASTER_INTERVAL_CACHE: dict[str, float] = {}
ASTER_INTERVAL_FETCHED = 0
EDGEX_INTERVAL_CACHE: dict[str, float] = {}
BACKPACK_INTERVAL_CACHE: dict[str, float] = {}
BINANCE_INTERVAL_CACHE: dict[str, float] = {}
BINANCE_INTERVAL_FETCHED = 0
EDGEX_INTERVAL_CACHE: dict[str, float] = {}

# Initialize exchanges
aster = Aster()
edgex = EdgeX()
lighter = Lighter()
hyperliquid = Hyperliquid()
binance = Binance()
backpack = Backpack()

@st.cache_data(ttl=60)
def get_all_rates():
    exchanges = [Aster(), EdgeX(), Lighter(), Hyperliquid(), Binance(), Backpack()]
    
    async def fetch_one(exchange):
        try:
            return {
                "exchange_name": exchange.name,
                "rates": await exchange.get_all_funding_rates()
            }
        except Exception as e:
            return {
                "exchange_name": exchange.name,
                "rates": e
            }

    async def fetch_all():
        tasks = [fetch_one(ex) for ex in exchanges]
        return await asyncio.gather(*tasks)

    return asyncio.run(fetch_all())

def normalize_symbol(symbol):
    # Remove common separators
    s = symbol.upper().replace("-", "").replace("_", "")
    # Specific fix for TRUMP2 -> TRUMP as per user request
    if "TRUMP2" in s:
        s = s.replace("TRUMP2", "TRUMP")
    return s

def calculate_apy(rate, interval_hours=8):
    if rate is None:
        return None
    return rate * (24 / interval_hours) * 365 * 100


def infer_aster_interval(symbol: str) -> float | None:
    global ASTER_INTERVAL_FETCHED
    symbol = symbol.upper()
    if not ENABLE_INTERVAL_INFERENCE:
        return None
    if MAX_INTERVAL_FETCH is not None and ASTER_INTERVAL_FETCHED >= MAX_INTERVAL_FETCH:
        return None
    if symbol in ASTER_INTERVAL_CACHE:
        return ASTER_INTERVAL_CACHE[symbol]
    try:
        url = f"https://fapi.asterdex.com/fapi/v1/fundingRate"
        resp = requests.get(url, params={"symbol": symbol, "limit": 2}, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) >= 2:
                t1 = data[0].get("fundingTime")
                t2 = data[1].get("fundingTime")
                if isinstance(t1, int) and isinstance(t2, int):
                    interval_h = abs(t2 - t1) / 3600_000
                    if 0.5 <= interval_h <= 24:
                        ASTER_INTERVAL_FETCHED += 1
                        ASTER_INTERVAL_CACHE[symbol] = interval_h
                        return interval_h
    except Exception:
        pass
    return None


def load_edgex_interval_map():
    if EDGEX_INTERVAL_CACHE:
        return
    try:
        url = "https://pro.edgex.exchange/api/v1/public/meta/getMetaData"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return
        data = resp.json()
        cl = data.get("data", {}).get("contractList", [])
        for c in cl:
            name = c.get("contractName")
            interval_min = c.get("fundingRateIntervalMin")
            if name and interval_min:
                hours = float(interval_min) / 60
                EDGEX_INTERVAL_CACHE[name.upper()] = hours
                # also store USDT alias if USD
                if name.endswith("USD") and not name.endswith("USDT"):
                    EDGEX_INTERVAL_CACHE[(name + "T").upper()] = hours
    except Exception:
        pass

def load_backpack_interval_map():
    if BACKPACK_INTERVAL_CACHE:
        return
    try:
        import requests
        r = requests.get("https://api.backpack.exchange/api/v1/markets", timeout=10)
        if r.status_code != 200:
            return
        markets = r.json()
        perps = [m for m in markets if m.get("marketType") == "PERP"]
        for m in perps:
            interval_ms = m.get("fundingInterval")
            symbol_api = m.get("symbol")
            base = m.get("baseSymbol", "").upper()
            if interval_ms and symbol_api and base:
                hours = float(interval_ms) / 3600_000
                unified = f"{base}USDT"
                BACKPACK_INTERVAL_CACHE[unified.upper()] = hours
    except Exception:
        pass

def infer_binance_interval(symbol: str) -> float | None:
    global BINANCE_INTERVAL_FETCHED
    symbol = symbol.upper()
    if not ENABLE_INTERVAL_INFERENCE:
        return None
    if MAX_BINANCE_INTERVAL_FETCH is not None and BINANCE_INTERVAL_FETCHED >= MAX_BINANCE_INTERVAL_FETCH:
        return None
    if symbol in BINANCE_INTERVAL_CACHE:
        return BINANCE_INTERVAL_CACHE[symbol]
    try:
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        resp = requests.get(url, params={"symbol": symbol, "limit": 2}, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) >= 2:
                t1 = data[0].get("fundingTime")
                t2 = data[1].get("fundingTime")
                if isinstance(t1, int) and isinstance(t2, int):
                    interval_h = abs(t2 - t1) / 3600_000
                    if 0.5 <= interval_h <= 24:
                        BINANCE_INTERVAL_FETCHED += 1
                        BINANCE_INTERVAL_CACHE[symbol] = interval_h
                        return interval_h
    except Exception:
        pass
    return None


def get_interval_hours(exchange_name: str, symbol: str) -> int:
    symbol = symbol.upper()
    exchange_overrides = SYMBOL_INTERVAL_OVERRIDES.get(exchange_name, {})
    if symbol in exchange_overrides:
        return exchange_overrides[symbol]
    if exchange_name == "Aster":
        inferred = infer_aster_interval(symbol)
        if inferred:
            return inferred
    if exchange_name == "EdgeX":
        load_edgex_interval_map()
        inferred = EDGEX_INTERVAL_CACHE.get(symbol)
        if inferred:
            return inferred
    if exchange_name == "Backpack":
        load_backpack_interval_map()
        inferred = BACKPACK_INTERVAL_CACHE.get(symbol) or BACKPACK_INTERVAL_CACHE.get(symbol.replace("USDT","_USDC"))
        if inferred:
            return inferred
    if exchange_name == "Binance":
        inferred = infer_binance_interval(symbol)
        if inferred:
            return inferred
    return DEFAULT_INTERVAL_HOURS.get(exchange_name, 8)

if st.button("Refresh Data"):
    st.cache_data.clear()

with st.spinner("Fetching funding rates..."):
    # Use the new get_all_rates function
    raw_results_with_names = get_all_rates()

# Process data
data_map = {} # symbol -> {exchange: rate}

for res_entry in raw_results_with_names:
    exchange_name = res_entry["exchange_name"]
    res = res_entry["rates"]

    if isinstance(res, Exception):
        # Error already displayed by st.error in get_all_rates, just skip processing for this exchange
        continue
    
    for item in res:
        symbol = item['symbol']
        # Normalize symbol using the new function
        normalized_symbol = normalize_symbol(symbol)
        if normalized_symbol not in data_map:
            data_map[normalized_symbol] = {}
        data_map[normalized_symbol][exchange_name] = item['rate']

# Create DataFrame
rows = []
for symbol, rates in data_map.items():
    row = {"Symbol": symbol}
    
    # Aster
    aster_rate = rates.get("Aster")
    aster_int = get_interval_hours("Aster", symbol)
    row["Aster Rate"] = aster_rate
    row["Aster Interval (h)"] = aster_int
    row["Aster APY%"] = calculate_apy(aster_rate, aster_int)

    # EdgeX
    edgex_rate = rates.get("EdgeX")
    edgex_int = get_interval_hours("EdgeX", symbol)
    row["EdgeX Rate"] = edgex_rate
    row["EdgeX Interval (h)"] = edgex_int
    row["EdgeX APY%"] = calculate_apy(edgex_rate, edgex_int)
    
    # Lighter
    lighter_rate = rates.get("Lighter")
    lighter_int = get_interval_hours("Lighter", symbol)
    row["Lighter Rate"] = lighter_rate
    row["Lighter Interval (h)"] = lighter_int
    row["Lighter APY%"] = calculate_apy(lighter_rate, lighter_int) # Lighter is 1h

    # Hyperliquid
    hyper_rate = rates.get("Hyperliquid")
    hyper_int = get_interval_hours("Hyperliquid", symbol)
    row["Hyperliquid Rate"] = hyper_rate
    row["Hyperliquid Interval (h)"] = hyper_int
    row["Hyperliquid APY%"] = calculate_apy(hyper_rate, hyper_int) # Hyperliquid is 1h

    # Binance
    binance_rate = rates.get("Binance")
    binance_int = get_interval_hours("Binance", symbol)
    row["Binance Rate"] = binance_rate
    row["Binance Interval (h)"] = binance_int
    row["Binance APY%"] = calculate_apy(binance_rate, binance_int)

    # Backpack
    backpack_rate = rates.get("Backpack")
    backpack_int = get_interval_hours("Backpack", symbol)
    row["Backpack Rate"] = backpack_rate
    row["Backpack Interval (h)"] = backpack_int
    row["Backpack APY%"] = calculate_apy(backpack_rate, backpack_int)

    # Arb Calculation based on annualized APY; only keep symbols that appear on 2+ exchanges
    valid_apys = [r for r in [row["Aster APY%"], row["EdgeX APY%"], row["Lighter APY%"], row["Hyperliquid APY%"], row["Binance APY%"], row["Backpack APY%"]] if r is not None]
    if len(valid_apys) < 2:
        continue

    apy_spread_pct = max(valid_apys) - min(valid_apys)  # percentage points
    row["APY Spread (%)"] = apy_spread_pct

    rows.append(row)

df = pd.DataFrame(rows)

# Formatting
st.markdown(
    "**说明**：资金费率按小时拆分，用对应交易所/符号的结算周期计算年化：`rate × (24/周期) × 365 × 100`；"
    " SPREAD 基于年化 APY：`(最高APY - 最低APY)`。"
)

display_exchanges = ["Aster", "EdgeX", "Lighter", "Hyperliquid", "Binance", "Backpack"]
for ex in display_exchanges:
    rate_col = f"{ex} Rate"
    int_col = f"{ex} Interval (h)"
    if rate_col in df.columns and int_col in df.columns:
        df[rate_col] = df.apply(
            lambda r: "-" if pd.isna(r[rate_col]) else f"{r[rate_col]*100:.4f}% ({r[int_col]:.1f}h)",
            axis=1
        )
        df.drop(columns=[int_col], inplace=True)

st.dataframe(
    df.style.format({
        "Aster APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
        "EdgeX APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
        "Lighter APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
        "Hyperliquid APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
        "Binance APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
        "Backpack APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
        "APY Spread (%)": lambda x: "{:.2f}%".format(x) if x is not None else "-",
    }).background_gradient(subset=["Aster APY%", "EdgeX APY%", "Lighter APY%", "Hyperliquid APY%", "Binance APY%", "Backpack APY%"], cmap="RdYlGn", vmin=-50, vmax=50),
    width="stretch",
    height=800
)

# Identify top spreads based on annualized APY across exchanges
spread_rows = []
for _, r in df.iterrows():
    apy_map = {
        "Aster": r.get("Aster APY%"),
        "EdgeX": r.get("EdgeX APY%"),
        "Lighter": r.get("Lighter APY%"),
        "Hyperliquid": r.get("Hyperliquid APY%"),
        "Binance": r.get("Binance APY%"),
        "Backpack": r.get("Backpack APY%"),
    }
    # drop None
    apy_items = [(ex, apy) for ex, apy in apy_map.items() if apy is not None]
    if len(apy_items) < 2:
        continue
    apy_items.sort(key=lambda x: x[1], reverse=True)
    top1, top2 = apy_items[0], apy_items[1]
    spread_rows.append({
        "Symbol": r["Symbol"],
        "Top Exchange": top1[0],
        "Top APY%": top1[1],
        "Second Exchange": top2[0],
        "Second APY%": top2[1],
        "APY Spread": top1[1] - top2[1],
    })

spread_df = pd.DataFrame(spread_rows).sort_values(by="APY Spread", ascending=False).head(20)
st.subheader("Top APY Spreads (Annualized)")
st.dataframe(
    spread_df.style.format({
        "Top APY%": "{:.2f}%",
        "Second APY%": "{:.2f}%",
        "APY Spread": "{:.2f}%"
    }).background_gradient(subset=["APY Spread"], cmap="Oranges"),
    width="stretch",
    height=500
)
