import asyncio
import time
import logging

import pandas as pd
import streamlit as st

import funding_core
import ui_components

# Optional auto-refresh
try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger("funding_monitor")

st.set_page_config(page_title="Funding Fee Monitor", layout="wide")


if st_autorefresh:
    st_autorefresh(interval=60_000, key="data_refresh")

# 顶部右下角社交链接
ui_components.render_social_links()

# ============ 数据获取 ============

USE_MOCK_DATA = True


@st.cache_data(ttl=60)
def get_all_rates_cached(use_mock_data: bool = False):
    if use_mock_data:
        return {"data": funding_core.generate_mock_data(), "ts": time.time()}

    # funding_core.fetch_all_raw 是 async，这里包一层
    async def fetch():
        return await funding_core.fetch_all_raw()

    raw_results = asyncio.run(fetch())
    return {"data": raw_results, "ts": time.time()}


res_bundle = get_all_rates_cached(USE_MOCK_DATA)
raw_results = res_bundle["data"]
last_update_ts = res_bundle["ts"]
last_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_update_ts))

# ============ 设置（齿轮+主题） ============
default_exchanges = ["Aster", "EdgeX", "Lighter", "HL", "Binance", "Backpack"]

# 标题 + 右上角齿轮在同一行
title_col, gear_col = st.columns([8, 1])

with title_col:
    st.markdown(
        '<div class="page-title">DEXs 资金费率面板</div>',
        unsafe_allow_html=True,
    )

with gear_col:
    selected_exchanges = ui_components.render_settings_popover(default_exchanges)

# 从 session 里读当前主题（render_settings_popover 里已经写入 theme_mode）
theme_mode = ui_components.render_theme_toggle()
ui_components.render_global_theme_styles(theme_mode)

# ============ 数据处理 ============

rows = funding_core.process_raw_results(raw_results, selected_exchanges)

if not rows:
    st.error("No data collected after fetch. Check logs above for per-exchange errors.")
else:
    df = pd.DataFrame(rows)

    # Reorder columns to surface spread near symbol
    if "APY Spread (%)" in df.columns:
        cols = df.columns.tolist()
        new_cols = ["Symbol", "APY Spread (%)"] + [
            c for c in cols if c not in ("Symbol", "APY Spread (%)")
        ]
        df = df[new_cols]

    # 顶部说明 + 右上角 Last update
    info_col, time_col = st.columns([4, 1])
    with info_col:
        ui_components.render_rate_explanation(theme_mode)

    with time_col:
        ui_components.render_last_update(last_update, theme_mode)

    # Merge Rate + Interval
    for ex in default_exchanges:
        rate_col = f"{ex} Rate"
        int_col = f"{ex} Interval (h)"
        if rate_col in df.columns and int_col in df.columns:
            df[rate_col] = df.apply(
                lambda r: "-"
                if pd.isna(r[rate_col])
                else f"{r[rate_col]*100:.4f}% ({r[int_col]:.1f}h)",
                axis=1,
            )
            df.drop(columns=[int_col], inplace=True)

    # Render Table（带 sticky header + 主题）
    ui_components.render_rates_table(
        df, theme_mode=theme_mode if theme_mode else "auto"
    )

# ============ 页面底部版权 + 访问量 ============

ui_components.render_visit_counter()
