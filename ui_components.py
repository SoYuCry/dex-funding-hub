import time
import json
import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd
import textwrap
import streamlit as st

logger = logging.getLogger("funding_monitor")

THEME_LABELS = {
    "auto": "跟随系统",
    "light": "明亮",
    "dark": "暗色",
}

SOCIAL_HTML = """
<style>
.social-container {
  position: fixed;
  bottom: 14px;
  right: 16px;
  z-index: 1000;
}
.social-row {display:flex; gap:10px; margin:0;}
.social-row a {
  display:inline-flex;
  align-items:center;
  gap:6px;
  padding:6px 10px;
  border-radius:10px;
  text-decoration:none;
  color:#fff;
  font-weight:600;
  font-size:14px;
  box-shadow:0 2px 6px rgba(0,0,0,0.15);
}
.social-row a:hover {opacity:0.92;}
.social-row .x-link {background:#111;}
.social-row .tg-link {background:#229ED9;}
</style>
<div class="social-container">
  <div class="social-row">
    <a class="x-link" href="https://x.com/0xYuCry" target="_blank" rel="noopener noreferrer">✕ <span>X</span></a>
    <a class="tg-link" href="https://t.me/journey_of_someone" target="_blank" rel="noopener noreferrer">✈ <span>Telegram</span></a>
  </div>
</div>
"""

VISIT_LOG_PATH = Path("visit_log.jsonl")

def _get_palette(theme_mode: str):
    palette_dark = {
        "wrapper_bg": "#0b0f19",
        "table_bg": "#0b0f19",
        "text": "#e9edf5",
        "border": "#303645",
        "row_border": "#333",
        "hover": "#141a25",
        "head_shadow": "#0b0f19",
    }
    palette_light = {
        "wrapper_bg": "#f8f9fb",
        "table_bg": "#ffffff",
        "text": "#0b0f19",
        "border": "#d7dbe3",
        "row_border": "#e6e8ef",
        "hover": "#eef1f5",
        "head_shadow": "#f8f9fb",
    }
    if theme_mode == "light":
        return palette_light
    if theme_mode == "dark":
        return palette_dark
    # auto: follow streamlit base (default dark palette here)
    return palette_dark


def render_social_links():
    st.markdown(SOCIAL_HTML, unsafe_allow_html=True)


def render_theme_toggle() -> str:
    """
    Getter for current theme_mode stored in session_state.

    主题调整已经移动到设置齿轮里，这里不再渲染控件，只返回当前值。
    """
    if "theme_mode" not in st.session_state:
        st.session_state["theme_mode"] = "auto"
    return st.session_state["theme_mode"]

def render_global_theme_styles(theme_mode: str):
    palette = _get_palette(theme_mode)
    st.markdown(
        f"""
        <style>
        /* overall spacing and base text */
        [data-testid="stAppViewContainer"] .main .block-container {{
            padding-top: 0.25rem;
            padding-bottom: 1.25rem;
            color: {palette["text"]};
        }}
        /* normalize markdown/text colors so small helper texts stay visible */
        .stMarkdown, .stMarkdown p, .stMarkdown div, .markdown-text-container, label, .stRadio > label {{
            color: {palette["text"]} !important;
        }}
        .stMarkdown p {{
            margin: 0.1rem 0 0.35rem;
        }}

        /* 页面标题专用样式 */
        .page-title {{
        font-size: 1.9rem;         /* 更显眼 */
        font-weight: 700;
        line-height: 1.25;
        margin-bottom: 0.5rem;
        color: {palette["text"]};
        }}

        h1, h2, h3, h4, h5, h6 {{
            color: {palette["text"]} !important;
            margin-bottom: 0.35rem;
        }}

        /* gear button */
        button[data-testid="stPopover"] {{
            background: {palette["table_bg"]};
            color: {palette["text"]};
            border: 1px solid {palette["border"]};
        }}
        button[data-testid="stPopover"]:hover {{
            background: {palette["hover"]};
            border-color: {palette["border"]};
            color: {palette["text"]};
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def record_visit_once():
    if "session_id" not in st.session_state:
        st.session_state["session_id"] = str(uuid4())
    if st.session_state.get("visit_recorded"):
        return

    ts_ms = int(time.time() * 1000)
    entry = {
        "ts": ts_ms,
        "ts_iso": time.strftime(
            "%Y-%m-%dT%H:%M:%S%z", time.localtime(ts_ms / 1000)
        ),
        "session": st.session_state["session_id"],
    }
    try:
        with VISIT_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        # Keep UI clean; log server-side only
        logger.error("Visit log write failed: %s", e)
    st.session_state["visit_recorded"] = True


def get_visit_count() -> int | None:
    try:
        with VISIT_LOG_PATH.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0
    except Exception as e:
        logger.error("Visit log read failed: %s", e)
        return None


def render_visit_counter():
    """底部版权 + 访问量"""
    record_visit_once()
    visit_count = get_visit_count()
    year = time.localtime().tm_year
    if visit_count is not None:
        st.markdown(
            f"""
            <div style="margin-top:2.5rem; text-align:center; font-size:12px; color:#9ca3af;">
              © {year} Nova-BTC. All rights reserved. 总访问量：{visit_count}
            </div>
            """,
            unsafe_allow_html=True,
        )
def render_settings_popover(default_exchanges):
    """
    设置齿轮：包含 主题切换 + 交易所选择

    主题写回 st.session_state["theme_mode"]，你可以用 render_theme_toggle() 读出来。
    """
    # 先初始化 theme_mode
    if "theme_mode" not in st.session_state:
        st.session_state["theme_mode"] = "auto"

    palette = _get_palette(st.session_state["theme_mode"])

    # ✅ 正确缩小 popover 按钮
    st.markdown(
        f"""
    <style>
    /* 让 popover 容器在当前列里靠右，不要拉伸按钮 */
    div[data-testid="stPopover"] {{
        display: flex;
        justify-content: flex-end;
    }}

    /* 真正的按钮在这个里面，强制变成小圆按钮 */
    div[data-testid="stPopover"] > button {{
        width: 28px;
        height: 28px;
        padding: 0;
        min-width: 0;                     /* 取消默认最小宽度 */
        border-radius: 999px;             /* 小圆按钮 */
        display: inline-flex;
        align-items: center;
        justify-content: center;

        background: {palette["table_bg"]};
        color: {palette["text"]};
        border: 1px solid {palette["border"]};

        transition: all 0.15s ease;
        cursor: pointer;
    }}

    div[data-testid="stPopover"] > button:hover {{
        background: {palette["hover"]};
        transform: scale(1.05);
    }}
    </style>
    """,
        unsafe_allow_html=True,
    )

    # ====== 下面保持你原来的逻辑 ======
    if "selected_exchanges" not in st.session_state:
        st.session_state["selected_exchanges"] = list(default_exchanges)

    for ex in default_exchanges:
        key = f"chk_{ex}"
        if key not in st.session_state:
            st.session_state[key] = ex in st.session_state["selected_exchanges"]

    try:
        # 这里建议把 width="stretch" 去掉
        with st.popover("⚙"):
            st.markdown("**主题**")
            options = list(THEME_LABELS.keys())
            default_idx = (
                options.index(st.session_state["theme_mode"])
                if st.session_state["theme_mode"] in options
                else 0
            )
            choice = st.radio(
                "主题切换",
                options=options,
                index=default_idx,
                format_func=lambda k: THEME_LABELS[k],
                key="theme_mode_radio",
                horizontal=True,
                label_visibility="collapsed",
            )
            st.session_state["theme_mode"] = choice

            st.markdown("---")
            st.markdown("**展示的交易所**")
            rows = [st.columns(3), st.columns(3)]
            for idx, ex in enumerate(default_exchanges):
                row = rows[idx // 3]
                with row[idx % 3]:
                    st.checkbox(ex, key=f"chk_{ex}")

    except Exception:
        with st.expander("⚙"):
            st.markdown("**主题**")
            options = list(THEME_LABELS.keys())
            default_idx = (
                options.index(st.session_state["theme_mode"])
                if st.session_state["theme_mode"] in options
                else 0
            )
            choice = st.radio(
                "主题切换",
                options=options,
                index=default_idx,
                format_func=lambda k: THEME_LABELS[k],
                key="theme_mode_radio",
                horizontal=True,
                label_visibility="collapsed",
            )
            st.session_state["theme_mode"] = choice

            st.markdown("---")
            rows = [st.columns(3), st.columns(3)]
            for idx, ex in enumerate(default_exchanges):
                row = rows[idx // 3]
                with row[idx % 3]:
                    st.checkbox(ex, key=f"chk_{ex}")

    current_selection = []
    for ex in default_exchanges:
        if st.session_state.get(f"chk_{ex}", False):
            current_selection.append(ex)

    st.session_state["selected_exchanges"] = current_selection
    return current_selection


def render_rate_explanation(theme_mode: str = "auto"):
    palette = _get_palette(theme_mode)
    st.markdown(
        f"""
        <div style="
            font-size:14px;
            color:{palette['text']};
            opacity:0.9;
            margin-top:0.35rem;
            margin-bottom:0.35rem;
        ">
          说明：资金费率按小时拆分，用对应交易所/符号的结算周期计算年化：
          <code>rate × (24 / 周期) × 365 × 100</code>；
          SPREAD 基于年化 APY：<code>(最高 APY - 最低 APY)</code>。
        </div>
        """,
        unsafe_allow_html=True,
    )

def render_last_update(ts: str, theme_mode: str = "auto"):
    palette = _get_palette(theme_mode)
    st.markdown(
        f"""
        <div style="
            text-align:right;
            font-size:14px;
            color:{palette['text']};
            font-weight:500;
            opacity:0.85;
            margin-top:0.35rem;
            margin-bottom:0.35rem;
        ">
          Last update: {ts}
        </div>
        """,
        unsafe_allow_html=True,
    )




def _highlight_extremes(row, spread_cols):
    # 与 spread_cols 对齐
    apy_cols = [c for c in spread_cols if c in row.index]
    styles = ["" for _ in apy_cols]
    if not apy_cols:
        return styles
    vals = row[apy_cols].dropna()
    if vals.empty:
        return styles
    max_v = vals.max()
    min_v = vals.min()
    for idx, col in enumerate(apy_cols):
        if pd.isna(row[col]):
            continue
        if row[col] == max_v:
            styles[idx] = (
                "box-shadow: 0 0 0 1px rgba(0,200,120,0.45); border-radius: 6px;"
            )
        elif row[col] == min_v:
            styles[idx] = (
                "box-shadow: 0 0 0 1px rgba(255,120,180,0.45); border-radius: 6px;"
            )
    return styles


def render_rates_table(df, theme_mode: str = "auto"):
    # Styling Logic
    fmt_dict = {}
    apy_cols_in_df = [
        c for c in df.columns if c.endswith("APY%") and c != "APY Spread (%)"
    ]
    for col in apy_cols_in_df:
        fmt_dict[col] = (lambda x: "{:.2f}%".format(x) if x is not None else "-")

    if "APY Spread (%)" in df.columns:
        fmt_dict["APY Spread (%)"] = (
            lambda x: "{:.2f}%".format(x) if x is not None else "-"
        )

    styler = df.style.format(fmt_dict)

    # Gradient Coloring
    spread_cols = apy_cols_in_df.copy()
    if spread_cols:
        styler = styler.background_gradient(
            subset=spread_cols, cmap="RdYlGn", vmin=-50, vmax=50
        )
    if "APY Spread (%)" in df.columns:
        spread_vmin, spread_vmax = 0, 100
        try:
            if not df["APY Spread (%)"].empty:
                spread_vmin = max(0, df["APY Spread (%)"].quantile(0.05))
                spread_vmax = df["APY Spread (%)"].quantile(0.95)
                if spread_vmax <= spread_vmin:
                    spread_vmax = spread_vmin + 1
        except Exception:
            pass
        styler = styler.background_gradient(
            subset=["APY Spread (%)"],
            cmap="Oranges",
            vmin=spread_vmin,
            vmax=spread_vmax,
        )

    if spread_cols:
        styler = styler.apply(
            _highlight_extremes, spread_cols=spread_cols, subset=spread_cols, axis=1
        )

    # HTML Rendering
    html = styler.to_html()

    # Generate unique ID for this table instance
    import random

    table_id = f"sortable_table_{random.randint(1000, 9999)}"

    # Inject table ID into the HTML
    html_with_id = html.replace("<table", f'<table id="{table_id}"', 1)

        # theme palette
    palette = _get_palette(theme_mode)
    top_offset = "2.5rem"

    css_block = textwrap.dedent(
        f"""
        <style>
        .custom-table-container thead {{
            position: sticky;
            top: {top_offset};  /* align with Streamlit top padding/header */
            z-index: 998;
            background-color: {palette["table_bg"]};
        }}

        /* 表头单元格的基础样式 */
        .custom-table-container thead th {{
            position: sticky;
            top: {top_offset};
            background-color: {palette["table_bg"]};
            z-index: 999;
            padding: 8px;
            text-align: right;
            border-bottom: 2px solid {palette["border"]};
            box-shadow: 0 2px 6px rgba(0,0,0,0.45);
            cursor: pointer;
            user-select: none;
            color: {palette["text"]};
        }}

        /* 默认的上下箭头提示 */
        .custom-table-container thead th::after {{
            content: ' ⇅';
            font-size: 0.75rem;
            opacity: 0.4;
            margin-left: 4px;
            color: {palette["text"]};
        }}

        /* 升序 ▲ */
        .custom-table-container thead th.sort-asc::after {{
            content: ' ▲';
            opacity: 1;
        }}

        /* 降序 ▼ */
        .custom-table-container thead th.sort-desc::after {{
            content: ' ▼';
            opacity: 1;
        }}

        /* 表体单元格稍微补一下，保证颜色一致 */
        .custom-table-container tbody td {{
            padding: 8px;
            text-align: right;
            border-bottom: 1px solid {palette["row_border"]};
            color: {palette["text"]};
        }}

        .custom-table-container tbody tr:hover {{
            background-color: rgba(255, 255, 255, 0.05);
        }}
        </style>
        """
    )



    # Render style and then the table HTML separately to avoid Markdown treating it as code
    st.markdown(css_block, unsafe_allow_html=True)
    st.markdown(
        f'<div class="custom-table-wrapper"><div class="custom-table-container">{html_with_id}</div></div>',
        unsafe_allow_html=True,
    )

    # JS 排序脚本：用普通字符串 + replace 避免 f-string 与 {{}} 冲突
    sort_script = textwrap.dedent(
        """
        <script>
        (function() {
            function initTableSort(attempt) {
                const doc = window.parent.document;
                const table = doc.getElementById('__TABLE_ID__');
                if (!table) {
                    if (attempt < 8) {
                        setTimeout(() => initTableSort(attempt + 1), 150);
                    }
                    return;
                }
                const headers = table.querySelectorAll('thead th');
                if (!headers.length) return;
                let currentSort = { col: -1, asc: true };
                headers.forEach((header, index) => {
                    header.addEventListener('click', function() {
                        const tbody = table.querySelector('tbody');
                        const rows = Array.from(tbody.querySelectorAll('tr'));
                        const asc = currentSort.col === index ? !currentSort.asc : true;
                        currentSort = { col: index, asc };
                        headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
                        header.classList.add(asc ? 'sort-asc' : 'sort-desc');
                        rows.sort((a, b) => {
                            const aCell = a.cells[index];
                            const bCell = b.cells[index];
                            if (!aCell || !bCell) return 0;
                            let aVal = aCell.textContent.trim().replace('%', '').replace(',', '');
                            let bVal = bCell.textContent.trim().replace('%', '').replace(',', '');
                            const aNum = parseFloat(aVal);
                            const bNum = parseFloat(bVal);
                            if (!isNaN(aNum) && !isNaN(bNum)) {
                                return asc ? aNum - bNum : bNum - aNum;
                            }
                            return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                        });
                        rows.forEach(row => tbody.appendChild(row));
                    });
                });
            }
            initTableSort(0);
            setTimeout(() => initTableSort(0), 300);
        })();
        </script>
        """
    ).replace("__TABLE_ID__", table_id)

    st.components.v1.html(sort_script, height=0, width=0, scrolling=False)
