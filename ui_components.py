import time
import json
import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd
import textwrap
import streamlit as st

logger = logging.getLogger("funding_monitor")

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

def render_social_links():
    st.markdown(SOCIAL_HTML, unsafe_allow_html=True)

def record_visit_once():
    if "session_id" not in st.session_state:
        st.session_state["session_id"] = str(uuid4())
    if st.session_state.get("visit_recorded"):
        return

    ts_ms = int(time.time() * 1000)
    entry = {
        "ts": ts_ms,
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts_ms / 1000)),
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
    record_visit_once()
    visit_count = get_visit_count()
    if visit_count is not None:
        st.caption(f"总访问量 {visit_count}")

def render_settings_popover(default_exchanges):
    # square-ish popover trigger for the gear
    st.markdown(
        """
    <style>
    button[data-testid="stPopover"] {
      width: 40px;
      height: 40px;
      padding: 6px;
      border-radius: 10px;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )
    
    # Initialize session state for checkboxes if not present
    if "selected_exchanges" not in st.session_state:
        st.session_state["selected_exchanges"] = list(default_exchanges)
        
    # Ensure individual keys exist for binding
    for ex in default_exchanges:
        key = f"chk_{ex}"
        if key not in st.session_state:
            st.session_state[key] = ex in st.session_state["selected_exchanges"]

    try:
        with st.popover("⚙", width="stretch"):
            st.markdown("**展示的交易所**")
            rows = [st.columns(3), st.columns(3)]
            
            for idx, ex in enumerate(default_exchanges):
                row = rows[idx // 3]
                with row[idx % 3]:
                    # Bind directly to session state key
                    st.checkbox(ex, key=f"chk_{ex}")
            
    except Exception:
        # Fallback
        with st.expander("⚙"):
            rows = [st.columns(3), st.columns(3)]
            for idx, ex in enumerate(default_exchanges):
                row = rows[idx // 3]
                with row[idx % 3]:
                    st.checkbox(ex, key=f"chk_{ex}")

    # Reconstruct selected_exchanges from keys
    current_selection = []
    for ex in default_exchanges:
        if st.session_state.get(f"chk_{ex}", False):
            current_selection.append(ex)
            
    st.session_state["selected_exchanges"] = current_selection
    return current_selection

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
            styles[idx] = "box-shadow: 0 0 0 1px rgba(0,200,120,0.45); border-radius: 6px;"
        elif row[col] == min_v:
            styles[idx] = "box-shadow: 0 0 0 1px rgba(255,120,180,0.45); border-radius: 6px;"
    return styles


def render_rates_table(df):
    # Styling Logic
    fmt_dict = {}
    apy_cols_in_df = [c for c in df.columns if c.endswith("APY%") and c != "APY Spread (%)"]
    for col in apy_cols_in_df:
        fmt_dict[col] = (lambda x: "{:.2f}%".format(x) if x is not None else "-")

    if "APY Spread (%)" in df.columns:
        fmt_dict["APY Spread (%)"] = (lambda x: "{:.2f}%".format(x) if x is not None else "-")

    styler = df.style.format(fmt_dict)

    # Gradient Coloring
    spread_cols = apy_cols_in_df.copy()
    if spread_cols:
        styler = styler.background_gradient(subset=spread_cols, cmap="RdYlGn", vmin=-50, vmax=50)
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
        styler = styler.background_gradient(subset=["APY Spread (%)"], cmap="Oranges", vmin=spread_vmin, vmax=spread_vmax)

    if spread_cols:
        styler = styler.apply(_highlight_extremes, spread_cols=spread_cols, subset=spread_cols, axis=1)

    # HTML Rendering
    html = styler.to_html()

    # Generate unique ID for this table instance
    import random
    table_id = f"sortable_table_{random.randint(1000, 9999)}"

    # Inject table ID into the HTML
    html_with_id = html.replace("<table", f'<table id="{table_id}"', 1)

    css_block = textwrap.dedent(
        """
        <style>
        /* Container styling */
        .custom-table-wrapper {
            width: 100%;
            overflow: visible; /* allow sticky to escape potential clipping */
            background-color: #0b0f19;
            padding-top: 0;
        }
        .custom-table-container {
            width: 100%;
            overflow: visible;
        }
        .custom-table-wrapper::before {
            content: none;
        }

        /* Table styling */
        .custom-table-container table {
            width: 100%;
            border-collapse: collapse;
            border-spacing: 0;
            font-family: "Source Sans", "Source Sans Pro", "Source Sans 3", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            font-size: 14px;
            background-color: #0b0f19;
            color: #e9edf5;
        }

        .custom-table-container thead {
            position: sticky;
            top: 3.75rem; /* align with Streamlit top padding/header */
            z-index: 998;
            background-color: #0b0f19;
            box-shadow: 0 -3.75rem 0 0 #0b0f19; /* cover gap above */
        }

        /* Sticky Header with opaque background */
        .custom-table-container thead tr th {
            position: sticky;
            top: 3.75rem;
            background-color: #0b0f19;
            z-index: 999;
            padding: 8px;
            text-align: right;
            border-bottom: 2px solid #303645;
            box-shadow: 0 2px 6px rgba(0,0,0,0.45);
            cursor: pointer;
            user-select: none;
            color: #e9edf5;
        }

        /* Light mode header background */
        @media (prefers-color-scheme: light) {
            .custom-table-wrapper {
                background-color: #f8f9fb;
            }
            .custom-table-wrapper::before {
                background-color: #f8f9fb;
            }
            .custom-table-container thead {
                background-color: #f8f9fb;
                box-shadow: 0 -3.75rem 0 0 #f8f9fb;
            }

            .custom-table-container thead tr th {
                background-color: #f8f9fb;
                border-bottom: 2px solid #d7dbe3;
                box-shadow: 0 2px 6px rgba(0,0,0,0.08);
                color: #0b0f19;
            }
        }

        /* Header hover effect */
        .custom-table-container thead tr th:hover {
            background-color: #141a25;
        }

        @media (prefers-color-scheme: light) {
            .custom-table-container thead tr th:hover {
                background-color: #eef1f5;
            }
        }

        /* Sort indicator */
        .custom-table-container thead tr th::after {
            content: ' ⇅';
            opacity: 0.3;
            font-size: 0.8em;
        }

        .custom-table-container thead tr th.sort-asc::after {
            content: ' ▲';
            opacity: 1;
        }

        .custom-table-container thead tr th.sort-desc::after {
            content: ' ▼';
            opacity: 1;
        }

        /* Cell styling */
        .custom-table-container tbody tr td {
            padding: 8px;
            text-align: right;
            border-bottom: 1px solid #333;
            color: #e9edf5;
        }

        /* Hover effect */
        .custom-table-container tbody tr:hover {
            background-color: rgba(255, 255, 255, 0.05);
        }

        @media (prefers-color-scheme: light) {
            .custom-table-container table {
                background-color: #ffffff;
                color: #0b0f19;
            }
            .custom-table-container tbody tr td {
                border-bottom: 1px solid #e6e8ef;
                color: #0b0f19;
            }
        }
        </style>
        """
    ).strip()

    # Render style and then the table HTML separately to avoid Markdown treating it as code
    st.markdown(css_block, unsafe_allow_html=True)
    st.markdown(f'<div class="custom-table-wrapper"><div class="custom-table-container">{html_with_id}</div></div>', unsafe_allow_html=True)

    # Inject sorting script via component; it manipulates parent DOM to avoid iframe scroll
    sort_script = f"""
    <script>
    (function() {{
        function initTableSort(attempt) {{
            const doc = window.parent.document;
            const table = doc.getElementById('{table_id}');
            if (!table) {{
                if (attempt < 8) {{
                    setTimeout(() => initTableSort(attempt + 1), 150);
                }}
                return;
            }}
            const headers = table.querySelectorAll('thead th');
            if (!headers.length) return;
            let currentSort = {{ col: -1, asc: true }};
            headers.forEach((header, index) => {{
                header.addEventListener('click', function() {{
                    const tbody = table.querySelector('tbody');
                    const rows = Array.from(tbody.querySelectorAll('tr'));
                    const asc = currentSort.col === index ? !currentSort.asc : true;
                    currentSort = {{ col: index, asc }};
                    headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
                    header.classList.add(asc ? 'sort-asc' : 'sort-desc');
                    rows.sort((a, b) => {{
                        const aCell = a.cells[index];
                        const bCell = b.cells[index];
                        if (!aCell || !bCell) return 0;
                        let aVal = aCell.textContent.trim().replace('%', '').replace(',', '');
                        let bVal = bCell.textContent.trim().replace('%', '').replace(',', '');
                        const aNum = parseFloat(aVal);
                        const bNum = parseFloat(bVal);
                        if (!isNaN(aNum) && !isNaN(bNum)) {{
                            return asc ? aNum - bNum : bNum - aNum;
                        }}
                        return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                    }});
                    rows.forEach(row => tbody.appendChild(row));
                }});
            }});
        }}
        initTableSort(0);
        setTimeout(() => initTableSort(0), 300);
    }})();
    </script>
    """
    st.components.v1.html(sort_script, height=0, width=0, scrolling=False)
