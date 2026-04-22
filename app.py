import math
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Popup Revenue Engine", page_icon="📈", layout="wide")

DEFAULT_XLSX = "lotte_dummy_sales_100k.xlsx"
DB_PATH = Path("popup_actuals.db")
WEEKDAY_LABELS_KO = ["월", "화", "수", "목", "금", "토", "일"]

# ── Design tokens ──────────────────────────────────────────────────────────────
PRIMARY   = "#6366f1"   # indigo
PRIMARY_D = "#4f46e5"
SURFACE   = "#ffffff"
BG        = "#f1f5f9"
BORDER    = "#e2e8f0"
TEXT_MAIN = "#0f172a"
TEXT_SUB  = "#64748b"
TEXT_MUTE = "#94a3b8"
SUCCESS   = "#10b981"
WARN      = "#f59e0b"
DANGER    = "#ef4444"
INFO_BG   = "#eef2ff"
# ───────────────────────────────────────────────────────────────────────────────

st.markdown(
    f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

html, body, [class*="css"] {{
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}}

.stApp {{ background: {BG}; }}
.block-container {{ max-width: 1380px; padding: 1.5rem 2rem 3rem; }}

/* ── Sidebar ─────────────────────────────────────────────── */
section[data-testid="stSidebar"] {{
  background: {SURFACE};
  border-right: 1px solid {BORDER};
}}
section[data-testid="stSidebar"] .block-container {{
  padding: 1.5rem 1rem;
}}
.sidebar-section {{
  margin-top: 1.25rem;
  margin-bottom: 0.25rem;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: {TEXT_MUTE};
  padding-bottom: 6px;
  border-bottom: 1px solid {BORDER};
}}

/* ── Cards ───────────────────────────────────────────────── */
.card {{
  background: {SURFACE};
  border: 1px solid {BORDER};
  border-radius: 16px;
  padding: 20px 24px;
  box-shadow: 0 1px 3px rgba(0,0,0,.04), 0 4px 12px rgba(0,0,0,.03);
}}
.card-accent {{
  background: {SURFACE};
  border: 1px solid {BORDER};
  border-radius: 16px;
  padding: 20px 24px;
  box-shadow: 0 1px 3px rgba(0,0,0,.04), 0 4px 12px rgba(0,0,0,.03);
  border-top: 3px solid {PRIMARY};
}}

/* ── KPI Cards ───────────────────────────────────────────── */
.kpi-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }}
.kpi {{
  background: {SURFACE};
  border: 1px solid {BORDER};
  border-radius: 14px;
  padding: 18px 20px;
  border-left: 4px solid {PRIMARY};
  box-shadow: 0 1px 3px rgba(0,0,0,.04);
}}
.kpi-label {{
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: {TEXT_SUB};
  margin-bottom: 8px;
}}
.kpi-value {{
  font-size: 26px;
  font-weight: 800;
  color: {TEXT_MAIN};
  line-height: 1.2;
  letter-spacing: -0.02em;
}}
.kpi-value-sm {{
  font-size: 17px;
  font-weight: 700;
  color: {TEXT_MAIN};
  line-height: 1.4;
  letter-spacing: -0.01em;
}}
.kpi-hint {{
  font-size: 11px;
  color: {TEXT_MUTE};
  margin-top: 6px;
}}

/* ── Badges & pills ──────────────────────────────────────── */
.badge {{
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.04em;
}}
.badge-indigo {{ background: {INFO_BG}; color: {PRIMARY_D}; }}
.badge-green  {{ background: #ecfdf5; color: #059669; }}
.badge-amber  {{ background: #fffbeb; color: #d97706; }}
.badge-gray   {{ background: #f8fafc; color: {TEXT_SUB}; border: 1px solid {BORDER}; }}

/* ── Hero header ─────────────────────────────────────────── */
.hero {{
  background: linear-gradient(135deg, {TEXT_MAIN} 0%, #1e293b 60%, #312e81 100%);
  border-radius: 20px;
  padding: 28px 32px;
  color: white;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
  margin-bottom: 4px;
}}
.hero-title {{
  font-size: 30px;
  font-weight: 900;
  letter-spacing: -0.03em;
  line-height: 1.15;
  margin: 8px 0 4px;
}}
.hero-sub {{
  font-size: 13px;
  color: rgba(255,255,255,.65);
  margin-top: 4px;
}}
.hero-meta {{
  background: rgba(255,255,255,.1);
  border: 1px solid rgba(255,255,255,.15);
  border-radius: 12px;
  padding: 14px 20px;
  min-width: 220px;
  backdrop-filter: blur(4px);
}}
.hero-meta-label {{ font-size: 11px; color: rgba(255,255,255,.5); }}
.hero-meta-value {{ font-size: 14px; font-weight: 700; margin-top: 4px; }}
.hero-meta-sub   {{ font-size: 11px; color: rgba(255,255,255,.45); margin-top: 4px; }}

/* ── Section header ──────────────────────────────────────── */
.section-header {{
  font-size: 15px;
  font-weight: 700;
  color: {TEXT_MAIN};
  margin-bottom: 14px;
  display: flex;
  align-items: center;
  gap: 8px;
}}
.section-header-dot {{
  width: 8px; height: 8px;
  border-radius: 50%;
  background: {PRIMARY};
  flex-shrink: 0;
}}

/* ── Empty state ─────────────────────────────────────────── */
.empty-state {{
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 56px 24px;
  text-align: center;
}}
.empty-icon {{
  font-size: 40px;
  margin-bottom: 14px;
  opacity: .5;
}}
.empty-title {{
  font-size: 16px;
  font-weight: 700;
  color: {TEXT_MAIN};
  margin-bottom: 6px;
}}
.empty-sub {{
  font-size: 13px;
  color: {TEXT_SUB};
}}

/* ── Quick-summary card ──────────────────────────────────── */
.summary-row {{
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 8px;
}}
.summary-chip {{
  background: {BG};
  border: 1px solid {BORDER};
  border-radius: 8px;
  padding: 6px 12px;
  font-size: 12px;
  color: {TEXT_SUB};
}}
.summary-chip strong {{ color: {TEXT_MAIN}; }}

/* ── Changed-input banner ────────────────────────────────── */
.stale-banner {{
  background: #fffbeb;
  border: 1px solid #fcd34d;
  border-radius: 10px;
  padding: 10px 16px;
  font-size: 13px;
  color: #92400e;
  margin-bottom: 14px;
}}

/* ── Streamlit widget overrides ──────────────────────────── */
div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div,
div[data-baseweb="textarea"] > div {{
  border: 1.5px solid {BORDER} !important;
  border-radius: 10px !important;
  background: {SURFACE} !important;
  transition: border-color .15s;
}}
div[data-baseweb="select"] > div:focus-within,
div[data-baseweb="input"] > div:focus-within {{
  border-color: {PRIMARY} !important;
  box-shadow: 0 0 0 3px rgba(99,102,241,.12) !important;
}}
.stButton > button {{
  border-radius: 10px !important;
  font-weight: 600 !important;
  letter-spacing: 0.01em;
  transition: all .15s;
}}
.stButton > button[kind="primary"] {{
  background: {PRIMARY} !important;
  border: none !important;
}}
.stButton > button[kind="primary"]:hover {{
  background: {PRIMARY_D} !important;
  box-shadow: 0 4px 14px rgba(99,102,241,.35) !important;
}}
div[data-testid="stTabs"] button {{
  font-weight: 600 !important;
  font-size: 14px !important;
}}
</style>
""",
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────

def format_krw(v: float) -> str:
    return f"{int(round(float(v))):,} 원"


def as_csv_download(df: pd.DataFrame) -> BytesIO:
    buf = BytesIO()
    buf.write(df.to_csv(index=False).encode("utf-8-sig"))
    buf.seek(0)
    return buf


def _normalize_col(c: str) -> str:
    return (
        str(c).strip().lower()
        .replace(" ", "").replace("_", "").replace("-", "")
        .replace(".", "").replace("/", "")
    )


def auto_map_columns(df: pd.DataFrame) -> dict[str, str | None]:
    cols = list(df.columns)
    norm = {_normalize_col(c): c for c in cols}

    def pick(cands: list[str]) -> str | None:
        for cand in cands:
            for n, orig in norm.items():
                if cand in n:
                    return orig
        return None

    return {
        "date":     pick(["일자", "날짜", "date", "dt"]),
        "store":    pick(["지점", "점포", "store", "branch"]),
        "brand":    pick(["브랜드", "brand"]),
        "sales":    pick(["매출", "sales", "revenue"]),
        "orders":   pick(["구매건수", "구매건", "주문수", "orders", "ordercount", "건수"]),
        "floor":    pick(["층", "floor"]),
        "category": pick(["상품군", "카테고리", "category"]),
    }


def date_sequence(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def calculate_duration_days(start_date: date, end_date: date) -> int:
    return (end_date - start_date).days + 1


# ─────────────────────────────────────────
# 기간 라이프사이클 팩터 (일별 적용)
# ─────────────────────────────────────────

def lifecycle_factor(day_index: int, duration: int, event_type: str) -> float:
    progress = day_index / max(duration - 1, 1)
    if event_type == "팝업":
        buzz    = 0.12 * math.exp(-progress * 4.0)
        closing = 0.07 * max(0.0, progress - 0.75)
        return 1.0 + buzz + closing
    else:
        buzz    = 0.07 * math.exp(-progress * 4.0)
        closing = 0.04 * max(0.0, progress - 0.75)
        return 1.0 + buzz + closing


# ─────────────────────────────────────────
# 시나리오 밴드
# ─────────────────────────────────────────

def scenario_band(total: float, brand_type: str):
    if brand_type == "신규 브랜드":
        return total * 0.72, total, total * 1.35
    else:
        return total * 0.88, total, total * 1.15


# ─────────────────────────────────────────
# 트렌드 보정
# ─────────────────────────────────────────

@dataclass(frozen=True)
class TrendInputs:
    naver_search_ratio: float
    sns_mentions_ratio: float
    sns_growth_ratio:   float
    sensitivity:        float
    clamp_low:          float = 0.85
    clamp_high:         float = 1.30


def weighted_trend_score(tr: TrendInputs) -> float:
    return (
        0.4 * tr.naver_search_ratio
        + 0.3 * tr.sns_mentions_ratio
        + 0.3 * tr.sns_growth_ratio
    )


def trend_coefficient(tr: TrendInputs) -> float:
    score = weighted_trend_score(tr)
    coef = 1 + (score - 1) * tr.sensitivity
    return float(np.clip(coef, tr.clamp_low, tr.clamp_high))


# ─────────────────────────────────────────
# 내부 통계 빌드
# ─────────────────────────────────────────

@st.cache_data(show_spinner=False)
def build_internal(
    df, date_col, store_col, brand_col,
    sales_col, orders_col, floor_col, category_col,
):
    x = df.copy()
    x[date_col]   = pd.to_datetime(x[date_col], errors="coerce")
    x[sales_col]  = pd.to_numeric(x[sales_col], errors="coerce")
    x[orders_col] = pd.to_numeric(x[orders_col], errors="coerce")
    x = x.dropna(subset=[date_col, store_col, brand_col, sales_col, floor_col, category_col])
    x["weekday"] = x[date_col].dt.weekday

    cat_avg   = x.groupby(category_col)[sales_col].mean().to_dict()
    brand_avg = x.groupby(brand_col)[sales_col].mean().to_dict()

    store_daily = (
        x.groupby([store_col, date_col])[sales_col].sum().groupby(level=0).mean()
    )
    floor_daily = (
        x.groupby([floor_col, date_col])[sales_col].sum().groupby(level=0).mean()
    )
    store_avg = store_daily.to_dict()
    floor_avg = floor_daily.to_dict()
    daily_total_overall = float(
        x.groupby(date_col)[sales_col].sum().mean()
    ) if len(x) else 1.0

    brand_weekday: dict[str, list[float]] = {}
    for b, g in x.groupby(brand_col):
        m = float(g[sales_col].mean())
        if m == 0:
            continue
        wd = g.groupby("weekday")[sales_col].mean()
        brand_weekday[str(b)] = [float(wd.get(i, m) / m) for i in range(7)]

    cat_weekday: dict[str, list[float]] = {}
    for c, g in x.groupby(category_col):
        m = float(g[sales_col].mean())
        if m == 0:
            continue
        wd = g.groupby("weekday")[sales_col].mean()
        cat_weekday[str(c)] = [float(wd.get(i, m) / m) for i in range(7)]

    peer = x.groupby([category_col, brand_col], as_index=False).agg(
        avg_daily_sales=(sales_col, "mean"),
        sales_sum=(sales_col, "sum"),
        orders_sum=(orders_col, "sum"),
    )
    peer["atv"] = np.where(
        peer["orders_sum"] > 0,
        peer["sales_sum"] / peer["orders_sum"],
        np.nan,
    )
    peer = peer.dropna(subset=["atv", "avg_daily_sales"])

    store_brand   = x.groupby([store_col, brand_col])[sales_col].mean().rename("store_brand").reset_index()
    brand_overall = x.groupby(brand_col)[sales_col].mean().rename("brand_overall").reset_index()
    sb = store_brand.merge(brand_overall, on=brand_col, how="left")
    sb["trend_index"] = (
        sb["store_brand"] / sb["brand_overall"]
    ).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(0.85, 1.30)
    peer_store_trend = {
        (str(r[store_col]), str(r[brand_col])): float(r["trend_index"])
        for _, r in sb.iterrows()
    }

    sc = (
        x.groupby([store_col, category_col], as_index=False)[sales_col]
        .sum().rename(columns={sales_col: "sc_sales"})
    )
    ss = (
        x.groupby(store_col, as_index=False)[sales_col]
        .sum().rename(columns={sales_col: "s_sales"})
    )
    sc = sc.merge(ss, on=store_col, how="left")
    sc["share"] = (sc["sc_sales"] / sc["s_sales"].replace(0, np.nan)).fillna(0)
    n_cat = sc.groupby(store_col)["share"].transform("count")
    rel = (sc["share"] * n_cat).clip(0.5, 2.5)
    sc["strength_weight"] = (
        0.85 + (rel - 1.0).clip(lower=0) / 1.5 * 0.40
    ).clip(0.85, 1.25)
    sc_strength = {
        (str(r[store_col]), str(r[category_col])): float(r["strength_weight"])
        for _, r in sc.iterrows()
    }

    brand_cat_sales = (
        x.groupby([brand_col, category_col])[sales_col]
        .sum().reset_index()
        .sort_values(sales_col, ascending=False)
    )
    brand_to_category: dict[str, str] = {}
    brand_all_categories: dict[str, list[str]] = {}
    for b, g in brand_cat_sales.groupby(brand_col):
        cats = g[category_col].astype(str).tolist()
        brand_to_category[str(b)] = cats[0]
        brand_all_categories[str(b)] = cats

    return {
        "overall":                 daily_total_overall,
        "cat_avg":                 {str(k): float(v) for k, v in cat_avg.items()},
        "store_avg":               {str(k): float(v) for k, v in store_avg.items()},
        "floor_avg":               {str(k): float(v) for k, v in floor_avg.items()},
        "brand_avg":               {str(k): float(v) for k, v in brand_avg.items()},
        "brand_weekday":           brand_weekday,
        "cat_weekday":             cat_weekday,
        "peer_pool":               peer.rename(columns={category_col: "category", brand_col: "brand"}).to_dict("records"),
        "peer_store_trend":        peer_store_trend,
        "store_category_strength": sc_strength,
        "brand_to_category":       brand_to_category,
        "brand_all_categories":    brand_all_categories,
        "date_max":                str(pd.to_datetime(x[date_col].max()).date()) if len(x) else None,
    }


# ─────────────────────────────────────────
# 예측 계산
# ─────────────────────────────────────────

def weekday_factor_for(internal, brand_name, category):
    if brand_name and brand_name in internal["brand_weekday"]:
        return {i: internal["brand_weekday"][brand_name][i] for i in range(7)}
    if category in internal["cat_weekday"]:
        return {i: internal["cat_weekday"][category][i] for i in range(7)}
    return {i: 1.0 for i in range(7)}


def compute_brand_coef(internal, brand_name, category, store, atv_for_new):
    cat_avg   = float(internal["cat_avg"].get(category, internal["overall"]))
    brand_avg = internal["brand_avg"].get(str(brand_name)) if brand_name else None

    if brand_avg:
        coef = float(np.clip(float(brand_avg) / cat_avg, 0.60, 1.80))
        return coef, {"mode": "existing", "peers": pd.DataFrame()}

    pool = pd.DataFrame(internal["peer_pool"])
    pool = pool[pool["category"].astype(str) == str(category)].copy()
    if pool.empty or not atv_for_new or atv_for_new <= 0:
        return 1.0, {"mode": "new_no_peers", "peers": pd.DataFrame()}

    lo, hi = atv_for_new * 0.70, atv_for_new * 1.30
    peers = pool[(pool["atv"] >= lo) & (pool["atv"] <= hi)].copy()
    if peers.empty:
        return 1.0, {"mode": "new_no_peers", "peers": pd.DataFrame()}

    base_coef = float(peers["avg_daily_sales"].mean() / cat_avg)
    lifts = [
        float(internal["peer_store_trend"][(str(store), str(b))])
        for b in peers["brand"].astype(str)
        if (str(store), str(b)) in internal["peer_store_trend"]
    ]
    peer_lift = float(np.mean(lifts)) if lifts else 1.0
    coef = float(np.clip(base_coef * np.clip(peer_lift, 0.85, 1.30), 0.60, 1.80))
    peers = (
        peers.sort_values("avg_daily_sales", ascending=False)
        .head(20)[["brand", "atv", "avg_daily_sales"]]
    )
    return coef, {"mode": "new_proxy", "peers": peers}


def compute_daily_forecast(
    baseline, store_w, sc_w, floor_w,
    weekday_factors, brand_coef, trend_coef,
    start_date, end_date, event_type,
):
    duration = calculate_duration_days(start_date, end_date)
    rows = []
    for day_index, d in enumerate(date_sequence(start_date, end_date)):
        wd = d.weekday()
        lc = lifecycle_factor(day_index, duration, event_type)
        sales = (
            baseline * store_w * sc_w * floor_w
            * weekday_factors.get(wd, 1.0)
            * brand_coef * trend_coef * lc
        )
        rows.append({
            "date":             d,
            "date_str":         d.strftime("%Y-%m-%d"),
            "weekday":          wd,
            "weekday_ko":       WEEKDAY_LABELS_KO[wd],
            "lifecycle_factor": round(lc, 4),
            "estimated_sales":  float(round(sales, 0)),
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────
# DB
# ─────────────────────────────────────────

def db_connect():
    con = sqlite3.connect(DB_PATH.as_posix(), check_same_thread=False)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS popup_actuals (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at TEXT NOT NULL,
          store TEXT NOT NULL,
          floor TEXT NOT NULL,
          category TEXT NOT NULL,
          brand TEXT NOT NULL,
          event_type TEXT NOT NULL,
          start_date TEXT NOT NULL,
          end_date TEXT NOT NULL,
          actual_total_sales REAL NOT NULL,
          actual_total_orders REAL,
          notes TEXT
        )
        """
    )
    con.commit()
    return con


def db_insert_actual(con, store, floor, category, brand, event_type,
                     start_date, end_date, sales, orders, notes):
    con.execute(
        """
        INSERT INTO popup_actuals
          (created_at, store, floor, category, brand, event_type,
           start_date, end_date, actual_total_sales, actual_total_orders, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            store, floor, category, brand, event_type,
            str(start_date), str(end_date),
            float(sales),
            float(orders) if orders else None,
            (notes or "").strip() or None,
        ),
    )
    con.commit()


def db_fetch_actuals(con):
    return pd.read_sql_query(
        """
        SELECT id, created_at, store, floor, category, brand, event_type,
               start_date, end_date, actual_total_sales, actual_total_orders, notes
        FROM popup_actuals ORDER BY id DESC
        """,
        con,
    )


def db_delete_actual(con, row_id: int):
    con.execute("DELETE FROM popup_actuals WHERE id = ?", (int(row_id),))
    con.commit()


# ─────────────────────────────────────────
# 데이터 로드
# ─────────────────────────────────────────

def _generate_sample_data() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    stores = ["잠실점", "본점", "영등포점", "인천점", "부산점"]
    floors = ["B1", "1F", "2F", "3F", "4F", "5F", "6F"]
    cats   = ["여성의류", "남성의류", "잡화", "화장품", "식품", "스포츠", "아동"]
    brands = {
        "여성의류": ["지오다노", "유니클로", "자라", "H&M", "에잇세컨즈", "스파오"],
        "남성의류": ["폴로", "타미힐피거", "라코스테", "닥스", "지이크"],
        "잡화":    ["MCM", "루이비통", "구찌", "코치", "케이트스페이드"],
        "화장품":  ["설화수", "헤라", "이니스프리", "MAC", "에스티로더"],
        "식품":    ["파리바게뜨", "뚜레쥬르", "스타벅스", "투썸플레이스", "맥도날드"],
        "스포츠":  ["나이키", "아디다스", "뉴발란스", "언더아머", "데상트"],
        "아동":    ["베이비갭", "MLB키즈", "모이몰른", "젤리멜리", "폴로키즈"],
    }
    st_m = {"잠실점": 1.3, "본점": 1.2, "영등포점": 1.0, "인천점": 0.9, "부산점": 0.95}
    fl_m = {"B1": 0.9, "1F": 1.2, "2F": 1.1, "3F": 1.0, "4F": 0.95, "5F": 0.9, "6F": 0.85}
    wd_m = [0.7, 0.75, 0.8, 0.85, 1.0, 1.4, 1.3]
    start = date(2024, 1, 1)
    rows = []
    for d_offset in range(456):
        d  = start + timedelta(days=d_offset)
        wd = d.weekday()
        for store in stores:
            fl = floors[rng.integers(0, len(floors))]
            for cat in cats:
                for brand in brands[cat]:
                    base   = int(rng.integers(300_000, 3_000_000))
                    noise  = float(rng.normal(1.0, 0.15))
                    sales  = max(10_000, int(base * st_m[store] * fl_m[fl] * wd_m[wd] * noise))
                    orders = max(1, int(sales / int(rng.integers(40_000, 120_000))))
                    rows.append({
                        "일자": d.strftime("%Y-%m-%d"),
                        "지점": store, "층": fl, "상품군": cat,
                        "브랜드": brand, "매출": sales, "구매건수": orders,
                    })
    return pd.DataFrame(rows)


xlsx_path = Path(__file__).with_name(DEFAULT_XLSX)
if xlsx_path.exists():
    raw = pd.read_excel(xlsx_path, sheet_name=0)
    raw.columns = [str(c).strip() for c in raw.columns]
else:
    raw = _generate_sample_data()

mapped  = auto_map_columns(raw)
missing = [k for k, v in mapped.items() if v is None]
if missing:
    st.error(f"필수 컬럼 자동매핑 실패: {', '.join(missing)}")
    st.stop()

internal = build_internal(
    raw,
    mapped["date"], mapped["store"], mapped["brand"],
    mapped["sales"], mapped["orders"], mapped["floor"], mapped["category"],
)

# ─────────────────────────────────────────
# Hero header
# ─────────────────────────────────────────

data_source = "샘플 데이터" if not xlsx_path.exists() else DEFAULT_XLSX
st.markdown(
    f"""
<div class="hero">
  <div>
    <div><span class="badge badge-indigo">Popup Revenue Engine</span></div>
    <div class="hero-title">팝업 매출 추정 & 실적 관리</div>
    <div class="hero-sub">브랜드 · 지점 · 상품군 · 층 · 트렌드를 종합해 팝업 예상 매출을 산출합니다</div>
  </div>
  <div class="hero-meta">
    <div class="hero-meta-label">데이터 소스</div>
    <div class="hero-meta-value">{data_source}</div>
    <div class="hero-meta-sub">{len(raw):,} rows &nbsp;·&nbsp; 최신일 {internal["date_max"]}</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)
st.write("")

# ─────────────────────────────────────────
# session_state 초기화
# ─────────────────────────────────────────

for _k, _v in [
    ("forecast_result", None),
    ("forecast_inputs_snapshot", None),
    ("prev_brand_type", "기존 브랜드"),
    ("sensitivity_val", 0.35),
]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

brand_candidates = sorted(list(internal["brand_avg"].keys()))

tab_forecast, tab_actuals, tab_compare = st.tabs(
    ["  📊  매출 추정  ", "  📋  실적 입력 · 관리  ", "  🔍  예측 vs 실적 비교  "]
)


# ════════════════════════════════════════════════
# TAB 1 : 매출 추정
# ════════════════════════════════════════════════

with tab_forecast:

    # ── Sidebar ────────────────────────────────
    with st.sidebar:
        st.markdown('<div class="sidebar-section">브랜드</div>', unsafe_allow_html=True)
        brand_type_label = st.selectbox("브랜드 유형", ["기존 브랜드", "신규 브랜드"], index=0, label_visibility="collapsed")

        if brand_type_label != st.session_state["prev_brand_type"]:
            st.session_state["sensitivity_val"] = 0.35 if brand_type_label == "기존 브랜드" else 0.45
            st.session_state["prev_brand_type"] = brand_type_label

        atv_for_new  = None
        brand_value  = None

        if brand_type_label == "기존 브랜드":
            brand_value = st.selectbox(
                "브랜드 선택", brand_candidates, index=0,
                help="타이핑으로 필터링됩니다.",
            )
        else:
            st.caption("ATV를 입력하면 유사 브랜드 실적으로 추정합니다.")
            atv_for_new = st.number_input("신규 브랜드 ATV (원)", min_value=1_000.0, value=85_000.0, step=1_000.0)

        st.markdown('<div class="sidebar-section">운영 조건</div>', unsafe_allow_html=True)
        store_value = st.selectbox("지점", sorted(internal["store_avg"].keys()))
        floor_value = st.selectbox("층", sorted(internal["floor_avg"].keys()))

        if brand_type_label == "기존 브랜드" and brand_value:
            locked_category = internal["brand_to_category"].get(brand_value)
            all_cats        = internal["brand_all_categories"].get(brand_value, [locked_category])
            category_value  = st.selectbox(
                "상품군", all_cats, index=0,
                disabled=(len(all_cats) == 1),
                help="매출 순 정렬. 단일 카테고리 브랜드는 자동 잠금.",
            )
        else:
            category_value = st.selectbox("상품군", sorted(internal["cat_avg"].keys()))

        event_type_value = st.selectbox("운영 유형", ["팝업", "MD"])

        st.markdown('<div class="sidebar-section">운영 기간</div>', unsafe_allow_html=True)
        start_date = st.date_input("시작일", value=date.today())
        end_date   = st.date_input("종료일", value=date.today() + timedelta(days=13))

        st.markdown('<div class="sidebar-section">트렌드 보정</div>', unsafe_allow_html=True)
        naver    = st.slider("네이버 검색량", 0.70, 1.50, 1.00, 0.01)
        mentions = st.slider("SNS 언급량",   0.70, 1.50, 1.00, 0.01)
        growth   = st.slider("SNS 증가율",   0.70, 1.50, 1.00, 0.01)
        sensitivity = st.slider(
            "트렌드 민감도", 0.20, 0.60,
            st.session_state["sensitivity_val"], 0.01,
            help="기존 브랜드 권장 0.30–0.40 / 신규 0.40–0.50",
        )
        st.session_state["sensitivity_val"] = sensitivity

        st.write("")
        estimate_button = st.button("매출 추정하기", use_container_width=True, type="primary")

    # ── 추정 실행 ───────────────────────────────
    if start_date > end_date:
        st.error("종료일은 시작일보다 빠를 수 없습니다.")
        st.stop()

    current_inputs = dict(
        brand_type=brand_type_label, brand=brand_value, atv=atv_for_new,
        store=store_value, floor=floor_value, category=category_value,
        event_type=event_type_value, start=start_date, end=end_date,
        naver=naver, mentions=mentions, growth=growth, sensitivity=sensitivity,
    )
    inputs_changed = current_inputs != st.session_state.get("forecast_inputs_snapshot")

    if estimate_button:
        with st.spinner("추정 중…"):
            duration_days = calculate_duration_days(start_date, end_date)
            baseline = float(internal["cat_avg"].get(category_value, internal["overall"]))
            overall  = float(internal["overall"])

            store_w = float(np.clip(float(internal["store_avg"].get(store_value, overall)) / overall, 0.70, 1.60))
            floor_w = float(np.clip(float(internal["floor_avg"].get(floor_value, overall)) / overall, 0.70, 1.60))
            sc_w    = float(internal["store_category_strength"].get((str(store_value), str(category_value)), 1.0))

            brand_coef, brand_details = compute_brand_coef(internal, brand_value, category_value, store_value, atv_for_new)
            weekday_factors = weekday_factor_for(internal, brand_value, category_value)
            trend  = TrendInputs(naver, mentions, growth, sensitivity)
            tcoef  = trend_coefficient(trend)

            df_daily = compute_daily_forecast(
                baseline, store_w, sc_w, floor_w,
                weekday_factors, float(brand_coef), float(tcoef),
                start_date, end_date, event_type_value,
            )

            total_sales = float(df_daily["estimated_sales"].sum())
            avg_daily   = total_sales / max(duration_days, 1)
            conservative, base, aggressive = scenario_band(total_sales, brand_type_label)

            st.session_state["forecast_result"] = dict(
                df_daily=df_daily, total_sales=total_sales, avg_daily=avg_daily,
                conservative=conservative, base=base, aggressive=aggressive,
                duration_days=duration_days, baseline=baseline,
                store_w=store_w, sc_w=sc_w, floor_w=floor_w,
                brand_coef=brand_coef, tcoef=tcoef, brand_details=brand_details,
                brand_type_label=brand_type_label, brand_value=brand_value,
                store_value=store_value, floor_value=floor_value,
                category_value=category_value, event_type_value=event_type_value,
                start_date=start_date, end_date=end_date,
            )
            st.session_state["forecast_inputs_snapshot"] = current_inputs

    # ── 결과 렌더링 ─────────────────────────────
    res = st.session_state.get("forecast_result")

    if res is None:
        st.markdown(
            """
<div class="card">
  <div class="empty-state">
    <div class="empty-icon">📊</div>
    <div class="empty-title">추정 결과가 없습니다</div>
    <div class="empty-sub">왼쪽 사이드바에서 조건을 설정하고<br><b>매출 추정하기</b> 버튼을 눌러주세요</div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )
    else:
        if inputs_changed:
            st.markdown(
                '<div class="stale-banner">⚠️ 입력값이 변경되었습니다. 결과를 갱신하려면 <b>매출 추정하기</b>를 다시 눌러주세요.</div>',
                unsafe_allow_html=True,
            )

        df_daily      = res["df_daily"]
        base          = res["base"]
        conservative  = res["conservative"]
        aggressive    = res["aggressive"]
        avg_daily     = res["avg_daily"]
        duration_days = res["duration_days"]
        baseline      = res["baseline"]
        store_w       = res["store_w"]
        sc_w          = res["sc_w"]
        floor_w       = res["floor_w"]
        brand_coef    = res["brand_coef"]
        tcoef         = res["tcoef"]
        brand_details = res["brand_details"]
        r_brand_type  = res["brand_type_label"]
        r_brand       = res["brand_value"]
        r_store       = res["store_value"]
        r_floor       = res["floor_value"]
        r_category    = res["category_value"]
        r_event       = res["event_type_value"]
        r_start       = res["start_date"]
        r_end         = res["end_date"]

        band_hint = "신규 브랜드 −28% / +35%" if r_brand_type == "신규 브랜드" else "기존 브랜드 −12% / +15%"

        # ── KPI ───────────────────────────────
        st.markdown(
            f"""
<div class="kpi-grid">
  <div class="kpi">
    <div class="kpi-label">최종 추정 매출</div>
    <div class="kpi-value">{format_krw(base)}</div>
    <div class="kpi-hint">라이프사이클 보정 포함 · {duration_days}일 합산</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">일평균 예상 매출</div>
    <div class="kpi-value">{format_krw(avg_daily)}</div>
    <div class="kpi-hint">{duration_days}일 기준 단순 평균</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">시나리오 범위</div>
    <div class="kpi-value-sm">{format_krw(conservative)}<br>~ {format_krw(aggressive)}</div>
    <div class="kpi-hint">{band_hint}</div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )
        st.write("")

        # ── 조건 요약 칩 ──────────────────────
        top_row = df_daily.sort_values("estimated_sales", ascending=False).iloc[0]
        low_row = df_daily.sort_values("estimated_sales").iloc[0]
        st.markdown(
            f"""
<div class="card" style="padding:16px 20px;">
  <div class="summary-row">
    <div class="summary-chip">🏬 <strong>{r_store}</strong></div>
    <div class="summary-chip">🏢 <strong>{r_floor}</strong></div>
    <div class="summary-chip">🏷 <strong>{r_category}</strong></div>
    <div class="summary-chip">📅 <strong>{r_start} ~ {r_end}</strong> ({duration_days}일)</div>
    <div class="summary-chip">{'🆕 신규 브랜드' if r_brand_type == '신규 브랜드' else f'🔖 {r_brand}'}</div>
    <div class="summary-chip">{'팝업' if r_event == '팝업' else 'MD'}</div>
  </div>
  <div style="font-size:12px;color:#64748b;margin-top:6px;">
    최고 예상일 <strong>{top_row['date_str']} ({top_row['weekday_ko']})</strong> &nbsp;·&nbsp;
    최저 예상일 <strong>{low_row['date_str']} ({low_row['weekday_ko']})</strong> &nbsp;·&nbsp;
    브랜드 산출 <strong>{brand_details['mode']}</strong>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )
        st.write("")

        # ── 차트 + 테이블 ─────────────────────
        chart_col, table_col = st.columns([1.4, 0.6])
        with chart_col:
            st.markdown('<div class="section-header"><div class="section-header-dot"></div>일자별 매출 흐름</div>', unsafe_allow_html=True)
            colors = [PRIMARY if wd >= 5 else "#94a3b8" for wd in df_daily["weekday"]]
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_daily["date_str"],
                y=df_daily["estimated_sales"],
                marker_color=colors,
                opacity=0.85,
                customdata=[format_krw(v) for v in df_daily["estimated_sales"]],
                hovertemplate="<b>%{x}</b><br>예상 매출: %{customdata}<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=df_daily["date_str"],
                y=df_daily["estimated_sales"],
                mode="lines",
                line=dict(color=PRIMARY_D, width=2.5, dash="dot"),
                hoverinfo="skip",
                showlegend=False,
            ))
            fig.update_layout(
                plot_bgcolor="#ffffff", paper_bgcolor="#ffffff",
                margin=dict(l=0, r=0, t=10, b=0), height=320,
                xaxis=dict(showgrid=False, tickangle=-45, tickfont=dict(size=11)),
                yaxis=dict(showgrid=True, gridcolor="#f1f5f9", tickfont=dict(size=11)),
                bargap=0.25,
                legend=dict(orientation="h", y=1.05),
            )
            st.plotly_chart(fig, use_container_width=True)

        with table_col:
            st.markdown('<div class="section-header"><div class="section-header-dot"></div>적용 가중치</div>', unsafe_allow_html=True)
            explain_df = pd.DataFrame({
                "항목":  ["카테고리 기준", "지점", "지점×상품군", "층", "브랜드", "트렌드"],
                "값":    [
                    format_krw(baseline),
                    f"{store_w:.3f}×",
                    f"{sc_w:.3f}×",
                    f"{floor_w:.3f}×",
                    f"{float(brand_coef):.3f}×",
                    f"{tcoef:.3f}×",
                ],
            })
            st.dataframe(explain_df, use_container_width=True, hide_index=True, height=248)

            st.markdown('<div class="section-header" style="margin-top:12px;"><div class="section-header-dot"></div>상위 3일</div>', unsafe_allow_html=True)
            top3 = df_daily.sort_values("estimated_sales", ascending=False).head(3).copy()
            top3["예상 매출"] = top3["estimated_sales"].map(format_krw)
            st.dataframe(
                top3.rename(columns={"date_str": "일자", "weekday_ko": "요일"})[["일자", "요일", "예상 매출"]],
                use_container_width=True, hide_index=True, height=148,
            )

        # ── 전체 일자 테이블 ──────────────────
        with st.expander("전체 일자 상세 보기"):
            view_df = df_daily.copy()
            view_df["estimated_sales"]  = view_df["estimated_sales"].map(format_krw)
            view_df["lifecycle_factor"] = view_df["lifecycle_factor"].map(lambda x: f"{x:.3f}×")
            st.dataframe(
                view_df.rename(columns={
                    "date_str": "일자", "weekday_ko": "요일",
                    "estimated_sales": "예상 매출", "lifecycle_factor": "LC 계수",
                })[["일자", "요일", "예상 매출", "LC 계수"]],
                use_container_width=True, hide_index=True,
            )
            st.download_button(
                "CSV 다운로드",
                data=as_csv_download(df_daily),
                file_name="sales_forecast_daily.csv",
                mime="text/csv",
            )

        if r_brand_type == "신규 브랜드" and not brand_details["peers"].empty:
            with st.expander("유사 브랜드 풀 (ATV ±30%)"):
                st.dataframe(brand_details["peers"], use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════
# TAB 2 : 실적 입력/현황 관리
# ════════════════════════════════════════════════

with tab_actuals:
    con = db_connect()
    form_col, list_col = st.columns([0.9, 1.1])

    with form_col:
        st.markdown(
            '<div class="section-header"><div class="section-header-dot"></div>팝업 실적 입력</div>',
            unsafe_allow_html=True,
        )

        prefill = {}
        if st.session_state.get("forecast_result"):
            r = st.session_state["forecast_result"]
            if st.checkbox("마지막 예측 조건으로 자동 채우기"):
                prefill = dict(
                    store=r["store_value"], floor=r["floor_value"],
                    category=r["category_value"], event=r["event_type_value"],
                    start=r["start_date"], end=r["end_date"],
                    brand=r["brand_value"] or "",
                )

        with st.form("actual_form"):
            store_list    = sorted(list(internal["store_avg"].keys()))
            floor_list    = sorted(list(internal["floor_avg"].keys()))
            category_list = sorted(list(internal["cat_avg"].keys()))

            c1, c2 = st.columns(2)
            with c1:
                a_store    = st.selectbox("지점",   store_list,
                                          index=store_list.index(prefill["store"]) if "store" in prefill else 0)
                a_floor    = st.selectbox("층",     floor_list,
                                          index=floor_list.index(prefill["floor"]) if "floor" in prefill else 0)
                a_category = st.selectbox("상품군", category_list,
                                          index=category_list.index(prefill["category"]) if "category" in prefill else 0)
            with c2:
                a_event = st.selectbox("운영 유형", ["팝업", "MD"],
                                       index=["팝업", "MD"].index(prefill.get("event", "팝업")))
                a_start = st.date_input("시작일", value=prefill.get("start", date.today() - timedelta(days=13)))
                a_end   = st.date_input("종료일", value=prefill.get("end",   date.today()))

            a_brand  = st.text_input("브랜드명", value=prefill.get("brand", ""))
            a_sales  = st.number_input("총 매출 (원)", min_value=0.0, value=150_000_000.0, step=1_000_000.0)
            a_orders = st.number_input("총 구매건수 (선택)", min_value=0.0, value=0.0, step=10.0)
            notes    = st.text_area("메모 (선택)", placeholder="예: 날씨 / 프로모션 / 재고 이슈 등", height=80)
            submitted = st.form_submit_button("실적 저장", use_container_width=True, type="primary")

        if submitted:
            if a_start > a_end:
                st.error("종료일은 시작일보다 빠를 수 없습니다.")
            elif not str(a_brand).strip():
                st.error("브랜드명을 입력하세요.")
            else:
                db_insert_actual(
                    con, a_store, a_floor, a_category,
                    str(a_brand).strip(), a_event,
                    a_start, a_end, float(a_sales),
                    a_orders if a_orders > 0 else None,
                    notes,
                )
                st.success("저장 완료!")

    with list_col:
        st.markdown(
            '<div class="section-header"><div class="section-header-dot"></div>저장된 실적</div>',
            unsafe_allow_html=True,
        )
        actuals = db_fetch_actuals(con)

        if actuals.empty:
            st.markdown(
                '<div class="card"><div class="empty-state"><div class="empty-icon">📋</div>'
                '<div class="empty-title">저장된 실적이 없습니다</div>'
                '<div class="empty-sub">왼쪽 폼에서 실적을 입력해주세요</div></div></div>',
                unsafe_allow_html=True,
            )
        else:
            actuals["기간(일)"]   = (pd.to_datetime(actuals["end_date"]) - pd.to_datetime(actuals["start_date"])).dt.days + 1
            actuals["일평균매출"] = actuals["actual_total_sales"] / actuals["기간(일)"].clip(lower=1)
            actuals["ATV"]        = np.where(
                actuals["actual_total_orders"].fillna(0) > 0,
                actuals["actual_total_sales"] / actuals["actual_total_orders"],
                np.nan,
            )
            st.dataframe(actuals, use_container_width=True, hide_index=True, height=380)

            with st.expander("레코드 삭제"):
                del_id = st.number_input("삭제할 ID", min_value=0, value=0, step=1)
                col_p, col_b = st.columns([2, 1])
                with col_p:
                    if del_id > 0:
                        row_preview = actuals[actuals["id"] == del_id]
                        if row_preview.empty:
                            st.warning(f"ID {del_id}를 찾을 수 없습니다.")
                        else:
                            r = row_preview.iloc[0]
                            st.info(f"삭제 대상: **{r['brand']}** · {r['store']} · {r['start_date']} ~ {r['end_date']}")
                with col_b:
                    if st.button("삭제 실행", use_container_width=True):
                        if del_id > 0:
                            db_delete_actual(con, int(del_id))
                            st.success(f"ID {int(del_id)} 삭제 완료")
                            st.rerun()
                        else:
                            st.warning("ID를 1 이상으로 입력하세요.")


# ════════════════════════════════════════════════
# TAB 3 : 예측 vs 실적 비교
# ════════════════════════════════════════════════

with tab_compare:
    con2     = db_connect()
    actuals2 = db_fetch_actuals(con2)

    if actuals2.empty:
        st.markdown(
            '<div class="card"><div class="empty-state"><div class="empty-icon">🔍</div>'
            '<div class="empty-title">비교할 실적 데이터가 없습니다</div>'
            '<div class="empty-sub">실적 탭에서 데이터를 먼저 입력해주세요</div></div></div>',
            unsafe_allow_html=True,
        )
    else:
        actuals2["기간(일)"] = (
            pd.to_datetime(actuals2["end_date"]) - pd.to_datetime(actuals2["start_date"])
        ).dt.days + 1

        cmp_rows = []
        for _, row in actuals2.iterrows():
            r_store    = str(row["store"])
            r_floor    = str(row["floor"])
            r_cat      = str(row["category"])
            r_brand_nm = str(row["brand"])
            r_event    = str(row["event_type"])
            r_s        = pd.to_datetime(row["start_date"]).date()
            r_e        = pd.to_datetime(row["end_date"]).date()
            actual_total = float(row["actual_total_sales"])
            dur = int(row["기간(일)"])

            baseline_c = float(internal["cat_avg"].get(r_cat, internal["overall"]))
            overall_c  = float(internal["overall"])
            store_w_c  = float(np.clip(float(internal["store_avg"].get(r_store, overall_c)) / overall_c, 0.70, 1.60))
            floor_w_c  = float(np.clip(float(internal["floor_avg"].get(r_floor, overall_c)) / overall_c, 0.70, 1.60))
            sc_w_c     = float(internal["store_category_strength"].get((r_store, r_cat), 1.0))

            b_name = r_brand_nm if r_brand_nm in internal["brand_avg"] else None
            brand_coef_c, _ = compute_brand_coef(internal, b_name, r_cat, r_store, None)
            wf_c = weekday_factor_for(internal, b_name, r_cat)

            df_c = compute_daily_forecast(
                baseline_c, store_w_c, sc_w_c, floor_w_c,
                wf_c, float(brand_coef_c), 1.0,
                r_s, r_e, r_event,
            )
            predicted_total = float(df_c["estimated_sales"].sum())
            error_pct = (predicted_total - actual_total) / actual_total * 100 if actual_total else None

            cmp_rows.append({
                "id":        int(row["id"]),
                "브랜드":    r_brand_nm,
                "지점":      r_store,
                "상품군":    r_cat,
                "기간":      f"{r_s} ~ {r_e} ({dur}일)",
                "실제 매출": actual_total,
                "예측 매출": predicted_total,
                "오차(%)":   round(error_pct, 1) if error_pct is not None else None,
            })

        cmp_df = pd.DataFrame(cmp_rows)
        valid_errors = cmp_df["오차(%)"].dropna()

        # ── 오차 KPI ──────────────────────────
        if len(valid_errors) > 0:
            mape      = float(valid_errors.abs().mean())
            mean_bias = float(valid_errors.mean())

            bias_color = SUCCESS if abs(mean_bias) < 10 else (WARN if abs(mean_bias) < 25 else DANGER)
            mape_color = SUCCESS if mape < 15 else (WARN if mape < 30 else DANGER)

            st.markdown(
                f"""
<div class="kpi-grid" style="grid-template-columns:repeat(3,1fr);margin-bottom:20px;">
  <div class="kpi" style="border-left-color:{mape_color};">
    <div class="kpi-label">MAPE (평균 절대 오차율)</div>
    <div class="kpi-value" style="color:{mape_color};">{mape:.1f}%</div>
    <div class="kpi-hint">낮을수록 정확한 예측</div>
  </div>
  <div class="kpi" style="border-left-color:{bias_color};">
    <div class="kpi-label">평균 편향</div>
    <div class="kpi-value" style="color:{bias_color};">{mean_bias:+.1f}%</div>
    <div class="kpi-hint">양수=과대추정 · 음수=과소추정</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">비교 건수</div>
    <div class="kpi-value">{len(cmp_df)}건</div>
    <div class="kpi-hint">저장된 실적 기준</div>
  </div>
</div>
""",
                unsafe_allow_html=True,
            )

        # ── 비교 테이블 ───────────────────────
        st.markdown('<div class="section-header"><div class="section-header-dot"></div>예측 vs 실적 상세</div>', unsafe_allow_html=True)
        display_df = cmp_df.copy()
        display_df["실제 매출"] = display_df["실제 매출"].map(format_krw)
        display_df["예측 매출"] = display_df["예측 매출"].map(format_krw)
        display_df["오차(%)"]   = display_df["오차(%)"].map(lambda x: f"{x:+.1f}%" if pd.notna(x) else "-")
        st.dataframe(display_df.drop(columns=["id"]), use_container_width=True, hide_index=True)

        # ── 오차 분포 차트 ────────────────────
        if len(valid_errors) >= 2:
            st.markdown('<div class="section-header" style="margin-top:8px;"><div class="section-header-dot"></div>오차 분포</div>', unsafe_allow_html=True)
            bar_colors = [DANGER if v > 0 else PRIMARY for v in cmp_df["오차(%)"].fillna(0)]
            fig_err = go.Figure()
            fig_err.add_trace(go.Bar(
                x=cmp_df["브랜드"] + " / " + cmp_df["지점"],
                y=cmp_df["오차(%)"],
                marker_color=bar_colors,
                marker_opacity=0.80,
                hovertemplate="<b>%{x}</b><br>오차: %{y:+.1f}%<extra></extra>",
            ))
            fig_err.add_hline(y=0, line_dash="dash", line_color=TEXT_MUTE, line_width=1.5)
            fig_err.update_layout(
                plot_bgcolor="#ffffff", paper_bgcolor="#ffffff",
                margin=dict(l=0, r=0, t=10, b=0), height=320,
                xaxis=dict(showgrid=False, tickangle=-35, tickfont=dict(size=11)),
                yaxis=dict(
                    showgrid=True, gridcolor="#f1f5f9",
                    title="오차(%)", tickfont=dict(size=11),
                    ticksuffix="%",
                ),
                bargap=0.3,
            )
            st.plotly_chart(fig_err, use_container_width=True)

        st.download_button(
            "비교 결과 CSV 다운로드",
            data=as_csv_download(cmp_df),
            file_name="forecast_vs_actual.csv",
            mime="text/csv",
        )
