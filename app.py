import math
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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

st.markdown(
    """
<style>
.stApp{background:#f6f8fc}
.block-container{max-width:1400px;padding-top:1rem;padding-bottom:2rem}
.card{background:#fff;border:1px solid #e5e7eb;border-radius:18px;padding:18px;box-shadow:0 10px 30px rgba(0,0,0,.05)}
.kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
.kpi{background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:16px}
.kpi .label{font-size:12px;color:#6b7280;margin-bottom:8px}
.kpi .value{font-size:28px;font-weight:800;line-height:1.2}
.kpi .hint{font-size:12px;color:#9ca3af;margin-top:6px}
.badge{display:inline-block;padding:6px 10px;border-radius:999px;background:#f3f4f6;font-size:12px;color:#6b7280}
div[data-baseweb="select"] > div,div[data-baseweb="input"] > div,div[data-baseweb="textarea"] > div{border:1px solid #cfd6e4 !important;border-radius:12px !important;background:#fff !important}
.stButton>button{border-radius:12px;font-weight:700}
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
#   - period_factor를 합산 총액에 곱하던 방식 제거
#   - 대신 일별 progress에 따른 버즈·안정·마감 커브 적용
# ─────────────────────────────────────────

def lifecycle_factor(day_index: int, duration: int, event_type: str) -> float:
    """
    day_index: 0-based (첫날=0)
    오픈 버즈(↑) → 중반 안정(→) → 마감 효과(↑) 패턴
    """
    progress = day_index / max(duration - 1, 1)
    if event_type == "팝업":
        buzz    = 0.12 * math.exp(-progress * 4.0)   # 오픈 버즈: 첫날 +12%
        closing = 0.07 * max(0.0, progress - 0.75)   # 마감 효과: 후반 25% 구간
        return 1.0 + buzz + closing
    else:  # MD
        buzz    = 0.07 * math.exp(-progress * 4.0)
        closing = 0.04 * max(0.0, progress - 0.75)
        return 1.0 + buzz + closing


# ─────────────────────────────────────────
# 시나리오 밴드 (신규/기존 브랜드 불확실성 차등)
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
#   - store_avg / floor_avg: row-level mean → 일별 합산 후 평균으로 수정
#   - sc_strength: share × count 스케일 보정
# ─────────────────────────────────────────

@st.cache_data(show_spinner=False)
def build_internal(
    df, date_col, store_col, brand_col,
    sales_col, orders_col, floor_col, category_col,
):
    x = df.copy()
    x[date_col]  = pd.to_datetime(x[date_col], errors="coerce")
    x[sales_col] = pd.to_numeric(x[sales_col], errors="coerce")
    x[orders_col] = pd.to_numeric(x[orders_col], errors="coerce")
    x = x.dropna(subset=[date_col, store_col, brand_col, sales_col, floor_col, category_col])
    x["weekday"] = x[date_col].dt.weekday

    overall = float(x[sales_col].mean()) if len(x) else 1.0

    # 카테고리 평균 (row-level mean: 카테고리 단위 baseline으로는 적합)
    cat_avg   = x.groupby(category_col)[sales_col].mean().to_dict()
    brand_avg = x.groupby(brand_col)[sales_col].mean().to_dict()

    # 지점·층 가중치: row-level mean 대신 일별 합산 매출의 평균 사용
    #   점포 규모·브랜드 수 차이로 인한 집계 편향 제거
    store_daily = (
        x.groupby([store_col, date_col])[sales_col].sum()
        .groupby(level=0).mean()
    )
    floor_daily = (
        x.groupby([floor_col, date_col])[sales_col].sum()
        .groupby(level=0).mean()
    )
    store_avg = store_daily.to_dict()
    floor_avg = floor_daily.to_dict()
    # overall도 일별 합산 기준으로 재정의
    daily_total_overall = float(
        x.groupby(date_col)[sales_col].sum().mean()
    ) if len(x) else 1.0

    # 요일별 가중치
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

    # 유사 브랜드 풀 (ATV 기반 신규 브랜드 추정용)
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

    # 점포 × 브랜드 트렌드 인덱스
    store_brand  = x.groupby([store_col, brand_col])[sales_col].mean().rename("store_brand").reset_index()
    brand_overall = x.groupby(brand_col)[sales_col].mean().rename("brand_overall").reset_index()
    sb = store_brand.merge(brand_overall, on=brand_col, how="left")
    sb["trend_index"] = (
        sb["store_brand"] / sb["brand_overall"]
    ).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(0.85, 1.30)
    peer_store_trend = {
        (str(r[store_col]), str(r[brand_col])): float(r["trend_index"])
        for _, r in sb.iterrows()
    }

    # 점포 × 상품군 강점 가중치
    #   share * count = 해당 카테고리의 점포 내 상대 비중
    #   (equal share 대비 몇 배인지) → 1.0이 균등 배분 기준
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
    # rel = share × n_cat : 1.0이면 균등 점유, >1이면 해당 카테고리가 강세
    rel = (sc["share"] * n_cat).clip(0.5, 2.5)
    sc["strength_weight"] = (
        0.85 + (rel - 1.0).clip(lower=0) / 1.5 * 0.40
    ).clip(0.85, 1.25)
    sc_strength = {
        (str(r[store_col]), str(r[category_col])): float(r["strength_weight"])
        for _, r in sc.iterrows()
    }

    # 브랜드 → 카테고리 매핑 (다중 카테고리 허용, 매출 기준 대표 카테고리)
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
        "overall":               daily_total_overall,
        "cat_avg":               {str(k): float(v) for k, v in cat_avg.items()},
        "store_avg":             {str(k): float(v) for k, v in store_avg.items()},
        "floor_avg":             {str(k): float(v) for k, v in floor_avg.items()},
        "brand_avg":             {str(k): float(v) for k, v in brand_avg.items()},
        "brand_weekday":         brand_weekday,
        "cat_weekday":           cat_weekday,
        "peer_pool":             peer.rename(columns={category_col: "category", brand_col: "brand"}).to_dict("records"),
        "peer_store_trend":      peer_store_trend,
        "store_category_strength": sc_strength,
        "brand_to_category":     brand_to_category,
        "brand_all_categories":  brand_all_categories,
        "date_max":              str(pd.to_datetime(x[date_col].max()).date()) if len(x) else None,
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
            baseline
            * store_w
            * sc_w
            * floor_w
            * weekday_factors.get(wd, 1.0)
            * brand_coef
            * trend_coef
            * lc
        )
        rows.append({
            "date":            d,
            "date_str":        d.strftime("%Y-%m-%d"),
            "weekday":         wd,
            "weekday_ko":      WEEKDAY_LABELS_KO[wd],
            "lifecycle_factor": round(lc, 4),
            "estimated_sales": float(round(sales, 0)),
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


def db_insert_actual(con, store, floor, category, brand, event_type, start_date, end_date, sales, orders, notes):
    con.execute(
        """
        INSERT INTO popup_actuals
          (created_at, store, floor, category, brand, event_type,
           start_date, end_date, actual_total_sales, actual_total_orders, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.utcnow().isoformat(timespec="seconds"),
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
    stores  = ["잠실점", "본점", "영등포점", "인천점", "부산점"]
    floors  = ["B1", "1F", "2F", "3F", "4F", "5F", "6F"]
    cats    = ["여성의류", "남성의류", "잡화", "화장품", "식품", "스포츠", "아동"]
    brands  = {
        "여성의류": ["지오다노", "유니클로", "자라", "H&M", "에잇세컨즈", "스파오"],
        "남성의류": ["폴로", "타미힐피거", "라코스테", "닥스", "지이크"],
        "잡화":    ["MCM", "루이비통", "구찌", "코치", "케이트스페이드"],
        "화장품":  ["설화수", "헤라", "이니스프리", "MAC", "에스티로더"],
        "식품":    ["파리바게뜨", "뚜레쥬르", "스타벅스", "투썸플레이스", "맥도날드"],
        "스포츠":  ["나이키", "아디다스", "뉴발란스", "언더아머", "데상트"],
        "아동":    ["베이비갭", "MLB키즈", "모이몰른", "젤리멜리", "폴로키즈"],
    }
    st_mult = {"잠실점": 1.3, "본점": 1.2, "영등포점": 1.0, "인천점": 0.9, "부산점": 0.95}
    fl_mult = {"B1": 0.9, "1F": 1.2, "2F": 1.1, "3F": 1.0, "4F": 0.95, "5F": 0.9, "6F": 0.85}
    wd_mult = [0.7, 0.75, 0.8, 0.85, 1.0, 1.4, 1.3]

    start = date(2024, 1, 1)
    rows = []
    for d_offset in range(456):  # ~15개월
        d = start + timedelta(days=d_offset)
        wd = d.weekday()
        for store in stores:
            fl = floors[rng.integers(0, len(floors))]
            for cat in cats:
                for brand in brands[cat]:
                    base = int(rng.integers(300_000, 3_000_000))
                    noise = float(rng.normal(1.0, 0.15))
                    sales = max(10_000, int(base * st_mult[store] * fl_mult[fl] * wd_mult[wd] * noise))
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

mapped = auto_map_columns(raw)
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
# 헤더
# ─────────────────────────────────────────

st.markdown(
    f"""
<div class="card">
  <div style="display:flex;justify-content:space-between;gap:18px;flex-wrap:wrap;align-items:flex-start;">
    <div>
      <div class="badge">Popup Revenue Engine</div>
      <div style="height:12px"></div>
      <div style="font-size:36px;font-weight:950;line-height:1.15;">팝업 매출 추정 & 실적 관리</div>
      <div style="margin-top:8px;color:#6b7280;font-size:13px;">브랜드 · 지점 · 상품군 · 층 · 트렌드를 종합해 팝업 예상 매출을 산출합니다.</div>
    </div>
    <div class="card" style="min-width:260px;padding:14px;box-shadow:none;">
      <div style="font-size:12px;color:#6b7280;">데이터 소스</div>
      <div style="font-weight:800;margin-top:6px;">{"샘플 데이터 (자동 생성)" if not xlsx_path.exists() else DEFAULT_XLSX}</div>
      <div style="font-size:12px;color:#6b7280;margin-top:8px;">Rows {len(raw):,} · Cols {len(raw.columns):,} · 최신일 {internal["date_max"]}</div>
    </div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)
st.write("")

# ─────────────────────────────────────────
# session_state 초기화
# ─────────────────────────────────────────

if "forecast_result" not in st.session_state:
    st.session_state["forecast_result"] = None
if "forecast_inputs_snapshot" not in st.session_state:
    st.session_state["forecast_inputs_snapshot"] = None
if "prev_brand_type" not in st.session_state:
    st.session_state["prev_brand_type"] = "기존 브랜드"
if "sensitivity_val" not in st.session_state:
    st.session_state["sensitivity_val"] = 0.35

brand_candidates = sorted(list(internal["brand_avg"].keys()))

tab_forecast, tab_actuals, tab_compare = st.tabs(["매출 추정", "실적 입력/현황 관리", "예측 vs 실적 비교"])

# ════════════════════════════════════════════════
# TAB 1 : 매출 추정
# ════════════════════════════════════════════════

with tab_forecast:

    with st.sidebar:
        st.markdown("### 브랜드")
        brand_type_label = st.selectbox("브랜드 유형", ["기존 브랜드", "신규 브랜드"], index=0)

        # 브랜드 타입 변경 시 sensitivity 기본값 리셋
        if brand_type_label != st.session_state["prev_brand_type"]:
            st.session_state["sensitivity_val"] = 0.35 if brand_type_label == "기존 브랜드" else 0.45
            st.session_state["prev_brand_type"] = brand_type_label

        atv_for_new = None
        brand_value = None

        if brand_type_label == "기존 브랜드":
            # 단일 selectbox로 검색 + 선택 통합
            brand_value = st.selectbox(
                "브랜드 선택",
                brand_candidates,
                index=0,
                help="목록에서 브랜드를 선택하세요. 타이핑으로 필터링됩니다.",
            )
        else:
            st.info("신규 브랜드: ATV(건당 평균 결제액)를 입력하면 유사 브랜드 실적으로 추정합니다.")
            atv_for_new = st.number_input(
                "신규 브랜드 ATV(원)", min_value=1_000.0, value=85_000.0, step=1_000.0
            )

        st.markdown("### 기본 조건")
        store_value = st.selectbox("타겟 지점", sorted(internal["store_avg"].keys()))
        floor_value = st.selectbox("타겟 층", sorted(internal["floor_avg"].keys()))

        # 기존 브랜드: 매출 기준 대표 카테고리 자동 잠금
        if brand_type_label == "기존 브랜드" and brand_value:
            locked_category = internal["brand_to_category"].get(brand_value)
            all_cats = internal["brand_all_categories"].get(brand_value, [locked_category])
            if len(all_cats) > 1:
                category_value = st.selectbox(
                    "상품군",
                    all_cats,
                    index=0,
                    help="해당 브랜드의 카테고리 목록(매출 순). 1순위가 기본 선택됩니다.",
                )
            else:
                category_value = st.selectbox(
                    "상품군", [locked_category], index=0, disabled=True,
                    help="기존 브랜드는 데이터셋 기준 상품군이 자동 적용됩니다.",
                )
        else:
            category_value = st.selectbox("상품군", sorted(internal["cat_avg"].keys()))

        event_type_value = st.selectbox("운영 유형", ["팝업", "MD"])

        st.markdown("### 기간")
        start_date = st.date_input("시작일", value=date.today())
        end_date   = st.date_input("종료일", value=date.today() + timedelta(days=13))

        st.markdown("### 트렌드 보정")
        naver    = st.slider("네이버 검색량(비율)", 0.70, 1.50, 1.00, 0.01)
        mentions = st.slider("SNS 언급량(비율)",   0.70, 1.50, 1.00, 0.01)
        growth   = st.slider("SNS 증가율(비율)",   0.70, 1.50, 1.00, 0.01)
        sensitivity = st.slider(
            "트렌드 민감도",
            0.20, 0.60,
            st.session_state["sensitivity_val"],
            0.01,
            help="기존 브랜드 권장 0.30~0.40 / 신규 브랜드 권장 0.40~0.50",
        )
        st.session_state["sensitivity_val"] = sensitivity

        estimate_button = st.button("매출 추정하기", use_container_width=True, type="primary")

    # ── 추정 실행 ──────────────────────────────

    if start_date > end_date:
        st.error("종료일은 시작일보다 빠를 수 없습니다.")
        st.stop()

    # 현재 입력 스냅샷 (변경 감지용)
    current_inputs = dict(
        brand_type=brand_type_label, brand=brand_value, atv=atv_for_new,
        store=store_value, floor=floor_value, category=category_value,
        event_type=event_type_value, start=start_date, end=end_date,
        naver=naver, mentions=mentions, growth=growth, sensitivity=sensitivity,
    )

    inputs_changed = current_inputs != st.session_state.get("forecast_inputs_snapshot")

    if estimate_button:
        with st.spinner("추정 중..."):
            duration_days = calculate_duration_days(start_date, end_date)
            baseline = float(internal["cat_avg"].get(category_value, internal["overall"]))
            overall  = float(internal["overall"])

            store_w = float(np.clip(
                float(internal["store_avg"].get(store_value, overall)) / overall, 0.70, 1.60
            ))
            floor_w = float(np.clip(
                float(internal["floor_avg"].get(floor_value, overall)) / overall, 0.70, 1.60
            ))
            sc_w = float(internal["store_category_strength"].get(
                (str(store_value), str(category_value)), 1.0
            ))
            brand_coef, brand_details = compute_brand_coef(
                internal, brand_value, category_value, store_value, atv_for_new
            )
            weekday_factors = weekday_factor_for(internal, brand_value, category_value)
            trend   = TrendInputs(naver, mentions, growth, sensitivity)
            tcoef   = trend_coefficient(trend)

            df_daily = compute_daily_forecast(
                baseline, store_w, sc_w, floor_w,
                weekday_factors, float(brand_coef), float(tcoef),
                start_date, end_date, event_type_value,
            )

            total_sales    = float(df_daily["estimated_sales"].sum())
            avg_daily      = total_sales / max(duration_days, 1)
            conservative, base, aggressive = scenario_band(total_sales, brand_type_label)

            st.session_state["forecast_result"] = dict(
                df_daily=df_daily,
                total_sales=total_sales,
                avg_daily=avg_daily,
                conservative=conservative,
                base=base,
                aggressive=aggressive,
                duration_days=duration_days,
                baseline=baseline,
                store_w=store_w,
                sc_w=sc_w,
                floor_w=floor_w,
                brand_coef=brand_coef,
                tcoef=tcoef,
                brand_details=brand_details,
                brand_type_label=brand_type_label,
                brand_value=brand_value,
                store_value=store_value,
                floor_value=floor_value,
                category_value=category_value,
                event_type_value=event_type_value,
                start_date=start_date,
                end_date=end_date,
            )
            st.session_state["forecast_inputs_snapshot"] = current_inputs

    # ── 결과 렌더링 ─────────────────────────────

    st.markdown(
        """
<div class="card">
  <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;">
    <div>
      <div style="font-size:16px;font-weight:900;">예측 결과 대시보드</div>
      <div style="font-size:13px;color:#6b7280;margin-top:6px;">입력한 조건을 기준으로 일자별 예상 매출과 주요 가중치를 보여줍니다.</div>
    </div>
    <div class="badge">실시간 계산</div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )
    st.write("")

    res = st.session_state.get("forecast_result")

    if res is None:
        st.info("왼쪽에서 조건을 입력하고 매출 추정하기를 누르세요.")
    else:
        if inputs_changed:
            st.warning("입력값이 변경되었습니다. 결과를 갱신하려면 **매출 추정하기**를 다시 눌러주세요.")

        df_daily       = res["df_daily"]
        total_sales    = res["total_sales"]
        avg_daily      = res["avg_daily"]
        conservative   = res["conservative"]
        base           = res["base"]
        aggressive     = res["aggressive"]
        duration_days  = res["duration_days"]
        baseline       = res["baseline"]
        store_w        = res["store_w"]
        sc_w           = res["sc_w"]
        floor_w        = res["floor_w"]
        brand_coef     = res["brand_coef"]
        tcoef          = res["tcoef"]
        brand_details  = res["brand_details"]
        r_brand_type   = res["brand_type_label"]
        r_brand        = res["brand_value"]
        r_store        = res["store_value"]
        r_floor        = res["floor_value"]
        r_category     = res["category_value"]
        r_event        = res["event_type_value"]
        r_start        = res["start_date"]
        r_end          = res["end_date"]

        st.markdown(
            f"""
<div class="kpis">
  <div class="kpi">
    <div class="label">최종 추정 매출</div>
    <div class="value">{format_krw(base)}</div>
    <div class="hint">일자별 합산 (라이프사이클 보정 포함)</div>
  </div>
  <div class="kpi">
    <div class="label">예상 일평균 매출</div>
    <div class="value">{format_krw(avg_daily)}</div>
    <div class="hint">총 {duration_days}일 기준</div>
  </div>
  <div class="kpi">
    <div class="label">예측 범위</div>
    <div class="value" style="font-size:20px;">{format_krw(conservative)} ~ {format_krw(aggressive)}</div>
    <div class="hint">{"신규 브랜드 ±35%/28%" if r_brand_type == "신규 브랜드" else "기존 브랜드 ±12%/15%"}</div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

        st.write("")
        summary_df = pd.DataFrame(
            {
                "항목": ["지점", "층", "상품군", "운영 유형", "브랜드 유형", "브랜드", "운영 기간"],
                "값": [
                    r_store, r_floor, r_category, r_event,
                    r_brand_type,
                    r_brand if r_brand else "(신규)",
                    f"{r_start} ~ {r_end} ({duration_days}일)",
                ],
            }
        )
        st.markdown("### 입력 요약")
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        left, right = st.columns([1.2, 0.8])
        with left:
            st.markdown("### 적용 가중치")
            explain_df = pd.DataFrame(
                {
                    "항목": ["기준 일매출(카테고리)", "지점 가중치", "점포×상품군", "층 가중치", "브랜드 계수", "트렌드 계수"],
                    "값": [
                        format_krw(baseline),
                        f"{store_w:.3f}x",
                        f"{sc_w:.3f}x",
                        f"{floor_w:.3f}x",
                        f"{float(brand_coef):.3f}x",
                        f"{tcoef:.3f}x",
                    ],
                    "설명": [
                        "상품군 내 전체 평균 일매출",
                        "일별 점포 합산 기준 상대값",
                        "해당 점포에서 이 카테고리 집중도",
                        "일별 층 합산 기준 상대값",
                        "기존: 브랜드 실적 / 신규: 유사 ATV 피어",
                        f"트렌드 종합 점수 × 민감도({tcoef:.2f})",
                    ],
                }
            )
            st.dataframe(explain_df, use_container_width=True, hide_index=True)

        with right:
            st.markdown("### 빠른 요약")
            top_row = df_daily.sort_values("estimated_sales", ascending=False).iloc[0]
            low_row = df_daily.sort_values("estimated_sales", ascending=True).iloc[0]
            st.write(
                "\n".join([
                    f"- 핵심 결론: **{format_krw(base)}**",
                    f"- 최고 예상일: {top_row['date_str']} ({top_row['weekday_ko']})",
                    f"- 최저 예상일: {low_row['date_str']} ({low_row['weekday_ko']})",
                    f"- 브랜드 계산 방식: {brand_details['mode']}",
                    f"- 라이프사이클 보정: 오픈 버즈 +{'12' if r_event == '팝업' else '7'}% → 마감 효과 +{'7' if r_event == '팝업' else '4'}%",
                ])
            )

        st.markdown("### 일자별 매출 추정")
        c1, c2 = st.columns([1.2, 0.8])
        with c1:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=df_daily["date_str"],
                    y=df_daily["estimated_sales"],
                    mode="lines+markers",
                    line=dict(color="#111827", width=3),
                    marker=dict(
                        size=9,
                        color=["#111827" if wd < 5 else "#9ca3af" for wd in df_daily["weekday"]],
                    ),
                    customdata=[format_krw(v) for v in df_daily["estimated_sales"]],
                    hovertemplate="일자=%{x}<br>예상 매출=%{customdata}<extra></extra>",
                )
            )
            fig.update_layout(
                title="일자별 예상 매출 흐름",
                xaxis_title="일자", yaxis_title="예상 매출(원)",
                plot_bgcolor="#ffffff", paper_bgcolor="#ffffff",
                margin=dict(l=20, r=20, t=50, b=20), height=430,
            )
            fig.update_yaxes(showgrid=True, gridcolor="#e5e7eb")
            fig.update_xaxes(tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            top3 = df_daily.sort_values("estimated_sales", ascending=False).head(3).copy()
            top3["estimated_sales"] = top3["estimated_sales"].map(format_krw)
            top3 = top3.rename(columns={"date_str": "일자", "weekday_ko": "요일", "estimated_sales": "예상 매출"})[["일자", "요일", "예상 매출"]]
            st.markdown("#### 고매출 예상 상위 일자")
            st.dataframe(top3, use_container_width=True, hide_index=True, height=180)

            view_df = df_daily.copy()
            view_df["estimated_sales"] = view_df["estimated_sales"].map(format_krw)
            view_df["lifecycle_factor"] = view_df["lifecycle_factor"].map(lambda x: f"{x:.3f}x")
            view_df = view_df.rename(columns={
                "date_str": "일자", "weekday_ko": "요일",
                "estimated_sales": "예상 매출", "lifecycle_factor": "LC 계수",
            })[["일자", "요일", "예상 매출", "LC 계수"]]
            st.markdown("#### 전체 일자 보기")
            st.dataframe(view_df, use_container_width=True, hide_index=True, height=240)
            st.download_button(
                "CSV 다운로드",
                data=as_csv_download(df_daily),
                file_name="sales_forecast_daily.csv",
                mime="text/csv",
            )

        if r_brand_type == "신규 브랜드" and not brand_details["peers"].empty:
            st.markdown("### 유사 브랜드 풀 (ATV ±30%)")
            st.dataframe(brand_details["peers"], use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════
# TAB 2 : 실적 입력/현황 관리
# ════════════════════════════════════════════════

with tab_actuals:
    con = db_connect()
    form_col, list_col = st.columns([0.95, 1.05])

    with form_col:
        st.markdown(
            "<div class='card'><div style='font-size:16px;font-weight:900;'>팝업 실적 입력</div>"
            "<div style='font-size:13px;color:#6b7280;margin-top:6px;'>실적을 저장하고 이후 예측 오차 분석에 활용합니다.</div></div>",
            unsafe_allow_html=True,
        )
        st.write("")

        # 예측 결과가 있으면 자동 채우기 옵션 제공
        prefill = {}
        if st.session_state.get("forecast_result"):
            r = st.session_state["forecast_result"]
            if st.checkbox("마지막 예측 조건으로 자동 채우기"):
                prefill = dict(
                    store=r["store_value"],
                    floor=r["floor_value"],
                    category=r["category_value"],
                    event=r["event_type_value"],
                    start=r["start_date"],
                    end=r["end_date"],
                    brand=r["brand_value"] or "",
                )

        with st.form("actual_form"):
            c1, c2 = st.columns(2)
            store_list    = sorted(list(internal["store_avg"].keys()))
            floor_list    = sorted(list(internal["floor_avg"].keys()))
            category_list = sorted(list(internal["cat_avg"].keys()))

            with c1:
                a_store    = st.selectbox("지점",    store_list,
                                          index=store_list.index(prefill["store"]) if "store" in prefill else 0)
                a_floor    = st.selectbox("층",      floor_list,
                                          index=floor_list.index(prefill["floor"]) if "floor" in prefill else 0)
                a_category = st.selectbox("상품군",  category_list,
                                          index=category_list.index(prefill["category"]) if "category" in prefill else 0)
            with c2:
                a_event  = st.selectbox("운영 유형", ["팝업", "MD"],
                                        index=["팝업", "MD"].index(prefill.get("event", "팝업")))
                a_start  = st.date_input("시작일", value=prefill.get("start", date.today() - timedelta(days=13)))
                a_end    = st.date_input("종료일", value=prefill.get("end", date.today()))
                a_sales  = st.number_input("총 매출(원)", min_value=0.0, value=150_000_000.0, step=1_000_000.0)

            a_brand  = st.text_input("브랜드명", value=prefill.get("brand", ""))
            a_orders = st.number_input("총 구매건수(선택)", min_value=0.0, value=0.0, step=10.0)
            notes    = st.text_area("메모(선택)", placeholder="예: 행사 이슈/날씨/프로모션/재고 상황 등", height=100)
            submitted = st.form_submit_button("실적 저장", use_container_width=True)

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
                st.success("저장 완료")

    with list_col:
        actuals = db_fetch_actuals(con)
        st.markdown(
            "<div class='card'><div style='font-size:16px;font-weight:900;'>저장된 실적 현황</div>"
            "<div style='font-size:13px;color:#6b7280;margin-top:6px;'>최근 저장한 실적을 확인하고 삭제할 수 있습니다.</div></div>",
            unsafe_allow_html=True,
        )
        st.write("")

        if not actuals.empty:
            actuals["기간(일)"]  = (pd.to_datetime(actuals["end_date"]) - pd.to_datetime(actuals["start_date"])).dt.days + 1
            actuals["일평균매출"] = actuals["actual_total_sales"] / actuals["기간(일)"].clip(lower=1)
            actuals["ATV"]       = np.where(
                actuals["actual_total_orders"].fillna(0) > 0,
                actuals["actual_total_sales"] / actuals["actual_total_orders"],
                np.nan,
            )
            st.dataframe(actuals, use_container_width=True, hide_index=True, height=420)

            with st.expander("레코드 삭제"):
                st.caption("삭제할 id를 위 테이블에서 확인한 후 입력하세요.")
                del_id = st.number_input("삭제할 id", min_value=0, value=0, step=1)
                col_preview, col_btn = st.columns([2, 1])
                with col_preview:
                    if del_id > 0:
                        row_preview = actuals[actuals["id"] == del_id]
                        if row_preview.empty:
                            st.warning(f"id={del_id} 를 찾을 수 없습니다.")
                        else:
                            r = row_preview.iloc[0]
                            st.info(f"삭제 대상: [{r['brand']}] {r['store']} / {r['start_date']}~{r['end_date']}")
                with col_btn:
                    if st.button("삭제 실행", use_container_width=True):
                        if del_id > 0:
                            db_delete_actual(con, int(del_id))
                            st.success(f"id={int(del_id)} 삭제 완료")
                            st.rerun()
                        else:
                            st.warning("id를 1 이상으로 입력하세요.")
        else:
            st.info("저장된 실적이 없습니다.")


# ════════════════════════════════════════════════
# TAB 3 : 예측 vs 실적 비교
# ════════════════════════════════════════════════

with tab_compare:
    st.markdown(
        "<div class='card'>"
        "<div style='font-size:16px;font-weight:900;'>예측 vs 실적 비교 분석</div>"
        "<div style='font-size:13px;color:#6b7280;margin-top:6px;'>"
        "저장된 실적과 동일 조건으로 추정했을 때의 예측치를 비교합니다. 오차 패턴을 파악해 모델 신뢰도를 개선하세요."
        "</div></div>",
        unsafe_allow_html=True,
    )
    st.write("")

    con2 = db_connect()
    actuals2 = db_fetch_actuals(con2)

    if actuals2.empty:
        st.info("실적 탭에서 실적 데이터를 먼저 입력해주세요.")
    else:
        actuals2["기간(일)"] = (
            pd.to_datetime(actuals2["end_date"]) - pd.to_datetime(actuals2["start_date"])
        ).dt.days + 1

        rows = []
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

            # 동일 조건으로 예측 재계산 (트렌드 보정 없음: 중립값 1.0)
            baseline_c = float(internal["cat_avg"].get(r_cat, internal["overall"]))
            overall_c  = float(internal["overall"])
            store_w_c  = float(np.clip(
                float(internal["store_avg"].get(r_store, overall_c)) / overall_c, 0.70, 1.60
            ))
            floor_w_c  = float(np.clip(
                float(internal["floor_avg"].get(r_floor, overall_c)) / overall_c, 0.70, 1.60
            ))
            sc_w_c = float(internal["store_category_strength"].get((r_store, r_cat), 1.0))

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

            rows.append({
                "id":         int(row["id"]),
                "브랜드":     r_brand_nm,
                "지점":       r_store,
                "상품군":     r_cat,
                "기간":       f"{r_s} ~ {r_e} ({dur}일)",
                "실제 매출":  actual_total,
                "예측 매출":  predicted_total,
                "오차(%)":    round(error_pct, 1) if error_pct is not None else None,
            })

        cmp_df = pd.DataFrame(rows)

        # 오차 요약
        valid_errors = cmp_df["오차(%)"].dropna()
        if len(valid_errors) > 0:
            mape = float(valid_errors.abs().mean())
            mean_bias = float(valid_errors.mean())
            c1, c2, c3 = st.columns(3)
            c1.metric("MAPE (평균 절대 오차율)", f"{mape:.1f}%")
            c2.metric("평균 편향", f"{mean_bias:+.1f}%", help="양수=과대추정, 음수=과소추정")
            c3.metric("비교 건수", f"{len(cmp_df)}건")

        # 포맷 후 표시
        display_df = cmp_df.copy()
        display_df["실제 매출"] = display_df["실제 매출"].map(format_krw)
        display_df["예측 매출"] = display_df["예측 매출"].map(format_krw)
        display_df["오차(%)"]   = display_df["오차(%)"].map(lambda x: f"{x:+.1f}%" if pd.notna(x) else "-")
        st.dataframe(display_df.drop(columns=["id"]), use_container_width=True, hide_index=True)

        # 오차 분포 차트
        if len(valid_errors) >= 2:
            st.markdown("### 예측 오차 분포")
            fig_err = go.Figure()
            fig_err.add_trace(go.Bar(
                x=cmp_df["브랜드"] + " / " + cmp_df["지점"],
                y=cmp_df["오차(%)"],
                marker_color=["#ef4444" if v > 0 else "#3b82f6" for v in cmp_df["오차(%)"].fillna(0)],
                hovertemplate="%{x}<br>오차: %{y:+.1f}%<extra></extra>",
            ))
            fig_err.add_hline(y=0, line_dash="dash", line_color="#6b7280")
            fig_err.update_layout(
                title="예측 오차율 (양수=과대추정 / 음수=과소추정)",
                xaxis_title="브랜드 / 지점",
                yaxis_title="오차(%)",
                plot_bgcolor="#ffffff", paper_bgcolor="#ffffff",
                margin=dict(l=20, r=20, t=50, b=80),
                height=380,
            )
            fig_err.update_xaxes(tickangle=-35)
            st.plotly_chart(fig_err, use_container_width=True)

        st.download_button(
            "비교 결과 CSV 다운로드",
            data=as_csv_download(cmp_df),
            file_name="forecast_vs_actual.csv",
            mime="text/csv",
        )
