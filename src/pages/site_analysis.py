"""
📊 현장 분석 페이지.

고객 질문:
  Q2: "지표는 무엇이고, 믿을 수 있나요?"
  Q3: "그 숫자가 현장에서 무슨 의미인가요?"

4개 서브탭:
  1. 현장 전체  — 오늘의 현장 KPI 요약
  2. 작업자별   — 개인 Journey 스토리라인
  3. 업체별     — 업체 간 비교
  4. 추이       — 날짜별 트렌드

토글 원칙: 모든 지표 계산 로직은 st.expander로 기본 닫힘
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.data.schema import RawColumns, ProcessedColumns
from src.metrics.aggregator import aggregate_by_worker, aggregate_by_company, get_worker_journey_summary
from src.metrics.productivity import calc_productivity_summary, calc_working_blocks
from src.metrics.safety import calc_safety_summary
from src.metrics.drill_down import generate_worker_insight
from src.utils.theme import Color, apply_theme
from src.utils.time_utils import format_duration
from src.utils.place_utils import sort_places_by_similarity, sort_places_smart
from src.utils.constants import (
    WORK_INTENSITY_HIGH_THRESHOLD,
    WORK_INTENSITY_LOW_THRESHOLD,
    ACTIVE_RATIO_ZERO_THRESHOLD,
    WORK_HOURS_START, WORK_HOURS_END,
    NIGHT_HOURS_START, DAWN_HOURS_END,
    LUNCH_START, LUNCH_END,
)
from src.utils.llm_interpreter import (
    is_llm_available,
    cached_worker_narrative,
    cached_site_summary,
)


_TIME_CATS = [
    ("high_work",     "고활성 작업",  "#1A5276"),
    ("low_work",      "저활성 작업",  "#5DADE2"),
    ("standby",       "현장 대기",    "#F5A623"),
    ("transit",       "이동",         "#F7DC6F"),
    ("rest_facility", "휴게실 이용",  "#27AE60"),
    ("off_duty",      "비근무",       "#95A5A6"),
]
_CAT_COLOR = {k: c for k, _, c in _TIME_CATS}
_CAT_LABEL = {k: l for k, l, _ in _TIME_CATS}


def render(
    df: pd.DataFrame,
    df_multi: Optional[pd.DataFrame],
    cache_dir: Path,
    data_dir: Path,
    selected_date: str = "",
    get_analytics=None,
) -> None:
    if df is None or df.empty:
        st.warning("데이터 없음")
        return

    date_str = df[ProcessedColumns.DATE].iloc[0] if ProcessedColumns.DATE in df.columns else (selected_date or "")
    analytics = get_analytics(selected_date or date_str, df) if callable(get_analytics) else None

    st.markdown(f"""
    <div class="kpi-banner">
        <h1>📊 현장 분석</h1>
        <p>Q2·Q3: 지표 + 맥락 &nbsp;·&nbsp; {date_str}</p>
    </div>""", unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs([
        "🏗️ 현장 전체",
        "👷 작업자별",
        "🏢 업체별",
        "📈 추이",
    ])

    with tab1:
        _render_site_overview(df, analytics)

    with tab2:
        _render_worker_detail(df)

    with tab3:
        _render_company_comparison(df, analytics)

    with tab4:
        _render_trend_tab(df_multi, cache_dir, data_dir)


def _get_top_zones(wdf: pd.DataFrame, n: int = 3) -> list:
    """작업자의 주요 활동 구역 추출."""
    if ProcessedColumns.CORRECTED_PLACE not in wdf.columns:
        return []
    place_counts = wdf[ProcessedColumns.CORRECTED_PLACE].value_counts()
    return place_counts.head(n).index.tolist()


def _get_anomaly_descriptions(wdf: pd.DataFrame) -> list:
    """작업자의 이상 패턴 설명 추출."""
    if ProcessedColumns.ANOMALY_FLAG not in wdf.columns:
        return []
    anomalies = wdf[wdf[ProcessedColumns.ANOMALY_FLAG].notna()]
    if anomalies.empty:
        return []
    
    descriptions = []
    type_map = {
        "abnormal_stop": "장시간 비활성 정지",
        "gate_congestion": "게이트 대기 병목",
        "lone_hazard": "단독 작업 위험",
        "transit_idle": "이동 경로 정체",
        "standby_excess": "과도한 대기",
    }
    for _, row in anomalies.iterrows():
        flag = row[ProcessedColumns.ANOMALY_FLAG]
        desc = type_map.get(flag, flag)
        place = row.get(ProcessedColumns.CORRECTED_PLACE, "")
        if place:
            descriptions.append(f"{desc} ({place})")
        else:
            descriptions.append(desc)
    
    return descriptions[:5]


def _render_worker_ai_narrative(
    worker_name: str,
    onsite_hours: float,
    ewi: float,
    high_work_min: int,
    low_work_min: int,
    standby_min: int,
    transit_min: int,
    rest_min: int,
    main_zones: list,
    journey_pattern: str,
    anomalies: list,
    date_str: str,
) -> None:
    """작업자 AI 내러티브 카드."""
    if not is_llm_available():
        return

    summary = {
        "date": date_str,
        "onsite_hours": onsite_hours,
        "ewi": ewi,
        "high_work_min": high_work_min,
        "low_work_min": low_work_min,
        "standby_min": standby_min,
        "transit_min": transit_min,
        "rest_min": rest_min,
        "main_zones": main_zones,
        "journey_pattern": journey_pattern,
        "anomalies": anomalies,
    }

    summary_frozen = tuple(sorted(
        {k: str(v) for k, v in summary.items()}.items()
    ))

    with st.spinner("🧠 AI 작업 분석 중..."):
        narrative = cached_worker_narrative(summary_frozen, worker_name)

    if narrative:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #F0F7FF 0%, #E8F4FD 100%);
                border-left: 4px solid #2E6FD9;
                border-radius: 10px;
                padding: 16px 20px;
                margin-bottom: 16px;
                box-shadow: 0 2px 8px rgba(46, 111, 217, 0.08);
            ">
                <div style="font-size:12px; color:#6B7A99; margin-bottom:6px; font-weight:500;">
                    🧠 AI 작업 분석
                </div>
                <div style="font-size:14px; color:#1B2A4A; line-height:1.7;">
                    {narrative}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.caption(
            "⚠️ 이 AI 해석은 BLE 센서 데이터 기반 추정입니다. "
            "실제 현장 상황과 다를 수 있으며, 관리자 판단을 대체하지 않습니다."
        )


def _render_site_ai_summary(
    worker_count: int,
    avg_ewi: float,
    total_standby_min: int,
    anomaly_count: int,
    top_zones: list,
    date_str: str,
) -> None:
    """현장 전체 AI 요약 카드."""
    if not is_llm_available():
        return

    site_summary = {
        "worker_count": worker_count,
        "avg_ewi": avg_ewi,
        "total_standby_min": total_standby_min,
        "anomaly_count": anomaly_count,
        "top_zones": top_zones,
        "prev_avg_ewi": None,
    }

    summary_frozen = tuple(sorted(
        {k: str(v) for k, v in site_summary.items()}.items()
    ))

    with st.spinner("🧠 AI 현장 분석 중..."):
        summary_text = cached_site_summary(summary_frozen, date_str)

    if summary_text:
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, #F0F7FF 0%, #E8F4FD 100%);
                border-left: 4px solid #2E6FD9;
                border-radius: 10px;
                padding: 16px 20px;
                margin-bottom: 16px;
                box-shadow: 0 2px 8px rgba(46, 111, 217, 0.08);
            ">
                <div style="font-size:12px; color:#6B7A99; margin-bottom:6px; font-weight:500;">
                    🧠 AI 현장 요약
                </div>
                <div style="font-size:14px; color:#1B2A4A; line-height:1.7;">
                    {summary_text}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_site_overview(df: pd.DataFrame, analytics: Optional[dict] = None) -> None:
    if analytics is not None and "worker_summary" in analytics and not analytics["worker_summary"].empty:
        worker_df = analytics["worker_summary"]
    else:
        worker_df = aggregate_by_worker(df, include_safety=True)

    total_w   = len(worker_df)
    total_co  = worker_df["company"].nunique() if not worker_df.empty else 0
    avg_ratio = worker_df["active_ratio"].mean() if "active_ratio" in worker_df.columns else 0
    avg_work  = worker_df.get("working_time_min", pd.Series([0.0])).mean()

    soif = analytics if analytics else {}
    if not soif or "site_ewi" not in soif:
        try:
            from src.metrics.soif import calc_soif_summary
            soif = calc_soif_summary(df)
        except Exception:
            soif = soif or {}
    
    # EWI 추출 (site_ewi는 dict) - 안전한 타입 변환
    site_ewi_dict = soif.get("site_ewi", {})
    try:
        ewi_raw = site_ewi_dict.get("ewi", avg_ratio * 0.7) if isinstance(site_ewi_dict, dict) else avg_ratio * 0.7
        ewi = float(ewi_raw) if ewi_raw is not None else 0.0
    except (TypeError, ValueError):
        ewi = 0.0
    
    # CRE 추출 - 안전한 타입 변환
    try:
        avg_cre = float(soif.get("avg_cre", 0) or 0)
    except (TypeError, ValueError):
        avg_cre = 0.0
    try:
        max_cre = float(soif.get("max_cre", 0) or 0)
    except (TypeError, ValueError):
        max_cre = 0.0
    
    # 병목 점수 추출 - 안전한 타입 변환
    top_bottlenecks = soif.get("top_bottlenecks", [])
    try:
        top_bs = float(top_bottlenecks[0]["bottleneck_score"]) if top_bottlenecks else 0.0
    except (TypeError, ValueError, KeyError, IndexError):
        top_bs = 0.0

    standby_total = 0
    for wk in df[ProcessedColumns.WORKER_KEY].unique():
        wdf = df[df[ProcessedColumns.WORKER_KEY] == wk]
        tb = _calc_time_breakdown(wdf)
        standby_total += tb.get("standby", 0)

    anomaly_n = 0
    if ProcessedColumns.ANOMALY_FLAG in df.columns:
        anomaly_n = int((df[ProcessedColumns.ANOMALY_FLAG].notna()).sum())

    top_zones = (
        df[ProcessedColumns.CORRECTED_PLACE]
        .value_counts().head(3).index.tolist()
        if ProcessedColumns.CORRECTED_PLACE in df.columns else []
    )

    date_str = df[ProcessedColumns.DATE].iloc[0] if ProcessedColumns.DATE in df.columns else ""

    _render_site_ai_summary(
        worker_count=total_w,
        avg_ewi=ewi,
        total_standby_min=standby_total,
        anomaly_count=anomaly_n,
        top_zones=top_zones,
        date_str=str(date_str),
    )

    # 핵심 KPI 카드 (6개 → 3행 2열)
    st.markdown("#### 📊 핵심 운영 KPI")
    row1_c1, row1_c2, row1_c3 = st.columns(3)
    with row1_c1:
        st.metric("👷 현장 인원", f"{total_w}명",
                  help="당일 신호가 수집된 작업자 수")
    with row1_c2:
        st.metric("⚡ EWI (생산성)", f"{ewi:.0%}",
                  help="유효작업집중도 = (고활성 × 1.0 + 저활성 × 0.5) / 현장 체류 시간")
    with row1_c3:
        st.metric("⏳ 현장대기", f"{standby_total}분",
                  help="작업공간 내 활성비율 < 15%인 시간 합계")
    
    row2_c1, row2_c2, row2_c3 = st.columns(3)
    with row2_c1:
        cre_color = "inverse" if avg_cre >= 0.5 else "off"
        st.metric("🛡️ CRE (안전)", f"{avg_cre:.2f}",
                  delta=f"최대 {max_cre:.2f}" if max_cre > avg_cre else None,
                  delta_color=cre_color,
                  help="복합위험노출도 = 개인위험 × 공간위험 × 동적부하. ≥1.0 고위험")
    with row2_c2:
        st.metric("🚧 BS (병목)", f"{top_bs:.2f}",
                  help="병목점수 = 흐름불균형(60%) + 대기부하(40%). 높을수록 병목")
    with row2_c3:
        st.metric("⚠️ 이상 감지", f"{anomaly_n}건",
                  help="abnormal_stop, gate_congestion 등 이상 신호")

    st.markdown("<br>", unsafe_allow_html=True)

    # 병목 구역 리스트 (상위 3개)
    if top_bottlenecks:
        st.markdown("**🚧 병목 주의 구역 (Top 3)**")
        bs_cols = st.columns(min(len(top_bottlenecks), 3))
        for idx, bn in enumerate(top_bottlenecks[:3]):
            with bs_cols[idx]:
                zone_name = bn.get("zone", "?")
                try:
                    bs_val = float(bn.get("bottleneck_score", 0) or 0)
                    flow_imb = float(bn.get("flow_imbalance", 0) or 0)
                    standby_t = float(bn.get("standby_total", 0) or 0)
                except (TypeError, ValueError):
                    bs_val, flow_imb, standby_t = 0.0, 0.0, 0.0
                st.info(f"**{zone_name}**  \nBS: `{bs_val:.2f}` | 흐름불균형: `{flow_imb:+.0f}` | 대기: `{standby_t:.0f}분`")
        st.markdown("<br>", unsafe_allow_html=True)

    col_a, col_b = st.columns([1.2, 1], gap="medium")

    with col_a:
        st.markdown("**📊 구역별 활동 히트맵**")
        _render_density_heatmap(df)

    with col_b:
        st.markdown("**⏱️ 시간대별 상태 분포**")
        _render_hourly_state_stack(df)

    with st.expander("📖 SOIF 핵심 KPI 정의", expanded=False):
        st.markdown("""
### 📈 생산성: EWI (Effective Work Intensity, 유효작업집중도)
```
EWI = (고활성 작업 × 1.0 + 저활성 작업 × 0.5) / 현장 체류 시간
```
- **의미**: 현장 체류 시간 중 실제 생산 활동 비율
- **해석**: ≥70% 높은 집중도, 50~70% 보통, <50% 비효율 가능성

---

### 🚧 운영: BS (Bottleneck Score, 병목 점수)
```
BS = Norm(ΔInflow − ΔOutflow) × 0.6 + Standby Pressure × 0.4
```
- **의미**: 구역의 흐름 불균형과 대기 압력을 복합 수치화
- **해석**: 유입↑ 유출↓이면서 대기 누적되는 '진짜 막히는 구간' 식별

---

### 🛡️ 안전: CRE (Combined Risk Exposure, 복합위험노출도)
```
CRE = Personal Risk × Static Space Risk × Dynamic Pressure
```
- **Personal Risk**: 피로(연속작업 120분+) + 고립(반경 내 동료 없음)
- **Static Space Risk**: 공간 고유 위험 (밀폐공간 2.0, 작업장 1.2, 휴게실 0.3 등)
- **Dynamic Pressure**: 해당 공간의 혼잡도
- **해석**: ≥1.0 고위험(즉시 확인), ≥0.5 중위험(주의), <0.5 저위험

---

### ⏳ 현장대기 (Standby)
```
현장대기 = 작업공간(WORK) 내 활성비율 < 15%인 시간 합계
```
- **의미**: 자재/장비/지시 대기로 인한 비생산 정지 시간
- 휴게실 체류는 별도 `rest_facility`로 구분
        """)


def _render_worker_detail(df: pd.DataFrame) -> None:
    worker_keys = sorted(df[ProcessedColumns.WORKER_KEY].unique())
    worker_label = {}
    for wk in worker_keys:
        sub = df[df[ProcessedColumns.WORKER_KEY] == wk]
        name    = sub[RawColumns.WORKER].iloc[0]
        company = sub[RawColumns.COMPANY].iloc[0]
        worker_label[wk] = f"{name}  ({company})"

    selected_key = st.selectbox(
        "👷 작업자 선택",
        options=worker_keys,
        format_func=lambda k: worker_label.get(k, k),
        key="site_analysis_worker_select",
    )

    wdf = df[df[ProcessedColumns.WORKER_KEY] == selected_key].copy()
    wdf = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)

    if wdf.empty:
        st.warning("데이터 없음")
        return

    name    = wdf[RawColumns.WORKER].iloc[0]
    company = wdf[RawColumns.COMPANY].iloc[0]
    summary = get_worker_journey_summary(df, selected_key)

    t_min_str = wdf[RawColumns.TIME].min().strftime("%H:%M")
    t_max_str = wdf[RawColumns.TIME].max().strftime("%H:%M")

    tb = _calc_time_breakdown(wdf)
    total = tb["total"] or 1
    high_work_min = tb["high_work"]
    low_work_min = tb["low_work"]
    ewi_personal = (high_work_min * 1.0 + low_work_min * 0.5) / total if total > 0 else 0

    top_place = ""
    if ProcessedColumns.CORRECTED_PLACE in wdf.columns:
        place_counts = wdf[ProcessedColumns.CORRECTED_PLACE].value_counts()
        if not place_counts.empty:
            top_place = place_counts.index[0]
            top_place_min = place_counts.iloc[0]
            top_place = f"{top_place} ({top_place_min}분)"

    prod = calc_productivity_summary(wdf)
    safety = calc_safety_summary(wdf, df)
    
    # CRE (복합 위험 노출도) 가져오기 - 안전한 타입 변환
    try:
        cre = float(safety.get("contextual_risk", 0) or 0)
    except (TypeError, ValueError):
        cre = 0.0
    cre_level = safety.get("contextual_risk_level", "LOW")
    cre_color = "#E74C3C" if cre_level == "HIGH" else ("#F39C12" if cre_level == "MEDIUM" else "#27AE60")

    st.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E8ECF4;border-radius:12px;
                padding:1rem 1.4rem;margin-bottom:1rem;">
        <div style="font-size:1.1rem;font-weight:700;color:{Color.PRIMARY};">오늘의 요약</div>
        <div style="font-size:0.9rem;color:{Color.TEXT_DARK};margin-top:8px;">
            <b>{t_min_str}</b> 출근 → <b>{t_max_str}</b> 퇴근
            &nbsp;|&nbsp; 현장 체류 <b>{format_duration(total)}</b>
            &nbsp;|&nbsp; 실작업 <b>{format_duration(high_work_min + low_work_min)}</b>
        </div>
        <div style="font-size:0.85rem;color:{Color.TEXT_MUTED};margin-top:4px;">
            주 작업구역: <b>{top_place}</b>
            &nbsp;|&nbsp; ⚡ EWI: <b>{ewi_personal:.0%}</b>
            &nbsp;|&nbsp; 🛡️ CRE: <b style="color:{cre_color};">{cre:.2f}</b> ({cre_level})
        </div>
    </div>""", unsafe_allow_html=True)
    insights = generate_worker_insight(df, selected_key, prod, safety)

    main_zones = _get_top_zones(wdf, n=3)
    anomalies = _get_anomaly_descriptions(wdf)
    journey_pattern = (
        wdf[ProcessedColumns.JOURNEY_PATTERN].iloc[0]
        if ProcessedColumns.JOURNEY_PATTERN in wdf.columns
        else "unknown"
    )
    date_val = wdf[ProcessedColumns.DATE].iloc[0] if ProcessedColumns.DATE in wdf.columns else ""

    _render_worker_ai_narrative(
        worker_name=name,
        onsite_hours=total / 60.0,
        ewi=ewi_personal,
        high_work_min=high_work_min,
        low_work_min=low_work_min,
        standby_min=tb["standby"],
        transit_min=tb["transit"],
        rest_min=tb["rest_facility"],
        main_zones=main_zones,
        journey_pattern=str(journey_pattern),
        anomalies=anomalies,
        date_str=str(date_val),
    )

    if insights:
        st.markdown("**오늘의 이야기 (Rule-based Journey 분석)**")
        gantt_df = _build_gantt(wdf)
        narrative = _generate_journey_narrative(gantt_df, name)
        st.markdown(narrative, unsafe_allow_html=True)
    else:
        st.success("✅ 특이사항 없음. 오늘 작업 패턴이 정상 범위입니다.")

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("**📍 하루 Journey 타임라인**")
    _render_journey_gantt(wdf, df)

    with st.expander("📖 시간 배분 상세", expanded=False):
        _render_time_breakdown_detail(tb)

    with st.expander("📖 생산성 지표 상세 + 계산 로직", expanded=False):
        _render_productivity_metrics(prod, wdf)

    with st.expander("📖 안전성 지표 상세 + 계산 로직", expanded=False):
        _render_safety_metrics(safety, wdf)


def _render_company_comparison(df: pd.DataFrame, analytics: Optional[dict] = None) -> None:
    if analytics is not None and "worker_summary" in analytics and "company_summary" in analytics:
        worker_df  = analytics["worker_summary"]
        company_df = analytics["company_summary"]
    else:
        worker_df  = aggregate_by_worker(df, include_safety=True)
        company_df = aggregate_by_company(df)

    if company_df.empty:
        st.info("업체 데이터 없음")
        return

    st.markdown("**업체별 비교**")

    col1, col2 = st.columns(2, gap="medium")

    with col1:
        if "active_ratio" in company_df.columns:
            sorted_df = company_df.sort_values("active_ratio", ascending=True)
            co_short  = sorted_df["company"].apply(lambda x: str(x).split("(")[0][:12])
            fig = go.Figure(go.Bar(
                x=sorted_df["active_ratio"],
                y=co_short,
                orientation="h",
                marker_color=[
                    Color.SAFE if v >= 0.7 else (Color.WARNING if v >= 0.4 else Color.DANGER)
                    for v in sorted_df["active_ratio"]
                ],
                text=sorted_df["active_ratio"].apply(lambda v: f"{v:.0%}"),
                textposition="outside",
            ))
            fig = apply_theme(fig, "업체별 EWI 비교", height=280)
            fig.update_xaxes(range=[0, 1.1])
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "safety_fatigue_risk" in company_df.columns:
            sorted_df = company_df.sort_values("safety_fatigue_risk", ascending=False)
            co_short  = sorted_df["company"].apply(lambda x: str(x).split("(")[0][:12])
            fig2 = go.Figure(go.Bar(
                x=co_short,
                y=sorted_df["safety_fatigue_risk"],
                marker_color=[
                    Color.DANGER if v >= 1.0 else (Color.WARNING if v >= 0.5 else Color.SAFE)
                    for v in sorted_df["safety_fatigue_risk"]
                ],
                text=sorted_df["safety_fatigue_risk"].apply(lambda v: f"{v:.2f}"),
                textposition="outside",
            ))
            fig2 = apply_theme(fig2, "업체별 안전 지표", height=280)
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown("""
    **해석**:
    EWI 차이는 개인 역량보다 작업 배치, 자재 공급, 동선 설계에 기인할 수 있습니다.
    """)


def _render_trend_tab(
    df_multi: Optional[pd.DataFrame],
    cache_dir: Path,
    data_dir: Path,
) -> None:
    if df_multi is None or df_multi.empty:
        st.info("날짜 범위 모드에서 멀티 날짜 데이터를 로드하면 트렌드를 볼 수 있습니다.")
        return

    from src.metrics.trend_analyzer import calc_site_daily_summary, detect_trend_anomaly

    summary = calc_site_daily_summary(df_multi)
    if summary.empty:
        st.info("트렌드 데이터 없음")
        return

    def _fmt(d: str) -> str:
        return f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d) == 8 else d

    metric_options = {
        "avg_active_ratio":     "평균 활성비율",
        "avg_working_time_min": "평균 작업시간(분)",
        "fatigue_risk_avg":     "피로 위험도",
    }

    selected = st.selectbox(
        "지표 선택",
        options=list(metric_options.keys()),
        format_func=lambda k: metric_options[k],
        key="trend_metric_select",
    )

    if len(summary) > 2:
        summary = detect_trend_anomaly(summary, selected)

    fig = go.Figure()
    x_vals = [_fmt(d) for d in summary["date"]]

    if selected in summary.columns:
        y_vals = summary[selected].tolist()
        anom = summary.get("is_anomaly", pd.Series([False] * len(summary)))

        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="lines+markers",
            name=metric_options[selected],
            line=dict(width=2, color=Color.SECONDARY),
            marker=dict(
                size=[10 if a else 6 for a in anom],
                color=[Color.DANGER if a else Color.SECONDARY for a in anom],
                symbol=["diamond" if a else "circle" for a in anom],
            ),
        ))

    fig = apply_theme(fig, f"날짜별 {metric_options[selected]} 추이", height=320)
    fig.update_xaxes(title_text="날짜", tickangle=-30)
    st.plotly_chart(fig, use_container_width=True)

    st.caption("◆ 빨간 다이아몬드 = 이상 날짜 감지 (이동평균 ±1.5σ)")


def _calc_time_breakdown(wdf: pd.DataFrame) -> dict:
    sorted_df = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)
    n = len(sorted_df)
    if n == 0:
        return {"high_work": 0, "low_work": 0, "standby": 0, "transit": 0, "rest_facility": 0, "off_duty": 0, "total": 0}

    work_hrs_mask = (
        (sorted_df[ProcessedColumns.HOUR] >= WORK_HOURS_START)
        & (sorted_df[ProcessedColumns.HOUR] < WORK_HOURS_END)
    )
    place_type   = sorted_df[ProcessedColumns.PLACE_TYPE]
    active_ratio = sorted_df[ProcessedColumns.ACTIVE_RATIO]

    cat = pd.Series([""] * n, index=sorted_df.index)

    cat[place_type == "HELMET_RACK"] = "off_duty"
    cat[~work_hrs_mask & (cat == "")] = "off_duty"

    rest_kw = ["휴게", "식당", "탈의실", "탈의", "로비"]
    if ProcessedColumns.CORRECTED_PLACE in sorted_df.columns:
        rest_mask = sorted_df[ProcessedColumns.CORRECTED_PLACE].fillna("").str.contains("|".join(rest_kw), na=False)
        cat[rest_mask] = "rest_facility"

    cat[(place_type == "GATE") & (cat == "")] = "transit"

    in_work_hrs = work_hrs_mask & (cat == "")
    cat[in_work_hrs & (active_ratio >= WORK_INTENSITY_HIGH_THRESHOLD)] = "high_work"
    cat[in_work_hrs & (active_ratio >= WORK_INTENSITY_LOW_THRESHOLD) & (active_ratio < WORK_INTENSITY_HIGH_THRESHOLD)] = "low_work"
    cat[in_work_hrs & (active_ratio < WORK_INTENSITY_LOW_THRESHOLD)] = "standby"

    cat[cat == ""] = "off_duty"

    counts = cat.value_counts()
    return {
        "high_work":     int(counts.get("high_work", 0)),
        "low_work":      int(counts.get("low_work", 0)),
        "standby":       int(counts.get("standby", 0)),
        "transit":       int(counts.get("transit", 0)),
        "rest_facility": int(counts.get("rest_facility", 0)),
        "off_duty":      int(counts.get("off_duty", 0)),
        "total":         n,
    }


def _render_density_heatmap(df: pd.DataFrame) -> None:
    work_df = df[
        (df[ProcessedColumns.HOUR] >= WORK_HOURS_START)
        & (df[ProcessedColumns.HOUR] < WORK_HOURS_END)
    ]
    if work_df.empty:
        st.info("근무시간 데이터 없음")
        return

    density = (
        work_df.groupby([ProcessedColumns.HOUR, ProcessedColumns.CORRECTED_PLACE])
        [ProcessedColumns.WORKER_KEY].nunique().reset_index(name="count")
    )
    pivot = density.pivot(
        index=ProcessedColumns.CORRECTED_PLACE,
        columns=ProcessedColumns.HOUR,
        values="count",
    ).fillna(0)

    top_places = pivot.sum(axis=1).nlargest(12).index
    pivot = pivot.loc[top_places]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h:02d}시" for h in pivot.columns],
        y=pivot.index.tolist(),
        colorscale=[[0, Color.BG_MUTED], [0.5, Color.SECONDARY], [1, Color.PRIMARY]],
        showscale=True,
        hovertemplate="<b>%{y}</b><br>%{x} · %{z}명<extra></extra>",
        colorbar=dict(title="인원", thickness=12),
    ))
    fig = apply_theme(fig, "", height=max(240, len(pivot) * 24 + 60))
    fig.update_xaxes(side="top")
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)


def _render_hourly_state_stack(df: pd.DataFrame) -> None:
    work_df = df[
        (df[ProcessedColumns.HOUR] >= WORK_HOURS_START)
        & (df[ProcessedColumns.HOUR] < WORK_HOURS_END)
    ]
    if work_df.empty:
        st.info("데이터 없음")
        return

    hourly_state = work_df.groupby([ProcessedColumns.HOUR, ProcessedColumns.PERIOD_TYPE]).size().unstack(fill_value=0)

    fig = go.Figure()
    for pt in ["high_work", "low_work", "standby", "transit", "rest", "off_duty"]:
        if pt in hourly_state.columns:
            fig.add_trace(go.Bar(
                x=[f"{h:02d}시" for h in hourly_state.index],
                y=hourly_state[pt],
                name=_CAT_LABEL.get(pt, pt),
                marker_color=_CAT_COLOR.get(pt, "#888"),
            ))

    fig = apply_theme(fig, "", height=260)
    fig.update_layout(barmode="stack", showlegend=True, legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)


def _build_gantt(wdf: pd.DataFrame, max_gap_min: int = 5) -> pd.DataFrame:
    """
    Gantt 차트용 DataFrame 생성.
    
    Args:
        wdf: 작업자 Journey DataFrame
        max_gap_min: 최대 허용 공백 (분). 이 이상 공백이면 별도 블록으로 분리.
    """
    sorted_df = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)
    rows, cur_place, start_ts, buf, prev_ts = [], None, None, [], None

    for _, row in sorted_df.iterrows():
        place = str(row.get(ProcessedColumns.CORRECTED_PLACE, "Unknown"))
        ts    = row[RawColumns.TIME]
        
        # 시간 공백 체크: 이전 데이터와 max_gap_min 이상 차이나면 블록 종료
        if prev_ts is not None and cur_place and buf:
            gap_min = (ts - prev_ts).total_seconds() / 60
            if gap_min > max_gap_min:
                _flush_block(rows, cur_place, start_ts, buf)
                cur_place = None
                start_ts = None
                buf = []
        
        if place != cur_place:
            if cur_place and buf:
                _flush_block(rows, cur_place, start_ts, buf)
            cur_place = place
            start_ts  = ts
            buf       = [row]
        else:
            buf.append(row)
        
        prev_ts = ts
        
    if cur_place and buf:
        _flush_block(rows, cur_place, start_ts, buf)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _flush_block(gantt_rows, place, start_ts, block_rows) -> None:
    end_ts    = block_rows[-1][RawColumns.TIME] + pd.Timedelta(minutes=1)
    ratios    = [r.get(ProcessedColumns.ACTIVE_RATIO, 0) for r in block_rows]
    avg_ratio = sum(ratios) / len(ratios) if ratios else 0
    high_min  = sum(1 for r in ratios if r >= WORK_INTENSITY_HIGH_THRESHOLD)
    low_min   = sum(1 for r in ratios if WORK_INTENSITY_LOW_THRESHOLD <= r < WORK_INTENSITY_HIGH_THRESHOLD)
    place_type = block_rows[0].get(ProcessedColumns.PLACE_TYPE, "UNKNOWN")

    activity = _classify_block_activity(place_type, avg_ratio, start_ts.hour)

    gantt_rows.append({
        "장소": place, "시작": start_ts, "종료": end_ts,
        "장소유형": place_type, "활동상태": activity,
        "평균활성비율": round(avg_ratio, 3), "체류(분)": len(block_rows),
        "고활성(분)": high_min, "저활성(분)": low_min,
    })


def _classify_block_activity(place_type: str, avg_ratio: float, hour: int) -> str:
    if place_type in ("HELMET_RACK", "RACK"):
        return "off_duty"
    if place_type in ("REST", "REST_FACILITY"):
        return "rest"
    if place_type in ("GATE", "TRANSIT_GATE", "TRANSIT_CORRIDOR"):
        return "transit"
    if hour < WORK_HOURS_START or hour >= WORK_HOURS_END:
        return "off_duty"
    if avg_ratio >= WORK_INTENSITY_HIGH_THRESHOLD:
        return "high_work"
    if avg_ratio >= WORK_INTENSITY_LOW_THRESHOLD:
        return "low_work"
    if avg_ratio >= ACTIVE_RATIO_ZERO_THRESHOLD:
        return "standby"
    return "off_duty"


def _render_journey_gantt(wdf: pd.DataFrame, df: pd.DataFrame) -> None:
    gantt_df = _build_gantt(wdf)
    if gantt_df.empty:
        st.info("Journey 데이터 없음")
        return

    date_val = df[ProcessedColumns.DATE].iloc[0] if ProcessedColumns.DATE in df.columns else df[RawColumns.TIME].iloc[0]
    if hasattr(date_val, "strftime"):
        date_str = date_val.strftime("%Y-%m-%d")
    else:
        s = str(date_val).replace("-", "")[:8]
        date_str = f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) >= 8 else "2000-01-01"

    t_min = pd.Timestamp(f"{date_str} 00:00:00")
    t_max = t_min + pd.Timedelta(days=1)

    fig = go.Figure()

    for _, row in gantt_df.iterrows():
        act = row.get("활동상태", "off_duty")
        bar_color = _CAT_COLOR.get(act, "#B0B8C8")
        dur_ms = (row["종료"] - row["시작"]).total_seconds() * 1000

        fig.add_trace(go.Bar(
            base=row["시작"].strftime("%Y-%m-%d %H:%M:%S"),
            x=[dur_ms],
            y=[row["장소"]],
            orientation="h",
            marker_color=bar_color,
            marker_line=dict(color="white", width=0.5),
            opacity=0.92,
            showlegend=False,
            customdata=[[row["평균활성비율"], row["체류(분)"], _CAT_LABEL.get(act, act)]],
            hovertemplate=(
                "<b>%{y}</b><br>"
                "체류: %{customdata[1]}분<br>"
                "활성비율: %{customdata[0]:.1%}<br>"
                "상태: %{customdata[2]}<extra></extra>"
            ),
        ))

    # ★ 전체 데이터(df)의 모든 장소 수집 — 작업자 간 일관된 Y축
    all_place_set: set = set()
    for col in [RawColumns.PLACE, ProcessedColumns.CORRECTED_PLACE]:
        if col in df.columns:
            for p in df[col].astype(str):
                if p and p != "nan":
                    all_place_set.add(p)
    
    # 이동 기반 스마트 정렬 (전체 데이터 활용)
    all_places = sort_places_smart(list(all_place_set), df, ProcessedColumns.CORRECTED_PLACE)
    chart_h = max(280, len(all_places) * 28 + 80)
    fig = apply_theme(fig, "", height=chart_h)
    fig.update_layout(
        barmode="overlay",
        bargap=0.28,
        xaxis=dict(
            type="date",
            tickformat="%H:%M",
            showgrid=True,
            gridcolor="#E4EAF4",
            range=[t_min.isoformat(), t_max.isoformat()],
            dtick=2 * 60 * 60 * 1000,  # 2시간 간격 (밀리초 단위)
            tickmode="linear",
            tick0=t_min.replace(hour=(t_min.hour // 2) * 2, minute=0, second=0).isoformat(),
        ),
        yaxis=dict(
            categoryorder="array", categoryarray=all_places,
            tickfont=dict(size=9),
        ),
        margin=dict(l=0, r=10, t=20, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    legend_html = " &nbsp;".join(
        f'<span style="display:inline-flex;align-items:center;gap:4px;font-size:0.75rem;">'
        f'<span style="width:10px;height:10px;border-radius:2px;background:{c};display:inline-block;"></span>'
        f'<span style="color:#4A5A78;">{l}</span></span>'
        for k, l, c in _TIME_CATS
    )
    st.markdown(f'<div style="text-align:center;margin-top:4px;">{legend_html}</div>', unsafe_allow_html=True)


def _generate_journey_narrative(gantt_df: pd.DataFrame, worker_name: str) -> str:
    if gantt_df.empty:
        return "<p>Journey 데이터가 없습니다.</p>"

    blocks = gantt_df.sort_values("시작").reset_index(drop=True)
    lines: list[str] = []

    first_work = None
    for _, blk in blocks.iterrows():
        if blk.get("활동상태") in ("high_work", "low_work", "standby"):
            first_work = blk
            break

    if first_work is not None:
        lines.append(f"**{first_work['시작'].strftime('%H:%M')}** 경 현장 활동 시작 ({first_work['장소']})")

    work_phases = []
    for _, blk in blocks.iterrows():
        act = blk.get("활동상태", "off_duty")
        if act in ("high_work", "low_work", "standby"):
            work_phases.append({
                "place": blk["장소"],
                "start": blk["시작"],
                "end": blk["종료"],
                "duration": blk["체류(분)"],
                "high_min": blk.get("고활성(분)", 0),
                "low_min": blk.get("저활성(분)", 0),
                "avg_ratio": blk.get("평균활성비율", 0),
            })

    for phase in work_phases[:5]:
        intensity = "고활성 위주" if phase["high_min"] > phase["low_min"] else "저활성 위주"
        lines.append(
            f"**{phase['start'].strftime('%H:%M')}~{phase['end'].strftime('%H:%M')}** "
            f"**{phase['place']}**에서 {phase['duration']}분간 작업 ({intensity})"
        )

    last_work = None
    for _, blk in blocks.iloc[::-1].iterrows():
        if blk.get("활동상태") in ("high_work", "low_work"):
            last_work = blk
            break

    if last_work is not None:
        lines.append(f"**{last_work['종료'].strftime('%H:%M')}** 경 마지막 작업 종료")

    total_work = sum(p["duration"] for p in work_phases)
    total_high = sum(p["high_min"] for p in work_phases)
    lines.append(f"📊 **요약**: 총 작업 {total_work}분 (고활성 {total_high}분)")

    html_lines = []
    for line in lines:
        converted = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", line)
        html_lines.append(converted)

    body = "<br>".join(html_lines)
    return (
        f'<div style="background:#F8FAFC;border-radius:10px;padding:1rem 1.2rem;'
        f'border-left:4px solid {Color.SECONDARY};line-height:1.9;font-size:0.88rem;">'
        f'{body}</div>'
    )


def _render_time_breakdown_detail(tb: dict) -> None:
    def safe_int(val):
        try:
            return int(val or 0)
        except (TypeError, ValueError):
            return 0
    
    total = safe_int(tb.get("total", 0)) or 1
    high_work = safe_int(tb.get('high_work', 0))
    low_work = safe_int(tb.get('low_work', 0))
    standby = safe_int(tb.get('standby', 0))
    transit = safe_int(tb.get('transit', 0))
    rest_facility = safe_int(tb.get('rest_facility', 0))
    off_duty = safe_int(tb.get('off_duty', 0))
    
    st.markdown(f"""
| 카테고리 | 시간 | 비율 |
|----------|------|------|
| 고활성 작업 | {high_work}분 | {high_work/total:.0%} |
| 저활성 작업 | {low_work}분 | {low_work/total:.0%} |
| 현장 대기 | {standby}분 | {standby/total:.0%} |
| 이동 | {transit}분 | {transit/total:.0%} |
| 휴게실 이용 | {rest_facility}분 | {rest_facility/total:.0%} |
| 비근무 | {off_duty}분 | {off_duty/total:.0%} |
    """)


def _render_productivity_metrics(prod: dict, wdf: pd.DataFrame) -> None:
    try:
        ewi = float(prod.get("effective_work_intensity", 0) or 0)
    except (TypeError, ValueError):
        ewi = 0.0
    try:
        standby_loss = float(prod.get("standby_loss", 0) or 0)
    except (TypeError, ValueError):
        standby_loss = 0.0
    try:
        work_cont = float(prod.get("work_continuity", 0) or 0)
    except (TypeError, ValueError):
        work_cont = 0.0

    st.markdown(f"""
**유효작업집중도 (EWI)**: `{ewi:.1%}`
```
EWI = (고활성 × 1.0 + 저활성 × 0.5) / 현장 체류
```

**Standby Loss**: `{standby_loss:.0f}분`
```
Standby Loss = 작업공간 내 활성비율 < 15% 구간의 총 시간
```

**Work Continuity**: `{work_cont:.1%}`
```
Work Continuity = 최장 연속 작업 시간 / 전체 작업 시간
```
    """)


def _render_safety_metrics(safety: dict, wdf: pd.DataFrame) -> None:
    try:
        fatigue = float(safety.get("fatigue_risk", 0) or 0)
    except (TypeError, ValueError):
        fatigue = 0.0
    try:
        alone = float(safety.get("alone_risk_ratio", 0) or 0)
    except (TypeError, ValueError):
        alone = 0.0
    try:
        contextual = float(safety.get("contextual_risk", 0) or 0)
    except (TypeError, ValueError):
        contextual = 0.0

    st.markdown(f"""
**피로 위험도**: `{fatigue:.2f}`
```
피로 위험 = Σ(연속 작업 120분 초과 구간) × 가중치
```
1.0 이상 = 고위험, 0.5~1.0 = 주의

**단독작업 비율**: `{alone:.1%}`
```
단독작업 비율 = 반경 내 동료 없는 시간 / 전체 작업 시간
```

**Contextual Risk**: `{contextual:.2f}`
```
Contextual Risk = Personal Risk × hazard_weight × Dynamic Pressure
```
hazard_weight는 공간 고유 위험도 (WORK_HAZARD=1.0, REST=0.0)
    """)
