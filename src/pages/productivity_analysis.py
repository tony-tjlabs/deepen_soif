"""
생산성 분석 페이지.

SOIF Layer 5 기반 생산성 지표 분석:
- EWI (Effective Work Intensity): 유효작업집중도
- 시간 배분 분석: 6대 운영 상태별 시간
- 업체별/작업자별 생산성 비교
- v6.4: 출퇴근 기반 개인별 상세 시간 분석
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from src.data.schema import RawColumns, ProcessedColumns
from src.metrics.soif import (
    calc_ewi,
    calc_ewi_by_worker,
    calc_ewi_by_company,
    calc_soif_summary,
    detect_work_shift,
)
from src.utils.theme import Color, apply_theme
from src.utils.time_utils import format_duration


def safe_float(val, default=0.0):
    """안전한 숫자 변환."""
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def render(df: pd.DataFrame, selected_date: str = "", get_analytics=None) -> None:
    """생산성 분석 메인 렌더 함수."""
    if df.empty:
        st.warning("데이터가 없습니다.")
        return

    date_str = selected_date or (df[ProcessedColumns.DATE].iloc[0] if ProcessedColumns.DATE in df.columns else "")

    st.markdown(f"""
    <div style="margin-bottom:1.2rem;">
        <h1>📈 생산성 분석</h1>
        <p style="color:{Color.TEXT_MUTED};">SOIF Layer 5: Effective Work Intensity (EWI) 기반 생산성 평가</p>
    </div>""", unsafe_allow_html=True)

    # SOIF 요약: 캐시 우선, 없으면 계산
    soif = {}
    if callable(get_analytics):
        try:
            soif = get_analytics(selected_date or date_str, df)
        except Exception:
            pass
    if not soif or "site_ewi" not in soif:
        try:
            soif = calc_soif_summary(df)
        except Exception:
            pass

    site_ewi_dict = soif.get("site_ewi", {})
    ewi = safe_float(site_ewi_dict.get("ewi", 0)) if isinstance(site_ewi_dict, dict) else 0.0
    high_work = safe_float(site_ewi_dict.get("high_work_min", 0)) if isinstance(site_ewi_dict, dict) else 0.0
    low_work = safe_float(site_ewi_dict.get("low_work_min", 0)) if isinstance(site_ewi_dict, dict) else 0.0
    standby = safe_float(site_ewi_dict.get("standby_min", 0)) if isinstance(site_ewi_dict, dict) else 0.0
    transit = safe_float(site_ewi_dict.get("transit_min", 0)) if isinstance(site_ewi_dict, dict) else 0.0
    rest = safe_float(site_ewi_dict.get("rest_min", 0)) if isinstance(site_ewi_dict, dict) else 0.0
    off_duty = safe_float(site_ewi_dict.get("off_duty_min", 0)) if isinstance(site_ewi_dict, dict) else 0.0
    onsite = safe_float(site_ewi_dict.get("onsite_min", 0)) if isinstance(site_ewi_dict, dict) else 0.0

    # 핵심 KPI
    st.markdown("### 📊 현장 생산성 Overview")
    
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ewi_color = "#27AE60" if ewi >= 0.5 else ("#F39C12" if ewi >= 0.3 else "#E74C3C")
        st.markdown(f"""
        <div style="background:#FFFFFF;border:1px solid #E8ECF4;border-radius:12px;padding:1rem;text-align:center;">
            <div style="font-size:0.8rem;color:{Color.TEXT_MUTED};">EWI (유효작업집중도)</div>
            <div style="font-size:2rem;font-weight:700;color:{ewi_color};">{ewi:.0%}</div>
        </div>""", unsafe_allow_html=True)
    with k2:
        st.metric("⚡ 고활성 작업", format_duration(high_work), help="활성비율 ≥60%인 시간")
    with k3:
        st.metric("🔧 저활성 작업", format_duration(low_work), help="활성비율 15~60%인 시간")
    with k4:
        st.metric("⏳ 현장 대기", format_duration(standby), help="작업공간 내 활성비율 <15%인 시간")

    st.markdown("<br>", unsafe_allow_html=True)

    # 로직 설명
    with st.expander("📖 EWI 계산 로직 상세", expanded=False):
        st.markdown("""
### EWI (Effective Work Intensity) — 유효작업집중도

**정의**: 출근~퇴근 사이 전체 시간 중 고활성/저활성 작업이 차지하는 비중입니다.

**계산식** (v6.5):
```
EWI = (고활성 작업 × 1.0 + 저활성 작업 × 0.5) / (퇴근시간 - 출근시간)
```

**★ 핵심 — 음영지역 고려**:
- **분모**: 출근~퇴근 실제 시간 차이 (기록된 데이터 수가 아님)
- 예) 출근 07:30, 퇴근 17:30 → 분모 = 600분
- 음영지역(점심시간, 통신 음영 등)으로 400분만 기록되어도 분모는 600분

**운영 상태 분류 기준**:

| 상태 | 판별 조건 | 설명 |
|------|-----------|------|
| **고활성 작업** | 활성비율 ≥ 60% | 실제 공정이 진행되는 고밀도 작업 |
| **저활성 작업** | 활성비율 15~60% | 감독, 측량, 도면 검토 등 보조 업무 |
| **현장 대기** | 활성비율 < 15%, 작업공간 | 자재/지시 대기 (생산성 Loss) |
| **이동** | 게이트 통과 또는 구역 이동 | 구역 간 이동 시간 |
| **휴게** | 휴게시설 내 체류 | 재충전 시간 |
| **음영지역** | 신호 미수집 구간 | 점심시간, 통신 음영 등 |

**해석 기준**:
- **≥ 50%**: 높은 집중도 (우수)
- **30~50%**: 보통 (개선 여지)
- **< 30%**: 대기, 이동, 또는 음영지역 과다 (점검 필요)

**EWI 개선 포인트**:
1. 현장 대기 시간 감소 → 자재/장비 적시 공급
2. 이동 시간 최적화 → 동선 재설계
3. 음영지역 감소 → 통신 인프라 개선
        """)

    # 시간 배분 파이 차트
    col1, col2 = st.columns([1, 1.2], gap="medium")

    with col1:
        st.markdown("**⏱️ 시간 배분 (현장 전체)**")
        
        time_data = {
            "상태": ["고활성 작업", "저활성 작업", "현장 대기", "이동", "휴게", "비근무"],
            "시간(분)": [high_work, low_work, standby, transit, rest, off_duty],
        }
        time_df = pd.DataFrame(time_data)
        time_df = time_df[time_df["시간(분)"] > 0]

        if not time_df.empty:
            colors = ["#27AE60", "#82E0AA", "#F39C12", "#3498DB", "#9B59B6", "#95A5A6"]
            fig = px.pie(
                time_df,
                values="시간(분)",
                names="상태",
                color_discrete_sequence=colors,
                hole=0.4,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig = apply_theme(fig, "", height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("시간 데이터 없음")

    with col2:
        st.markdown("**👷 작업자별 EWI 비교**")
        
        ewi_by_worker = soif.get("ewi_by_worker", pd.DataFrame())
        if not ewi_by_worker.empty:
            ewi_by_worker = ewi_by_worker.sort_values("ewi", ascending=True).copy()
            # EWI 값을 숫자로 변환
            ewi_by_worker["ewi"] = ewi_by_worker["ewi"].apply(lambda x: safe_float(x))
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=ewi_by_worker["worker_key"],
                x=ewi_by_worker["ewi"],
                orientation="h",
                marker_color=[
                    "#27AE60" if e >= 0.5 else ("#F39C12" if e >= 0.3 else "#E74C3C")
                    for e in ewi_by_worker["ewi"]
                ],
                text=[f"{e:.0%}" for e in ewi_by_worker["ewi"]],
                textposition="outside",
            ))
            fig.add_vline(x=0.5, line_dash="dash", line_color="#27AE60", annotation_text="목표 50%")
            fig = apply_theme(fig, "", height=300)
            fig.update_layout(xaxis_title="EWI", yaxis_title="", xaxis_tickformat=".0%")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("작업자별 EWI 데이터 없음")

    st.markdown("<br>", unsafe_allow_html=True)

    # 업체별 비교
    st.markdown("### 🏢 업체별 생산성 비교")
    
    ewi_by_company = soif.get("ewi_by_company", pd.DataFrame())
    if not ewi_by_company.empty:
        # 숫자 타입 변환
        ewi_by_company = ewi_by_company.copy()
        ewi_by_company["ewi_avg"] = ewi_by_company["ewi_avg"].apply(lambda x: safe_float(x))
        ewi_by_company["ewi_max"] = ewi_by_company["ewi_max"].apply(lambda x: safe_float(x))
        ewi_by_company["ewi_min"] = ewi_by_company["ewi_min"].apply(lambda x: safe_float(x))
        
        col_a, col_b = st.columns([1.5, 1])
        
        with col_a:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=ewi_by_company["company"],
                y=ewi_by_company["ewi_avg"],
                name="평균 EWI",
                marker_color=Color.PRIMARY,
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=ewi_by_company["ewi_max"] - ewi_by_company["ewi_avg"],
                    arrayminus=ewi_by_company["ewi_avg"] - ewi_by_company["ewi_min"],
                ),
                text=[f"{e:.0%}" for e in ewi_by_company["ewi_avg"]],
                textposition="outside",
            ))
            fig.add_hline(y=0.5, line_dash="dash", line_color="#27AE60", annotation_text="목표 50%")
            fig = apply_theme(fig, "업체별 평균 EWI (오차막대: min~max)", height=300)
            fig.update_layout(yaxis_tickformat=".0%", yaxis_title="EWI")
            st.plotly_chart(fig, use_container_width=True)
        
        with col_b:
            st.markdown("**📋 업체별 상세**")
            # 필요한 컬럼만 선택
            display_df = ewi_by_company[["company", "ewi_avg", "ewi_max", "ewi_min"]].copy()
            display_df["ewi_avg"] = display_df["ewi_avg"].apply(lambda x: f"{safe_float(x):.0%}")
            display_df["ewi_max"] = display_df["ewi_max"].apply(lambda x: f"{safe_float(x):.0%}")
            display_df["ewi_min"] = display_df["ewi_min"].apply(lambda x: f"{safe_float(x):.0%}")
            display_df.columns = ["업체", "평균 EWI", "최대", "최소"]
            st.table(display_df)
    else:
        st.info("업체별 EWI 데이터 없음")

    # ─── 작업자별 상세 시간 분석 ─────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 👤 작업자별 상세 시간 분석")
    
    workers = df[ProcessedColumns.WORKER_KEY].unique().tolist()
    worker_names = {
        wk: df[df[ProcessedColumns.WORKER_KEY] == wk][RawColumns.WORKER].iloc[0]
        for wk in workers
    }
    
    selected_worker = st.selectbox(
        "작업자 선택",
        options=workers,
        format_func=lambda x: worker_names.get(x, x),
        key="prod_worker_select",
    )
    
    if selected_worker:
        wdf = df[df[ProcessedColumns.WORKER_KEY] == selected_worker].copy()
        worker_name = worker_names.get(selected_worker, selected_worker)
        worker_company = wdf[RawColumns.COMPANY].iloc[0] if RawColumns.COMPANY in wdf.columns else ""
        
        # EWI 계산 (출퇴근 기반)
        ewi_result = calc_ewi(df, worker_key=selected_worker)
        
        # 출퇴근 시간 포맷
        clock_in = ewi_result.get("clock_in_time")
        clock_out = ewi_result.get("clock_out_time")
        clock_in_str = clock_in.strftime('%H:%M') if pd.notna(clock_in) else "N/A"
        clock_out_str = clock_out.strftime('%H:%M') if pd.notna(clock_out) else "N/A"
        
        work_dur = safe_float(ewi_result.get("work_duration_min", 0))
        recorded_min = safe_float(ewi_result.get("recorded_min", 0))
        gap_min = safe_float(ewi_result.get("gap_min", 0))
        eff_dur = safe_float(ewi_result.get("effective_work_min", 0))
        pre_work = safe_float(ewi_result.get("pre_work_min", 0))
        post_work = safe_float(ewi_result.get("post_work_min", 0))
        
        high_w = safe_float(ewi_result.get("high_work_min", 0))
        low_w = safe_float(ewi_result.get("low_work_min", 0))
        standby_w = safe_float(ewi_result.get("standby_min", 0))
        transit_w = safe_float(ewi_result.get("transit_min", 0))
        rest_w = safe_float(ewi_result.get("rest_min", 0))
        off_duty_w = safe_float(ewi_result.get("off_duty_min", 0))
        ewi_w = safe_float(ewi_result.get("ewi", 0))
        
        st.markdown(f"**{worker_name}** ({worker_company})")
        
        # 출퇴근 + EWI 요약
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        with c1:
            st.metric("🕐 출근", clock_in_str)
        with c2:
            st.metric("🕕 퇴근", clock_out_str)
        with c3:
            st.metric("⏱️ 근무 시간", f"{work_dur:.0f}분", help="출근~퇴근 실제 시간 차이")
        with c4:
            st.metric("📡 수집 시간", f"{recorded_min:.0f}분", help="실제 신호가 수집된 시간")
        with c5:
            gap_color = "#E74C3C" if gap_min > 60 else ("#F39C12" if gap_min > 30 else "#27AE60")
            st.metric("🔇 음영지역", f"{gap_min:.0f}분", help="신호 미수집 구간 (점심, 음영 등)")
        with c6:
            ewi_color = "#27AE60" if ewi_w >= 0.5 else ("#F39C12" if ewi_w >= 0.3 else "#E74C3C")
            st.markdown(f"""
            <div style="text-align:center;">
                <div style="font-size:0.75rem;color:{Color.TEXT_MUTED};">EWI</div>
                <div style="font-size:1.5rem;font-weight:700;color:{ewi_color};">{ewi_w:.0%}</div>
            </div>""", unsafe_allow_html=True)
        
        # 음영지역 경고
        if gap_min > 60:
            st.warning(f"⚠️ 음영지역 시간 {gap_min:.0f}분 — 점심시간, 통신 음영지역 등으로 신호가 수집되지 않은 시간입니다.")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # 상세 시간 분석 (절대값 + 비율)
        col_chart, col_table = st.columns([1.2, 1])
        
        with col_chart:
            # 파이 차트 (근무 시간 기준)
            worker_time_data = []
            if high_w > 0:
                worker_time_data.append({"활동": "⚡ 고활성 작업", "시간(분)": high_w, "색상": "#27AE60"})
            if low_w > 0:
                worker_time_data.append({"활동": "🔧 저활성 작업", "시간(분)": low_w, "색상": "#82E0AA"})
            if standby_w > 0:
                worker_time_data.append({"활동": "⏳ 현장 대기", "시간(분)": standby_w, "색상": "#F39C12"})
            if transit_w > 0:
                worker_time_data.append({"활동": "🚶 이동", "시간(분)": transit_w, "색상": "#3498DB"})
            if rest_w > 0:
                worker_time_data.append({"활동": "☕ 휴게", "시간(분)": rest_w, "색상": "#9B59B6"})
            
            if worker_time_data:
                wtdf = pd.DataFrame(worker_time_data)
                fig = px.pie(
                    wtdf,
                    values="시간(분)",
                    names="활동",
                    color_discrete_sequence=[d["색상"] for d in worker_time_data],
                    hole=0.4,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig = apply_theme(fig, "근무 시간 배분", height=280)
                st.plotly_chart(fig, use_container_width=True)
        
        with col_table:
            # 상세 시간표 (절대값 + 비율) — 분모: 근무 시간 전체 (음영지역 포함)
            total_work = max(work_dur, 1)  # 0 나누기 방지, 근무 시간 기준
            
            time_breakdown = [
                {"항목": "⚡ 고활성 작업", "시간": f"{high_w:.0f}분", "비율": f"{high_w/total_work*100:.1f}%"},
                {"항목": "🔧 저활성 작업", "시간": f"{low_w:.0f}분", "비율": f"{low_w/total_work*100:.1f}%"},
                {"항목": "⏳ 현장 대기", "시간": f"{standby_w:.0f}분", "비율": f"{standby_w/total_work*100:.1f}%"},
                {"항목": "🚶 이동", "시간": f"{transit_w:.0f}분", "비율": f"{transit_w/total_work*100:.1f}%"},
                {"항목": "☕ 휴게", "시간": f"{rest_w:.0f}분", "비율": f"{rest_w/total_work*100:.1f}%"},
                {"항목": "🔇 음영지역", "시간": f"{gap_min:.0f}분", "비율": f"{gap_min/total_work*100:.1f}%"},
            ]
            time_breakdown_df = pd.DataFrame(time_breakdown)
            
            st.markdown("**📋 시간 분석표** (근무 시간 기준)")
            st.table(time_breakdown_df)
            
            # 추가 정보
            st.markdown(f"""
            <div style="background:#F8FAFC;border-radius:8px;padding:0.8rem;margin-top:0.5rem;">
                <div style="font-size:0.75rem;color:{Color.TEXT_MUTED};">
                    📌 출근 전: {pre_work:.0f}분 | 퇴근 후: {post_work:.0f}분<br>
                    🔇 음영지역: 점심시간, 통신 음영 등 신호 미수집 구간<br>
                    📊 EWI = (고활성 + 저활성×0.5) / 근무시간({work_dur:.0f}분)
                </div>
            </div>""", unsafe_allow_html=True)
        
        # 시간대별 활동 패턴 (Gantt-like)
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("**📈 시간대별 활동 패턴**")
        
        # 시간대별 집계
        wdf_sorted = wdf.sort_values(RawColumns.TIME).copy()
        wdf_sorted["hour"] = wdf_sorted[ProcessedColumns.HOUR].astype(int)
        wdf_sorted["활성비율"] = wdf_sorted[ProcessedColumns.ACTIVE_RATIO].fillna(0)
        
        hourly_stats = wdf_sorted.groupby("hour").agg({
            "활성비율": "mean",
        }).reset_index()
        
        if not hourly_stats.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=hourly_stats["hour"],
                y=hourly_stats["활성비율"],
                marker_color=[
                    "#27AE60" if r >= 0.6 else ("#82E0AA" if r >= 0.15 else "#F39C12")
                    for r in hourly_stats["활성비율"]
                ],
                text=[f"{r:.0%}" for r in hourly_stats["활성비율"]],
                textposition="outside",
            ))
            fig.add_hline(y=0.6, line_dash="dash", line_color="#27AE60", annotation_text="고활성 기준")
            fig.add_hline(y=0.15, line_dash="dot", line_color="#F39C12", annotation_text="저활성 기준")
            fig = apply_theme(fig, "시간대별 평균 활성비율", height=250)
            fig.update_layout(
                xaxis_title="시간",
                yaxis_title="활성비율",
                yaxis_tickformat=".0%",
                xaxis=dict(tickmode="linear", dtick=1),
            )
            st.plotly_chart(fig, use_container_width=True)

    # 개선 인사이트
    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("💡 생산성 개선 인사이트", expanded=False):
        st.markdown("""
### EWI 차이 요인 분석

**EWI 차이는 개인 역량보다 운영 환경에 기인할 가능성이 높습니다:**

1. **작업 배치**: 고활성 작업 vs 저활성 작업 비율
2. **자재 공급**: 자재 대기로 인한 Standby 시간
3. **동선 설계**: 불필요한 이동 시간
4. **장비 가용성**: 장비 대기 시간

### 데이터 기반 공정 벤치마킹 (향후 계획)

- 공종별 평균 EWI 데이터셋 구축
- 협력사 간 효율성 객관적 비교·관리
- 운영 표준(Operational Standard) 수립
        """)
