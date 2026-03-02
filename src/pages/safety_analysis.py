"""
안전성 분석 페이지.

SOIF Layer 5 기반 안전성 지표 분석:
- CRE (Combined Risk Exposure): 복합위험노출도
- 피로 위험, 단독작업 위험
- 공간별 위험 가중치
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from src.data.schema import RawColumns, ProcessedColumns
from src.metrics.soif import calc_cre, calc_cre_by_worker, calc_soif_summary
from src.metrics.safety import calc_safety_summary
from src.utils.theme import Color, apply_theme


# 공간별 고유 위험 가중치 (Static Space Risk)
_STATIC_RISK_TABLE = [
    ("CONFINED_SPACE", "밀폐공간", 2.0, "탱크, 맨홀 등 산소결핍·질식 위험"),
    ("WORK_HAZARD", "고위험 작업장", 1.5, "고소작업, 중장비 구역"),
    ("WORK", "작업공간", 1.2, "일반 작업장 (FAB, CUB 등)"),
    ("OUTDOOR", "옥외", 1.1, "야적장, 공사현장"),
    ("INDOOR", "실내", 1.0, "일반 실내 공간"),
    ("GATE", "게이트", 0.8, "출입구, 통과 구역"),
    ("REST", "휴게시설", 0.3, "휴게실, 식당, 탈의실"),
    ("HELMET_RACK", "헬멧 거치대", 0.2, "보호구 보관 구역"),
]


def render(df: pd.DataFrame, selected_date: str = "", get_analytics=None) -> None:
    """안전성 분석 메인 렌더 함수."""
    if df.empty:
        st.warning("데이터가 없습니다.")
        return

    date_str = selected_date or (df[ProcessedColumns.DATE].iloc[0] if ProcessedColumns.DATE in df.columns else "")

    st.markdown(f"""
    <div style="margin-bottom:1.2rem;">
        <h1>🛡️ 안전성 분석</h1>
        <p style="color:{Color.TEXT_MUTED};">SOIF Layer 5: Combined Risk Exposure (CRE) 기반 안전성 평가</p>
    </div>""", unsafe_allow_html=True)

    # SOIF 요약: 캐시 우선, 없으면 계산
    soif = {}
    if callable(get_analytics):
        try:
            soif = get_analytics(selected_date or date_str, df)
        except Exception:
            pass
    if not soif or "avg_cre" not in soif:
        try:
            soif = calc_soif_summary(df)
        except Exception:
            pass

    # 안전한 숫자 변환 헬퍼
    def safe_float(val, default=0.0):
        try:
            return float(val) if val is not None else default
        except (TypeError, ValueError):
            return default
    
    avg_cre = safe_float(soif.get("avg_cre", 0))
    max_cre = safe_float(soif.get("max_cre", 0))
    cre_by_worker = soif.get("cre_by_worker", pd.DataFrame())

    # 위험 레벨 판정
    def get_risk_level(cre: float) -> tuple[str, str, str]:
        if cre >= 1.0:
            return "HIGH", "#E74C3C", "즉시 확인 필요"
        elif cre >= 0.5:
            return "MEDIUM", "#F39C12", "주의 필요"
        else:
            return "LOW", "#27AE60", "정상"

    level, color, desc = get_risk_level(avg_cre)
    max_level, max_color, max_desc = get_risk_level(max_cre)

    # 핵심 KPI
    st.markdown("### 🚨 현장 안전 Overview")
    
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f"""
        <div style="background:#FFFFFF;border:1px solid #E8ECF4;border-radius:12px;padding:1rem;text-align:center;">
            <div style="font-size:0.8rem;color:{Color.TEXT_MUTED};">평균 CRE</div>
            <div style="font-size:2rem;font-weight:700;color:{color};">{avg_cre:.2f}</div>
            <div style="font-size:0.75rem;color:{color};">{level} - {desc}</div>
        </div>""", unsafe_allow_html=True)
    with k2:
        st.markdown(f"""
        <div style="background:#FFFFFF;border:1px solid #E8ECF4;border-radius:12px;padding:1rem;text-align:center;">
            <div style="font-size:0.8rem;color:{Color.TEXT_MUTED};">최대 CRE</div>
            <div style="font-size:2rem;font-weight:700;color:{max_color};">{max_cre:.2f}</div>
            <div style="font-size:0.75rem;color:{max_color};">{max_level}</div>
        </div>""", unsafe_allow_html=True)
    with k3:
        high_risk_count = len(cre_by_worker[cre_by_worker["cre"] >= 1.0]) if not cre_by_worker.empty else 0
        st.metric("⚠️ 고위험 작업자", f"{high_risk_count}명", help="CRE ≥ 1.0인 작업자 수")
    with k4:
        med_risk_count = len(cre_by_worker[(cre_by_worker["cre"] >= 0.5) & (cre_by_worker["cre"] < 1.0)]) if not cre_by_worker.empty else 0
        st.metric("⚡ 주의 작업자", f"{med_risk_count}명", help="CRE 0.5~1.0인 작업자 수")

    st.markdown("<br>", unsafe_allow_html=True)

    # 로직 설명
    with st.expander("📖 CRE 계산 로직 상세", expanded=False):
        st.markdown("""
### CRE (Combined Risk Exposure) — 복합위험노출도

**정의**: 작업자 개인 상태(피로, 고립)와 공간의 물리적 위험도를 결합한 상황적 안전 지표입니다.

**계산식**:
```
CRE = Personal Risk × Static Space Risk × Dynamic Pressure
```

**구성 요소**:

| 요소 | 설명 | 계산 방식 |
|------|------|-----------|
| **Personal Risk** | 개인 위험도 | 피로(연속작업 120분+) + 고립(반경 내 동료 없음) |
| **Static Space Risk** | 공간 고유 위험 | 공간 유형별 가중치 (아래 표 참조) |
| **Dynamic Pressure** | 동적 부하 | 해당 공간의 혼잡도 |

**해석 기준**:
- **≥ 1.0**: 고위험 (즉시 확인 필요)
- **0.5 ~ 1.0**: 중위험 (주의 필요)
- **< 0.5**: 저위험 (정상)

**핵심 로직**:
- 휴게실에서의 정지 → **정상** (휴식)
- 밀폐공간에서의 정지 → **비상 상황** (Abnormal Stop)
- 같은 "정지"도 공간 맥락에 따라 해석이 달라집니다.
        """)

    # 공간별 위험 가중치 표
    with st.expander("📋 공간별 위험 가중치 (Static Space Risk)", expanded=False):
        risk_df = pd.DataFrame(_STATIC_RISK_TABLE, columns=["코드", "분류", "가중치", "설명"])
        st.table(risk_df)
        st.caption("밀폐공간(2.0)에서의 CRE는 휴게실(0.3)에서의 약 7배로 계산됩니다.")

    # 작업자별 CRE 차트
    col1, col2 = st.columns([1.2, 1], gap="medium")

    with col1:
        st.markdown("**👷 작업자별 CRE 분포**")
        
        if not cre_by_worker.empty:
            cre_sorted = cre_by_worker.sort_values("cre", ascending=True).copy()
            # CRE 값을 숫자로 변환
            cre_sorted["cre"] = cre_sorted["cre"].apply(lambda x: safe_float(x))
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=cre_sorted["worker_key"],
                x=cre_sorted["cre"],
                orientation="h",
                marker_color=[
                    "#E74C3C" if c >= 1.0 else ("#F39C12" if c >= 0.5 else "#27AE60")
                    for c in cre_sorted["cre"]
                ],
                text=[f"{c:.2f}" for c in cre_sorted["cre"]],
                textposition="outside",
            ))
            fig.add_vline(x=1.0, line_dash="dash", line_color="#E74C3C", annotation_text="고위험 (1.0)")
            fig.add_vline(x=0.5, line_dash="dot", line_color="#F39C12", annotation_text="주의 (0.5)")
            fig = apply_theme(fig, "", height=350)
            fig.update_layout(xaxis_title="CRE", yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("작업자별 CRE 데이터 없음")

    with col2:
        st.markdown("**📊 CRE 구성 요소 분해**")
        
        if not cre_by_worker.empty and "personal_risk" in cre_by_worker.columns:
            avg_personal = cre_by_worker["personal_risk"].mean()
            avg_static = cre_by_worker["static_risk"].mean() if "static_risk" in cre_by_worker.columns else 1.0
            avg_dynamic = cre_by_worker["dynamic_pressure"].mean() if "dynamic_pressure" in cre_by_worker.columns else 1.0

            component_df = pd.DataFrame({
                "요소": ["개인 위험", "공간 위험", "동적 부하"],
                "평균값": [avg_personal, avg_static, avg_dynamic],
            })

            fig = px.bar(
                component_df,
                x="요소",
                y="평균값",
                color="요소",
                color_discrete_sequence=["#E74C3C", "#3498DB", "#9B59B6"],
            )
            fig = apply_theme(fig, "CRE 구성 요소 평균", height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("CRE 구성 요소 데이터 없음")

    st.markdown("<br>", unsafe_allow_html=True)

    # 피로/단독작업 상세
    st.markdown("### 🔍 위험 요소 상세")
    
    tab1, tab2 = st.tabs(["😓 피로 위험", "🚶 단독작업 위험"])

    with tab1:
        with st.expander("📖 피로 위험 계산 로직", expanded=False):
            st.markdown("""
**피로 위험도 (Fatigue Risk)**

```
피로 위험 = Σ(연속 작업 120분 초과 구간) × 가중치
```

- **120분 이상** 연속 작업 시 피로 위험 증가
- 휴식 없이 장시간 작업 시 사고 위험 상승
- **해석**: 1.0 이상 = 고위험, 0.5~1.0 = 주의
            """)
        
        if not cre_by_worker.empty and "fatigue_score" in cre_by_worker.columns:
            fatigue_df = cre_by_worker[["worker_key", "fatigue_score"]].sort_values("fatigue_score", ascending=False)
            fatigue_df.columns = ["작업자", "피로 점수"]
            st.dataframe(fatigue_df, use_container_width=True, hide_index=True)
        else:
            st.info("피로 데이터 없음")

    with tab2:
        with st.expander("📖 단독작업 위험 계산 로직", expanded=False):
            st.markdown("""
**단독작업 비율 (Alone Risk Ratio)**

```
단독작업 비율 = 반경 내 동료 없는 시간 / 전체 작업 시간
```

- 작업 중 **반경 내 동료가 없는 시간**의 비율
- 단독작업 시 사고 발생 시 즉각적인 도움 받기 어려움
- **해석**: 50% 이상 = 주의 필요
            """)
        
        if not cre_by_worker.empty and "alone_score" in cre_by_worker.columns:
            alone_df = cre_by_worker[["worker_key", "alone_score"]].sort_values("alone_score", ascending=False)
            alone_df["alone_score"] = alone_df["alone_score"].apply(lambda x: f"{safe_float(x):.1%}")
            alone_df.columns = ["작업자", "단독작업 비율"]
            st.dataframe(alone_df, use_container_width=True, hide_index=True)
        else:
            st.info("단독작업 데이터 없음")

    # 향후 계획
    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("🔮 향후 안전 분석 확장 계획", expanded=False):
        st.markdown("""
### 운영 표준(Operational Standard) 로드맵

1. **장소별 위험 가중치 정교화**
   - SK에코플랜트 안전 기준과 연동
   - 고위험 구역의 위험 가중치 최적화
   - 상황별 맞춤형 알람 체계 구축

2. **실시간 알림 시스템**
   - CRE ≥ 1.0 즉시 알림
   - 밀폐공간 Abnormal Stop 감지
   - 피로 누적 경고

3. **패턴 기반 예측**
   - 과거 데이터 기반 위험 시간대 예측
   - 작업자별 피로 패턴 분석
   - 선제적 휴식 권고
        """)
