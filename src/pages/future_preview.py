"""
🔮 확장 가능성 페이지.

고객 질문: Q4: "앞으로 이 데이터로 뭘 더 할 수 있나요?"

화면 목적:
  - 현재 구현된 것과 향후 가능한 기능을 명확히 구분
  - Phase 2, 3는 Mock/Placeholder로 시각화
  - Mock임을 명시하여 신뢰도 훼손 없이 비전 전달

섹션:
  1. 현재 구현 (√)
  2. Phase 2: 공간 인텔리전스 (Mock)
  3. Phase 3: 예측 및 실시간 (Mock)
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.data.schema import ProcessedColumns, RawColumns
from src.utils.theme import Color, apply_theme


def render(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        st.warning("데이터 없음")
        return

    st.markdown("""
    <div class="kpi-banner">
        <h1>🔮 확장 가능성</h1>
        <p>Q4: 앞으로 이 데이터로 뭘 더 할 수 있나요?</p>
    </div>""", unsafe_allow_html=True)

    _render_current_capabilities()

    st.markdown("<br>", unsafe_allow_html=True)

    _render_phase2_preview(df)

    st.markdown("<br>", unsafe_allow_html=True)

    _render_phase3_preview()


def _render_current_capabilities() -> None:
    st.markdown("### ✅ 현재 구현 완료")

    capabilities = [
        ("✓", "작업자별 1분 단위 Journey 보정", "DBSCAN 기반 노이즈 제거, 헬멧 거치 통일"),
        ("✓", "space_function 기반 맥락 해석", "9개 공간 기능 분류 (WORK, REST, TRANSIT_GATE 등)"),
        ("✓", "6-state 생산성 분류", "고활성/저활성/대기/이동/휴게/비근무"),
        ("✓", "EWI (유효작업집중도)", "(고활성×1.0 + 저활성×0.5) / 현장 체류"),
        ("✓", "Contextual Risk", "Personal Risk × hazard_weight × Dynamic Pressure"),
        ("✓", "Zone-Time Table", "구역별 시간대 인원 밀도 히트맵"),
        ("✓", "Flow Edge Table", "구역 간 이동 경로 분석"),
    ]

    cols = st.columns(2)
    for i, (icon, title, desc) in enumerate(capabilities):
        with cols[i % 2]:
            st.markdown(f"""
            <div style="background:#D4EDDA;border-radius:8px;padding:0.8rem 1rem;margin:4px 0;">
                <div style="font-weight:600;color:#155724;">{icon} {title}</div>
                <div style="font-size:0.82rem;color:#1E5631;margin-top:2px;">{desc}</div>
            </div>""", unsafe_allow_html=True)


def _render_phase2_preview(df: pd.DataFrame) -> None:
    st.markdown("### 🔮 Phase 2: 공간 인텔리전스")
    st.markdown("""
    <div style="background:#FFF3CD;border:1px solid #F5A623;border-radius:8px;
                padding:0.6rem 1rem;margin-bottom:1rem;font-size:0.85rem;color:#856404;">
        ⚠️ <b>Mock 데이터 기반 Preview</b> — 다수 작업자 데이터 확보 후 실제 구현 예정
    </div>""", unsafe_allow_html=True)

    st.markdown("**🔥 구역별 혼잡도 히트맵 — 시간대별 인원 집중 분석**")
    st.caption("특정 시간대 특정 구역에 인원이 집중되면 병목 발생 → 시차 배치 또는 동선 분산 권고")

    mock_zones = ["FAB 1F", "FAB 2F", "WWT B1F", "CUB 1F", "게이트A", "휴게실"]
    mock_hours = list(range(7, 19))
    mock_data = [
        [2, 3, 5, 8, 12, 15, 18, 12, 8, 5, 3, 2],
        [1, 2, 3, 5, 8, 10, 12, 8, 5, 3, 2, 1],
        [0, 1, 2, 3, 5, 6, 8, 6, 4, 2, 1, 0],
        [1, 2, 4, 6, 8, 10, 12, 10, 8, 6, 4, 2],
        [5, 8, 12, 3, 2, 1, 1, 2, 3, 10, 15, 8],
        [0, 0, 1, 5, 15, 3, 1, 0, 0, 3, 10, 5],
    ]

    fig = go.Figure(go.Heatmap(
        z=mock_data,
        x=[f"{h:02d}시" for h in mock_hours],
        y=mock_zones,
        colorscale=[
            [0, "#F8FAFC"],
            [0.3, "#5DADE2"],
            [0.6, "#F5A623"],
            [1.0, "#E74C3C"],
        ],
        hovertemplate="구역: %{y}<br>시간: %{x}<br>인원: %{z}명<extra></extra>",
        colorbar=dict(title="인원", thickness=12),
    ))
    fig = apply_theme(fig, "", height=280)
    fig.update_layout(
        margin=dict(l=0, r=10, t=20, b=0),
        annotations=[
            dict(
                x="12시", y="FAB 1F",
                text="🔥 병목",
                showarrow=True,
                arrowhead=2,
                font=dict(color="#E74C3C", size=10),
            )
        ],
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**🚧 게이트 병목 시뮬레이션**")

    st.markdown(f"""
    <div style="background:#F8F9FB;border:1px solid #E0E7F0;border-radius:10px;
                padding:1rem 1.2rem;margin-top:0.5rem;">
        <div style="font-weight:600;color:{Color.PRIMARY};margin-bottom:8px;">
            게이트 병목 탐지 로직
        </div>
        <div style="font-size:0.85rem;color:#4A5A78;line-height:1.7;">
            <b>데이터</b>: 작업자별 <code>TRANSIT_GATE</code> 체류 시간 + <code>dwell_exceeded</code> 플래그<br>
            <b>조건</b>: 게이트 dwell > 5분 → <code>gate_congestion</code> 이벤트 생성<br>
            <b>집계</b>: 시간대별 게이트 congestion 빈도 → 출퇴근 피크 식별<br>
            <b>활용</b>: 타각기 추가 배치 또는 시차 출근 권고
        </div>
    </div>""", unsafe_allow_html=True)

    mock_gate_hours = ["07시", "08시", "09시", "12시", "13시", "17시", "18시"]
    mock_gate_wait = [8, 15, 12, 10, 8, 18, 12]

    fig2 = go.Figure(go.Bar(
        x=mock_gate_hours,
        y=mock_gate_wait,
        marker_color=[
            Color.DANGER if w >= 15 else Color.WARNING if w >= 10 else Color.SAFE
            for w in mock_gate_wait
        ],
        text=[f"{w}분" for w in mock_gate_wait],
        textposition="outside",
    ))
    fig2 = apply_theme(fig2, "시간대별 평균 게이트 대기 시간 (Mock)", height=250)
    fig2.update_yaxes(title_text="대기 시간 (분)")
    st.plotly_chart(fig2, use_container_width=True)


def _render_phase3_preview() -> None:
    st.markdown("### 🔮 Phase 3: 예측 및 실시간")
    st.markdown("""
    <div style="background:#FFF3CD;border:1px solid #F5A623;border-radius:8px;
                padding:0.6rem 1rem;margin-bottom:1rem;font-size:0.85rem;color:#856404;">
        ⚠️ <b>개념도 (실시간 연동 및 누적 데이터 필요)</b>
    </div>""", unsafe_allow_html=True)

    st.markdown("**🔮 미래 공간 상태 예측**")

    st.markdown(f"""
    <div style="background:#F0F8FF;border:1px solid {Color.SECONDARY};border-radius:10px;
                padding:1rem 1.2rem;margin:0.5rem 0;">
        <div style="font-size:0.9rem;color:{Color.PRIMARY};font-weight:600;">예측 시나리오 예시</div>
        <div style="font-size:0.95rem;color:#1B2A4A;margin-top:8px;line-height:1.8;">
            🔵 "15분 후 <b>FAB 게이트 병목 확률 78%</b>"<br>
            🟠 "점심 직전 <b>REST 구역 과밀 예상</b> (정원 대비 120%)"<br>
            🔴 "14:30 <b>WWT B1F 밀폐구역 피로 위험</b> — 홍길동 연속 작업 2시간 초과"
        </div>
    </div>""", unsafe_allow_html=True)

    st.markdown("**🚨 실시간 알림 시나리오**")

    alerts = [
        ("🔴", "즉시 확인", "밀폐공간 비정상 정지", "FAB B2 맨홀구역 — 홍길동 비정상 정지 22분", Color.DANGER),
        ("🟠", "주의 필요", "피로 위험", "김철수 — 연속 작업 140분 (기준: 120분)", Color.WARNING),
        ("🟠", "주의 필요", "단독 작업", "이영희 — WWT 1F 혼자 45분째", Color.WARNING),
        ("🟢", "해소됨", "게이트 병목 해소", "08:30 정문게이트 — 대기 평균 3분으로 감소", Color.SAFE),
    ]

    for icon, level, title, detail, color in alerts:
        st.markdown(f"""
        <div style="background:{color}15;border-left:4px solid {color};border-radius:6px;
                    padding:0.7rem 1rem;margin:6px 0;">
            <div style="font-size:0.78rem;color:{color};font-weight:600;margin-bottom:2px;">
                {icon} {level}
            </div>
            <div style="font-weight:600;color:#1B2A4A;">{title}</div>
            <div style="font-size:0.85rem;color:#4A5A78;margin-top:2px;">{detail}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(f"""
    <div style="background:#F8F9FB;border:1px solid #E0E7F0;border-radius:10px;
                padding:1rem 1.2rem;">
        <div style="font-weight:600;color:{Color.PRIMARY};margin-bottom:8px;">
            Phase 3 구현 요건
        </div>
        <div style="font-size:0.85rem;color:#4A5A78;line-height:1.7;">
            <b>1. 누적 데이터</b>: 최소 2주 이상의 과거 패턴 데이터<br>
            <b>2. 실시간 연동</b>: T-Ward BLE 신호 실시간 수신 (현재: 배치 처리)<br>
            <b>3. 알림 채널</b>: 담당자 모바일 앱 또는 Slack/Teams 연동<br>
            <b>4. 예측 모델</b>: 시간대별 구역 밀집도 패턴 학습 (시계열 모델)
        </div>
    </div>""", unsafe_allow_html=True)

    with st.expander("📖 SOIF 지표 정의 (참고)", expanded=False):
        st.markdown("""
| 지표 | 산출 로직 | 해석 |
|------|----------|------|
| **EWI** (유효 작업 집중도) | (고활성×1.0 + 저활성×0.5) / 현장 체류시간 | 높을수록 생산적. ≥0.4 양호 |
| **OFI** (운영 마찰 지수) | (대기 + 초과이동) / 현장 체류시간 | 낮을수록 효율적. ≤0.1 양호 |
| **CRE** (복합 위험 노출도) | 개인위험 × 공간위험 × 동적부하 | ≥1.0 위험. 피로·고립·밀집 복합 반영 |
| **BS** (병목 점수) | 흐름불균형(60%) + 대기부하(40%) | 유입↑ 유출↓ 구간 식별 |
| **Zone Utilization** | 생산적 시간 / 총 점유 시간 | 작업 구역이 의도대로 활용되는 비율 |
        """)
