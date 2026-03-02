"""
공간 속성/특성 관리 페이지 (Admin 전용).
각 장소의 기능(space_function), 위험도(hazard_weight), 정상 체류시간 등을 확인한다.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.data.schema import ProcessedColumns, RawColumns
from src.utils.theme import GLOBAL_CSS, Color
from src.utils.constants import (
    SpaceFunction,
    SPACE_KEYWORDS,
    SPACE_KEYWORD_PRIORITY,
    HAZARD_WEIGHT_DEFAULT,
    ALONE_RISK_MULTIPLIER,
    DWELL_NORMAL_MAX,
    ABNORMAL_STOP_THRESHOLD,
    SSMP_ZONE_TYPE_MAPPING,
)


def render(df: pd.DataFrame, datafile_root: Path) -> None:
    """공간 속성 페이지 렌더링."""
    st.markdown("""
    <div class="kpi-banner">
        <h1>🗺️ 공간 속성 관리</h1>
        <p>장소별 기능(Space Function), 위험도, 정상 체류시간 설정 확인</p>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs([
        "📋 Space Function 정의",
        "🏗️ 현재 데이터 장소 목록",
        "⚙️ 파라미터 테이블",
    ])

    with tab1:
        _render_space_function_definitions()

    with tab2:
        _render_current_places(df)

    with tab3:
        _render_parameter_tables()


def _render_space_function_definitions() -> None:
    """Space Function 정의 및 키워드 매핑."""
    st.markdown("### Space Function 분류 체계")

    st.markdown("""
    Space Function은 장소의 **기능적 역할**을 정의합니다.  
    같은 건물 내에서도 구역에 따라 다른 기능을 가질 수 있습니다.
    """)

    definitions = [
        (SpaceFunction.WORK, "실내 작업공간", "FAB, CUB, WWT 등 일반 작업 구역", "🔧"),
        (SpaceFunction.WORK_HAZARD, "고위험 작업공간", "밀폐공간, 맨홀, 고소작업 구역", "🚨"),
        (SpaceFunction.TRANSIT_WORK, "실외 공사/이동", "옥외 작업 + 이동이 혼재된 구역", "🚧"),
        (SpaceFunction.TRANSIT_GATE, "출입구/타각기", "정문, 게이트, 타각기 등", "🚪"),
        (SpaceFunction.TRANSIT_CORRIDOR, "이동통로", "복도, 계단, 엘리베이터 홀", "🚶"),
        (SpaceFunction.REST, "휴게시설", "휴게실, 식당, 탈의실, 화장실", "☕"),
        (SpaceFunction.RACK, "헬멧 거치대", "보호구 걸이대", "🪝"),
        (SpaceFunction.OUTDOOR_MISC, "실외 기타", "주차장, 야적장 등", "🅿️"),
        (SpaceFunction.UNKNOWN, "미분류", "분류 키워드가 매칭되지 않은 장소", "❓"),
    ]

    cols = st.columns(3)
    for i, (func, name, desc, icon) in enumerate(definitions):
        with cols[i % 3]:
            hazard = HAZARD_WEIGHT_DEFAULT.get(func, 0)
            dwell = DWELL_NORMAL_MAX.get(func, "N/A")
            alone_mult = ALONE_RISK_MULTIPLIER.get(func, 1.0)

            hazard_color = Color.DANGER if hazard >= 0.8 else (Color.WARNING if hazard >= 0.5 else Color.SAFE)
            
            st.markdown(f"""
            <div style="background:{Color.BG_MUTED};border-radius:10px;padding:0.8rem;margin-bottom:0.8rem;
                        border-left:4px solid {hazard_color};">
                <div style="font-size:1.2rem;margin-bottom:0.3rem;">{icon} <b>{name}</b></div>
                <div style="font-size:0.85rem;color:{Color.TEXT_MUTED};">{desc}</div>
                <div style="margin-top:0.5rem;font-size:0.78rem;">
                    <span style="color:{hazard_color};">위험도: {hazard:.1f}</span> · 
                    <span>체류한도: {dwell}분</span> · 
                    <span>단독배수: {alone_mult:.1f}x</span>
                </div>
                <div style="font-size:0.72rem;color:{Color.TEXT_MUTED};margin-top:0.3rem;">
                    <code>{func}</code>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 키워드 매핑 테이블")
    st.markdown("장소명에 포함된 키워드로 Space Function을 자동 분류합니다.")

    keyword_rows = []
    for func in SPACE_KEYWORD_PRIORITY:
        keywords = SPACE_KEYWORDS.get(func, [])
        keyword_rows.append({
            "Space Function": func,
            "우선순위": SPACE_KEYWORD_PRIORITY.index(func) + 1,
            "매칭 키워드": ", ".join(keywords),
        })

    st.dataframe(pd.DataFrame(keyword_rows), use_container_width=True, hide_index=True)

    st.info("""
    **매칭 규칙:**  
    1. 장소명에 키워드가 포함되면 해당 Space Function 할당  
    2. 여러 키워드가 매칭되면 **우선순위가 높은 것**을 선택  
    3. 키워드가 없으면 SSMP zone_type으로 분류 시도  
    4. 모두 실패 시 `UNKNOWN` 할당
    """)


def _render_current_places(df: pd.DataFrame) -> None:
    """현재 데이터의 장소 목록 및 분류 결과."""
    st.markdown("### 현재 데이터 장소 분류 결과")

    if df.empty:
        st.warning("데이터가 없습니다.")
        return

    place_col = ProcessedColumns.CORRECTED_PLACE if ProcessedColumns.CORRECTED_PLACE in df.columns else RawColumns.PLACE
    space_func_col = ProcessedColumns.SPACE_FUNCTION if ProcessedColumns.SPACE_FUNCTION in df.columns else None
    hazard_col = ProcessedColumns.HAZARD_WEIGHT if ProcessedColumns.HAZARD_WEIGHT in df.columns else None
    place_type_col = ProcessedColumns.PLACE_TYPE if ProcessedColumns.PLACE_TYPE in df.columns else None

    # 장소별 집계
    place_stats = (
        df.groupby(place_col)
        .agg(
            기록수=(RawColumns.TIME, "count"),
            작업자수=(RawColumns.WORKER, "nunique"),
        )
        .reset_index()
        .rename(columns={place_col: "장소명"})
    )

    # Space Function, Hazard Weight 추가
    if space_func_col and space_func_col in df.columns:
        sf_map = df.drop_duplicates(subset=[place_col])[[place_col, space_func_col]].set_index(place_col)[space_func_col].to_dict()
        place_stats["Space Function"] = place_stats["장소명"].map(sf_map)
    else:
        place_stats["Space Function"] = "N/A"

    if hazard_col and hazard_col in df.columns:
        hz_map = df.drop_duplicates(subset=[place_col])[[place_col, hazard_col]].set_index(place_col)[hazard_col].to_dict()
        place_stats["위험도"] = place_stats["장소명"].map(hz_map).apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
    else:
        place_stats["위험도"] = "N/A"

    if place_type_col and place_type_col in df.columns:
        pt_map = df.drop_duplicates(subset=[place_col])[[place_col, place_type_col]].set_index(place_col)[place_type_col].to_dict()
        place_stats["장소유형"] = place_stats["장소명"].map(pt_map)
    else:
        place_stats["장소유형"] = "N/A"

    # 정렬
    place_stats = place_stats.sort_values("기록수", ascending=False).reset_index(drop=True)

    # Space Function별 색상 표시
    st.markdown(f"**총 {len(place_stats)}개 장소 · {place_stats['기록수'].sum():,}개 기록**")

    # 필터
    funcs_in_data = place_stats["Space Function"].unique().tolist()
    selected_func = st.selectbox(
        "Space Function 필터",
        ["전체"] + [f for f in funcs_in_data if f != "N/A"],
    )

    if selected_func != "전체":
        place_stats = place_stats[place_stats["Space Function"] == selected_func]

    st.dataframe(
        place_stats,
        use_container_width=True,
        hide_index=True,
        column_config={
            "기록수": st.column_config.NumberColumn(format="%d"),
            "작업자수": st.column_config.NumberColumn(format="%d"),
        },
    )

    # Space Function별 요약
    st.markdown("---")
    st.markdown("### Space Function별 요약")

    if "Space Function" in place_stats.columns:
        summary = (
            df.groupby(space_func_col if space_func_col else place_col)
            .agg(
                장소수=(place_col, "nunique"),
                기록수=(RawColumns.TIME, "count"),
                작업자수=(RawColumns.WORKER, "nunique"),
            )
            .reset_index()
            .rename(columns={space_func_col if space_func_col else place_col: "Space Function"})
            .sort_values("기록수", ascending=False)
        )
        st.dataframe(summary, use_container_width=True, hide_index=True)


def _render_parameter_tables() -> None:
    """파라미터 테이블 (참조용)."""
    st.markdown("### 파라미터 테이블")

    st.markdown("#### Hazard Weight (위험도 가중치)")
    st.markdown("공간 기능에 따른 기본 위험 가중치입니다.")

    hazard_rows = [
        {"Space Function": k, "기본 위험도": f"{v:.1f}"}
        for k, v in HAZARD_WEIGHT_DEFAULT.items()
    ]
    st.dataframe(pd.DataFrame(hazard_rows), use_container_width=True, hide_index=True)

    st.markdown("#### Dwell Normal Max (정상 체류 한도, 분)")
    st.markdown("이 시간을 초과하면 `dwell_exceeded = True`로 표시됩니다.")

    dwell_rows = [
        {"Space Function": k, "체류 한도 (분)": v}
        for k, v in DWELL_NORMAL_MAX.items()
    ]
    st.dataframe(pd.DataFrame(dwell_rows), use_container_width=True, hide_index=True)

    st.markdown("#### Abnormal Stop Threshold (이상 정지 감지 기준, 분)")
    st.markdown("비활성 상태가 이 시간을 초과하면 `anomaly_flag = abnormal_stop`으로 표시됩니다.")

    abnormal_rows = [
        {"Space Function": k, "이상 정지 기준 (분)": v}
        for k, v in ABNORMAL_STOP_THRESHOLD.items()
    ]
    st.dataframe(pd.DataFrame(abnormal_rows), use_container_width=True, hide_index=True)

    st.markdown("#### Alone Risk Multiplier (단독 작업 위험 배수)")
    st.markdown("단독 작업 시 위험도에 곱해지는 배수입니다.")

    alone_rows = [
        {"Space Function": k, "단독 배수": f"{v:.1f}x"}
        for k, v in ALONE_RISK_MULTIPLIER.items()
    ]
    st.dataframe(pd.DataFrame(alone_rows), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("#### SSMP zone_type → Space Function 매핑")

    ssmp_rows = [
        {"SSMP zone_type": k, "Space Function": v}
        for k, v in SSMP_ZONE_TYPE_MAPPING.items()
    ]
    st.dataframe(pd.DataFrame(ssmp_rows), use_container_width=True, hide_index=True)
