"""
🔍 Journey 검증 페이지.

고객 질문: "Journey 데이터가 제대로 보정되었나요?"
목적: 보정 전/후를 직관적으로 비교하고, 보정 로직이 합리적임을 확인시킨다.

UI 구조:
  - 보정 전/후 Gantt 차트 나란히 비교
  - 보정 요약 (장소 변경 건수, 비율)
  - 보정 로직 설명 (st.expander 기본 닫힘)
  - space_function 분류 기준 표 (투명성 확보)
"""
from __future__ import annotations

import io
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.data.schema import RawColumns, ProcessedColumns
from src.utils.theme import Color, apply_theme, get_block_color, SHORT_BLOCK_THRESHOLD_MIN
from src.utils.place_utils import sort_places_by_similarity, sort_places_smart
from src.utils.constants import (
    ACTIVE_RATIO_ZERO_THRESHOLD,
    LOCATION_SMOOTHING_WINDOW,
    HELMET_RACK_MIN_DURATION_MIN,
    COORD_OUTLIER_THRESHOLD,
    NIGHT_HOURS_START,
    DAWN_HOURS_END,
    LUNCH_START,
    LUNCH_END,
    SpaceFunction,
    SPACE_KEYWORDS,
    DBSCAN_EPS_MULTIPLIER,
    DWELL_NORMAL_MAX,
    ACTIVITY_COLORS,
    ACTIVITY_LABELS,
    MIN_DISPLAY_MINUTES,
)


def render(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        st.warning("데이터 없음")
        return

    # ── 헤더 ───────────────────────────────────────────────────────
    st.markdown("""
    <div class="kpi-banner">
        <h1>🔍 Journey 검증</h1>
        <p>Q1: Journey 데이터가 제대로 보정되었나요?</p>
    </div>""", unsafe_allow_html=True)

    # ── 작업자 / 날짜 선택 ─────────────────────────────────────────
    worker_keys = sorted(df[ProcessedColumns.WORKER_KEY].unique())
    worker_label = {}
    for wk in worker_keys:
        sub = df[df[ProcessedColumns.WORKER_KEY] == wk]
        name    = sub[RawColumns.WORKER].iloc[0]
        company = sub[RawColumns.COMPANY].iloc[0]
        n_corr  = (sub[RawColumns.PLACE] != sub[ProcessedColumns.CORRECTED_PLACE]).sum()
        badge   = f"  ✦ {n_corr}건 보정" if n_corr > 0 else ""
        worker_label[wk] = f"{name}  ({company}){badge}"

    col_sel, col_stat = st.columns([1, 2])
    with col_sel:
        selected_key = st.selectbox(
            "👷 작업자 선택",
            options=worker_keys,
            format_func=lambda k: worker_label.get(k, k),
        )

    wdf = df[df[ProcessedColumns.WORKER_KEY] == selected_key].copy()
    wdf = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)

    name    = wdf[RawColumns.WORKER].iloc[0]
    company = wdf[RawColumns.COMPANY].iloc[0]
    tag_id  = wdf[RawColumns.TAG].iloc[0]

    stats = _compute_correction_stats(wdf)

    with col_stat:
        _render_worker_stat_bar(name, company, tag_id, stats)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 보정 전/후 Journey 비교 ────────────────────────────────────
    _render_journey_comparison(wdf, name, df)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 보정 요약 ──────────────────────────────────────────────────
    _render_correction_summary(wdf, stats)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 보정 전후 시간 분류 비교 ──────────────────────────────────
    _render_time_category_comparison(wdf)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 보정 로직 설명 (토글 — 기본 닫힘) ──────────────────────────
    with st.expander("📖 보정 로직 설명", expanded=False):
        _render_correction_logic_explanation(wdf)

    # ── space_function 분류 기준 표 (항상 노출) ─────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    _render_space_function_table()


def _compute_correction_stats(wdf: pd.DataFrame) -> dict:
    total_n     = len(wdf)
    is_corr     = wdf[ProcessedColumns.IS_CORRECTED] == True
    # NaN 안전 비교
    raw_places = wdf[RawColumns.PLACE].fillna("").astype(str)
    corr_places = wdf[ProcessedColumns.CORRECTED_PLACE].fillna("").astype(str)
    place_diff  = raw_places != corr_places
    n_place_changed = int(place_diff.sum())
    n_coord_only    = int((is_corr & ~place_diff).sum())
    n_unchanged     = int((~is_corr).sum())
    place_change_rate = n_place_changed / total_n if total_n > 0 else 0.0
    return {
        "total_n": total_n,
        "n_place_changed": n_place_changed,
        "n_coord_only": n_coord_only,
        "n_unchanged": n_unchanged,
        "place_change_rate": place_change_rate,
    }


def _render_worker_stat_bar(name: str, company: str, tag_id: str, stats: dict) -> None:
    total_n          = stats["total_n"]
    n_place_changed  = stats["n_place_changed"]
    n_coord_only     = stats["n_coord_only"]
    n_unchanged      = stats["n_unchanged"]
    place_change_rate = stats["place_change_rate"]

    has_corr = n_place_changed > 0 or n_coord_only > 0
    border_color = Color.ACCENT if has_corr else Color.SAFE

    st.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E8ECF4;border-radius:12px;
                padding:1rem 1.4rem;display:flex;gap:2rem;align-items:center;
                border-left:5px solid {border_color};">
        <div>
            <div style="font-size:1.1rem;font-weight:700;color:{Color.PRIMARY};">{name}</div>
            <div style="font-size:0.8rem;color:{Color.TEXT_MUTED};">{company} · {tag_id}</div>
        </div>
        <div style="display:flex;gap:1.6rem;margin-left:auto;flex-wrap:wrap;">
            <div style="text-align:center;">
                <div style="font-size:1.35rem;font-weight:700;color:{Color.PRIMARY};">{total_n:,}</div>
                <div style="font-size:0.7rem;color:{Color.TEXT_MUTED};">전체 기록(분)</div>
            </div>
            <div style="text-align:center;">
                <div style="font-size:1.35rem;font-weight:700;color:{Color.DANGER if n_place_changed>0 else Color.SAFE};">{n_place_changed:,}</div>
                <div style="font-size:0.7rem;color:{Color.TEXT_MUTED};">장소명 변경</div>
            </div>
            <div style="text-align:center;">
                <div style="font-size:1.35rem;font-weight:700;color:{Color.SAFE};">{n_unchanged:,}</div>
                <div style="font-size:0.7rem;color:{Color.TEXT_MUTED};">원본 유지</div>
            </div>
            <div style="text-align:center;">
                <div style="font-size:1.35rem;font-weight:700;color:{Color.ACCENT};">{place_change_rate:.1%}</div>
                <div style="font-size:0.7rem;color:{Color.TEXT_MUTED};">장소 변경률</div>
            </div>
        </div>
    </div>""", unsafe_allow_html=True)


def _get_global_axes(df: pd.DataFrame, full_df: pd.DataFrame = None, use_original: bool = False) -> tuple:
    """
    차트 축 정보 계산.
    
    ★ 핵심: Y축에 전체 데이터(full_df)의 모든 장소를 포함하여 작업자 간 일관된 비교 가능.
    
    Args:
        df: 현재 작업자 데이터
        full_df: 전체 데이터 (모든 작업자 포함 — Y축 장소 목록 + 이동 기반 정렬에 사용)
        use_original: 원본 장소 사용 여부
        
    Returns:
        (t_min, t_max, place_order) 튜플
    """
    if df is None or df.empty:
        return pd.Timestamp("2000-01-01 00:00:00"), pd.Timestamp("2000-01-02 00:00:00"), []

    date_val = df[ProcessedColumns.DATE].iloc[0] if ProcessedColumns.DATE in df.columns else df[RawColumns.TIME].iloc[0]
    if hasattr(date_val, "strftime"):
        date_str = date_val.strftime("%Y-%m-%d")
    else:
        s = str(date_val).replace("-", "")[:8]
        date_str = f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) >= 8 else "2000-01-01"

    # x축: 00:00 ~ 24:00 고정
    t_min = pd.Timestamp(f"{date_str} 00:00:00")
    t_max = t_min + pd.Timedelta(days=1)

    # y축: ★ 전체 데이터(full_df)의 모든 장소 수집 — 작업자 간 일관된 Y축
    place_set: set = set()
    
    # 전체 데이터에서 모든 장소 수집 (작업자 무관)
    data_for_places = full_df if full_df is not None and not full_df.empty else df
    
    for col in [RawColumns.PLACE, ProcessedColumns.CORRECTED_PLACE]:
        if col in data_for_places.columns:
            for p in data_for_places[col].astype(str):
                if p and p != "nan":
                    place_set.add(p)
    
    # 이동 기반 스마트 정렬 (전체 데이터 활용)
    place_col = ProcessedColumns.CORRECTED_PLACE if ProcessedColumns.CORRECTED_PLACE in (full_df.columns if full_df is not None else []) else RawColumns.PLACE
    place_order = sort_places_smart(list(place_set), full_df, place_col)
    
    return t_min, t_max, place_order


def _render_journey_comparison(wdf: pd.DataFrame, name: str, df: pd.DataFrame) -> None:
    orig_gantt = _build_gantt(wdf, RawColumns.PLACE)
    corr_gantt = _build_gantt(wdf, ProcessedColumns.CORRECTED_PLACE)

    if orig_gantt.empty and corr_gantt.empty:
        st.info("Journey 데이터 없음")
        return

    diff_n = (wdf[RawColumns.PLACE] != wdf[ProcessedColumns.CORRECTED_PLACE]).sum()

    t_min, t_max, all_places = _get_global_axes(wdf, full_df=df)
    x_range = [t_min, t_max]

    n_y = len(all_places) if all_places else max(
        len(orig_gantt["장소"].unique()) if not orig_gantt.empty else 1,
        len(corr_gantt["장소"].unique()) if not corr_gantt.empty else 1
    )
    chart_h = max(280, n_y * 34 + 80)

    if diff_n == 0:
        st.success("✅ 원본과 보정 Journey가 동일합니다. 보정이 적용되지 않았습니다.")
        st.markdown("**📍 Journey Timeline**")
        fig = _make_gantt_figure(orig_gantt, f"{name} · Journey", height=chart_h, x_range=x_range, y_places=all_places)
        st.plotly_chart(fig, use_container_width=True)
        return

    st.markdown(f"""
    <div style="background:#FEF5E7;border:1px solid {Color.ACCENT};border-radius:10px;
                padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.88rem;color:{Color.TEXT_DARK};">
        ⚠️ &nbsp;<b>{diff_n}개 행</b>의 장소명이 보정되었습니다.
        &nbsp;|&nbsp; <span style="color:#E74C3C;font-weight:600;">●</span> = 보정 포인트
        &nbsp;|&nbsp; x축: 00:00~24:00 · y축: 전체 장소 ({len(all_places)}개)
    </div>""", unsafe_allow_html=True)

    col_l, col_r = st.columns(2, gap="medium")

    with col_l:
        st.markdown(f"""
        <div style="text-align:center;background:{Color.BG_MUTED};border-radius:8px;
                    padding:0.5rem;margin-bottom:0.5rem;font-weight:600;
                    color:{Color.TEXT_DARK};font-size:0.9rem;">
            📂 보정 전 (Raw) — <span style="color:#E74C3C;">●</span> 보정이 필요한 구간
        </div>""", unsafe_allow_html=True)
        fig_orig = _make_gantt_figure(orig_gantt, "", height=chart_h, x_range=x_range, y_places=all_places)
        fig_orig = _add_correction_markers(fig_orig, wdf, RawColumns.PLACE, is_before=True)
        st.plotly_chart(fig_orig, use_container_width=True)

    with col_r:
        st.markdown(f"""
        <div style="text-align:center;background:#EAF4EA;border-radius:8px;
                    padding:0.5rem;margin-bottom:0.5rem;font-weight:600;
                    color:{Color.SAFE};font-size:0.9rem;">
            ✅ 보정 후 (Corrected) — <span style="color:#E74C3C;">●</span> 보정이 적용된 구간
        </div>""", unsafe_allow_html=True)
        fig_corr = _make_gantt_figure(corr_gantt, "", height=chart_h, x_range=x_range, y_places=all_places)
        fig_corr = _add_correction_markers(fig_corr, wdf, ProcessedColumns.CORRECTED_PLACE, is_before=False)
        st.plotly_chart(fig_corr, use_container_width=True)


def _render_correction_summary(wdf: pd.DataFrame, stats: dict) -> None:
    st.markdown("### ▼ 보정 요약")

    if wdf.empty:
        st.info("데이터 없음")
        return

    total_n = stats.get("total_n", len(wdf))
    n_place_changed = stats.get("n_place_changed", 0)
    place_change_rate = stats.get("place_change_rate", 0.0)

    # 간단한 텍스트로 표시 (CSS 호환성 확보)
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        st.metric("전체 기록", f"{total_n:,}분")
    with col_s2:
        st.metric("장소 변경", f"{n_place_changed}건")
    with col_s3:
        st.metric("변경 비율", f"{place_change_rate:.1%}")

    if n_place_changed > 0:
        st.markdown("**주요 보정 패턴 (원본 → 보정)**")
        try:
            # NaN 안전 비교: fillna로 빈 문자열 변환 후 비교
            raw_places = wdf[RawColumns.PLACE].fillna("").astype(str)
            corr_places = wdf[ProcessedColumns.CORRECTED_PLACE].fillna("").astype(str)
            diff_mask = raw_places != corr_places
            diff_df = wdf[diff_mask].copy()
            
            if diff_df.empty:
                st.caption(f"보정된 행 없음 (stats={n_place_changed}, mask_sum={diff_mask.sum()})")
            else:
                # 문자열로 변환하여 groupby
                diff_df["_raw"] = diff_df[RawColumns.PLACE].fillna("").astype(str)
                diff_df["_corr"] = diff_df[ProcessedColumns.CORRECTED_PLACE].fillna("").astype(str)
                change_count = (
                    diff_df.groupby(["_raw", "_corr"])
                    .size()
                    .reset_index(name="건수")
                    .sort_values("건수", ascending=False)
                    .head(10)
                )
                change_count["변경"] = change_count["_raw"].str[:25] + " → " + change_count["_corr"].str[:25]
                # st.dataframe 대신 st.table 사용 (렌더링 안정성)
                st.table(change_count[["변경", "건수"]])
        except Exception as e:
            st.error(f"보정 패턴 계산 오류: {e}")
            import traceback
            st.code(traceback.format_exc())
    else:
        st.info("보정된 행이 없습니다.")


def _render_time_category_comparison(wdf: pd.DataFrame) -> None:
    """보정 전후 시간 분류(6카테고리) 비교를 표시합니다."""
    st.markdown("### ▼ 보정 전후 시간 분류 비교")
    st.caption("보정 전(Raw) 장소 기준 vs 보정 후(Corrected) 장소 기준 활동 시간 분류")
    
    if wdf.empty:
        st.info("데이터 없음")
        return
    
    # 6개 카테고리 라벨
    categories = ["high_work", "low_work", "standby", "transit", "rest", "off_duty"]
    labels = {
        "high_work": "고활성 작업",
        "low_work": "저활성 작업",
        "standby": "대기",
        "transit": "이동",
        "rest": "휴게",
        "off_duty": "비근무",
    }
    colors = {
        "high_work": "#1E5AA8",
        "low_work": "#6FA8DC",
        "standby": "#F4D03F",
        "transit": "#E67E22",
        "rest": "#27AE60",
        "off_duty": "#BDC3C7",
    }
    
    def classify_row_by_place(place: str, active_ratio: float, hour: int) -> str:
        """장소와 활성비율로 6카테고리 분류."""
        place_lower = str(place).lower()
        
        # 헬멧 거치대
        if any(kw in place_lower for kw in ["걸이대", "거치대", "보호구"]):
            return "off_duty"
        
        # 휴게 시설
        if any(kw in place_lower for kw in ["휴게", "식당", "탈의실", "흡연"]):
            return "rest"
        
        # 게이트/이동
        if any(kw in place_lower for kw in ["게이트", "gate", "타각기", "통로", "복도"]):
            return "transit"
        
        # 작업 공간 (활성비율 기반)
        if active_ratio >= 0.6:
            return "high_work"
        elif active_ratio >= 0.15:
            return "low_work"
        elif active_ratio >= 0.05:
            return "standby"
        else:
            return "off_duty"
    
    try:
        # 보정 전/후 분류 계산
        before_counts = {cat: 0 for cat in categories}
        after_counts = {cat: 0 for cat in categories}
        
        # 컬럼 존재 확인
        ar_col = ProcessedColumns.ACTIVE_RATIO if ProcessedColumns.ACTIVE_RATIO in wdf.columns else None
        hour_col = ProcessedColumns.HOUR if ProcessedColumns.HOUR in wdf.columns else None
        raw_col = RawColumns.PLACE if RawColumns.PLACE in wdf.columns else None
        corr_col = ProcessedColumns.CORRECTED_PLACE if ProcessedColumns.CORRECTED_PLACE in wdf.columns else None
        
        if not raw_col or not corr_col:
            st.warning(f"필수 컬럼 누락: PLACE={raw_col is not None}, CORRECTED_PLACE={corr_col is not None}")
            return
        
        for idx, row in wdf.iterrows():
            ar = float(row[ar_col]) if ar_col and pd.notna(row[ar_col]) else 0.0
            hour = int(row[hour_col]) if hour_col and pd.notna(row[hour_col]) else 12
            
            raw_place = str(row[raw_col]) if pd.notna(row[raw_col]) else ""
            corr_place = str(row[corr_col]) if pd.notna(row[corr_col]) else ""
            
            before_cat = classify_row_by_place(raw_place, ar, hour)
            after_cat = classify_row_by_place(corr_place, ar, hour)
            
            before_counts[before_cat] += 1
            after_counts[after_cat] += 1
        
        # 비교 테이블 생성
        comparison_data = []
        for cat in categories:
            before = before_counts[cat]
            after = after_counts[cat]
            delta = after - before
            delta_str = f"+{delta}" if delta > 0 else str(delta)
            comparison_data.append({
                "카테고리": labels[cat],
                "보정 전 (분)": before,
                "보정 후 (분)": after,
                "변화": delta_str,
            })
        
        col1, col2 = st.columns([1.5, 1])
        
        with col1:
            # 테이블 표시
            comp_df = pd.DataFrame(comparison_data)
            if not comp_df.empty:
                st.table(comp_df)
            else:
                st.caption("비교 데이터 없음")
        
        with col2:
            # 간단한 변화 요약
            st.markdown(f"""
            <div style="background:#F0F8FF;border-radius:8px;padding:0.8rem;font-size:0.85rem;">
                <b>📊 주요 변화</b><br>
                • 휴게: {before_counts['rest']}분 → {after_counts['rest']}분 
                  <span style="color:{'#27AE60' if after_counts['rest'] >= before_counts['rest'] else '#E74C3C'};">
                    ({'+' if after_counts['rest'] >= before_counts['rest'] else ''}{after_counts['rest'] - before_counts['rest']}분)
                  </span><br>
                • 비근무: {before_counts['off_duty']}분 → {after_counts['off_duty']}분
                  <span style="color:#666;">
                    ({'+' if after_counts['off_duty'] >= before_counts['off_duty'] else ''}{after_counts['off_duty'] - before_counts['off_duty']}분)
                  </span><br>
                • 이동: {before_counts['transit']}분 → {after_counts['transit']}분
                  <span style="color:#E67E22;">
                    ({'+' if after_counts['transit'] >= before_counts['transit'] else ''}{after_counts['transit'] - before_counts['transit']}분)
                  </span>
            </div>
            """, unsafe_allow_html=True)
        # 경고: 휴게/흡연장 데이터 감소 시
        rest_change = after_counts['rest'] - before_counts['rest']
        if rest_change < -5:
            st.warning(f"⚠️ 휴게 시간이 {abs(rest_change)}분 감소했습니다. 휴게실/흡연장 데이터가 보정 과정에서 다른 장소로 변경되었을 수 있습니다.")
            
    except Exception as e:
        st.error(f"시간 분류 비교 오류: {e}")
        import traceback
        st.code(traceback.format_exc())


def _render_correction_logic_explanation(wdf: pd.DataFrame) -> None:
    st.markdown("""
### 🧠 지능형 Journey 보정 (Multi-Pass Refinement v6.3)

**Deep Con의 철학**: 4대 증거 기반의 "대배심(Grand Jury)" 모델을 통한 반복적 참값 수렴

---

#### 📋 4대 판단 증거 (Evidence Layers)

| 증거 | 데이터 소스 | 판단 질문 |
|------|------------|-----------|
| **E1. 활성 신호** | Ward 가속도 센서 | "사람이 움직였는가?" |
| **E2. 공간 속성** | 장소 기능 및 우선순위 | "이 장소에서 머무는 게 정상인가?" |
| **E3. 시간 속성** | 근무 시간대 및 점심 맥락 | "지금 시각에 여기 있는 게 자연스러운가?" |
| **E4. 이동 패턴** | 위치 연속성 및 엔트로피 | "위치가 안정적인가, 점프하는가?" |

---

#### 🔄 Multi-Pass 수렴 프로세스

한 번에 보정하지 않고, 데이터가 '참값'에 도달할 때까지 최대 3회 루프를 수행합니다.
    """)

    st.markdown(f"""
**Pass 1: Ghost Signal 보정**
- 조건: 무활성(E1) + 위치 불안정(E4) + 야간(E3)
- BLE 다중 반사 노이즈로 판정 → 인근 헬멧 거치대(RACK)로 통일

**Pass 1.5: Journey 문장화 보정**
- 하루 Journey를 "문장"으로 보고 짧은 "단어(Run)"를 인접 Run으로 흡수
- 예: 1분짜리 공사현장 → 양옆이 FAB이면 FAB으로 흡수

**Pass 2: 번갈음 패턴 해소**
- 두 장소가 번갈아 찍힐 때 공간 우선순위 적용
- 우선순위: RACK > REST > WORK > TRANSIT
- 유의미한 체류지로 통일

**Pass 3: 전체 맥락 검증**
- 하루 전체 스토리라인 일관성 확인
- 비정상적 도약이나 누락 검증

**Pass 4: 물리적 이상치 탐지**
- 1분 내 도보 이동 불가능한 '텔레포트' 탐지
- 좌표가 **{COORD_OUTLIER_THRESHOLD}unit** 이상 급변 시 재보정
    """)

    st.markdown("""
---

#### 🏗️ Phase 0: 공간 클러스터링 (DBSCAN)

같은 구역에서 짧게 다른 장소로 튀는 노이즈를 제거합니다.

| space_function | eps 배수 | 설명 |
|----------------|---------|------|
| WORK | ×1.0 | 기본 클러스터링 |
| WORK_HAZARD | ×0.7 | 더 공격적 (작은 이탈도 감지) |
| TRANSIT_WORK | ×1.5 | 완화 (이동 중 좌표 흔들림 허용) |
| REST / RACK / GATE | 스킵 | 앵커 공간 (클러스터링에서 보호) |

**앵커 공간 보호**: 휴게실, 흡연장, 헬멧 거치대 등 중요 장소는 DBSCAN으로 덮어쓰지 않음
    """)


def _render_space_function_table() -> None:
    with st.expander("📖 Space Function 분류 기준", expanded=False):
        st.caption("장소명 키워드 → space_function 매핑 규칙. 이 기준에 따라 보정 전략과 상태 해석이 달라집니다.")

        rows = []
        for sf, keywords in SPACE_KEYWORDS.items():
            eps_mult = DBSCAN_EPS_MULTIPLIER.get(sf, 1.0)
            dwell_max = DWELL_NORMAL_MAX.get(sf, "-")
            rows.append({
                "분류": sf,
                "키워드 예시": ", ".join(keywords[:5]) + ("..." if len(keywords) > 5 else ""),
                "DBSCAN eps 배수": f"×{eps_mult}" if eps_mult > 0 else "스킵",
                "정상 체류(분)": str(dwell_max) if dwell_max else "-",
            })

        if rows:
            table_df = pd.DataFrame(rows)
            st.table(table_df)
        else:
            st.info("SPACE_KEYWORDS가 비어 있습니다.")


def _build_gantt(df: pd.DataFrame, place_col: str, max_gap_min: int = 5) -> pd.DataFrame:
    """
    Gantt 차트용 DataFrame 생성.
    
    Args:
        df: 작업자 Journey DataFrame
        place_col: 장소 컬럼명 (PLACE 또는 CORRECTED_PLACE)
        max_gap_min: 최대 허용 공백 (분). 이 이상 공백이면 별도 블록으로 분리.
    """
    # 시간순 정렬 필수
    sorted_df = df.sort_values(RawColumns.TIME).reset_index(drop=True)
    
    rows, cur_place, start_ts, buf, prev_ts = [], None, None, [], None

    for _, row in sorted_df.iterrows():
        place = str(row.get(place_col, "Unknown"))
        ts    = row[RawColumns.TIME]
        
        # 시간 공백 체크: 이전 데이터와 max_gap_min 이상 차이나면 블록 종료
        if prev_ts is not None and cur_place and buf:
            gap_min = (ts - prev_ts).total_seconds() / 60
            if gap_min > max_gap_min:
                # 현재 블록 종료 (공백 전까지만)
                _flush(rows, cur_place, start_ts, buf)
                cur_place = None
                start_ts = None
                buf = []
        
        if place != cur_place:
            if cur_place and buf:
                _flush(rows, cur_place, start_ts, buf)
            cur_place = place
            start_ts  = ts
            buf       = [row]
        else:
            buf.append(row)
        
        prev_ts = ts
        
    if cur_place and buf:
        _flush(rows, cur_place, start_ts, buf)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _flush(rows: list, place: str, start_ts, buf: list) -> None:
    from src.utils.constants import WORK_INTENSITY_HIGH_THRESHOLD, WORK_INTENSITY_LOW_THRESHOLD

    end_ts     = buf[-1][RawColumns.TIME] + pd.Timedelta(minutes=1)
    ratios     = [r.get(ProcessedColumns.ACTIVE_RATIO, 0) for r in buf]
    avg_ratio  = sum(ratios) / len(ratios) if ratios else 0
    high_min   = sum(1 for r in ratios if r >= WORK_INTENSITY_HIGH_THRESHOLD)
    low_min    = sum(1 for r in ratios if WORK_INTENSITY_LOW_THRESHOLD <= r < WORK_INTENSITY_HIGH_THRESHOLD)
    place_type = buf[0].get(ProcessedColumns.PLACE_TYPE, "UNKNOWN")
    
    # state_detail에서 transit_arrival 확인 (전환 이동 태깅 반영)
    state_details = [r.get(ProcessedColumns.STATE_DETAIL, "") for r in buf]
    transit_arrival_count = sum(1 for s in state_details if s == "transit_arrival")
    
    # 블록의 과반이 transit_arrival이면 transit으로 표시
    if transit_arrival_count > len(buf) / 2:
        activity = "transit"
    else:
        activity = _classify_block_activity(place_type, avg_ratio, start_ts.hour)
    
    rows.append({
        "장소":        place,
        "시작":        start_ts,
        "종료":        end_ts,
        "장소유형":    place_type,
        "활동상태":    activity,
        "평균활성비율": round(avg_ratio, 3),
        "체류(분)":    len(buf),
        "고활성(분)":  high_min,
        "저활성(분)":  low_min,
    })


def _classify_block_activity(place_type: str, avg_ratio: float, hour: int) -> str:
    from src.utils.constants import (
        WORK_INTENSITY_HIGH_THRESHOLD,
        WORK_INTENSITY_LOW_THRESHOLD,
        ACTIVE_RATIO_ZERO_THRESHOLD,
        NIGHT_HOURS_START, DAWN_HOURS_END,
        LUNCH_START, LUNCH_END,
    )
    if place_type in ("HELMET_RACK", "RACK"):
        return "off_duty"
    if place_type in ("REST", "REST_FACILITY"):
        return "rest"
    if place_type in ("GATE", "TRANSIT_GATE", "TRANSIT_CORRIDOR"):
        return "transit"

    is_night = hour >= NIGHT_HOURS_START or hour < DAWN_HOURS_END
    is_lunch = LUNCH_START <= hour < LUNCH_END

    if avg_ratio >= WORK_INTENSITY_HIGH_THRESHOLD:
        return "high_work"
    elif avg_ratio >= WORK_INTENSITY_LOW_THRESHOLD:
        return "low_work"
    elif avg_ratio >= ACTIVE_RATIO_ZERO_THRESHOLD:
        return "standby"
    elif is_night or is_lunch:
        return "rest" if avg_ratio < ACTIVE_RATIO_ZERO_THRESHOLD else "standby"
    else:
        return "off_duty"


_ONE_MIN_MS = 60_000


def _add_correction_markers(
    fig: go.Figure, 
    wdf: pd.DataFrame, 
    place_col: str,
    is_before: bool = True,
) -> go.Figure:
    """
    보정 포인트를 작은 빨간색 X 마커로 표시.
    
    보정 전 차트: place_col=RawColumns.PLACE → 원본 장소 위치에 표시
    보정 후 차트: place_col=ProcessedColumns.CORRECTED_PLACE → 보정된 장소 위치에 표시
    
    Args:
        fig: Plotly Figure
        wdf: 작업자 DataFrame (시간순 정렬되어 있어야 함)
        place_col: 해당 차트에서 표시할 장소 컬럼명
        is_before: True=보정 전 차트, False=보정 후 차트
    """
    # 보정이 발생한 행만 추출
    diff_mask = wdf[RawColumns.PLACE] != wdf[ProcessedColumns.CORRECTED_PLACE]
    diff_rows = wdf[diff_mask].copy()
    
    if diff_rows.empty:
        return fig

    # 마커 데이터 수집: place_col에 해당하는 장소 위치에 마커 표시
    x_vals, y_vals, customdata = [], [], []
    
    for _, row in diff_rows.iterrows():
        ts         = row[RawColumns.TIME]
        y_place    = str(row[place_col])  # 해당 차트의 장소
        orig_place = str(row[RawColumns.PLACE])
        corr_place = str(row[ProcessedColumns.CORRECTED_PLACE])
        time_str   = ts.strftime("%H:%M")
        ratio      = row.get(ProcessedColumns.ACTIVE_RATIO, 0.0)
        
        x_vals.append(ts)
        y_vals.append(y_place)
        customdata.append([orig_place, corr_place, ratio, time_str])

    # 마커 스타일: 작은 빨간색 점
    label = "● 보정 포인트"
    
    fig.add_trace(go.Scatter(
        x=x_vals,
        y=y_vals,
        mode="markers",
        marker=dict(
            symbol="circle",
            size=6,
            color="#E74C3C",
            line=dict(color="#C0392B", width=0.5),
        ),
        name=label,
        legendgroup="correction_marker",
        showlegend=True,
        customdata=customdata,
        hovertemplate=(
            "<b>%{customdata[3]} · 보정 포인트</b><br>"
            "원본: <b>%{customdata[0]}</b><br>"
            "보정: <b>%{customdata[1]}</b><br>"
            "활성비율: %{customdata[2]:.1%}"
            "<extra></extra>"
        ),
    ))

    return fig


def _make_gantt_figure(
    gantt_df: pd.DataFrame,
    title: str,
    height: int = 320,
    x_range: list | None = None,
    y_places: list | None = None,
) -> go.Figure:
    fig = go.Figure()
    
    if gantt_df.empty:
        return fig
    
    # 활동 상태별 첫 번째 바에만 legend 표시
    legend_order = ["high_work", "low_work", "standby", "transit", "rest", "off_duty"]
    shown_in_legend = set()

    for _, row in gantt_df.iterrows():
        act         = row.get("활동상태", "off_duty")
        act_label   = ACTIVITY_LABELS.get(act, act)
        actual_dur  = int(row.get("체류(분)", 1))
        
        # 최소 표시 폭 적용 (짧은 블록도 보이게)
        display_dur = max(actual_dur, MIN_DISPLAY_MINUTES)
        dur_ms      = display_dur * 60 * 1000  # 분 → 밀리초
        
        # 짧은 블록 강조: get_block_color로 색상+테두리 가져오기
        block_style = get_block_color(act, actual_dur)
        bar_color   = block_style.get("color", ACTIVITY_COLORS.get(act, "#B0B8C8"))
        line_style  = block_style.get("line", {"color": "white", "width": 0.5})
        
        # 각 활동 상태의 첫 번째 바에만 legend 표시
        show_leg = act not in shown_in_legend
        if show_leg:
            shown_in_legend.add(act)

        fig.add_trace(go.Bar(
            base=row["시작"].strftime("%Y-%m-%d %H:%M:%S"),
            x=[dur_ms],
            y=[row["장소"]],
            orientation="h",
            marker_color=bar_color,
            marker_line=dict(color=line_style.get("color", "white"), width=line_style.get("width", 0.5)),
            opacity=0.92,
            name=act_label,
            legendgroup=act,
            showlegend=show_leg,
            customdata=[[row["평균활성비율"], actual_dur, row.get("고활성(분)", 0), row.get("저활성(분)", 0), act_label]],
            hovertemplate=(
                "<b>%{y}</b><br>"
                "체류: %{customdata[1]}분<br>"
                "활성비율: %{customdata[0]:.1%}<br>"
                "상태: %{customdata[4]}<br>"
                "고활성: %{customdata[2]}분 · 저활성: %{customdata[3]}분"
                "<extra></extra>"
            ),
        ))
    
    # 데이터에 없는 활동 상태도 legend에 추가 (빈 scatter로)
    for act in legend_order:
        if act not in shown_in_legend:
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode="markers",
                marker=dict(size=10, color=ACTIVITY_COLORS.get(act, "#B0B8C8")),
                name=ACTIVITY_LABELS.get(act, act),
                legendgroup=act,
                showlegend=True,
            ))

    xaxis_cfg: dict = dict(
        type="date",
        tickformat="%H:%M",
        showgrid=True,
        gridcolor="#E4EAF4",
        tickfont=dict(size=10, color="#3A4A6A"),
        dtick=2 * 60 * 60 * 1000,  # 2시간 간격 (밀리초 단위)
        tickmode="linear",
    )
    if x_range:
        xaxis_cfg["range"] = [
            x_range[0].isoformat(),
            x_range[1].isoformat(),
        ]
        # tick0을 x_range 시작 시간의 짝수 시간으로 설정
        tick0_hour = (x_range[0].hour // 2) * 2
        tick0_time = x_range[0].replace(hour=tick0_hour, minute=0, second=0)
        xaxis_cfg["tick0"] = tick0_time.isoformat()

    fig = apply_theme(fig, title, height=height)
    
    # y축 설정: 전체 장소 목록 고정
    yaxis_cfg = dict(
        title_text="장소",
        tickfont=dict(size=9, color="#1B2A4A"),
        showgrid=True,
        gridcolor="#F0F4FA",
    )
    if y_places:
        yaxis_cfg["categoryorder"] = "array"
        yaxis_cfg["categoryarray"] = list(reversed(y_places))  # 역순으로 (위에서 아래로)
        yaxis_cfg["range"] = [-0.5, len(y_places) - 0.5]
    
    fig.update_layout(
        barmode="overlay",
        bargap=0.28,
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.01,
            xanchor="left", x=0,
            font=dict(size=10),
        ),
        xaxis=xaxis_cfg,
        yaxis=yaxis_cfg,
    )
    return fig
