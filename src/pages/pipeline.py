"""
3단계 데이터 파이프라인 페이지.

Stage 1 — Journey Correction  : 작업자 이동 데이터 보정
Stage 2 — Metrics Extraction  : 생산성 / 안전성 지표 추출
Stage 3 — Visualization Ready : 대시보드 가시화 준비

전처리 실행 → 캐시 생성 → 결과 요약을 단계별로 표시한다.
CLOUD_MODE 시 이 페이지는 노출되지 않으며, 클라우드에서는 사전 생성된 캐시만 사용.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.utils.theme import Color, apply_theme


# ─── 단계 정의 ─────────────────────────────────────────────────────────
STAGES = [
    {
        "id": "correction",
        "number": "01",
        "title": "Journey Correction",
        "subtitle": "작업자 이동 데이터 보정",
        "icon": "🔧",
        "desc": "Raw 데이터의 오차 제거\n헬멧 거치 패턴 보정\n이동 노이즈 스무딩\n좌표 이상치 보간",
        "color": Color.SECONDARY,
    },
    {
        "id": "metrics",
        "number": "02",
        "title": "Metrics Extraction",
        "subtitle": "생산성 · 안전성 지표 추출",
        "icon": "📐",
        "desc": "활성비율 · 작업 블록 분석\n이동 효율 · 분절 지수 계산\n피로 위험도 · 이상 이동 패턴 탐지",
        "color": Color.ACCENT,
    },
    {
        "id": "visualization",
        "number": "03",
        "title": "Visualization",
        "subtitle": "인터랙티브 대시보드 준비",
        "icon": "📊",
        "desc": "캐시 데이터 최적화 저장\n작업자 / 업체 / 현장 뷰\n실시간 전환 대비 구조\n파트너사 공유 대시보드",
        "color": Color.SAFE,
    },
]


def render(
    date_str: str,
    datafile_root: Path,
    cache_dir: Path,
    on_complete,
    show_batch: bool = True,
) -> None:
    """
    파이프라인 페이지 렌더링.

    Args:
        date_str: 처리할 날짜 (YYYYMMDD)
        datafile_root: Datafile 루트 경로
        cache_dir: 캐시 디렉토리 경로
        on_complete: 처리 완료 후 호출할 콜백 (cache invalidation 등)
    """
    if os.getenv("CLOUD_MODE", "false").lower() == "true":
        st.info(
            "☁️ 클라우드 모드에서는 전처리를 실행할 수 없습니다. "
            "로컬에서 Pipeline을 실행한 후 생성된 캐시(processed_*.parquet, analytics_*)를 배포해 주세요."
        )
        return

    from src.data.cache_manager import ParquetCacheManager
    from src.utils.time_utils import extract_date_from_folder

    fmt_date = _fmt(date_str)

    # ── 헤더 배너 ────────────────────────────────────────────────────
    st.markdown(f"""
    <div class="kpi-banner">
        <h1>⚙️ 데이터 파이프라인</h1>
        <p>{fmt_date} · Y-Project 작업자 데이터 전처리 및 지표 추출</p>
    </div>
    """, unsafe_allow_html=True)

    # ── 캐시 상태 확인 ─────────────────────────────────────────────
    mgr = ParquetCacheManager(cache_dir)
    cache_exists = mgr.is_valid(date_str)

    # ── 3단계 카드 ──────────────────────────────────────────────────
    cols = st.columns(3, gap="medium")
    for i, (col, stage) in enumerate(zip(cols, STAGES)):
        done = cache_exists
        active = not cache_exists and i == 0
        with col:
            badge_class = "ready" if done else ("running" if active else "pending")
            badge_text  = "완료" if done else ("대기 중" if not active else "준비됨")
            st.markdown(f"""
            <div class="step-card {'done' if done else 'active' if active else ''}">
                <div class="step-number {'done' if done else ''}">{stage['number']}</div>
                <div class="step-title">{stage['icon']} {stage['title']}</div>
                <div style="font-size:0.78rem;color:{Color.TEXT_MUTED};margin:2px 0 8px;">{stage['subtitle']}</div>
                <div class="step-desc">{stage['desc'].replace(chr(10), '<br>')}</div>
                <div class="step-badge {badge_class}">{badge_text}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── 실행 버튼 ──────────────────────────────────────────────────
    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        run_label = "🔄 재처리 실행" if cache_exists else "▶ 파이프라인 실행"
        run_type = "secondary" if cache_exists else "primary"
        run_clicked = st.button(run_label, type=run_type, use_container_width=True)

    with col_info:
        if cache_exists:
            info = mgr.get_cache_info()
            target = [x for x in info if x["date"] == date_str]
            if target:
                t = target[0]
                st.markdown(f"""
                <div style="background:#EAF4EA;border-radius:10px;padding:0.7rem 1rem;
                            border-left:4px solid {Color.SAFE};">
                    ✅ &nbsp;캐시 준비됨 &nbsp;·&nbsp; <b>{t['row_count']:,}행</b>
                    &nbsp;·&nbsp; {t['size_kb']} KB &nbsp;·&nbsp;
                    <span style="color:{Color.TEXT_MUTED};">{fmt_date}</span>
                </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="background:#EBF2FD;border-radius:10px;padding:0.7rem 1rem;
                        border-left:4px solid {Color.SECONDARY};">
                💡 &nbsp;<b>{fmt_date}</b> 날짜의 Raw CSV 파일이 감지되었습니다.
                파이프라인을 실행하면 캐시가 생성되고 대시보드가 활성화됩니다.
            </div>""", unsafe_allow_html=True)

    # ── 파이프라인 실행 ────────────────────────────────────────────
    if run_clicked:
        _run_pipeline(date_str, datafile_root, cache_dir, mgr, on_complete)
        return

    # ── 결과 요약 (캐시 있을 때) ───────────────────────────────────
    if cache_exists:
        st.divider()
        _render_result_summary(date_str, cache_dir, mgr)

    # ── 일괄 처리 섹션 ─────────────────────────────────────────────
    if show_batch:
        st.divider()
        _render_batch_section(datafile_root, cache_dir, on_complete)


def _load_spatial_context(datafile_root: Path):
    """SpatialContext를 생성하여 반환. 실패 시 None 반환."""
    try:
        from src.data.spatial_loader import SpatialContext
    except ImportError:
        return None

    ssmp_dir = datafile_root / "ssmp_structure"
    if not ssmp_dir.exists():
        ssmp_dir = Path("Datafile") / "ssmp_structure"

    if not ssmp_dir.exists():
        st.warning(f"⚠️ SSMP 폴더 없음 → 키워드 매칭으로 폴백")
        return None

    try:
        ctx = SpatialContext(ssmp_dir)
        st.info(
            f"✅ SSMP 공간 구조 로드 완료 — "
            f"서비스구역 {ctx.service_section_count}개 · "
            f"Zone {ctx.zone_count}개"
        )
        return ctx
    except Exception as e:
        st.warning(f"⚠️ SSMP 로드 실패 → 키워드 매칭으로 폴백: {e}")
        return None


def _show_ssmp_match_summary(processed_df, spatial_ctx) -> None:
    """SSMP 매칭 결과 요약을 UI에 표시."""
    if spatial_ctx is None or "ssmp_matched" not in processed_df.columns:
        return
    matched = int(processed_df["ssmp_matched"].sum())
    total   = len(processed_df)
    pct     = matched / total * 100 if total else 0
    st.success(f"SSMP 매칭: {matched:,} / {total:,}행 ({pct:.1f}%)")

    unmatched_places = (
        processed_df[~processed_df["ssmp_matched"]]["장소"]
        .value_counts()
        .head(10)
    )
    if not unmatched_places.empty:
        with st.expander("⚠️ SSMP 미매칭 장소 (키워드 폴백 적용됨)", expanded=False):
            st.dataframe(
                unmatched_places.rename("출현 횟수"),
                use_container_width=True,
            )
            st.caption(
                "이 장소들은 ssmp_structure에 없어 키워드 매칭으로 분류됨. "
                "필요 시 ssmp_service_sections.csv에 추가 가능."
            )


def _run_pipeline(
    date_str: str,
    datafile_root: Path,
    cache_dir: Path,
    mgr,
    on_complete,
) -> None:
    """파이프라인 실행 UI (진행 표시 포함)."""
    from src.data.loader import load_date_folder, get_folder_for_date
    from src.data.preprocessor import preprocess

    progress_container = st.container()

    with progress_container:
        status_box = st.empty()
        prog_bar   = st.progress(0)
        log_box    = st.empty()

        def _status(msg: str, pct: int, log: str = "") -> None:
            status_box.markdown(
                f'<div style="font-weight:600;color:{Color.PRIMARY};margin-bottom:4px;">{msg}</div>',
                unsafe_allow_html=True,
            )
            prog_bar.progress(pct)
            if log:
                log_box.caption(log)

        try:
            # SSMP 공간 구조 로드
            _status("🗺️ SSMP 공간 구조 로드 중...", 3)
            spatial_ctx = _load_spatial_context(datafile_root)

            # Stage 1 — Journey Correction
            _status("🔧 Stage 1 · Journey Correction — Raw CSV 로드 중...", 8)
            time.sleep(0.1)

            folder = get_folder_for_date(datafile_root, date_str)
            if folder is None:
                st.error(f"Raw 데이터 폴더를 찾을 수 없습니다: {date_str}")
                return

            raw_df = load_date_folder(folder)
            if raw_df is None or raw_df.empty:
                st.error("CSV 파일 로드 실패")
                return

            _status(f"🔧 Stage 1 · Journey Correction — {len(raw_df):,}행 보정 중...", 20,
                    f"작업자 {raw_df['작업자'].nunique()}명 · 파일 {raw_df['_source_file'].nunique()}개 감지")
            time.sleep(0.1)

            processed_df = preprocess(raw_df, spatial_ctx=spatial_ctx)
            corrected_n = processed_df["보정여부"].sum() if "보정여부" in processed_df.columns else 0

            _status(f"🔧 Stage 1 완료 — {corrected_n:,}행 보정됨", 45,
                    f"SSMP 장소분류 + 헬멧 거치 보정 + DBSCAN 노이즈 제거 + 좌표 이상치 처리")
            _show_ssmp_match_summary(processed_df, spatial_ctx)
            time.sleep(0.15)

            # Stage 2 — Metrics Extraction
            _status("📐 Stage 2 · Metrics Extraction — 지표 계산 중...", 55)
            time.sleep(0.15)

            _status("📐 Stage 2 완료 — 생산성·안전성 지표 준비됨", 75,
                    "활성비율 · 작업블록 · 피로위험도 · 이상이동 계산 완료")
            time.sleep(0.15)

            # Stage 3 — Cache Save
            _status("📊 Stage 3 · Visualization — 캐시 저장 중...", 85)
            mgr.save(processed_df, date_str)
            time.sleep(0.1)

            _status("✅ 파이프라인 완료!", 100,
                    f"총 {len(processed_df):,}행 → cache/processed_{date_str}.parquet")
            time.sleep(0.3)

            on_complete()
            st.rerun()

        except Exception as e:
            st.error(f"파이프라인 오류: {e}")
            raise


def _render_result_summary(date_str: str, cache_dir: Path, mgr) -> None:
    """캐시 처리 결과 요약 표시."""
    df = mgr.load(date_str)
    if df is None or df.empty:
        return

    from src.data.schema import RawColumns, ProcessedColumns
    from src.metrics.aggregator import aggregate_by_worker

    st.markdown("### 📋 처리 결과 요약")

    worker_df = aggregate_by_worker(df, include_safety=True)

    # ── 요약 지표 카드 ─────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("작업자 수", f"{len(worker_df)}명")
    with c2:
        st.metric("업체 수", f"{worker_df['company'].nunique()}개사")
    with c3:
        corrected = df["보정여부"].sum() if "보정여부" in df.columns else 0
        st.metric("보정 행수", f"{corrected:,}건")
    with c4:
        avg_ratio = worker_df["active_ratio"].mean() if "active_ratio" in worker_df.columns else 0
        st.metric("평균 활성비율", f"{avg_ratio:.1%}")
    with c5:
        avg_fatigue = worker_df["safety_fatigue_risk"].mean() if "safety_fatigue_risk" in worker_df.columns else 0
        st.metric("평균 피로 위험도", f"{avg_fatigue:.2f}",
                  help="연속 2시간 초과 작업 발생 시 피로 위험 점수 누적.\n1.0 이상이면 고위험입니다.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Stage별 처리 결과 탭 ───────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["🔧 Stage 1 · Journey", "📐 Stage 2 · Metrics", "📊 Stage 3 · Preview"])

    with tab1:
        _render_stage1_result(df)

    with tab2:
        _render_stage2_result(worker_df)

    with tab3:
        _render_stage3_preview(df, worker_df)


# 기능적 대분류 매핑 테이블
# PLACE_TYPE(raw) → (기능 카테고리 레이블, 색상, 설명)
_PLACE_FUNC_MAP: dict[str, tuple[str, str, str]] = {
    "HELMET_RACK":    ("🪖 헬멧 걸이대",    "#95A5A6", "헬멧·보호구 걸이대 위치"),
    "REST":           ("🛋️ 휴게 시설",      "#27AE60", "휴게실·식당·탈의실"),
    "OFFICE":         ("🏢 사무소",          "#2E6FD9", "현장 사무소·관리동"),
    "GATE":           ("🚪 게이트/출입구",   "#F5A623", "타각기·출입 게이트"),
    "WORK_AREA":      ("🔨 작업 공간",       "#1A5276", "지정 작업 구역 (SSMP 매칭)"),
    "CONFINED_SPACE": ("⚠️ 밀폐 공간",      "#E74C3C", "밀폐 공간 (특별 관리 구역)"),
    "INDOOR":         ("🔨 작업 공간",       "#1A5276", "실내 작업 공간"),
    "OUTDOOR":        ("🌿 실외 작업 공간",  "#8BC34A", "실외 작업 공간"),
    "UNKNOWN":        ("❓ 미분류",           "#BDC3C7", "SSMP·키워드 모두 매칭 실패"),
}


def _render_place_function_dist(df: pd.DataFrame) -> None:
    """
    공간 기능 속성 분포 시각화.

    INDOOR / WORK_AREA 등 물리 위치 구분 대신,
    공간의 실제 기능(작업·휴게·헬멧걸이대·게이트 등)으로 집계하여 표시.
    CORRECTED_PLACE(보정 후 장소) 기준으로 집계.
    """
    from src.data.schema import ProcessedColumns, RawColumns

    place_col = (
        ProcessedColumns.CORRECTED_PLACE
        if ProcessedColumns.CORRECTED_PLACE in df.columns
        else RawColumns.PLACE
    )
    type_col = ProcessedColumns.PLACE_TYPE

    st.markdown("#### 🗺️ 공간 기능 분포")
    st.caption(
        f"{'보정 후 장소(CORRECTED_PLACE)' if place_col == ProcessedColumns.CORRECTED_PLACE else '원본 장소'} 기준 · "
        "물리 위치(실내/실외)가 아닌 공간의 기능적 속성으로 분류"
    )

    # ── 1) PLACE_TYPE → 기능 대분류 매핑 ─────────────────────────
    func_series = df[type_col].map(
        {k: v[0] for k, v in _PLACE_FUNC_MAP.items()}
    ).fillna("❓ 미분류")

    func_dist = (
        func_series.value_counts()
        .rename_axis("기능분류")
        .reset_index(name="분(기록수)")
    )
    total = func_dist["분(기록수)"].sum()
    func_dist["비율"] = func_dist["분(기록수)"] / total * 100

    label_to_color = {v[0]: v[1] for v in _PLACE_FUNC_MAP.values()}
    colors = [label_to_color.get(lbl, "#BDC3C7") for lbl in func_dist["기능분류"]]

    col_chart, col_top = st.columns([3, 2])

    with col_chart:
        fig = go.Figure(go.Bar(
            x=func_dist["분(기록수)"],
            y=func_dist["기능분류"],
            orientation="h",
            marker_color=colors,
            text=[
                f"{v:,}분 ({r:.1f}%)"
                for v, r in zip(func_dist["분(기록수)"], func_dist["비율"])
            ],
            textposition="outside",
            hovertemplate=(
                "<b>%{y}</b><br>"
                "기록: %{x:,}분<br>"
                "비율: %{customdata:.1f}%<extra></extra>"
            ),
            customdata=func_dist["비율"],
        ))
        fig = apply_theme(fig, "", height=max(200, len(func_dist) * 54 + 60))
        fig.update_layout(
            xaxis_title="기록 수 (분)",
            yaxis=dict(autorange="reversed"),
            margin=dict(l=0, r=90, t=20, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_top:
        st.markdown("**카테고리별 주요 장소 (상위 5개)**")
        merged = df[[type_col, place_col]].copy()
        merged["기능분류"] = func_series.values

        for _, row in func_dist.iterrows():
            cat   = row["기능분류"]
            mins  = int(row["분(기록수)"])
            ratio = row["비율"]
            color = label_to_color.get(cat, "#BDC3C7")

            top_places = (
                merged[merged["기능분류"] == cat][place_col]
                .value_counts()
                .head(5)
            )
            if top_places.empty:
                continue

            with st.expander(f"{cat}  —  {mins:,}분 ({ratio:.1f}%)", expanded=False):
                for place_name, cnt in top_places.items():
                    pct = cnt / mins * 100
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;'
                        f'font-size:0.82rem;padding:2px 0;border-bottom:1px solid #F0F2F6;">'
                        f'<span style="color:#1B2A4A;">{place_name}</span>'
                        f'<span style="color:{color};font-weight:600;">{cnt:,}분 ({pct:.0f}%)</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

    # ── 2) 활동 유형 분포 (PERIOD_TYPE) ─────────────────────────
    if ProcessedColumns.PERIOD_TYPE in df.columns:
        st.markdown("**활동 유형 분포**")
        _PERIOD_LABEL = {"work": "🔧 작업", "rest": "🛋️ 휴게", "off": "😴 비근무"}
        _PERIOD_COLOR = {
            "work": Color.SECONDARY,
            "rest": Color.SAFE,
            "off":  Color.TEXT_MUTED,
        }
        period_dist = (
            df[ProcessedColumns.PERIOD_TYPE]
            .map(_PERIOD_LABEL).fillna("기타")
            .value_counts()
            .reset_index()
        )
        period_dist.columns = ["활동유형", "분(기록수)"]
        period_dist["비율"] = period_dist["분(기록수)"] / total * 100

        p_color_map = {v: _PERIOD_COLOR.get(k, Color.TEXT_MUTED) for k, v in _PERIOD_LABEL.items()}
        p_colors    = [p_color_map.get(lbl, Color.TEXT_MUTED) for lbl in period_dist["활동유형"]]

        fig_p = go.Figure(go.Bar(
            x=period_dist["활동유형"],
            y=period_dist["분(기록수)"],
            marker_color=p_colors,
            text=[
                f"{v:,}분 ({r:.1f}%)"
                for v, r in zip(period_dist["분(기록수)"], period_dist["비율"])
            ],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>%{y:,}분<extra></extra>",
        ))
        fig_p = apply_theme(fig_p, "", height=220)
        fig_p.update_layout(
            yaxis_title="기록 수 (분)",
            margin=dict(l=0, r=0, t=20, b=40),
            showlegend=False,
        )
        st.plotly_chart(fig_p, use_container_width=True)


def _render_stage1_result(df: pd.DataFrame) -> None:
    """Stage 1: Journey 보정 결과."""
    from src.data.schema import RawColumns, ProcessedColumns

    st.markdown("#### 작업자별 보정 현황")

    if "보정여부" not in df.columns:
        st.info("보정 정보 없음")
        return

    # ── 3-카테고리 보정 통계 집계 ─────────────────────────────
    # 장소명 변경 여부 계산
    place_diff = df[RawColumns.PLACE] != df[ProcessedColumns.CORRECTED_PLACE]
    is_corr    = df[ProcessedColumns.IS_CORRECTED] == True

    def _worker_stats(grp: pd.DataFrame) -> pd.Series:
        total    = len(grp)
        is_c     = grp[ProcessedColumns.IS_CORRECTED] == True
        p_diff   = grp[RawColumns.PLACE] != grp[ProcessedColumns.CORRECTED_PLACE]
        n_place  = int(p_diff.sum())
        n_coord  = int((is_c & ~p_diff).sum())
        n_keep   = int((~is_c).sum())
        return pd.Series({
            "total":        total,
            "n_place":      n_place,
            "n_coord":      n_coord,
            "n_unchanged":  n_keep,
            "place_rate":   n_place / total if total > 0 else 0.0,
        })

    summary = (
        df.groupby([ProcessedColumns.WORKER_KEY, RawColumns.WORKER, RawColumns.COMPANY])
        .apply(_worker_stats)
        .reset_index()
    )

    # 파라미터 정의 토글
    with st.expander("📖 보정 파라미터 정의", expanded=False):
        st.markdown("""
        | 파라미터 | 정의 |
        |---|---|
        | **전체 기록(분)** | 작업자의 분 단위 위치 기록 총 수. 하루 최대 1,440분 |
        | **장소명 변경** | 보정 전후 장소명이 실제로 달라진 행 수. 헬멧 거치 통일 또는 이동 노이즈 제거로 발생 |
        | **좌표만 보정** | 장소명은 동일하나 X·Y 좌표가 보정된 행 수. 보정 블록 내 대표 좌표로 통일된 경우 |
        | **원본 유지** | 보정 알고리즘이 적용되지 않은 행 수 (`보정여부=False`) |
        | **장소 변경률** | 장소명 변경 건수 ÷ 전체 기록. 실제 의미 있는 보정 비율 |

        > ⚠️ `보정여부=True`(보정 블록 포함)는 "장소명 변경" + "좌표만 보정"의 합계입니다.
        > 이 두 가지는 의미가 다르므로 구분하여 해석해야 합니다.
        """)

    # 3-카테고리 스택 바 차트
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=summary[RawColumns.WORKER],
        y=summary["n_unchanged"],
        name="원본 유지",
        marker_color=Color.BG_MUTED,
        marker_line_color="#D0D7E8",
        marker_line_width=1,
    ))
    fig.add_trace(go.Bar(
        x=summary[RawColumns.WORKER],
        y=summary["n_coord"],
        name="좌표만 보정",
        marker_color="#9B59B6",
    ))
    fig.add_trace(go.Bar(
        x=summary[RawColumns.WORKER],
        y=summary["n_place"],
        name="장소명 변경",
        marker_color=Color.ACCENT,
    ))
    fig = apply_theme(fig, "작업자별 보정 유형 분포 (분 단위)", height=300)
    fig.update_layout(
        barmode="stack",
        yaxis_title="기록 수 (분)",
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
    )
    st.plotly_chart(fig, use_container_width=True)

    # 장소 변경률 요약 테이블
    disp = summary[[RawColumns.WORKER, RawColumns.COMPANY, "total", "n_place", "n_coord", "n_unchanged", "place_rate"]].copy()
    disp.columns = ["작업자", "업체", "전체기록(분)", "장소명변경", "좌표만보정", "원본유지", "장소변경률"]
    disp["장소변경률"] = disp["장소변경률"].apply(lambda x: f"{x:.1%}")
    st.dataframe(disp, use_container_width=True, hide_index=True)

    # 공간 기능 분포
    if ProcessedColumns.PLACE_TYPE in df.columns:
        st.markdown("---")
        _render_place_function_dist(df)


def _render_stage2_result(worker_df: pd.DataFrame) -> None:
    """Stage 2: 지표 추출 결과."""
    st.markdown("#### 작업자별 핵심 지표")

    if worker_df.empty:
        st.warning("⚠️ worker_df가 비어있습니다. aggregate_by_worker 결과를 확인하세요.")
        return
    
    # 디버깅: 사용 가능한 컬럼 표시
    with st.expander("🔧 디버그: worker_df 컬럼 정보", expanded=False):
        st.write(f"**행 수:** {len(worker_df)}")
        st.write(f"**컬럼 목록:** {list(worker_df.columns)}")

    display_cols = {
        "worker_name": "작업자",
        "company": "업체",
        "active_ratio": "활성비율",
        "working_time_min": "작업시간(분)",
        "onsite_duration_min": "체류시간(분)",
        "working_block_count": "작업블록",
        "fragmentation_index": "분절지수",
        "safety_fatigue_risk": "피로위험도",
    }
    avail = {k: v for k, v in display_cols.items() if k in worker_df.columns}
    
    if not avail:
        st.warning("⚠️ 표시할 수 있는 컬럼이 없습니다.")
        st.write("사용 가능한 컬럼:", list(worker_df.columns))
        return
    
    disp = worker_df[list(avail.keys())].rename(columns=avail)

    for col in ["활성비율"]:
        if col in disp.columns:
            disp[col] = disp[col].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")
    for col in ["분절지수", "피로위험도"]:
        if col in disp.columns:
            disp[col] = disp[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")

    st.dataframe(disp, use_container_width=True, hide_index=True)


def _render_stage3_preview(df: pd.DataFrame, worker_df: pd.DataFrame) -> None:
    """Stage 3: 대시보드 미리보기."""
    from src.data.schema import RawColumns, ProcessedColumns

    st.markdown("#### 대시보드 준비 상태")

    checks = [
        ("캐시 파일", True),
        ("작업자 Journey 데이터", not df.empty),
        ("생산성 지표", "active_ratio" in worker_df.columns),
        ("안전성 지표", "safety_fatigue_risk" in worker_df.columns),
        ("보정 데이터", "보정여부" in df.columns),
        ("시간 파생 컬럼", "시" in df.columns),
    ]
    for name, ok in checks:
        icon = "✅" if ok else "❌"
        color = Color.SAFE if ok else Color.DANGER
        st.markdown(
            f'<div style="padding:4px 0;color:{color};">{icon} &nbsp;{name}</div>',
            unsafe_allow_html=True,
        )

    st.markdown(f"""
    <div style="background:{Color.BG_MUTED};border-radius:10px;padding:1rem;margin-top:1rem;">
        <div style="font-weight:600;color:{Color.PRIMARY};margin-bottom:6px;">
            📌 다음 단계
        </div>
        <div style="font-size:0.88rem;color:{Color.TEXT_MUTED};line-height:1.8;">
            사이드바에서 <b>Overview / 작업자 상세 / 업체 분석 / 안전 알림</b> 탭을 선택하여
            인터랙티브 대시보드를 확인하세요.
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_batch_section(datafile_root: Path, cache_dir: Path, on_complete) -> None:
    """미처리 날짜 자동 감지 및 일괄 처리 UI."""
    from src.data.cache_manager import get_date_cache_status
    from src.data.loader import load_date_folder, get_folder_for_date
    from src.data.preprocessor import preprocess
    from src.data.cache_manager import ParquetCacheManager

    st.subheader("📦 일괄 처리")

    try:
        status_df = get_date_cache_status(datafile_root, cache_dir)
    except Exception as e:
        st.error(f"상태 조회 실패: {e}")
        return

    unprocessed = status_df[status_df["status"] == "needs_processing"]

    if unprocessed.empty:
        st.success("✅ 모든 날짜가 처리되었습니다.")

        st.markdown("**전체 날짜 현황**")
        _STATUS_ICON  = {"synced": "✅", "needs_processing": "⚙️", "cache_only": "⚠️"}
        _STATUS_LABEL = {"synced": "완료", "needs_processing": "미처리", "cache_only": "캐시만"}
        disp = status_df[["date", "status", "cache_rows"]].copy()
        disp["날짜"]   = disp["date"].apply(_fmt)
        disp["상태"]   = disp["status"].apply(lambda s: f"{_STATUS_ICON.get(s,'')} {_STATUS_LABEL.get(s,s)}")
        disp["캐시 행"] = disp["cache_rows"].apply(lambda v: f"{v:,}" if v >= 0 else "-")
        st.dataframe(disp[["날짜", "상태", "캐시 행"]], use_container_width=True, hide_index=True)
        return

    st.warning(f"미처리 날짜 {len(unprocessed)}개 발견")

    selected_batch = st.multiselect(
        "처리할 날짜 선택",
        options=unprocessed["date"].tolist(),
        default=unprocessed["date"].tolist(),
        format_func=_fmt,
        key="batch_date_select",
    )

    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        batch_clicked = st.button(
            f"▶ 선택 날짜 일괄 처리 ({len(selected_batch)}개)",
            type="primary" if selected_batch else "secondary",
            use_container_width=True,
            disabled=not selected_batch,
            key="batch_run_btn",
        )

    with col_info:
        st.markdown(f"""
        <div style="background:#EBF2FD;border-radius:10px;padding:0.7rem 1rem;
                    border-left:4px solid {Color.SECONDARY};font-size:0.88rem;">
            💡 선택한 {len(selected_batch)}개 날짜를 순차 처리합니다.
            처리 완료 후 <b>Trend</b> 탭에서 다중 날짜 분석이 가능합니다.
        </div>""", unsafe_allow_html=True)

    if batch_clicked and selected_batch:
        mgr = ParquetCacheManager(cache_dir)
        progress_bar = st.progress(0)
        status_box   = st.empty()

        # SSMP는 날짜에 무관하므로 한 번만 로드
        spatial_ctx = _load_spatial_context(datafile_root)

        for i, d in enumerate(selected_batch):
            status_box.markdown(
                f'<div style="font-weight:600;color:{Color.PRIMARY};">'
                f'처리 중: {_fmt(d)} ({i+1}/{len(selected_batch)})</div>',
                unsafe_allow_html=True,
            )
            try:
                folder = get_folder_for_date(datafile_root, d)
                if folder is None:
                    st.warning(f"폴더 없음: {d}")
                    continue
                raw_df = load_date_folder(folder)
                if raw_df is None or raw_df.empty:
                    st.warning(f"CSV 없음: {d}")
                    continue
                processed = preprocess(raw_df, spatial_ctx=spatial_ctx)
                mgr.save(processed, d)
            except Exception as e:
                st.error(f"{d} 처리 실패: {e}")

            progress_bar.progress((i + 1) / len(selected_batch))

        status_box.markdown(
            f'<div style="font-weight:600;color:{Color.SAFE};">✅ 일괄 처리 완료!</div>',
            unsafe_allow_html=True,
        )
        on_complete()
        st.rerun()


def _fmt(date_str: str) -> str:
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return date_str
