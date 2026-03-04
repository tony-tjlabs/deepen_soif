"""
SKEP DataAnalysis_Productivity — Streamlit 대시보드 메인 진입점.

실행: streamlit run main.py
파이프라인: Raw CSV → Journey 보정 → 지표 추출 → 캐시(.parquet) → 대시보드

UI 구조 (v2.0 — 2026-03-02):
  1. 🔍 Journey 검증   — Q1: 보정이 제대로 됐나?
  2. 📊 현장 분석      — Q2·Q3: 지표 + 맥락 (4개 서브탭)
  3. 🔮 확장 가능성    — Q4: 앞으로 뭘 더 할 수 있나?
  4. ⚙️ Admin          — 내부용 (기본 숨김)
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st

# 클라우드 배포 시 True. Admin(Pipeline/Journey Debug/공간 속성) 메뉴 숨김, CSV 없음 가정.
CLOUD_MODE = os.getenv("CLOUD_MODE", "false").lower() == "true"

ROOT = Path(__file__).parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 로컬 .env 로드 (APP_PASSWORD, ANTHROPIC_API_KEY 등)
try:
    from dotenv import load_dotenv
    _env_path = ROOT / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

from src.data.cache_manager import (
    ParquetCacheManager,
    load_multi_date_cache,
    load_analytics_or_compute,
)
from src.data.loader import scan_data_folders_with_sector
from src.utils.theme import GLOBAL_CSS, Color
from src.utils.time_utils import extract_date_from_folder
from src.utils.llm_interpreter import get_llm_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATAFILE_ROOT = ROOT / "Datafile"
CACHE_DIR     = ROOT / "cache"


def _get_app_password() -> Optional[str]:
    """앱 비밀번호 조회. 코드에 비밀번호를 두지 않음. Streamlit Cloud: Secrets에 APP_PASSWORD 설정."""
    try:
        p = st.secrets.get("APP_PASSWORD")
        if p and str(p).strip():
            return str(p).strip()
    except (FileNotFoundError, KeyError):
        pass
    p = os.getenv("APP_PASSWORD", "").strip()
    return p if p else None


# ── 페이지 설정 ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SKEP Productivity & Safety Dashboard",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


# ── 캐시 함수 ──────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_cached_data(date_str: str, sector: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Sector별 캐시 로드. sector 없으면 기존 형식(processed_YYYYMMDD) 사용."""
    return ParquetCacheManager(CACHE_DIR).load(date_str, sector or None)


@st.cache_data(show_spinner=False, ttl=300)
def load_multi_date(dates_tuple: tuple, sector: Optional[str] = None) -> pd.DataFrame:
    return load_multi_date_cache(list(dates_tuple), CACHE_DIR, sector)


@st.cache_data(show_spinner=False)
def load_analytics_cached(date_str: str, sector: Optional[str] = None):
    """분석 캐시 로드. sector 있으면 해당 Sector 캐시."""
    return ParquetCacheManager(CACHE_DIR).load_analytics(date_str, sector or None)


def get_analytics(date_str: str, df: pd.DataFrame, sector: Optional[str] = None) -> dict:
    """분석 결과 반환: 캐시 있으면 로드, 없으면 계산. Sector별 항목 구분."""
    a = load_analytics_cached(date_str, sector)
    if a is not None:
        return a
    return load_analytics_or_compute(ParquetCacheManager(CACHE_DIR), date_str, df, sector)


@st.cache_data(show_spinner=False)
def get_cache_entries() -> list[tuple[str, str]]:
    """(sector, date_str) 목록. sector ''는 legacy(Y1 호환)."""
    return ParquetCacheManager(CACHE_DIR).get_available_entries()


@st.cache_data(show_spinner=False)
def get_raw_entries() -> list[tuple[str, str]]:
    """
    배포(Release) 빌드에서는 Raw 폴더를 사용하지 않는다.
    데이터 소스는 항상 cache/processed_*.parquet 기준으로만 동작한다.
    """
    return []


def _fmt(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d) == 8 else d


def _fmt_sector_date(sector: str, date_str: str, cached: bool) -> str:
    s = sector if sector else "Y1"
    return f"{s} · {_fmt(date_str)}" + (" ✓" if cached else "  (미처리)")


def _invalidate_cache() -> None:
    load_cached_data.clear()
    load_multi_date.clear()
    get_cache_entries.clear()
    get_raw_entries.clear()
    load_analytics_cached.clear()


# ── 사이드바 ────────────────────────────────────────────────────────────
def render_sidebar() -> Tuple[Optional[str], str]:
    with st.sidebar:
        # 로고 / 브랜드
        st.markdown("""
        <div style="padding:1rem 0.4rem 0.6rem;">
            <div style="font-size:1.35rem;font-weight:800;color:#FFFFFF;letter-spacing:-0.5px;">
                🏗️ SKEP Analytics
            </div>
            <div style="font-size:0.78rem;color:#8AAEC8;margin-top:2px;">
                SK하이닉스 용인 건설현장 · Y-Project
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.divider()

        # Sector · 날짜 선택 (Y1 / M15X 등 Sector별 장소·항목 구분)
        st.markdown('<div style="font-size:0.75rem;color:#8AAEC8;font-weight:600;letter-spacing:0.5px;margin-bottom:6px;">SECTOR · DATE</div>', unsafe_allow_html=True)

        cache_entries = get_cache_entries()
        cache_set     = set(cache_entries)

        # 배포 버전: 항상 캐시 기준으로만 Sector·날짜 목록 생성
        all_entries = sorted(set(cache_entries), key=lambda x: (x[0], x[1]), reverse=True)
        if not all_entries:
            st.warning("데이터 없음")
            st.caption("cache/ 폴더에 processed_*.parquet와 analytics_* 파일을 포함해 주세요.")
            return None, "pipeline"

        def _label(entry: tuple) -> str:
            s, d = entry
            cached = (s or "", d) in cache_set or (s in (None, "", "Y1") and ("", d) in cache_set)
            return _fmt_sector_date(s or "Y1", d, cached)

        choice = st.selectbox(
            "Sector · 날짜",
            options=all_entries,
            format_func=_label,
            label_visibility="collapsed",
            key="sector_date_select",
        )
        selected_sector = (choice[0] or None) if choice else None
        selected_date   = choice[1] if choice else None
        st.session_state["selected_sector"] = selected_sector

        st.divider()

        # 네비게이션 (4개 메뉴로 압축)
        st.markdown('<div style="font-size:0.75rem;color:#8AAEC8;font-weight:600;letter-spacing:0.5px;margin-bottom:8px;">NAVIGATION</div>', unsafe_allow_html=True)

        NAV_MAIN = {
            "journey_verify":       ("🔍", "Journey 검증",    "보정 로직 검증"),
            "site_analysis":        ("📊", "현장 분석",       "현장 전체 Overview"),
            "productivity_analysis":("📈", "생산성 분석",     "EWI 기반 생산성"),
            "safety_analysis":      ("🛡️", "안전성 분석",     "CRE 기반 안전성"),
            "future_preview":       ("🔮", "확장 가능성",     "향후 기능 로드맵"),
        }

        if "page" not in st.session_state:
            st.session_state.page = "journey_verify"

        for key, (icon, label, desc) in NAV_MAIN.items():
            if st.sidebar.button(
                f"{icon}  {label}",
                key=f"nav_{key}",
                use_container_width=True,
            ):
                st.session_state.page = key
                st.rerun()

        st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

        if not CLOUD_MODE:
            with st.expander("⚙️ Admin", expanded=False):
                admin_pages = {
                    "pipeline":       ("⚙️", "Pipeline",        "데이터 처리"),
                    "journey_debug":  ("🔧", "Journey Debug",   "보정 상세 디버그"),
                    "space_config":   ("🗺️", "공간 속성",        "장소별 기능/위험도"),
                }
                for key, (icon, label, desc) in admin_pages.items():
                    if st.button(
                        f"{icon}  {label}",
                        key=f"nav_{key}",
                        use_container_width=True,
                    ):
                        st.session_state.page = key
                        st.rerun()

        st.divider()

        # 캐시 정보 (Sector·날짜 일치 시, legacy 캐시는 Y1으로 인정)
        cache_set = set(cache_entries)
        _cached = (selected_sector or "", selected_date) in cache_set or (
            selected_sector in (None, "", "Y1") and ("", selected_date) in cache_set
        )
        if _cached:
            mgr = ParquetCacheManager(CACHE_DIR)
            for info in mgr.get_cache_info():
                if info["date"] == selected_date and (info.get("sector") or "Y1") == (selected_sector or "Y1"):
                    st.markdown(f"""
                    <div style="background:rgba(39,174,96,0.12);border-radius:8px;padding:0.6rem 0.8rem;">
                        <div style="font-size:0.75rem;color:#8AAEC8;font-weight:600;">CACHE</div>
                        <div style="font-size:0.82rem;color:#C8D6E8;">
                            {info['row_count']:,}행 · {info['size_kb']} KB
                        </div>
                    </div>""", unsafe_allow_html=True)
                    break
        
        # LLM 상태 표시
        llm_status = get_llm_status()
        if llm_status["ready"]:
            llm_color = "rgba(39,174,96,0.12)"
            llm_icon = "✅"
            llm_label = "AI 해석 활성화"
        else:
            llm_color = "rgba(241,196,15,0.15)"
            llm_icon = "⚠️"
            llm_label = "AI 해석 비활성화"
        
        st.markdown(f"""
        <div style="background:{llm_color};border-radius:8px;padding:0.5rem 0.8rem;margin-top:8px;">
            <div style="font-size:0.75rem;color:#8AAEC8;font-weight:600;">🧠 CLAUDE API</div>
            <div style="font-size:0.78rem;color:#C8D6E8;">
                {llm_icon} {llm_label}
            </div>
        </div>""", unsafe_allow_html=True)
        
        if not llm_status["ready"]:
            with st.expander("❓ API 설정 방법", expanded=False):
                st.markdown("""
                1. [Anthropic Console](https://console.anthropic.com/)에서 API 키 발급
                2. 프로젝트 루트의 `.env` 파일 열기
                3. `ANTHROPIC_API_KEY=sk-ant-...` 형식으로 실제 키 입력
                4. 대시보드 새로고침
                """, unsafe_allow_html=True)

        # TJLABS 저작권 및 버전 정보
        st.markdown("""
        <div style="margin-top:2rem;padding:0.8rem;border-top:1px solid #3A4A5A;">
            <div style="font-size:0.68rem;color:#6A8AAA;text-align:center;line-height:1.5;">
                Designed, Developed & Deployed by<br>
                <span style="font-weight:700;color:#8AAEC8;letter-spacing:0.5px;">TJLABS</span><br>
                <span style="font-size:0.62rem;color:#5A7A9A;">
                    All Rights Reserved © 2026
                </span>
            </div>
            <div style="font-size:0.65rem;color:#4A6A8A;text-align:center;margin-top:6px;">
                v1.0 · Post-Processing Analytics
            </div>
        </div>""", unsafe_allow_html=True)

    return selected_date, st.session_state.get("page", "pipeline")


# ── 메인 ────────────────────────────────────────────────────────────────
def main() -> None:
    # ── 비밀번호 인증 (첫 접속 시) ─────────────────────────────────────
    if not st.session_state.get("auth_ok", False):
        expected = _get_app_password()
        if not expected:
            st.error(
                "비밀번호가 설정되지 않았습니다. "
                "로컬: `.streamlit/secrets.toml` 또는 환경변수 `APP_PASSWORD` 설정. "
                "Streamlit Cloud: 앱 설정 → Secrets에 `APP_PASSWORD` 추가."
            )
            st.stop()
        st.markdown("""
        <div style="max-width:420px;margin:4rem auto;padding:2rem;background:#F8FAFC;border-radius:12px;border:1px solid #E2E8F0;">
            <div style="font-size:1.1rem;font-weight:600;color:#1E293B;margin-bottom:0.5rem;">🏗️ SKEP 대시보드</div>
            <div style="font-size:0.85rem;color:#64748B;">비밀번호를 입력하세요.</div>
        </div>
        """, unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            pw = st.text_input("비밀번호", type="password", key="app_password", label_visibility="collapsed", placeholder="비밀번호")
            if st.button("입장", type="primary", use_container_width=True):
                if pw == expected:
                    st.session_state["auth_ok"] = True
                    st.rerun()
                else:
                    st.error("비밀번호가 올바르지 않습니다.")
        st.stop()

    selected_date, selected_page = render_sidebar()
    selected_sector = st.session_state.get("selected_sector")

    if selected_date is None:
        _render_landing()
        return

    cache_entries_set = set(get_cache_entries())

    def _is_cached(sector: Optional[str], date_str: str) -> bool:
        if ((sector or "", date_str) in cache_entries_set):
            return True
        if sector in (None, "", "Y1") and ("", date_str) in cache_entries_set:
            return True  # legacy 캐시(파일명에 sector 없음)를 Y1으로 인정
        return False

    # ── Admin: Pipeline (CLOUD_MODE에서는 노출 안 함) ─────────────────
    if selected_page == "pipeline" and not CLOUD_MODE:
        from src.pages.pipeline import render as render_pipeline
        render_pipeline(selected_date, DATAFILE_ROOT, CACHE_DIR, _invalidate_cache, sector=selected_sector)
        return

    # ── 캐시 미준비 시 → 파이프라인으로 안내 (Sector·날짜 기준) ───────
    if not _is_cached(selected_sector, selected_date):
        _render_need_process(selected_date)
        return

    # ── 단일 날짜 데이터 로드 (Sector별 캐시) ──────────────────────────
    with st.spinner("데이터 로드 중..."):
        df = load_cached_data(selected_date, selected_sector)

    if df is None or df.empty:
        st.error("캐시 로드 실패. 파이프라인을 다시 실행해 주세요.")
        return

    # ── 멀티 날짜 데이터 (현장 분석 추이 등, 동일 Sector만) ─────────────
    mgr = ParquetCacheManager(CACHE_DIR)
    sector_dates = sorted(
        [d for s, d in get_cache_entries() if (s or "") == (selected_sector or "")],
        reverse=True,
    )
    date_range = st.session_state.get("date_range_selection")
    if date_range and len(date_range) == 2:
        dates_in_range = [d for d in sector_dates if date_range[0] <= d <= date_range[1]]
    else:
        dates_in_range = [selected_date]  # 단일 날짜만 사용 (범위 선택 UI는 추후 추가 가능)

    df_multi = None
    if len(dates_in_range) > 1:
        with st.spinner("멀티 날짜 데이터 로드 중..."):
            df_multi = load_multi_date(tuple(dates_in_range), selected_sector)

    # ── 페이지 라우팅 (5개 메인 + 3개 Admin) ────────────────────────
    get_analytics_fn = lambda date_str, df: get_analytics(date_str, df, selected_sector)

    if selected_page == "journey_verify":
        from src.pages.journey_verify import render as render_journey_verify
        render_journey_verify(df)

    elif selected_page == "site_analysis":
        from src.pages.site_analysis import render as render_site_analysis
        render_site_analysis(df, df_multi, CACHE_DIR, DATAFILE_ROOT, selected_date, get_analytics_fn)

    elif selected_page == "productivity_analysis":
        from src.pages.productivity_analysis import render as render_productivity
        render_productivity(df, selected_date, get_analytics_fn)

    elif selected_page == "safety_analysis":
        from src.pages.safety_analysis import render as render_safety
        render_safety(df, selected_date, get_analytics_fn)

    elif selected_page == "future_preview":
        from src.pages.future_preview import render as render_future_preview
        render_future_preview(df)

    elif selected_page == "journey_debug" and not CLOUD_MODE:
        from src.pages.journey_review import render as render_journey_debug
        render_journey_debug(df)

    elif selected_page == "space_config" and not CLOUD_MODE:
        from src.pages.space_config import render as render_space_config
        render_space_config(df, DATAFILE_ROOT)

    else:
        from src.pages.journey_verify import render as render_journey_verify
        render_journey_verify(df)


def _render_landing() -> None:
    st.markdown("""
    <div class="kpi-banner" style="margin-bottom:2rem;">
        <h1>🏗️ SKEP Productivity & Safety Dashboard</h1>
        <p>SK하이닉스 용인 반도체 클러스터 건설현장 · 작업자 생산성 & 안전성 분석 플랫폼</p>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    cards = [
        ("🔍", "Journey 검증", "Q1: 보정이 제대로 됐나?"),
        ("📊", "현장 분석", "Q2·Q3: 지표 + 맥락"),
        ("🔮", "확장 가능성", "Q4: 앞으로 뭘 더 할 수 있나?"),
    ]
    for col, (icon, title, desc) in zip([c1, c2, c3], cards):
        with col:
            st.markdown(f"""
            <div class="step-card">
                <div style="font-size:2rem;margin-bottom:0.6rem;">{icon}</div>
                <div class="step-title">{title}</div>
                <div class="step-desc">{desc}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.info("👈 사이드바에서 Datafile을 추가한 후 날짜를 선택하세요.")


def _render_need_process(date_str: str) -> None:
    st.markdown(f"""
    <div class="kpi-banner">
        <h1>⚙️ 데이터 처리 필요</h1>
        <p>{_fmt(date_str)} · Raw CSV가 감지되었으나 캐시 파일이 없습니다.</p>
    </div>""", unsafe_allow_html=True)

    st.markdown("""
    <div style="background:#EBF2FD;border-radius:12px;padding:1.2rem 1.4rem;margin-top:1rem;">
        <div style="font-weight:600;color:#1B3A6B;margin-bottom:8px;">파이프라인 실행이 필요합니다</div>
        <ol style="color:#2E6FD9;margin:0;padding-left:1.2rem;line-height:2;">
            <li>왼쪽 사이드바에서 <b>⚙️ Pipeline</b> 탭 클릭</li>
            <li><b>▶ 파이프라인 실행</b> 버튼 클릭</li>
            <li>처리 완료 후 각 분석 탭 확인</li>
        </ol>
    </div>""", unsafe_allow_html=True)

    if st.button("⚙️ Pipeline 페이지로 이동", type="primary"):
        st.session_state.page = "pipeline"
        st.rerun()


if __name__ == "__main__":
    main()
