"""
디자인 테마 & Plotly 차트 설정.
모든 페이지에서 공유하는 색상, 폰트, 차트 스타일을 정의한다.
"""
from __future__ import annotations

import plotly.graph_objects as go

# ─── 브랜드 컬러 팔레트 ──────────────────────────────────────────────
class Color:
    # 주 색상
    PRIMARY       = "#1B3A6B"   # Deep Navy — 신뢰, 안정
    SECONDARY     = "#2E6FD9"   # Sky Blue  — 활동, 데이터
    ACCENT        = "#F5A623"   # Amber     — 인사이트, 강조
    ACCENT_LIGHT  = "#FFD580"   # Light Amber

    # 상태 색상
    SAFE          = "#27AE60"   # Green
    WARNING       = "#F39C12"   # Orange
    DANGER        = "#E74C3C"   # Red
    INFO          = "#2980B9"   # Blue

    # 배경
    BG_DARK       = "#0F1C33"   # Sidebar bg
    BG_CARD       = "#FFFFFF"   # Card bg
    BG_PAGE       = "#F4F6FA"   # Page bg
    BG_MUTED      = "#EEF2F7"   # Muted section bg

    # 텍스트
    TEXT_DARK     = "#1B2A4A"
    TEXT_MUTED    = "#6B7A99"
    TEXT_LIGHT    = "#FFFFFF"

    # 차트 시리즈 (순서대로 사용)
    SERIES = [
        "#2E6FD9", "#F5A623", "#27AE60", "#E74C3C",
        "#9B59B6", "#1ABC9C", "#E67E22", "#34495E",
    ]

    # 장소 유형별
    PLACE = {
        "HELMET_RACK": "#95A5A6",
        "REST":        "#27AE60",
        "OFFICE":      "#2E6FD9",
        "GATE":        "#F5A623",
        "OUTDOOR":     "#8BC34A",
        "INDOOR":      "#3F51B5",
        "UNKNOWN":     "#BDC3C7",
    }

    # 활동 유형별 (PERIOD_TYPE)
    PERIOD = {
        "work":    "#2E6FD9",
        "rest":    "#27AE60",
        "transit": "#F5A623",
        "off":     "#95A5A6",
    }

    # 6개 시간 카테고리
    TIME_CAT = {
        "high_work":     "#1A5276",
        "low_work":      "#5DADE2",
        "standby":       "#F5A623",
        "transit":       "#F7DC6F",
        "rest":          "#27AE60",  # 휴게 (초록)
        "rest_facility": "#27AE60",  # 휴게 시설 (별칭)
        "off_duty":      "#95A5A6",
    }


# ─── Plotly 기본 레이아웃 ────────────────────────────────────────────
_BASE_LAYOUT = dict(
    font=dict(family="Inter, Noto Sans KR, sans-serif", size=12, color="#1B2A4A"),
    paper_bgcolor="#FFFFFF",
    plot_bgcolor="#FAFBFD",
    margin=dict(l=16, r=16, t=44, b=16),
    legend=dict(
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="right", x=1,
        font=dict(size=11, color="#1B2A4A"),
        bgcolor="rgba(255,255,255,0.95)",
        bordercolor="#D0D7E8",
        borderwidth=1,
    ),
    colorway=Color.SERIES,
    xaxis=dict(
        gridcolor="#E4EAF4",
        linecolor="#C8D0E0",
        tickfont=dict(color="#3A4A6A", size=11),
        title_font=dict(color="#1B2A4A"),
        showgrid=True,
        zeroline=False,
        zerolinecolor="#C8D0E0",
    ),
    yaxis=dict(
        gridcolor="#E4EAF4",
        linecolor="#C8D0E0",
        tickfont=dict(color="#3A4A6A", size=11),
        title_font=dict(color="#1B2A4A"),
        showgrid=True,
        zeroline=False,
        zerolinecolor="#C8D0E0",
    ),
    hoverlabel=dict(
        bgcolor="#FFFFFF",
        bordercolor="#2E6FD9",
        font=dict(size=12, color="#1B2A4A"),
    ),
)

def apply_theme(fig: go.Figure, title: str = "", height: int = 340) -> go.Figure:
    """
    Plotly Figure에 표준 테마를 적용.

    Args:
        fig: Plotly Figure
        title: 차트 제목
        height: 차트 높이 (px)

    Returns:
        테마가 적용된 Figure
    """
    layout_update = dict(**_BASE_LAYOUT, height=height)
    if title:
        layout_update["title"] = dict(
            text=title,
            font=dict(size=14, color="#1B2A4A", family="Inter, Noto Sans KR", weight=600),
            x=0,
            xanchor="left",
            pad=dict(l=4),
        )
    fig.update_layout(**layout_update)

    # 모든 subplot 축에도 밝은 배경 강제 적용
    fig.update_xaxes(
        gridcolor="#E4EAF4",
        linecolor="#C8D0E0",
        tickfont=dict(color="#3A4A6A", size=11),
        title_font=dict(color="#1B2A4A"),
        zerolinecolor="#C8D0E0",
    )
    fig.update_yaxes(
        gridcolor="#E4EAF4",
        linecolor="#C8D0E0",
        tickfont=dict(color="#3A4A6A", size=11),
        title_font=dict(color="#1B2A4A"),
        zerolinecolor="#C8D0E0",
    )
    return fig


def get_gauge_color(value: float, thresholds: tuple = (0.6, 0.8)) -> str:
    """
    수치에 따라 게이지 색상 반환.

    Args:
        value: 0~1 사이 값
        thresholds: (danger_limit, warning_limit) 튜플

    Returns:
        색상 문자열
    """
    if value < thresholds[0]:
        return Color.DANGER
    elif value < thresholds[1]:
        return Color.WARNING
    return Color.SAFE


def get_risk_color(risk: float) -> str:
    """피로 위험도 수치에 따른 색상 반환."""
    if risk >= 1.0:
        return Color.DANGER
    elif risk >= 0.5:
        return Color.WARNING
    return Color.SAFE


# ─── Gantt 블록 색상 (v5: 짧은 블록 강조) ─────────────────────────────────

SHORT_BLOCK_THRESHOLD_MIN = 3  # 3분 이하 = 짧은 블록


def _darken(hex_color: str, factor: float) -> str:
    """
    hex 색상을 factor 비율로 어둡게.
    
    Args:
        hex_color: #RRGGBB 형식
        factor: 0~1 (1이면 완전 검정)
    
    Returns:
        어두워진 #RRGGBB 색상
    """
    if not hex_color or not hex_color.startswith("#") or len(hex_color) != 7:
        return hex_color
    
    try:
        r = int(hex_color[1:3], 16)
        g = int(hex_color[3:5], 16)
        b = int(hex_color[5:7], 16)
        r = int(r * (1 - factor))
        g = int(g * (1 - factor))
        b = int(b * (1 - factor))
        return f"#{r:02x}{g:02x}{b:02x}"
    except (ValueError, IndexError):
        return hex_color


def get_block_color(state: str, duration_min: int) -> dict:
    """
    Gantt 블록 색상 반환. 짧은 블록(≤3분)은 테두리를 진하게 하여 가시성 향상.
    
    Args:
        state: 활동 상태 (high_work, low_work, transit 등)
        duration_min: 블록 길이 (분)
    
    Returns:
        {"color": ..., "line": {"color": ..., "width": ...}}
    """
    base_color = Color.TIME_CAT.get(state, Color.TIME_CAT.get("off_duty", "#95A5A6"))
    
    if duration_min <= SHORT_BLOCK_THRESHOLD_MIN:
        # 짧은 블록: 동일 색상 + 진한 테두리
        return {
            "color": base_color,
            "line": {"color": _darken(base_color, 0.4), "width": 2}
        }
    return {
        "color": base_color,
        "line": {"color": base_color, "width": 0.5}
    }


# ─── 공통 CSS ──────────────────────────────────────────────────────────
GLOBAL_CSS = """
<style>
/* ── 전역 폰트 & 배경 ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', 'Noto Sans KR', sans-serif !important;
    color: #1B2A4A !important;
}

/* 메인 영역 배경 */
.main, .block-container, [data-testid="stAppViewContainer"] {
    background-color: #F4F6FA !important;
}
.main .block-container {
    padding-top: 1.2rem;
    padding-bottom: 2rem;
    max-width: 1400px;
}

/* Streamlit 기본 텍스트 색상 강제 */
p, span, label, div, h1, h2, h3, h4, h5, h6,
[data-testid="stMarkdownContainer"] *,
[data-testid="stText"] * {
    color: #1B2A4A;
}

/* ── 사이드바 ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0F1C33 0%, #1B2E50 100%) !important;
    border-right: 1px solid rgba(255,255,255,0.08);
}
[data-testid="stSidebar"] * { color: #C8D6E8 !important; }
[data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.12) !important; }
[data-testid="stSidebar"] .stButton button {
    background: rgba(46,111,217,0.25) !important;
    border: 1px solid rgba(46,111,217,0.5) !important;
    color: #FFFFFF !important;
    border-radius: 8px !important;
    transition: all 0.2s;
}
[data-testid="stSidebar"] .stButton button:hover {
    background: rgba(46,111,217,0.5) !important;
}

/* ── 사이드바 selectbox 선택된 값 텍스트 — 흰 배경이므로 검정색 강제 ── */
[data-testid="stSidebar"] [data-testid="stSelectbox"] > div > div,
[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] span,
[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] div[class],
[data-testid="stSidebar"] [data-baseweb="select"] [data-baseweb="select-option"],
[data-testid="stSidebar"] div[data-baseweb="select"] > div:first-child {
    color: #1B2A4A !important;
    background-color: #FFFFFF !important;
}
/* 선택된 값 span 텍스트 */
[data-testid="stSidebar"] [data-testid="stSelectbox"] span[class] {
    color: #1B2A4A !important;
}
/* selectbox 아이콘(화살표)은 어두운색 */
[data-testid="stSidebar"] [data-testid="stSelectbox"] svg {
    fill: #4A5A78 !important;
    color: #4A5A78 !important;
}

/* ── 카드 / 컨테이너 배경 ── */
[data-testid="stExpander"],
[data-testid="stForm"],
div[data-testid="column"] > div {
    background-color: #FFFFFF;
}

/* ── 메트릭 카드 ── */
[data-testid="stMetric"] {
    background: #FFFFFF !important;
    border: 1px solid #D8E2F0 !important;
    border-radius: 12px !important;
    padding: 1rem 1.2rem !important;
    box-shadow: 0 2px 6px rgba(27,42,74,0.07) !important;
}
[data-testid="stMetricLabel"] p,
[data-testid="stMetricLabel"] span,
[data-testid="stMetricLabel"] {
    font-size: 0.78rem !important;
    color: #4A5A78 !important;
    font-weight: 600 !important;
    letter-spacing: 0.3px !important;
    text-transform: uppercase !important;
}
[data-testid="stMetricValue"],
[data-testid="stMetricValue"] * {
    font-size: 1.8rem !important;
    font-weight: 700 !important;
    color: #1B2A4A !important;
}
[data-testid="stMetricDelta"] * { font-size: 0.82rem !important; }

/* ── Plotly 차트 컨테이너 ── */
.js-plotly-plot, .plotly, .plot-container {
    background: #FFFFFF !important;
    border-radius: 12px !important;
}
[data-testid="stPlotlyChart"] {
    background: #FFFFFF !important;
    border: 1px solid #D8E2F0 !important;
    border-radius: 12px !important;
    padding: 0.4rem !important;
    box-shadow: 0 2px 8px rgba(27,42,74,0.06) !important;
}

/* ── 데이터 테이블 ── */
[data-testid="stDataFrame"] {
    border-radius: 10px !important;
    overflow: hidden !important;
    border: 1px solid #D8E2F0 !important;
    box-shadow: 0 1px 4px rgba(27,42,74,0.05) !important;
}
[data-testid="stDataFrame"] * {
    color: #1B2A4A !important;
    background-color: #FFFFFF;
}
/* 테이블 헤더 */
[data-testid="stDataFrame"] thead th,
[data-testid="stDataFrame"] [data-testid="glideDataEditorContainer"] {
    background-color: #EEF3FA !important;
    color: #1B3A6B !important;
    font-weight: 600 !important;
}

/* ── selectbox / input ── */
[data-testid="stSelectbox"] > div > div,
[data-testid="stTextInput"] > div > div > input,
[data-testid="stNumberInput"] > div > div > input {
    background-color: #FFFFFF !important;
    color: #1B2A4A !important;
    border: 1px solid #C8D4E8 !important;
    border-radius: 8px !important;
}
[data-testid="stSelectbox"] label,
[data-testid="stTextInput"] label,
[data-testid="stNumberInput"] label {
    color: #1B2A4A !important;
    font-weight: 500 !important;
}

/* ── slider ── */
[data-testid="stSlider"] * { color: #1B2A4A !important; }

/* ── 토글 / 체크박스 라벨 ── */
[data-testid="stCheckbox"] label,
[data-testid="stToggle"] label,
[data-baseweb="checkbox"] span {
    color: #1B2A4A !important;
    font-weight: 500 !important;
}

/* ── expander ── */
[data-testid="stExpander"] {
    background: #FFFFFF !important;
    border: 1px solid #D8E2F0 !important;
    border-radius: 10px !important;
    margin-bottom: 0.5rem !important;
}
[data-testid="stExpander"] summary,
[data-testid="stExpander"] summary * {
    color: #1B2A4A !important;
    font-weight: 600 !important;
}

/* ── 사이드바 내 expander 버튼 (Admin 메뉴) ── */
[data-testid="stSidebar"] [data-testid="stExpander"] .stButton button,
[data-testid="stSidebar"] [data-testid="stExpander"] .stButton button *,
[data-testid="stSidebar"] [data-testid="stExpander"] .stButton button p,
[data-testid="stSidebar"] [data-testid="stExpander"] .stButton button span,
[data-testid="stSidebar"] [data-testid="stExpander"] button,
[data-testid="stSidebar"] [data-testid="stExpander"] button * {
    background: rgba(27, 58, 107, 0.1) !important;
    border: 1px solid rgba(27, 58, 107, 0.3) !important;
    color: #1B2A4A !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] .stButton button:hover,
[data-testid="stSidebar"] [data-testid="stExpander"] .stButton button:hover *,
[data-testid="stSidebar"] [data-testid="stExpander"] button:hover,
[data-testid="stSidebar"] [data-testid="stExpander"] button:hover * {
    background: rgba(27, 58, 107, 0.2) !important;
    color: #1B2A4A !important;
}
/* expander 내부 전체 텍스트 */
[data-testid="stSidebar"] [data-testid="stExpander"] * {
    color: #1B2A4A !important;
}

/* ── KPI 헤더 배너 ── */
.kpi-banner {
    background: linear-gradient(135deg, #1B3A6B 0%, #2E6FD9 100%);
    border-radius: 14px;
    padding: 1.4rem 1.8rem;
    margin-bottom: 1.2rem;
    color: white;
    box-shadow: 0 4px 16px rgba(27,58,107,0.25);
}
.kpi-banner h1, .kpi-banner h1 * { color: white !important; margin: 0; font-size: 1.5rem; }
.kpi-banner p,  .kpi-banner p  * { color: rgba(255,255,255,0.85) !important; margin: 0.2rem 0 0; font-size: 0.9rem; }

/* ── 섹션 카드 ── */
.section-card {
    background: #FFFFFF;
    border: 1px solid #D8E2F0;
    border-radius: 14px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 1rem;
    box-shadow: 0 2px 8px rgba(27,42,74,0.05);
}
.section-card h3, .section-card h3 * {
    color: #1B2A4A !important;
    font-size: 1rem;
    font-weight: 600;
    margin: 0 0 0.8rem;
    padding-bottom: 0.6rem;
    border-bottom: 2px solid #EEF2F7;
}

/* ── 파이프라인 스텝 카드 ── */
.step-card {
    background: #FFFFFF;
    border: 1px solid #D8E2F0;
    border-radius: 14px;
    padding: 1.6rem;
    text-align: center;
    box-shadow: 0 2px 10px rgba(27,42,74,0.06);
    transition: box-shadow 0.2s;
    height: 100%;
}
.step-card:hover { box-shadow: 0 4px 20px rgba(27,42,74,0.12); }
.step-card.active {
    border-color: #2E6FD9;
    box-shadow: 0 4px 20px rgba(46,111,217,0.15);
}
.step-card.done { border-color: #27AE60; }
.step-number {
    width: 44px; height: 44px;
    border-radius: 50%;
    background: linear-gradient(135deg, #2E6FD9, #1B3A6B);
    color: white;
    font-size: 1.1rem;
    font-weight: 700;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 0.8rem;
}
.step-number.done { background: linear-gradient(135deg, #27AE60, #1e8449); }
.step-title { font-size: 0.95rem; font-weight: 600; color: #1B2A4A; margin-bottom: 0.4rem; }
.step-desc  { font-size: 0.82rem; color: #4A5A78; line-height: 1.5; }
.step-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
    margin-top: 0.8rem;
}
.step-badge.ready   { background:#E3F6EC; color:#1e8449; }
.step-badge.pending { background:#EEF2F7; color:#4A5A78; }
.step-badge.running { background:#EBF2FD; color:#2E6FD9; }

/* ── 상태 뱃지 ── */
.status-badge {
    display: inline-block;
    padding: 3px 12px;
    border-radius: 20px;
    font-size: 0.78rem;
    font-weight: 600;
    letter-spacing: 0.2px;
}
.status-safe    { background:#E3F6EC; color:#1e8449; }
.status-warning { background:#FEF5E7; color:#c87c10; }
.status-danger  { background:#FDEDEC; color:#c0392b; }

/* ── 작업자 행 카드 ── */
.worker-row {
    display: flex;
    align-items: center;
    background: #FFFFFF;
    border: 1px solid #D8E2F0;
    border-radius: 10px;
    padding: 0.8rem 1.2rem;
    margin-bottom: 0.5rem;
    gap: 1rem;
}

/* ── 진행바 커스텀 ── */
[data-testid="stProgress"] > div > div {
    background: linear-gradient(90deg, #2E6FD9, #F5A623) !important;
    border-radius: 6px !important;
}

/* ── 탭 스타일 ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: #E8EDF6;
    padding: 4px;
    border-radius: 10px;
    border: 1px solid #D0D9EC;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px !important;
    font-weight: 500 !important;
    color: #4A5A78 !important;
    padding: 6px 16px !important;
    background: transparent !important;
}
.stTabs [aria-selected="true"] {
    background: #FFFFFF !important;
    color: #1B3A6B !important;
    font-weight: 600 !important;
    box-shadow: 0 1px 4px rgba(27,42,74,0.12) !important;
}

/* ── info / warning / error 박스 ── */
[data-testid="stInfo"],
[data-testid="stAlert"] {
    border-radius: 10px !important;
    border-left-width: 4px !important;
}
[data-testid="stInfo"] * { color: #1B3A6B !important; }

/* ── caption / small text ── */
[data-testid="stCaptionContainer"] * { color: #4A5A78 !important; }

/* ── 구분선 ── */
hr { border-color: #D8E2F0 !important; }
</style>
"""
