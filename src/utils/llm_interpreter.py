"""
Claude API 기반 LLM 해석 레이어.

보정 완료된 집계 데이터(숫자)를 받아 자연어 내러티브를 생성.
Journey 보정 자체는 rule-based 유지 — LLM은 '해석'에만 사용.

사용처:
  - site_analysis.py: 작업자별 탭 상단 요약 카드
  - site_analysis.py: 현장 전체 탭 일일 요약
  - site_analysis.py: 이상 패턴 설명 (anomaly_flag)
"""

from __future__ import annotations
import os
import logging

logger = logging.getLogger(__name__)

# anthropic 패키지 선택적 임포트 (미설치 시 graceful fallback)
try:
    import anthropic
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _ANTHROPIC_AVAILABLE = False
    logger.info("anthropic 패키지 미설치 — LLM 해석 비활성화, rule-based fallback 사용")

# dotenv: 로컬 폴백용 (클라우드에서는 st.secrets만 사용)
from pathlib import Path

try:
    from dotenv import load_dotenv
    _DOTENV_AVAILABLE = True
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    _ENV_PATH = _PROJECT_ROOT / ".env"
except ImportError:
    _DOTENV_AVAILABLE = False
    _ENV_PATH = None

# Streamlit 선택적 임포트
try:
    import streamlit as st
    _STREAMLIT_AVAILABLE = True
except ImportError:
    _STREAMLIT_AVAILABLE = False


# ── 모델 설정 ────────────────────────────────────────────────────────────────
_MODEL = "claude-sonnet-4-5"
_MAX_TOKENS = 400
_TEMPERATURE = 0.3


# ── API 클라이언트 ────────────────────────────────────────────────────────────

def _get_api_key() -> str | None:
    """API 키 조회. 1순위: st.secrets(클라우드), 2순위: .env/환경변수(로컬)."""
    # 1순위: Streamlit Secrets (클라우드 배포 시 동작)
    if _STREAMLIT_AVAILABLE:
        try:
            secret_key = st.secrets.get("ANTHROPIC_API_KEY")
            if secret_key and "여기에" not in str(secret_key):
                return secret_key
        except (FileNotFoundError, KeyError, Exception):
            pass

    # 2순위: .env (로컬, 경로 명시로 cwd 독립)
    if _DOTENV_AVAILABLE and _ENV_PATH and _ENV_PATH.exists():
        load_dotenv(_ENV_PATH)
    key = os.getenv("ANTHROPIC_API_KEY")
    if key and "여기에" not in str(key):
        return key
    return None


def get_llm_status() -> dict:
    """LLM 연결 상태 진단 정보 반환."""
    status = {
        "anthropic_installed": _ANTHROPIC_AVAILABLE,
        "api_key_configured": False,
        "api_key_source": None,
        "ready": False,
        "message": "",
    }
    
    if not _ANTHROPIC_AVAILABLE:
        status["message"] = "anthropic 패키지가 설치되지 않았습니다."
        return status

    # API 키 확인 (_get_api_key와 동일 순서: st.secrets → .env)
    api_key = _get_api_key()
    if api_key:
        status["api_key_configured"] = True
        status["ready"] = True
        status["message"] = "Claude API 연결 준비 완료"
        if _STREAMLIT_AVAILABLE:
            try:
                if st.secrets.get("ANTHROPIC_API_KEY") and "여기에" not in str(st.secrets.get("ANTHROPIC_API_KEY", "")):
                    status["api_key_source"] = "Streamlit secrets"
                else:
                    status["api_key_source"] = ".env / 환경변수"
            except Exception:
                status["api_key_source"] = ".env / 환경변수"
        else:
            status["api_key_source"] = ".env / 환경변수"
        return status

    status["message"] = "ANTHROPIC_API_KEY가 설정되지 않았습니다. (Streamlit Cloud: Secrets에 추가)"
    return status


def _get_client():
    """Anthropic 클라이언트 반환. 키 없거나 패키지 없으면 None."""
    if not _ANTHROPIC_AVAILABLE:
        return None
    
    api_key = _get_api_key()
    if not api_key:
        return None
    
    return anthropic.Anthropic(api_key=api_key)


def _call(prompt: str, max_tokens: int = _MAX_TOKENS) -> str | None:
    """
    Claude API 단일 호출. 실패 시 None 반환 (UI는 fallback 처리).
    """
    client = _get_client()
    if client is None:
        return None
    
    try:
        # UTF-8 인코딩 보장 (ASCII 인코딩 에러 방지)
        if isinstance(prompt, bytes):
            prompt = prompt.decode('utf-8')
        else:
            prompt = str(prompt).encode('utf-8').decode('utf-8')
        
        msg = client.messages.create(
            model=_MODEL,
            max_tokens=max_tokens,
            temperature=_TEMPERATURE,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except UnicodeEncodeError as e:
        logger.warning(f"Claude API 인코딩 오류: {e}")
        if _STREAMLIT_AVAILABLE:
            st.warning("AI 해석 생성 실패: 텍스트 인코딩 오류")
        return None
    except Exception as e:
        error_msg = str(e)
        logger.warning(f"Claude API 호출 실패: {error_msg}")
        if _STREAMLIT_AVAILABLE:
            # 에러 메시지도 안전하게 처리
            try:
                st.warning(f"AI 해석 생성 실패: {error_msg}")
            except UnicodeEncodeError:
                st.warning("AI 해석 생성 실패 (상세 내용 표시 불가)")
        return None


def is_llm_available() -> bool:
    """LLM 기능 사용 가능 여부 확인."""
    return _ANTHROPIC_AVAILABLE and _get_api_key() is not None


# ── 1. 작업자 Journey 일일 내러티브 ──────────────────────────────────────────

def generate_worker_narrative(summary: dict, worker_name: str) -> str:
    """
    작업자 하루 집계 데이터 → 자연어 내러티브.

    summary 필수 키:
        onsite_hours    : float  현장 체류 시간
        ewi             : float  유효작업집중도 (0~1)
        high_work_min   : int    고활성 작업 분
        low_work_min    : int    저활성 작업 분
        standby_min     : int    현장 대기 분
        transit_min     : int    이동 분
        rest_min        : int    휴식 분
        main_zones      : list   주요 작업 구역 이름 목록
        journey_pattern : str    zone_fixed / zone_cycle / explorer
        anomalies       : list   이상 패턴 설명 문자열 목록
        date            : str    날짜 (YYYY-MM-DD)

    반환: 자연어 내러티브 (실패 시 rule-based fallback)
    """
    # 안전한 타입 변환
    def safe_float(val, default=0.0):
        try:
            return float(val) if val is not None else default
        except (TypeError, ValueError):
            return default
    
    def safe_int(val, default=0):
        try:
            return int(val) if val is not None else default
        except (TypeError, ValueError):
            return default
    
    onsite_hours = safe_float(summary.get('onsite_hours', 0))
    ewi = safe_float(summary.get('ewi', 0))
    high_work_min = safe_int(summary.get('high_work_min', 0))
    low_work_min = safe_int(summary.get('low_work_min', 0))
    standby_min = safe_int(summary.get('standby_min', 0))
    transit_min = safe_int(summary.get('transit_min', 0))
    rest_min = safe_int(summary.get('rest_min', 0))
    main_zones = summary.get('main_zones', []) or []
    journey_pattern = summary.get('journey_pattern', 'unknown') or 'unknown'
    anomalies = summary.get('anomalies', []) or []
    date_str = summary.get('date', '') or ''
    
    prompt = f"""
당신은 건설현장 작업 분석 전문가입니다.
BLE 센서로 수집한 1분 단위 위치/활동 기록을 분석한 결과를 바탕으로
작업자의 하루 활동을 자연스럽게 설명해주세요.

[작업자 정보]
이름: {worker_name}
날짜: {date_str}

[오늘 하루 요약]
- 현장 체류: {onsite_hours:.1f}시간
- 유효작업집중도(EWI): {ewi*100:.0f}%
  (EWI = 고활성×1.0 + 저활성×0.5 / 총 체류시간. 높을수록 생산적)
- 고활성 작업 (활발한 신체 활동): {high_work_min}분
- 저활성 작업 (감독·측량·정밀작업): {low_work_min}분
- 현장 대기 (자재·지시·장비 대기 추정): {standby_min}분
- 이동: {transit_min}분
- 휴식: {rest_min}분
- 주요 작업 구역: {', '.join(main_zones) if main_zones else '정보 없음'}
- 이동 패턴: {journey_pattern}
- 특이사항: {'; '.join(anomalies) if anomalies else '없음'}

[작성 규칙]
1. 오늘 하루 흐름을 2~3문장으로 자연스럽게 서술 (숫자 나열 금지)
2. 주목할 점이 있으면 1문장 추가
3. 필요하면 내일을 위한 짧은 제안 1문장
4. 전체 4~5문장 이내
5. 한국어, 전문적이지만 읽기 쉬운 톤
6. "데이터에 따르면" 같은 불필요한 서두 없이 바로 시작
""".strip()

    result = _call(prompt, max_tokens=300)
    if result:
        return result
    
    return _fallback_worker_narrative(summary, worker_name)


def _fallback_worker_narrative(summary: dict, worker_name: str) -> str:
    """API 실패 시 rule-based fallback."""
    try:
        ewi = float(summary.get('ewi', 0) or 0)
    except (TypeError, ValueError):
        ewi = 0.0
    
    ewi_label = (
        "높은 집중도" if ewi >= 0.6 else
        "보통 집중도" if ewi >= 0.4 else
        "낮은 집중도"
    )
    zones = summary.get('main_zones', []) or []
    zones_str = ', '.join(zones[:2]) if zones else '현장 전역'
    
    try:
        onsite = float(summary.get('onsite_hours', 0) or 0)
    except (TypeError, ValueError):
        onsite = 0.0
    
    return (
        f"{worker_name} 작업자는 오늘 {onsite:.1f}시간 현장에 체류하며 "
        f"{zones_str}에서 주로 활동했습니다. "
        f"유효작업집중도(EWI) {ewi*100:.0f}%로 {ewi_label}를 보였습니다."
    )


# ── 2. 현장 전체 일일 요약 ────────────────────────────────────────────────────

def generate_site_daily_summary(site_summary: dict, date_str: str) -> str:
    """
    현장 전체 일일 지표 → 자연어 요약 (Overview 화면 상단 표시).

    site_summary 필수 키:
        worker_count      : int   총 작업자 수
        avg_ewi           : float 평균 EWI
        total_standby_min : int   전체 대기 분 합산
        anomaly_count     : int   이상 패턴 발생 건수
        top_zones         : list  오늘 가장 활발한 구역 이름 목록
        prev_avg_ewi      : float 이전 기간 평균 EWI (없으면 None)
    """
    prev_compare = ""
    prev_ewi = site_summary.get('prev_avg_ewi')
    avg_ewi = site_summary.get('avg_ewi', 0)
    
    # 숫자 타입 변환 (문자열로 들어올 수 있음)
    try:
        avg_ewi = float(avg_ewi) if avg_ewi is not None else 0.0
    except (ValueError, TypeError):
        avg_ewi = 0.0
    
    if prev_ewi is not None:
        try:
            prev_ewi = float(prev_ewi)
        except (ValueError, TypeError):
            prev_ewi = None
    
    if prev_ewi is not None:
        diff = avg_ewi - prev_ewi
        direction = "상승" if diff > 0 else "하락"
        prev_compare = f"지난 평균 대비 {abs(diff)*100:.0f}%p {direction}."

    top_zones = site_summary.get('top_zones', [])
    top_zones_str = ', '.join(top_zones) if top_zones else '전체 현장'

    prompt = f"""
당신은 건설현장 운영 분석 전문가입니다.
오늘 현장 전체 데이터를 바탕으로 간결한 현황 요약을 작성해주세요.

[오늘 현장 현황] {date_str}
- 총 작업자: {site_summary.get('worker_count', 0)}명
- 평균 유효작업집중도(EWI): {avg_ewi*100:.0f}% {prev_compare}
- 전체 대기 시간 합산: {site_summary.get('total_standby_min', 0)}분
- 이상 패턴 감지: {site_summary.get('anomaly_count', 0)}건
- 주요 활동 구역: {top_zones_str}

[작성 규칙]
1. 오늘 현장 전반 상황 1~2문장
2. 주목할 수치나 이슈 1문장
3. 필요시 운영 제안 1문장
4. 전체 3~4문장, 한국어, 간결하고 실용적인 톤
""".strip()

    result = _call(prompt, max_tokens=250)
    if result:
        return result
    
    return _fallback_site_summary(site_summary, date_str)


def _fallback_site_summary(site_summary: dict, date_str: str) -> str:
    """API 실패 시 rule-based fallback."""
    try:
        avg_ewi = float(site_summary.get('avg_ewi', 0) or 0)
    except (TypeError, ValueError):
        avg_ewi = 0.0
    try:
        worker_count = int(site_summary.get('worker_count', 0) or 0)
    except (TypeError, ValueError):
        worker_count = 0
    try:
        standby_min = int(site_summary.get('total_standby_min', 0) or 0)
    except (TypeError, ValueError):
        standby_min = 0
    try:
        anomaly_count = int(site_summary.get('anomaly_count', 0) or 0)
    except (TypeError, ValueError):
        anomaly_count = 0
    
    return (
        f"오늘 {worker_count}명이 현장에 투입되어 "
        f"평균 유효작업집중도 {avg_ewi*100:.0f}%를 기록했습니다. "
        f"총 {standby_min}분의 대기가 발생했으며 "
        f"{anomaly_count}건의 이상 패턴이 감지되었습니다."
    )


# ── 3. 이상 패턴 설명 ─────────────────────────────────────────────────────────

def generate_anomaly_explanation(anomaly: dict) -> str:
    """
    anomaly_flag 이벤트 → 자연어 설명 (Safety Alert 화면).

    anomaly 필수 키:
        worker_name   : str   작업자 이름
        anomaly_type  : str   abnormal_stop / gate_congestion / lone_hazard 등
        space_name    : str   발생 장소명
        space_function: str   WORK / WORK_HAZARD / TRANSIT_GATE 등
        duration_min  : int   지속 시간 (분)
        hour          : int   발생 시간 (시)
        hazard_weight : float 공간 위험 가중치 (0~1)
        active_ratio  : float 발생 당시 활성비율
    """
    type_desc_map = {
        "abnormal_stop":    "장시간 비활성 정지",
        "gate_congestion":  "게이트 대기 병목",
        "lone_hazard":      "단독 작업 위험",
        "transit_idle":     "이동 경로 장시간 정체",
        "standby_excess":   "작업 구역 과도한 대기",
    }
    
    anomaly_type = anomaly.get('anomaly_type', 'unknown')
    type_desc = type_desc_map.get(anomaly_type, anomaly_type)
    
    # 안전한 타입 변환
    try:
        hazard_weight = float(anomaly.get('hazard_weight', 0) or 0)
    except (TypeError, ValueError):
        hazard_weight = 0.0
    try:
        active_ratio = float(anomaly.get('active_ratio', 0) or 0)
    except (TypeError, ValueError):
        active_ratio = 0.0
    try:
        hour = int(anomaly.get('hour', 0) or 0)
    except (TypeError, ValueError):
        hour = 0
    try:
        duration_min = int(anomaly.get('duration_min', 0) or 0)
    except (TypeError, ValueError):
        duration_min = 0
    
    prompt = f"""
건설현장 안전 분석 결과를 현장 관리자가 이해하기 쉽게 설명해주세요.

[이상 패턴 감지]
- 유형: {type_desc}
- 작업자: {anomaly.get('worker_name', '알 수 없음')}
- 발생 장소: {anomaly.get('space_name', '알 수 없음')} (공간유형: {anomaly.get('space_function', 'UNKNOWN')})
- 발생 시간: {hour}시
- 지속 시간: {duration_min}분
- 공간 위험도: {hazard_weight*10:.0f}/10
- 당시 활성비율: {active_ratio*100:.0f}%

[작성 규칙]
1. 무슨 상황인지 1문장으로 명확하게
2. 가능한 원인 1~2가지 간략히
3. 권장 조치 1문장
4. 전체 3문장, 한국어, 실용적 톤
5. 과도한 경고 표현 자제 (팩트 중심)
""".strip()

    result = _call(prompt, max_tokens=200)
    if result:
        return result
    
    return _fallback_anomaly_explanation(anomaly)


def _fallback_anomaly_explanation(anomaly: dict) -> str:
    """API 실패 시 rule-based fallback."""
    type_desc_map = {
        "abnormal_stop":    "장시간 비활성 정지",
        "gate_congestion":  "게이트 대기 병목",
        "lone_hazard":      "단독 작업 위험",
        "transit_idle":     "이동 경로 장시간 정체",
        "standby_excess":   "작업 구역 과도한 대기",
    }
    anomaly_type = anomaly.get('anomaly_type', 'unknown')
    type_desc = type_desc_map.get(anomaly_type, anomaly_type)
    
    worker_name = anomaly.get('worker_name', '작업자') or '작업자'
    space_name = anomaly.get('space_name', '현장') or '현장'
    try:
        duration_min = int(anomaly.get('duration_min', 0) or 0)
    except (TypeError, ValueError):
        duration_min = 0
    
    return (
        f"{worker_name}님이 {space_name}에서 "
        f"{duration_min}분간 {type_desc}이(가) 감지되었습니다. "
        f"현장 관리자의 확인을 권장합니다."
    )


# ── 4. 캐싱 래퍼 (같은 입력 반복 호출 방지) ──────────────────────────────────

if _STREAMLIT_AVAILABLE:
    @st.cache_data(ttl=3600, show_spinner=False)
    def cached_worker_narrative(summary_frozen: tuple, worker_name: str) -> str:
        """
        summary dict를 tuple로 변환하여 캐싱.
        같은 작업자, 같은 날짜는 1시간 내 재호출 방지.

        사용법:
            summary_frozen = tuple(sorted(summary.items()))
            narrative = cached_worker_narrative(summary_frozen, worker_name)
        """
        summary = dict(summary_frozen)
        return generate_worker_narrative(summary, worker_name)

    @st.cache_data(ttl=3600, show_spinner=False)
    def cached_site_summary(summary_frozen: tuple, date_str: str) -> str:
        """캐싱된 현장 요약 생성."""
        summary = dict(summary_frozen)
        return generate_site_daily_summary(summary, date_str)

    @st.cache_data(ttl=3600, show_spinner=False)
    def cached_anomaly_explanation(anomaly_frozen: tuple) -> str:
        """캐싱된 이상 패턴 설명 생성."""
        anomaly = dict(anomaly_frozen)
        return generate_anomaly_explanation(anomaly)
else:
    def cached_worker_narrative(summary_frozen: tuple, worker_name: str) -> str:
        summary = dict(summary_frozen)
        return generate_worker_narrative(summary, worker_name)

    def cached_site_summary(summary_frozen: tuple, date_str: str) -> str:
        summary = dict(summary_frozen)
        return generate_site_daily_summary(summary, date_str)

    def cached_anomaly_explanation(anomaly_frozen: tuple) -> str:
        anomaly = dict(anomaly_frozen)
        return generate_anomaly_explanation(anomaly)
