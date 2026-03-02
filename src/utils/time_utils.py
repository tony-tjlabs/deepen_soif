"""
시간 처리 유틸리티.
시간대 분류, 시간 파싱, 기간 계산 등 시간 관련 공통 함수를 제공한다.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.utils.constants import (
    DATETIME_FORMAT,
    TIME_PERIODS,
    WORK_HOURS_START,
    WORK_HOURS_END,
    LUNCH_START,
    LUNCH_END,
    NIGHT_HOURS_START,
    DAWN_HOURS_END,
    ACTIVE_RATIO_ZERO_THRESHOLD,
    ACTIVE_RATIO_WORKING_THRESHOLD,
)

logger = logging.getLogger(__name__)


def parse_datetime(time_str: str) -> Optional[pd.Timestamp]:
    """
    Raw CSV의 시간 문자열을 pandas Timestamp로 변환.

    Args:
        time_str: "YYYY.MM.DD HH:MM:SS" 형식의 시간 문자열

    Returns:
        pandas Timestamp, 파싱 실패 시 None
    """
    try:
        return pd.Timestamp(datetime.strptime(time_str.strip(), DATETIME_FORMAT))
    except (ValueError, AttributeError) as e:
        logger.warning(f"시간 파싱 실패: '{time_str}' - {e}")
        return None


def classify_time_period(hour: int) -> str:
    """
    시간(0~23)을 시간대 이름으로 분류.

    Args:
        hour: 시간 (0~23)

    Returns:
        시간대 이름 (예: "오전작업", "점심", "야간" 등)
    """
    for period_name, (start, end) in TIME_PERIODS.items():
        if start <= hour < end:
            return period_name
    return "야간"


def classify_activity_period(hour: int, active_ratio: float) -> str:
    """
    시간대와 활성비율을 기반으로 작업자 상태를 분류.

    이 함수는 preprocessor._classify_activity_period에서
    place_type이 HELMET_RACK / REST가 아닌 행에만 적용된다.
    즉, 작업 구역(INDOOR/OUTDOOR/WORK_AREA) 내 행에 한정된 분류이다.

    ⚠️ 중요: 2026-02 기준 경계값 변경
    - 기존: active_ratio >= 0.3 → work, 0.05~0.3 → rest (오류: 현장대기를 휴식으로 분류)
    - 변경: active_ratio >= ACTIVE_RATIO_ZERO_THRESHOLD(0.05) → work
      이유: 작업 구역에 있는 작업자가 잠시 대기(standby) 중이어도 "휴식"이 아니라
            여전히 현장에서 작업 관련 활동 중임. 6-카테고리(high_work/low_work/standby)
            의 세분화는 worker_detail._calc_time_breakdown에서 별도 처리.
    - REST 분류: place_type == "REST" 또는 REST 키워드 → preprocessor에서 먼저 처리
    - 점심시간은 장소 무관하게 "rest" 유지

    Args:
        hour: 시간 (0~23)
        active_ratio: 활성비율 (0.0~1.0)

    Returns:
        상태 문자열: "work" / "rest" / "off"
    """
    # 근무 시간 외
    if hour < WORK_HOURS_START or hour >= WORK_HOURS_END:
        return "off"

    # 점심시간: 활성비율 ≥ WORKING_THRESHOLD → 실제 작업 중이므로 work
    # 점심에 작업 구역에서 활발히 움직이면 잔업(잔여 작업)으로 판단
    if LUNCH_START <= hour < LUNCH_END:
        if active_ratio >= ACTIVE_RATIO_WORKING_THRESHOLD:
            return "work"
        return "rest"

    # 근무 시간 내 작업 구역: ACTIVE_RATIO_ZERO_THRESHOLD(0.05) 이상 → work
    # (고활성/저활성/현장대기 세분화는 6-카테고리에서 별도 처리)
    if active_ratio >= ACTIVE_RATIO_ZERO_THRESHOLD:
        return "work"
    else:
        return "off"


def is_night_or_dawn(hour: int) -> bool:
    """
    야간 또는 새벽 시간대 여부 확인.

    Args:
        hour: 시간 (0~23)

    Returns:
        야간/새벽이면 True
    """
    return hour >= NIGHT_HOURS_START or hour < DAWN_HOURS_END


def is_lunch_time(hour: int) -> bool:
    """
    점심시간 여부 확인.

    Args:
        hour: 시간 (0~23)

    Returns:
        점심시간이면 True
    """
    return LUNCH_START <= hour < LUNCH_END


def calc_duration_minutes(df: pd.DataFrame, time_col: str = "시간(분)") -> float:
    """
    DataFrame에서 시작~끝 시간 기간을 분 단위로 계산.

    Args:
        df: 시간 컬럼을 포함한 DataFrame
        time_col: 시간 컬럼명

    Returns:
        기간 (분)
    """
    if df.empty or time_col not in df.columns:
        return 0.0
    sorted_times = df[time_col].dropna().sort_values()
    if len(sorted_times) < 2:
        return 0.0
    return (sorted_times.iloc[-1] - sorted_times.iloc[0]).total_seconds() / 60.0


def get_onsite_duration(df: pd.DataFrame, time_col: str = "시간(분)") -> timedelta:
    """
    현장 체류 시간 계산 (첫 신호 ~ 마지막 신호).

    Args:
        df: 작업자의 시간 데이터
        time_col: 시간 컬럼명

    Returns:
        체류 시간 timedelta
    """
    if df.empty:
        return timedelta(0)
    sorted_times = df[time_col].dropna().sort_values()
    if len(sorted_times) < 2:
        return timedelta(minutes=1)
    return sorted_times.iloc[-1] - sorted_times.iloc[0]


def extract_date_from_folder(folder_name: str) -> Optional[str]:
    """
    데이터 폴더명에서 날짜 문자열 추출.
    예: "Y1_Worker_TWard_20260225" → "20260225"

    Args:
        folder_name: 데이터 폴더명

    Returns:
        날짜 문자열 (YYYYMMDD), 추출 실패 시 None
    """
    try:
        # 폴더명의 마지막 8자리 숫자 추출
        parts = folder_name.replace(" ", "_").split("_")
        for part in reversed(parts):
            if len(part) == 8 and part.isdigit():
                return part
        return None
    except Exception as e:
        logger.warning(f"날짜 추출 실패: '{folder_name}' - {e}")
        return None


def format_duration(minutes: float) -> str:
    """
    분 단위 시간을 "Xh Ym" 형식 문자열로 변환.

    Args:
        minutes: 분 단위 시간

    Returns:
        "Xh Ym" 형식 문자열
    """
    if minutes < 0:
        return "0m"
    h = int(minutes // 60)
    m = int(minutes % 60)
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"
