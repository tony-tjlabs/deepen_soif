"""
생산성 지표 계산 모듈.
작업자 Journey 데이터를 기반으로 생산성 관련 지표를 계산한다.

모든 함수는 단일 작업자의 DataFrame을 입력으로 받으며,
지표값 또는 집계 DataFrame을 반환한다.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns
from src.utils.constants import (
    ACTIVE_RATIO_WORKING_THRESHOLD,
    ACTIVE_RATIO_ZERO_THRESHOLD,
    WORK_HOURS_START,
    WORK_HOURS_END,
    FATIGUE_THRESHOLD_MIN,
)

logger = logging.getLogger(__name__)


def calc_active_ratio(df: pd.DataFrame) -> float:
    """
    전체 활성비율 계산 (가중 평균).
    총 신호갯수 대비 총 활성신호갯수의 비율.

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        활성비율 (0.0~1.0)
    """
    total_sig = df[RawColumns.SIGNAL_COUNT].sum()
    total_active = df[RawColumns.ACTIVE_SIGNAL_COUNT].sum()
    if total_sig == 0:
        return 0.0
    return float(total_active / total_sig)


def calc_working_time(df: pd.DataFrame) -> timedelta:
    """
    작업 구역 체류 시간 계산.
    period_type == "work" 인 행의 개수 × 1분

    2026-02 변경: classify_activity_period 임계값이 0.3 → 0.05(ACTIVE_RATIO_ZERO_THRESHOLD)로
    조정되어, 현장 대기(standby, 0.05~0.15) 구간도 "work"로 포함됨.
    고활성/저활성/현장대기 세분화는 worker_detail._calc_time_breakdown 참조.

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        작업 시간 timedelta
    """
    work_rows = df[df[ProcessedColumns.PERIOD_TYPE] == "work"]
    return timedelta(minutes=len(work_rows))


def calc_idle_time(df: pd.DataFrame) -> timedelta:
    """
    비생산적 대기 시간 계산.
    근무시간 내 활성비율이 낮고, 작업 구역 외에 있는 시간.

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        유휴 시간 timedelta
    """
    work_hours_mask = (
        (df[ProcessedColumns.HOUR] >= WORK_HOURS_START)
        & (df[ProcessedColumns.HOUR] < WORK_HOURS_END)
    )
    non_rest_mask = df[ProcessedColumns.PERIOD_TYPE].isin(["off"])
    idle_rows = df[work_hours_mask & non_rest_mask]
    return timedelta(minutes=len(idle_rows))


def calc_rest_time(df: pd.DataFrame) -> timedelta:
    """
    휴식/휴게 시간 계산.
    period_type == "rest" 인 행의 개수 × 1분

    2026-02 변경: 분류 체계 정비로 인해 "rest"는 아래 두 경우만 해당:
      1. place_type == "REST" (휴게실·식당·흡연장 등 실제 휴게 시설 체류)
      2. 점심시간(12:00~13:00), 장소 무관
    기존에 "rest"로 분류되던 현장 대기(standby, 0.05~0.15) 구간은
    classify_activity_period 변경으로 이제 "work"로 분류됨.

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        휴식 시간 timedelta
    """
    rest_rows = df[df[ProcessedColumns.PERIOD_TYPE] == "rest"]
    return timedelta(minutes=len(rest_rows))


def calc_onsite_duration(df: pd.DataFrame) -> timedelta:
    """
    현장 체류 시간 계산 (최초 신호 ~ 마지막 신호).

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        체류 시간 timedelta
    """
    if df.empty:
        return timedelta(0)
    ts = df[RawColumns.TIME].dropna().sort_values()
    if len(ts) < 2:
        return timedelta(minutes=1)
    return ts.iloc[-1] - ts.iloc[0]


def calc_working_blocks(df: pd.DataFrame) -> pd.DataFrame:
    """
    연속 작업 블록 분석.
    연속된 "work" 상태 구간을 블록으로 묶어 각 블록의 정보를 반환.

    Args:
        df: 작업자 Journey DataFrame (시간순 정렬 필요)

    Returns:
        작업 블록 DataFrame:
        - block_id: 블록 번호
        - start_time: 시작 시간
        - end_time: 종료 시간
        - duration_min: 지속 시간 (분)
        - avg_active_ratio: 평균 활성비율
    """
    # period_type == "work" 기준으로 블록 탐색
    # 2026-02: "work"에는 high_work(≥0.6) + low_work(0.15~0.6) + standby(0.05~0.15) 포함
    sorted_df = df.sort_values(RawColumns.TIME).reset_index(drop=True)
    is_work = sorted_df[ProcessedColumns.PERIOD_TYPE] == "work"

    blocks = []
    block_id = 0
    in_block = False
    block_start = None
    block_rows = []

    for i, (_, row) in enumerate(sorted_df.iterrows()):
        if is_work.iloc[i]:
            if not in_block:
                in_block = True
                block_start = row[RawColumns.TIME]
                block_rows = []
            block_rows.append(row)
        else:
            if in_block:
                in_block = False
                if block_rows:
                    end_time = block_rows[-1][RawColumns.TIME]
                    duration = len(block_rows)
                    avg_ratio = np.mean([r[ProcessedColumns.ACTIVE_RATIO] for r in block_rows])
                    blocks.append({
                        "block_id": block_id,
                        "start_time": block_start,
                        "end_time": end_time,
                        "duration_min": duration,
                        "avg_active_ratio": round(avg_ratio, 3),
                    })
                    block_id += 1

    # 마지막 블록 처리
    if in_block and block_rows:
        end_time = block_rows[-1][RawColumns.TIME]
        duration = len(block_rows)
        avg_ratio = np.mean([r[ProcessedColumns.ACTIVE_RATIO] for r in block_rows])
        blocks.append({
            "block_id": block_id,
            "start_time": block_start,
            "end_time": end_time,
            "duration_min": duration,
            "avg_active_ratio": round(avg_ratio, 3),
        })

    if not blocks:
        return pd.DataFrame(columns=["block_id", "start_time", "end_time",
                                      "duration_min", "avg_active_ratio"])
    return pd.DataFrame(blocks)


def calc_fragmentation_index(df: pd.DataFrame) -> float:
    """
    작업 분절 지수 계산.
    Fragmentation Index = 작업 블록 수 / 총 작업 시간(시간)

    높을수록 작업이 자주 끊김을 의미 → 집중력 저하 가능성

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        분절 지수 (블록/시간). 작업 없으면 0.0.
    """
    blocks = calc_working_blocks(df)
    if blocks.empty:
        return 0.0
    working_hours = len(df[df[ProcessedColumns.PERIOD_TYPE] == "work"]) / 60.0
    if working_hours == 0:
        return 0.0
    return len(blocks) / working_hours


def calc_total_distance(
    df: pd.DataFrame,
    spatial_ctx=None,
) -> dict:
    """
    하루 총 이동 거리 계산 (좌표계 분리 기반).

    보정된 좌표(CORRECTED_X, CORRECTED_Y) 사용.
    SpatialContext.calc_distance()를 활용하여 같은 LOCATION_KEY 내에서만
    거리를 계산하며, 다른 좌표계 간 이동은 건물 간 추정 거리로 처리한다.

    좌표계 원칙:
      - 같은 location_key 연속 행 → 층 좌표 유클리드 거리
      - location_key 변경 → 비교 불가 (현재 건물 outdoor 좌표 미지원)

    ⚠️ 이동 거리는 같은 층 내 이동 거리의 합입니다.
       층 간 이동(계단/엘리베이터)과 건물 간 이동은 현재 계산에 포함되지 않습니다.

    Args:
        df: 작업자 Journey DataFrame
        spatial_ctx: SpatialContext 인스턴스 (선택, 없으면 location_key만 사용)

    Returns:
        이동 거리 상세 딕셔너리:
        {
            "total": float,                 # 전체 거리 합계
            "indoor_distance_total": float, # 실내(층별) 이동 거리 합
            "outdoor_distance": float,      # 실외(OUTDOOR 좌표계) 이동 거리
            "distance_by_location": dict,   # location_key별 이동 거리
            "note": str,                    # 계산 방법 설명
        }
    """
    sorted_df = df.sort_values(RawColumns.TIME).reset_index(drop=True)
    distance_by_loc: dict[str, float] = {}
    indoor_total = 0.0
    outdoor_total = 0.0

    for loc_key in sorted_df[ProcessedColumns.LOCATION_KEY].unique():
        loc_df = sorted_df[sorted_df[ProcessedColumns.LOCATION_KEY] == loc_key].copy()
        xs = loc_df[ProcessedColumns.CORRECTED_X].values
        ys = loc_df[ProcessedColumns.CORRECTED_Y].values

        dist = 0.0
        prev_x, prev_y = None, None
        for x, y in zip(xs, ys):
            if pd.isna(x) or pd.isna(y):
                prev_x, prev_y = None, None
                continue
            if prev_x is not None:
                dist += float(np.sqrt((x - prev_x) ** 2 + (y - prev_y) ** 2))
            prev_x, prev_y = x, y

        distance_by_loc[loc_key] = round(dist, 2)
        if loc_key == "OUTDOOR":
            outdoor_total += dist
        else:
            indoor_total += dist

    total = round(indoor_total + outdoor_total, 2)
    return {
        "total":                   total,
        "indoor_distance_total":   round(indoor_total, 2),
        "outdoor_distance":        round(outdoor_total, 2),
        "inter_building_distance": 0.0,  # 향후 outdoor 기준 좌표 확보 시 구현
        "distance_by_location":    distance_by_loc,
        "note": (
            "⚠️ 이동 거리는 같은 층 내 이동과 실외 이동 거리의 합입니다. "
            "층 간 이동(계단/엘리베이터)과 건물 간 이동은 현재 계산에 포함되지 않습니다."
        ),
    }


def calc_transition_efficiency(df: pd.DataFrame) -> float:
    """
    이동 효율 계산.
    이동 거리 / 이동 시간 (단위: 좌표/분)

    높을수록 이동이 빠름 (효율적 이동).

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        이동 효율 (좌표/분). 이동 없으면 0.0.
    """
    dist_info = calc_total_distance(df)
    total_dist = dist_info["total"]
    onsite = calc_onsite_duration(df)
    total_min = onsite.total_seconds() / 60.0

    if total_min == 0:
        return 0.0
    return round(total_dist / total_min, 4)


def calc_productivity_summary(df: pd.DataFrame) -> dict:
    """
    단일 작업자의 생산성 지표 전체 요약.

    Args:
        df: 단일 작업자의 Journey DataFrame

    Returns:
        생산성 지표 딕셔너리
    """
    if df.empty:
        return {}

    working_time = calc_working_time(df)
    idle_time = calc_idle_time(df)
    rest_time = calc_rest_time(df)
    onsite = calc_onsite_duration(df)
    blocks = calc_working_blocks(df)

    working_min = working_time.total_seconds() / 60
    onsite_min = onsite.total_seconds() / 60

    transit_time = calc_transit_time(df)
    transit_min = transit_time.total_seconds() / 60
    
    return {
        "active_ratio":          round(calc_active_ratio(df), 3),
        "working_time_min":      round(working_min, 1),
        "idle_time_min":         round(idle_time.total_seconds() / 60, 1),
        "rest_time_min":         round(rest_time.total_seconds() / 60, 1),
        "transit_time_min":      round(transit_min, 1),
        "onsite_duration_min":   round(onsite_min, 1),
        "active_ratio_working":  round(working_min / onsite_min, 3) if onsite_min > 0 else 0.0,
        "transit_ratio":         round(calc_transit_ratio(df), 3),
        "working_block_count":   len(blocks),
        "avg_block_duration_min": round(blocks["duration_min"].mean(), 1) if not blocks.empty else 0.0,
        "max_block_duration_min": int(blocks["duration_min"].max()) if not blocks.empty else 0,
        "fragmentation_index":   round(calc_fragmentation_index(df), 4),
        "total_distance":        calc_total_distance(df)["total"],
        "transition_efficiency": calc_transition_efficiency(df),
    }


def calc_transit_time(df: pd.DataFrame) -> timedelta:
    """
    전체 이동 시간 계산.
    
    포함 항목:
      - state_detail == "transit" (GATE 통과 등 기존 이동)
      - state_detail == "transit_arrival" (장소 전환 이동)
      - state_detail.startswith("transit_") (transit_queue, transit_slow 등)
    
    Args:
        df: 작업자 Journey DataFrame
    
    Returns:
        이동 시간 (timedelta)
    """
    if df.empty:
        return timedelta(0)
    
    state_col = ProcessedColumns.STATE_DETAIL
    if state_col not in df.columns:
        return timedelta(0)
    
    states = df[state_col].fillna("").astype(str)
    transit_mask = states.str.startswith("transit")
    
    transit_count = transit_mask.sum()
    return timedelta(minutes=int(transit_count))


def calc_transit_ratio(df: pd.DataFrame) -> float:
    """
    현장 체류 시간 대비 이동 시간 비율.
    
    이동에 얼마나 시간을 쓰는가 = 대규모 현장 효율 핵심 지표.
    
    비즈니스 의미:
      transit_ratio = 12% →
        "작업자가 하루 현장 체류 시간의 12%를 이동에 소비합니다.
         작업 배치 최적화로 약 N분의 추가 생산 시간 확보 가능합니다."
    
    Args:
        df: 작업자 Journey DataFrame
    
    Returns:
        이동 시간 비율 (0.0~1.0)
    """
    onsite = calc_onsite_duration(df)
    transit = calc_transit_time(df)
    
    onsite_min = onsite.total_seconds() / 60
    transit_min = transit.total_seconds() / 60
    
    if onsite_min == 0:
        return 0.0
    return transit_min / onsite_min


def calc_transit_breakdown(df: pd.DataFrame) -> dict:
    """
    이동 시간 세부 분류.
    
    Returns:
        {
            "gate_transit_min": GATE 통과 이동 시간,
            "transition_arrival_min": 장소 전환 이동 시간,
            "other_transit_min": 기타 이동 시간,
            "total_transit_min": 전체 이동 시간,
        }
    """
    if df.empty:
        return {
            "gate_transit_min": 0,
            "transition_arrival_min": 0,
            "other_transit_min": 0,
            "total_transit_min": 0,
        }
    
    state_col = ProcessedColumns.STATE_DETAIL
    sf_col = ProcessedColumns.SPACE_FUNCTION
    
    if state_col not in df.columns:
        return {
            "gate_transit_min": 0,
            "transition_arrival_min": 0,
            "other_transit_min": 0,
            "total_transit_min": 0,
        }
    
    states = df[state_col].fillna("").astype(str)
    
    # transit_arrival (장소 전환 이동)
    arrival_count = (states == "transit_arrival").sum()
    
    # GATE 통과 이동
    gate_count = 0
    if sf_col in df.columns:
        sf_vals = df[sf_col].fillna("")
        gate_mask = (sf_vals == "TRANSIT_GATE") | (sf_vals == "GATE")
        gate_count = gate_mask.sum()
    
    # 기타 이동 (transit, transit_queue, transit_slow 등)
    other_transit_mask = (
        states.str.startswith("transit") & 
        (states != "transit_arrival")
    )
    other_count = other_transit_mask.sum() - gate_count
    other_count = max(0, other_count)
    
    total = arrival_count + gate_count + other_count
    
    return {
        "gate_transit_min": int(gate_count),
        "transition_arrival_min": int(arrival_count),
        "other_transit_min": int(other_count),
        "total_transit_min": int(total),
    }
