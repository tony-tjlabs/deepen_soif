"""
개인/업체/날짜 단위 집계 모듈.
전처리된 DataFrame을 기반으로 다양한 단위의 집계를 수행한다.
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns
from src.metrics.productivity import calc_productivity_summary
from src.metrics.safety import calc_safety_summary

logger = logging.getLogger(__name__)


def aggregate_by_worker(
    df: pd.DataFrame,
    include_safety: bool = True,
) -> pd.DataFrame:
    """
    작업자별 생산성 및 안전성 지표 집계.

    Args:
        df: 전체 작업자 DataFrame
        include_safety: 안전성 지표 포함 여부

    Returns:
        작업자별 집계 DataFrame
    """
    if df.empty:
        return pd.DataFrame()

    results = []
    worker_keys = df[ProcessedColumns.WORKER_KEY].unique()

    for wk in worker_keys:
        worker_df = df[df[ProcessedColumns.WORKER_KEY] == wk].copy()
        if worker_df.empty:
            continue

        row = _get_worker_base_info(worker_df)

        # 생산성 지표
        prod = calc_productivity_summary(worker_df)
        row.update(prod)

        # 안전성 지표
        if include_safety:
            safety = calc_safety_summary(worker_df, df)
            row.update({f"safety_{k}": v for k, v in safety.items()})

        results.append(row)

    if not results:
        return pd.DataFrame()

    result_df = pd.DataFrame(results)
    return result_df.sort_values("worker_key").reset_index(drop=True)


def aggregate_by_company(df: pd.DataFrame) -> pd.DataFrame:
    """
    업체별 생산성 및 안전성 지표 집계.

    Args:
        df: 전체 작업자 DataFrame

    Returns:
        업체별 집계 DataFrame
    """
    if df.empty:
        return pd.DataFrame()

    per_worker = aggregate_by_worker(df, include_safety=True)
    if per_worker.empty:
        return pd.DataFrame()

    numeric_cols = per_worker.select_dtypes(include="number").columns.tolist()

    result = per_worker.groupby("company")[numeric_cols].mean().reset_index()
    result["worker_count"] = per_worker.groupby("company")["worker_key"].count().values

    return result.sort_values("worker_count", ascending=False).reset_index(drop=True)


def aggregate_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    날짜별 전체 현황 집계 (단일 날짜 DataFrame에 대한 요약).

    Args:
        df: 전체 작업자 DataFrame

    Returns:
        단일 행 집계 DataFrame (날짜별 KPI 요약)
    """
    if df.empty:
        return pd.DataFrame()

    per_worker = aggregate_by_worker(df, include_safety=True)
    if per_worker.empty:
        return pd.DataFrame()

    date_str = df[ProcessedColumns.DATE].iloc[0] if not df.empty else "Unknown"

    numeric_cols = per_worker.select_dtypes(include="number").columns.tolist()
    avg_metrics = per_worker[numeric_cols].mean().to_dict()

    summary = {
        "date": date_str,
        "total_workers": len(per_worker),
        "total_companies": per_worker["company"].nunique(),
    }
    summary.update(avg_metrics)

    return pd.DataFrame([summary])


def get_zone_density_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    """
    시간대별 구역(장소) 인원 밀도 계산.

    Args:
        df: 전체 작업자 DataFrame

    Returns:
        {hour, location_key, place, worker_count} DataFrame
    """
    if df.empty:
        return pd.DataFrame()

    result = (
        df.groupby([ProcessedColumns.HOUR, ProcessedColumns.LOCATION_KEY, ProcessedColumns.CORRECTED_PLACE])
        [ProcessedColumns.WORKER_KEY]
        .nunique()
        .reset_index()
        .rename(columns={ProcessedColumns.WORKER_KEY: "worker_count"})
    )
    return result.sort_values(["hour", "worker_count"], ascending=[True, False])


def get_worker_journey_summary(df: pd.DataFrame, worker_key: str) -> dict:
    """
    특정 작업자의 Journey 요약 정보.

    Args:
        df: 전체 작업자 DataFrame
        worker_key: 작업자 키

    Returns:
        작업자 요약 딕셔너리
    """
    worker_df = df[df[ProcessedColumns.WORKER_KEY] == worker_key].copy()
    if worker_df.empty:
        return {}

    base = _get_worker_base_info(worker_df)
    prod = calc_productivity_summary(worker_df)
    safety = calc_safety_summary(worker_df, df)

    return {**base, **prod, **{f"safety_{k}": v for k, v in safety.items()}}


def get_place_dwell_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    작업자별/장소별 체류 시간 집계.

    Args:
        df: 단일 작업자 또는 전체 DataFrame

    Returns:
        {worker_key, place, dwell_min} DataFrame
    """
    if df.empty:
        return pd.DataFrame()

    result = (
        df.groupby([ProcessedColumns.WORKER_KEY, ProcessedColumns.CORRECTED_PLACE])
        .size()
        .reset_index(name="dwell_min")
    )
    return result.sort_values("dwell_min", ascending=False).reset_index(drop=True)


def get_active_ratio_timeseries(df: pd.DataFrame, worker_key: str) -> pd.DataFrame:
    """
    특정 작업자의 활성비율 시계열 데이터.

    Args:
        df: 전체 DataFrame
        worker_key: 작업자 키

    Returns:
        {timestamp, active_ratio, place, period_type} DataFrame
    """
    worker_df = df[df[ProcessedColumns.WORKER_KEY] == worker_key].copy()
    if worker_df.empty:
        return pd.DataFrame()

    cols = [
        RawColumns.TIME,
        ProcessedColumns.ACTIVE_RATIO,
        ProcessedColumns.CORRECTED_PLACE,
        ProcessedColumns.PERIOD_TYPE,
        RawColumns.SIGNAL_COUNT,
        RawColumns.ACTIVE_SIGNAL_COUNT,
    ]
    available = [c for c in cols if c in worker_df.columns]
    return worker_df[available].sort_values(RawColumns.TIME).reset_index(drop=True)


def _get_worker_base_info(df: pd.DataFrame) -> dict:
    """작업자 기본 정보 추출."""
    first = df.iloc[0]
    return {
        "worker_key":  first[ProcessedColumns.WORKER_KEY],
        "worker_name": first[RawColumns.WORKER],
        "tag_id":      first.get(RawColumns.TAG, ""),
        "company":     first[RawColumns.COMPANY],
        "date":        first.get(ProcessedColumns.DATE, ""),
    }
