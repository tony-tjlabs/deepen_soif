"""
다중 날짜 트렌드 분석 모듈.
날짜에 걸친 작업자/업체/현장 단위의 지표 변화를 추적하고,
이상 날짜를 감지하거나 두 날짜를 비교하는 기능을 제공한다.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns
from src.metrics.productivity import calc_productivity_summary
from src.metrics.safety import calc_safety_summary

logger = logging.getLogger(__name__)


def calc_worker_trend(
    df_multi: pd.DataFrame,
    worker_key: str,
) -> pd.DataFrame:
    """
    작업자의 날짜별 지표 트렌드.

    Args:
        df_multi: 여러 날짜가 병합된 DataFrame (날짜 컬럼 필수)
        worker_key: 대상 작업자 키

    Returns:
        날짜별 지표 DataFrame (1행 = 1날짜):
          date, working_time_min, idle_time_min, rest_time_min,
          active_ratio, fragmentation_index, fatigue_risk, onsite_duration_min
    """
    wdf_all = df_multi[df_multi[ProcessedColumns.WORKER_KEY] == worker_key].copy()
    if wdf_all.empty:
        return pd.DataFrame()

    date_col = ProcessedColumns.DATE
    if date_col not in wdf_all.columns:
        logger.warning("날짜 컬럼 없음 — 트렌드 계산 불가")
        return pd.DataFrame()

    records: list[dict] = []
    for date_str, day_df in wdf_all.groupby(date_col):
        prod   = calc_productivity_summary(day_df)
        safety = calc_safety_summary(day_df)
        records.append({
            "date":                 date_str,
            "working_time_min":     prod.get("working_time_min", 0),
            "idle_time_min":        prod.get("idle_time_min", 0),
            "rest_time_min":        prod.get("rest_time_min", 0),
            "active_ratio":         prod.get("active_ratio", 0),
            "fragmentation_index":  prod.get("fragmentation_index", 0),
            "onsite_duration_min":  prod.get("onsite_duration_min", 0),
            "fatigue_risk":         safety.get("fatigue_risk", 0.0),
            "alone_risk":           safety.get("alone_risk", 0.0),
        })

    result = pd.DataFrame(records).sort_values("date").reset_index(drop=True)
    return result


def calc_company_trend(
    df_multi: pd.DataFrame,
    company: str,
) -> pd.DataFrame:
    """
    업체의 날짜별 평균 지표 트렌드.

    Args:
        df_multi: 여러 날짜가 병합된 DataFrame
        company: 업체명

    Returns:
        날짜별 평균 지표 DataFrame
    """
    comp_df = df_multi[df_multi[RawColumns.COMPANY] == company].copy()
    if comp_df.empty:
        return pd.DataFrame()

    date_col = ProcessedColumns.DATE
    if date_col not in comp_df.columns:
        return pd.DataFrame()

    records: list[dict] = []
    for date_str, day_df in comp_df.groupby(date_col):
        worker_keys = day_df[ProcessedColumns.WORKER_KEY].unique()
        day_records: list[dict] = []
        for wk in worker_keys:
            wdf = day_df[day_df[ProcessedColumns.WORKER_KEY] == wk]
            prod   = calc_productivity_summary(wdf)
            safety = calc_safety_summary(wdf)
            day_records.append({
                "active_ratio":        prod.get("active_ratio", 0),
                "working_time_min":    prod.get("working_time_min", 0),
                "fatigue_risk":        safety.get("fatigue_risk", 0.0),
                "fragmentation_index": prod.get("fragmentation_index", 0),
            })
        if day_records:
            avg = pd.DataFrame(day_records).mean().to_dict()
            avg["date"] = date_str
            avg["worker_count"] = len(worker_keys)
            records.append(avg)

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records).sort_values("date").reset_index(drop=True)


def calc_site_daily_summary(df_multi: pd.DataFrame) -> pd.DataFrame:
    """
    전체 현장의 날짜별 요약.

    Args:
        df_multi: 여러 날짜가 병합된 DataFrame

    Returns:
        날짜별 요약 DataFrame:
          date, total_workers, avg_active_ratio, avg_working_time_min,
          total_idle_min, fatigue_risk_avg
    """
    date_col = ProcessedColumns.DATE
    if df_multi.empty or date_col not in df_multi.columns:
        return pd.DataFrame()

    records: list[dict] = []
    for date_str, day_df in df_multi.groupby(date_col):
        worker_keys = day_df[ProcessedColumns.WORKER_KEY].unique()
        day_records: list[dict] = []
        for wk in worker_keys:
            wdf = day_df[day_df[ProcessedColumns.WORKER_KEY] == wk]
            prod   = calc_productivity_summary(wdf)
            safety = calc_safety_summary(wdf)
            day_records.append({
                "active_ratio":      prod.get("active_ratio", 0),
                "working_time_min":  prod.get("working_time_min", 0),
                "idle_time_min":     prod.get("idle_time_min", 0),
                "fatigue_risk":      safety.get("fatigue_risk", 0.0),
            })

        if day_records:
            dr = pd.DataFrame(day_records)
            records.append({
                "date":                  date_str,
                "total_workers":         len(worker_keys),
                "avg_active_ratio":      round(dr["active_ratio"].mean(), 3),
                "avg_working_time_min":  round(dr["working_time_min"].mean(), 1),
                "total_idle_min":        int(dr["idle_time_min"].sum()),
                "fatigue_risk_avg":      round(dr["fatigue_risk"].mean(), 3),
            })

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records).sort_values("date").reset_index(drop=True)


def detect_trend_anomaly(
    trend_df: pd.DataFrame,
    metric_col: str,
    window: int = 3,
) -> pd.DataFrame:
    """
    이동평균 기반 이상치 감지.
    rolling mean ± 1.5σ를 벗어나는 날짜에 is_anomaly=True 플래그.

    Args:
        trend_df: calc_worker_trend 또는 calc_site_daily_summary 결과
        metric_col: 분석할 지표 컬럼명
        window: 이동평균 윈도우 크기 (기본 3일)

    Returns:
        is_anomaly 컬럼이 추가된 DataFrame (데이터 수 ≤ window 이면 모두 False)
    """
    result = trend_df.copy()
    result["is_anomaly"] = False

    if len(result) <= window or metric_col not in result.columns:
        return result

    values = result[metric_col].astype(float)
    roll_mean = values.rolling(window=window, min_periods=1).mean()
    roll_std  = values.rolling(window=window, min_periods=1).std().fillna(0)

    threshold = 1.5
    result["is_anomaly"] = (
        (values > roll_mean + threshold * roll_std)
        | (values < roll_mean - threshold * roll_std)
    )
    return result


def compare_two_dates(
    df_multi: pd.DataFrame,
    date_a: str,
    date_b: str,
) -> dict[str, dict]:
    """
    두 날짜의 전체 지표를 비교.

    Args:
        df_multi: 여러 날짜가 병합된 DataFrame
        date_a: 비교 기준 날짜 (YYYYMMDD)
        date_b: 비교 대상 날짜 (YYYYMMDD)

    Returns:
        {metric_name: {date_a: value, date_b: value, delta: value, delta_pct: value}}
    """
    date_col = ProcessedColumns.DATE

    def _summarize(day_df: pd.DataFrame) -> dict:
        if day_df.empty:
            return {}
        worker_keys = day_df[ProcessedColumns.WORKER_KEY].unique()
        all_prod: list[dict] = []
        all_safety: list[dict] = []
        for wk in worker_keys:
            wdf = day_df[day_df[ProcessedColumns.WORKER_KEY] == wk]
            all_prod.append(calc_productivity_summary(wdf))
            all_safety.append(calc_safety_summary(wdf))

        p_df = pd.DataFrame(all_prod)
        s_df = pd.DataFrame(all_safety)
        result = {
            "total_workers":        len(worker_keys),
            "avg_active_ratio":     round(p_df.get("active_ratio", pd.Series([0])).mean(), 3),
            "avg_working_time_min": round(p_df.get("working_time_min", pd.Series([0])).mean(), 1),
            "avg_idle_time_min":    round(p_df.get("idle_time_min", pd.Series([0])).mean(), 1),
            "avg_fragmentation":    round(p_df.get("fragmentation_index", pd.Series([0])).mean(), 3),
            "fatigue_risk":         round(s_df.get("fatigue_risk", pd.Series([0.0])).mean(), 3),
        }
        return result

    df_a = df_multi[df_multi[date_col] == date_a] if date_col in df_multi.columns else pd.DataFrame()
    df_b = df_multi[df_multi[date_col] == date_b] if date_col in df_multi.columns else pd.DataFrame()

    summary_a = _summarize(df_a)
    summary_b = _summarize(df_b)

    comparison: dict[str, dict] = {}
    all_metrics = set(summary_a.keys()) | set(summary_b.keys())
    for metric in all_metrics:
        va = summary_a.get(metric, 0) or 0
        vb = summary_b.get(metric, 0) or 0
        delta = round(float(vb) - float(va), 4)
        delta_pct = round(delta / float(va) * 100, 1) if va != 0 else None
        comparison[metric] = {
            date_a:      va,
            date_b:      vb,
            "delta":     delta,
            "delta_pct": delta_pct,
        }

    return comparison
