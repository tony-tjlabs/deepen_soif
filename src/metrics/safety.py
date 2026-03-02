"""
안전성 지표 계산 모듈.
피로 위험도, 이상 이동 감지, 단독 작업 위험도 등을 계산한다.

참고: 헬멧 착용 준수율은 2026-02 기준 제거됨.
이유: BLE 신호만으로는 헬멧 착용 여부를 신뢰성 있게 추정 불가.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns
from src.utils.constants import (
    ACTIVE_RATIO_ZERO_THRESHOLD,
    FATIGUE_THRESHOLD_MIN,
    ALONE_RISK_RADIUS,
    WORK_HOURS_START,
    WORK_HOURS_END,
    SpaceFunction,
    ALONE_RISK_MULTIPLIER,
    DWELL_NORMAL_MAX,
    ABNORMAL_STOP_THRESHOLD,
)
from src.metrics.productivity import calc_working_blocks

logger = logging.getLogger(__name__)


def calc_fatigue_risk(
    df: pd.DataFrame,
    threshold_min: int = FATIGUE_THRESHOLD_MIN,
) -> float:
    """
    피로 위험도 계산.
    연속 작업 시간이 임계값을 초과하는 경우 피로 위험 점수를 계산.

    Fatigue Risk Score = Σ (초과 시간 / 임계값) for each block

    Args:
        df: 작업자 Journey DataFrame
        threshold_min: 피로 위험 임계값 (기본 120분)

    Returns:
        피로 위험도 (0.0~∞, 1.0 이상이면 위험)
    """
    blocks = calc_working_blocks(df)
    if blocks.empty:
        return 0.0

    total_risk = 0.0
    for _, block in blocks.iterrows():
        duration = block["duration_min"]
        if duration > threshold_min:
            excess = duration - threshold_min
            total_risk += excess / threshold_min

    return round(total_risk, 3)



def detect_anomaly_movement(df: pd.DataFrame) -> pd.DataFrame:
    """
    이상 이동 패턴 감지.

    탐지 패턴:
    1. 비정상 정지: 근무시간 내 예상치 못한 장소에서 장시간 정지
    2. 급격한 장소 변화: 연속되지 않는 장소 전환 (노이즈 제거 후에도 발생)

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        이상 감지 DataFrame:
        - timestamp: 감지 시간
        - anomaly_type: "ABNORMAL_STOP" / "RAPID_TRANSITION"
        - description: 설명
        - severity: "LOW" / "MEDIUM" / "HIGH"
    """
    anomalies = []
    sorted_df = df.sort_values(RawColumns.TIME).reset_index(drop=True)

    # 1. 비정상 정지 감지 (같은 장소에서 60분 이상 비활성)
    place_col = ProcessedColumns.CORRECTED_PLACE
    current_place = None
    static_start = None
    static_count = 0

    for _, row in sorted_df.iterrows():
        hour = row[ProcessedColumns.HOUR]
        if hour < WORK_HOURS_START or hour >= WORK_HOURS_END:
            current_place = None
            static_count = 0
            continue

        place = row[place_col]
        active_ratio = row[ProcessedColumns.ACTIVE_RATIO]

        if place == current_place and active_ratio <= ACTIVE_RATIO_ZERO_THRESHOLD:
            static_count += 1
            if static_count == 60:  # 60분 연속 비활성 + 같은 장소
                anomalies.append({
                    "timestamp": row[RawColumns.TIME],
                    "anomaly_type": "ABNORMAL_STOP",
                    "description": f"60분 이상 정지: {place}",
                    "severity": "MEDIUM",
                    "place": place,
                })
        else:
            current_place = place
            static_count = 1 if active_ratio <= ACTIVE_RATIO_ZERO_THRESHOLD else 0

    # 2. 급격한 장소 전환 감지
    places = sorted_df[place_col].tolist()
    times = sorted_df[RawColumns.TIME].tolist()

    for i in range(1, len(places)):
        if places[i] != places[i - 1]:
            hour = sorted_df.iloc[i][ProcessedColumns.HOUR]
            if WORK_HOURS_START <= hour < WORK_HOURS_END:
                # 직전 5분 내 이미 한 번 전환 → 급격한 전환
                if i >= 2 and places[i - 2] != places[i - 1] and places[i - 2] != places[i]:
                    anomalies.append({
                        "timestamp": times[i],
                        "anomaly_type": "RAPID_TRANSITION",
                        "description": f"급격한 위치 전환: {places[i-1]} → {places[i]}",
                        "severity": "LOW",
                        "place": places[i],
                    })

    if not anomalies:
        return pd.DataFrame(columns=["timestamp", "anomaly_type", "description", "severity", "place"])
    return pd.DataFrame(anomalies)


def calc_alone_risk(
    df_all: pd.DataFrame,
    worker_key: str,
    radius: float = ALONE_RISK_RADIUS,
) -> float:
    """
    단독 작업 위험도 계산.
    근무시간 중 주변 반경 내 다른 작업자가 없는 시간 비율.
    (벡터화: LOCATION_KEY 기준 merge 후 거리 일괄 계산, O(n²) 루프 제거.)

    ⚠️ 좌표계: 같은 LOCATION_KEY 내에서만 거리 비교.

    Args:
        df_all: 전체 작업자 DataFrame (당일 데이터)
        worker_key: 대상 작업자 키
        radius: 근접 반경 (좌표 단위, 기본 50)

    Returns:
        단독 작업 비율 (0.0~1.0)
    """
    worker_df = df_all[df_all[ProcessedColumns.WORKER_KEY] == worker_key].copy()
    others_df = df_all[df_all[ProcessedColumns.WORKER_KEY] != worker_key].copy()

    if worker_df.empty or others_df.empty:
        return 1.0

    work_mask = (
        (worker_df[ProcessedColumns.HOUR] >= WORK_HOURS_START)
        & (worker_df[ProcessedColumns.HOUR] < WORK_HOURS_END)
    )
    work_df = worker_df[work_mask].dropna(
        subset=[ProcessedColumns.CORRECTED_X, ProcessedColumns.CORRECTED_Y, ProcessedColumns.LOCATION_KEY]
    )
    work_df = work_df[
        work_df[ProcessedColumns.LOCATION_KEY].astype(str).str.strip() != ""
    ]
    if work_df.empty:
        return 0.0

    valid_count = len(work_df)
    others_df = others_df.dropna(
        subset=[ProcessedColumns.CORRECTED_X, ProcessedColumns.CORRECTED_Y, ProcessedColumns.LOCATION_KEY]
    )
    if others_df.empty:
        return round(1.0, 3)

    # LOCATION_KEY별로 merge → 같은 구역 내 시간(±60s)·거리만 계산
    time_col = RawColumns.TIME
    loc_col = ProcessedColumns.LOCATION_KEY
    x_col, y_col = ProcessedColumns.CORRECTED_X, ProcessedColumns.CORRECTED_Y

    work_df = work_df.reset_index(drop=True)
    work_df["_wi"] = work_df.index
    w = work_df[["_wi", time_col, loc_col, x_col, y_col]].rename(
        columns={time_col: "wt", x_col: "wx", y_col: "wy"}
    )
    o = others_df[[time_col, loc_col, x_col, y_col]].rename(
        columns={time_col: "ot", x_col: "ox", y_col: "oy"}
    )
    merged = w.merge(o, left_on=loc_col, right_on=loc_col, suffixes=("", "_o"))
    merged["dt"] = (merged["wt"] - merged["ot"]).dt.total_seconds().abs()
    merged = merged[merged["dt"] <= 60]
    merged["dist"] = np.sqrt((merged["wx"] - merged["ox"]) ** 2 + (merged["wy"] - merged["oy"]) ** 2)
    min_dist = merged.groupby("_wi")["dist"].min().reindex(work_df["_wi"])
    # 해당 분에 같은 loc·시간에 다른 작업자 없거나, 최소 거리 > radius 이면 단독
    alone_count = int((min_dist.isna() | (min_dist > radius)).sum())

    return round(alone_count / valid_count, 3)


# ═══════════════════════════════════════════════════════════════════════════
# Space-Aware Contextual Risk (v4)
# ═══════════════════════════════════════════════════════════════════════════

def calc_contextual_risk(
    df: pd.DataFrame,
    df_all: Optional[pd.DataFrame] = None,
) -> dict:
    """
    공간 맥락 기반 복합 위험도 계산.

    Contextual Risk = Personal Risk × hazard_weight × Dynamic Pressure

    - Personal Risk: 피로도 + 단독작업 비율
    - hazard_weight: 공간 고유 위험 가중치 (WORK_HAZARD = 1.0, REST = 0.0)
    - Dynamic Pressure: 구역 밀집도 (현재 단순화)

    Args:
        df: 단일 작업자 Journey DataFrame
        df_all: 전체 작업자 DataFrame (밀집도 계산용)

    Returns:
        {
            contextual_risk, personal_risk, fatigue_score, alone_score,
            avg_hazard_weight, dynamic_pressure, risk_level
        }
    """
    if df.empty:
        return {"contextual_risk": 0.0, "risk_level": "LOW"}

    # 1. Personal Risk (피로 + 고립)
    fatigue = calc_fatigue_risk(df)
    fatigue_score = min(fatigue, 2.0)

    alone_score = 0.0
    if df_all is not None and not df_all.empty:
        wk = df[ProcessedColumns.WORKER_KEY].iloc[0]
        alone_score = calc_alone_risk(df_all, wk)

    personal_risk = 0.5 * fatigue_score + 0.5 * alone_score
    personal_risk = min(personal_risk, 2.0)

    # 2. Space Hazard Weight (작업 시간 동안의 평균)
    work_mask = (
        (df[ProcessedColumns.HOUR] >= WORK_HOURS_START)
        & (df[ProcessedColumns.HOUR] < WORK_HOURS_END)
    )
    work_df = df[work_mask]

    if ProcessedColumns.HAZARD_WEIGHT in work_df.columns and not work_df.empty:
        avg_hazard = work_df[ProcessedColumns.HAZARD_WEIGHT].fillna(0.3).mean()
    else:
        avg_hazard = 0.3

    # 3. Alone Risk Multiplier (공간에 따른 단독 위험 배수)
    if ProcessedColumns.SPACE_FUNCTION in work_df.columns and not work_df.empty:
        sf_counts = work_df[ProcessedColumns.SPACE_FUNCTION].value_counts()
        if not sf_counts.empty:
            dominant_sf = sf_counts.index[0]
            alone_mult = ALONE_RISK_MULTIPLIER.get(dominant_sf, 1.0)
            alone_score *= alone_mult

    # 4. Dynamic Pressure (밀집도 — 단순화된 버전)
    dynamic_pressure = 1.0
    if df_all is not None and ProcessedColumns.LOCATION_KEY in df.columns:
        for loc_key in work_df[ProcessedColumns.LOCATION_KEY].unique():
            if pd.isna(loc_key):
                continue
            same_loc = df_all[df_all[ProcessedColumns.LOCATION_KEY] == loc_key]
            workers = same_loc[ProcessedColumns.WORKER_KEY].nunique()
            if workers > 5:
                dynamic_pressure = min(workers / 5.0, 2.0)
                break

    # 5. 최종 Contextual Risk
    contextual_risk = personal_risk * avg_hazard * dynamic_pressure
    contextual_risk = round(contextual_risk, 4)

    risk_level = "HIGH" if contextual_risk >= 1.0 else ("MEDIUM" if contextual_risk >= 0.5 else "LOW")

    return {
        "contextual_risk": contextual_risk,
        "personal_risk": round(personal_risk, 4),
        "fatigue_score": round(fatigue_score, 4),
        "alone_score": round(alone_score, 4),
        "avg_hazard_weight": round(avg_hazard, 4),
        "dynamic_pressure": round(dynamic_pressure, 4),
        "risk_level": risk_level,
    }


def detect_anomaly_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    공간 맥락 기반 이상 이벤트 감지.

    v4 변경: space_function과 anomaly_flag 컬럼 활용.

    감지 패턴:
    1. WORK_HAZARD abnormal_stop (5분+ 비활성)
    2. TRANSIT_GATE dwell_exceeded (게이트 병목)
    3. TRANSIT_CORRIDOR dwell_exceeded (통로 정체)
    4. 장시간 단독 작업 (WORK 구역에서 30분+ 혼자)

    Args:
        df: 작업자 Journey DataFrame

    Returns:
        이벤트 DataFrame:
        - timestamp, event_type, space_function, place, severity, description
    """
    events = []
    sorted_df = df.sort_values(RawColumns.TIME).reset_index(drop=True)

    place_col = ProcessedColumns.CORRECTED_PLACE

    # 1. anomaly_flag 컬럼 기반 이벤트
    if ProcessedColumns.ANOMALY_FLAG in sorted_df.columns:
        anomaly_rows = sorted_df[sorted_df[ProcessedColumns.ANOMALY_FLAG].notna()]
        for _, row in anomaly_rows.iterrows():
            flag = row[ProcessedColumns.ANOMALY_FLAG]
            sf = row.get(ProcessedColumns.SPACE_FUNCTION, SpaceFunction.UNKNOWN)
            place = row.get(place_col, "")

            severity = "HIGH" if sf == SpaceFunction.WORK_HAZARD else "MEDIUM"

            events.append({
                "timestamp": row[RawColumns.TIME],
                "event_type": flag,
                "space_function": sf,
                "place": place,
                "severity": severity,
                "description": _get_anomaly_description(flag, place, sf),
            })

    # 2. dwell_exceeded 기반 이벤트
    if ProcessedColumns.DWELL_EXCEEDED in sorted_df.columns:
        exceeded_rows = sorted_df[sorted_df[ProcessedColumns.DWELL_EXCEEDED] == True]
        for _, row in exceeded_rows.iterrows():
            sf = row.get(ProcessedColumns.SPACE_FUNCTION, SpaceFunction.UNKNOWN)
            place = row.get(place_col, "")

            # TRANSIT 계열만 dwell_exceeded 이벤트로 취급
            if sf in (SpaceFunction.TRANSIT_GATE, SpaceFunction.TRANSIT_CORRIDOR):
                event_type = "dwell_exceeded"
                severity = "MEDIUM"
                events.append({
                    "timestamp": row[RawColumns.TIME],
                    "event_type": event_type,
                    "space_function": sf,
                    "place": place,
                    "severity": severity,
                    "description": f"체류 초과: {place} ({sf})",
                })

    # 3. 레거시: 기존 detect_anomaly_movement 결과 통합
    legacy = detect_anomaly_movement(df)
    for _, row in legacy.iterrows():
        events.append({
            "timestamp": row["timestamp"],
            "event_type": row["anomaly_type"].lower(),
            "space_function": SpaceFunction.UNKNOWN,
            "place": row.get("place", ""),
            "severity": row["severity"],
            "description": row["description"],
        })

    if not events:
        return pd.DataFrame(columns=[
            "timestamp", "event_type", "space_function", "place", "severity", "description"
        ])

    result = pd.DataFrame(events)
    result = result.drop_duplicates(subset=["timestamp", "event_type"]).sort_values("timestamp")
    return result


def _get_anomaly_description(flag: str, place: str, sf: str) -> str:
    """이상 플래그에 대한 설명 문자열 생성."""
    if flag == "abnormal_stop":
        if sf == SpaceFunction.WORK_HAZARD:
            return f"🚨 고위험 구역 비정상 정지: {place}"
        return f"비정상 정지 감지: {place}"
    if flag == "gate_congestion":
        return f"🚧 게이트 병목: {place}"
    if flag == "corridor_block":
        return f"🚧 통로 정체: {place}"
    return f"이상 감지: {place} ({flag})"


def calc_safety_summary(df: pd.DataFrame, df_all: Optional[pd.DataFrame] = None) -> dict:
    """
    단일 작업자의 안전성 지표 전체 요약.

    ─── 내부 호출 구조 (2026-02 명확화) ─────────────────────────────────
    - calc_fatigue_risk(df)          → fatigue_risk, fatigue_status
    - detect_anomaly_movement(df)    → anomaly_count, abnormal_stop_count,
                                       rapid_transition_count
      ※ detect_anomaly_movement는 이 함수 내부에서만 호출됨.
         safety_alert.py는 calc_safety_summary 결과를 사용하며,
         detect_anomaly_movement를 직접 호출하지 않는다.
         journey_review.py는 보정 검증 탭에서 detect_anomaly_movement를
         별도로 호출하여 보정 전/후 이상 패턴 비교에 활용할 수 있다.
    - calc_alone_risk(df_all, wk)    → alone_risk, alone_status
      ※ df_all이 None이면 단독 작업 위험도는 계산하지 않음.
    ─────────────────────────────────────────────────────────────────────

    Args:
        df: 단일 작업자의 Journey DataFrame
        df_all: 전체 작업자 DataFrame (단독 작업 위험도 계산용, 없으면 생략)

    Returns:
        안전성 지표 딕셔너리
    """
    if df.empty:
        return {}

    fatigue_risk = calc_fatigue_risk(df)
    anomalies = detect_anomaly_movement(df)

    result = {
        "fatigue_risk":             fatigue_risk,
        "fatigue_status":           "HIGH" if fatigue_risk >= 1.0 else ("MEDIUM" if fatigue_risk >= 0.5 else "LOW"),
        "anomaly_count":            len(anomalies),
        "abnormal_stop_count":      len(anomalies[anomalies["anomaly_type"] == "ABNORMAL_STOP"]) if not anomalies.empty else 0,
        "rapid_transition_count":   len(anomalies[anomalies["anomaly_type"] == "RAPID_TRANSITION"]) if not anomalies.empty else 0,
    }

    # 단독 작업 위험도 (전체 데이터 있을 때만)
    if df_all is not None and not df_all.empty:
        worker_key = df[ProcessedColumns.WORKER_KEY].iloc[0]
        alone_risk = calc_alone_risk(df_all, worker_key)
        result["alone_risk"] = alone_risk
        result["alone_status"] = "HIGH" if alone_risk >= 0.7 else ("MEDIUM" if alone_risk >= 0.4 else "LOW")

    # v4 신규: Contextual Risk
    ctx_risk = calc_contextual_risk(df, df_all)
    result["contextual_risk"] = ctx_risk.get("contextual_risk", 0.0)
    result["contextual_risk_level"] = ctx_risk.get("risk_level", "LOW")
    result["avg_hazard_weight"] = ctx_risk.get("avg_hazard_weight", 0.3)

    # v4 신규: 공간 맥락 이벤트
    events = detect_anomaly_events(df)
    result["contextual_event_count"] = len(events)
    result["high_severity_events"] = len(events[events["severity"] == "HIGH"]) if not events.empty else 0

    return result


