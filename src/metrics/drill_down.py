"""
드릴다운 분석 엔진.
작업자의 시간 분류 결과를 에피소드 단위로 분해하고,
원인을 분류하여 현장 관리자가 즉시 이해할 수 있는 인사이트를 생성한다.

핵심 철학:
  "idle 84분" → "언제, 어디서, 얼마나 자주, 왜 idle이 발생했는가"
  "fatigue_risk 0.8" → "어느 구간에서 얼마나 연속 작업했는가"
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns
from src.metrics.productivity import calc_working_blocks
from src.utils.constants import (
    WORK_HOURS_START,
    WORK_HOURS_END,
    FATIGUE_THRESHOLD_MIN,
    ACTIVE_RATIO_ZERO_THRESHOLD,
    ACTIVE_RATIO_WORKING_THRESHOLD,
)

logger = logging.getLogger(__name__)

_REST_PLACE_KEYWORDS = ["휴게", "식당", "탈의실", "탈의", "로비"]


# ── 에피소드 분해 헬퍼 ───────────────────────────────────────────────


def _to_episodes(df: pd.DataFrame, cat_series: pd.Series, target_cat: str) -> list[dict]:
    """
    연속된 같은 카테고리 구간을 에피소드 단위로 묶어 반환.

    Args:
        df: 시간순 정렬된 작업자 DataFrame
        cat_series: 각 행의 카테고리 레이블 Series
        target_cat: 에피소드로 묶을 카테고리 값

    Returns:
        에피소드 딕셔너리 목록 ({start_idx, end_idx, rows})
    """
    episodes: list[dict] = []
    in_ep = False
    start_idx = 0

    for i, val in enumerate(cat_series):
        if val == target_cat and not in_ep:
            in_ep = True
            start_idx = i
        elif val != target_cat and in_ep:
            in_ep = False
            episodes.append({"start_idx": start_idx, "end_idx": i - 1})

    if in_ep:
        episodes.append({"start_idx": start_idx, "end_idx": len(cat_series) - 1})

    return episodes


def _classify_idle_cause(
    ep_df: pd.DataFrame,
    prev_place: str | None,
    next_place: str | None,
) -> str:
    """
    Idle 에피소드의 원인을 규칙 기반으로 분류.

    우선순위:
      helmet_off  → active_ratio_avg ≤ 0.05 AND NOT HELMET_RACK (비활성 정지)
      transition  → 장소유형==GATE OR 직전/직후 장소가 다름
      waiting     → active_ratio 0.05~0.15 AND 같은 장소 10분 이상
      slow_work   → active_ratio 0.15~0.3 AND 이동 없음
      unknown     → 분류 불가
    """
    avg_ratio = ep_df[ProcessedColumns.ACTIVE_RATIO].mean()
    place_types = ep_df[ProcessedColumns.PLACE_TYPE].value_counts()
    places = ep_df[ProcessedColumns.CORRECTED_PLACE].unique()
    duration = len(ep_df)

    if avg_ratio <= ACTIVE_RATIO_ZERO_THRESHOLD and "HELMET_RACK" not in place_types:
        return "helmet_off"

    if "GATE" in place_types:
        return "transition"

    ep_places = ep_df[ProcessedColumns.CORRECTED_PLACE].tolist()
    unique_places = len(set(ep_places))
    main_place = ep_df[ProcessedColumns.CORRECTED_PLACE].mode().iloc[0] if len(ep_df) > 0 else ""
    prev_diff = prev_place and prev_place != main_place
    next_diff = next_place and next_place != main_place
    if unique_places >= 3 or (prev_diff and next_diff):
        return "transition"

    if 0.05 < avg_ratio <= 0.15 and duration >= 10:
        return "waiting"

    if 0.15 < avg_ratio < ACTIVE_RATIO_WORKING_THRESHOLD:
        return "slow_work"

    return "unknown"


# ── 공개 분석 함수 ───────────────────────────────────────────────────


def analyze_idle_episodes(df: pd.DataFrame, worker_key: str) -> pd.DataFrame:
    """
    근무시간 내 idle 구간을 에피소드 단위로 분해하여 원인 분류.

    Args:
        df: 전체 또는 단일 작업자 DataFrame
        worker_key: 대상 작업자 키

    Returns:
        에피소드별 DataFrame:
          start_time, end_time, duration_min, location,
          place_type, cause, active_ratio_avg
    """
    wdf = df[df[ProcessedColumns.WORKER_KEY] == worker_key].copy()
    wdf = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)

    if wdf.empty:
        return pd.DataFrame(columns=[
            "start_time", "end_time", "duration_min", "location",
            "place_type", "cause", "active_ratio_avg",
        ])

    work_mask = (
        (wdf[ProcessedColumns.HOUR] >= WORK_HOURS_START)
        & (wdf[ProcessedColumns.HOUR] < WORK_HOURS_END)
    )
    pt = wdf[ProcessedColumns.PERIOD_TYPE]
    place_type = wdf[ProcessedColumns.PLACE_TYPE]
    corr_place = wdf[ProcessedColumns.CORRECTED_PLACE]
    rest_mask = corr_place.fillna("").str.contains("|".join(_REST_PLACE_KEYWORDS), na=False)

    cat = pd.Series([""] * len(wdf), index=wdf.index)
    cat[(pt == "off") & ~work_mask] = "off_duty"
    cat[place_type == "HELMET_RACK"] = "off_duty"
    cat[(pt == "work") & work_mask & ~rest_mask] = "work"
    cat[place_type == "GATE"] = "transit"
    cat[(cat == "") & work_mask & ~rest_mask] = "idle"
    cat[rest_mask] = "rest"
    cat[cat == ""] = "off_duty"

    episodes_raw = _to_episodes(wdf, cat, "idle")

    records: list[dict] = []
    places_list = wdf[ProcessedColumns.CORRECTED_PLACE].tolist()

    for ep in episodes_raw:
        s, e = ep["start_idx"], ep["end_idx"]
        ep_df = wdf.iloc[s: e + 1]

        prev_place = places_list[s - 1] if s > 0 else None
        next_place = places_list[e + 1] if e + 1 < len(wdf) else None

        cause = _classify_idle_cause(ep_df, prev_place, next_place)
        main_place = ep_df[ProcessedColumns.CORRECTED_PLACE].mode()
        main_place_str = main_place.iloc[0] if not main_place.empty else "Unknown"
        main_ptype = ep_df[ProcessedColumns.PLACE_TYPE].mode()
        main_ptype_str = main_ptype.iloc[0] if not main_ptype.empty else "UNKNOWN"

        records.append({
            "start_time":      ep_df[RawColumns.TIME].iloc[0],
            "end_time":        ep_df[RawColumns.TIME].iloc[-1],
            "duration_min":    len(ep_df),
            "location":        main_place_str,
            "place_type":      main_ptype_str,
            "cause":           cause,
            "active_ratio_avg": round(ep_df[ProcessedColumns.ACTIVE_RATIO].mean(), 3),
        })

    return pd.DataFrame(records)


def analyze_work_blocks(df: pd.DataFrame, worker_key: str) -> pd.DataFrame:
    """
    연속 작업 블록을 상세 분해.

    Args:
        df: 전체 또는 단일 작업자 DataFrame
        worker_key: 대상 작업자 키

    Returns:
        블록별 DataFrame:
          block_id, start_time, end_time, duration_min,
          location_sequence, avg_active_ratio, intensity, interrupted_by
    """
    wdf = df[df[ProcessedColumns.WORKER_KEY] == worker_key].copy()
    wdf = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)

    if wdf.empty:
        return pd.DataFrame(columns=[
            "block_id", "start_time", "end_time", "duration_min",
            "location_sequence", "avg_active_ratio", "intensity", "interrupted_by",
        ])

    base_blocks = calc_working_blocks(wdf)
    if base_blocks.empty:
        return pd.DataFrame(columns=[
            "block_id", "start_time", "end_time", "duration_min",
            "location_sequence", "avg_active_ratio", "intensity", "interrupted_by",
        ])

    pt = wdf[ProcessedColumns.PERIOD_TYPE]
    place_type = wdf[ProcessedColumns.PLACE_TYPE]
    corr_place = wdf[ProcessedColumns.CORRECTED_PLACE]
    rest_mask = corr_place.fillna("").str.contains("|".join(_REST_PLACE_KEYWORDS), na=False)

    work_mask = (wdf[ProcessedColumns.HOUR] >= WORK_HOURS_START) & (wdf[ProcessedColumns.HOUR] < WORK_HOURS_END)
    cat = pd.Series([""] * len(wdf), index=wdf.index)
    cat[(pt == "off") & ~work_mask] = "off_duty"
    cat[place_type == "HELMET_RACK"] = "off_duty"
    cat[(pt == "work") & work_mask & ~rest_mask] = "work"
    cat[place_type == "GATE"] = "transit"
    cat[(cat == "") & work_mask & ~rest_mask] = "idle"
    cat[rest_mask] = "rest"
    cat[cat == ""] = "off_duty"

    records: list[dict] = []
    for _, block in base_blocks.iterrows():
        block_mask = (
            (wdf[RawColumns.TIME] >= block["start_time"])
            & (wdf[RawColumns.TIME] <= block["end_time"])
        )
        block_df = wdf[block_mask]

        avg_ratio = block["avg_active_ratio"]
        if avg_ratio >= 0.6:
            intensity = "high"
        elif avg_ratio >= 0.3:
            intensity = "medium"
        else:
            intensity = "low"

        loc_seq: list[str] = []
        prev_loc = None
        for loc in block_df[ProcessedColumns.CORRECTED_PLACE]:
            if loc != prev_loc:
                loc_seq.append(str(loc))
                prev_loc = loc

        after_idx = wdf[wdf[RawColumns.TIME] > block["end_time"]].index
        interrupted_by = cat.loc[after_idx[0]] if len(after_idx) > 0 else "end"

        records.append({
            "block_id":         int(block["block_id"]),
            "start_time":       block["start_time"],
            "end_time":         block["end_time"],
            "duration_min":     int(block["duration_min"]),
            "location_sequence": " → ".join(loc_seq[:5]),
            "avg_active_ratio": avg_ratio,
            "intensity":        intensity,
            "interrupted_by":   interrupted_by,
        })

    return pd.DataFrame(records)


def analyze_fatigue_pattern(df: pd.DataFrame, worker_key: str) -> dict[str, Any]:
    """
    피로 패턴 상세 분석.

    Args:
        df: 전체 또는 단일 작업자 DataFrame
        worker_key: 대상 작업자 키

    Returns:
        {
          risk_segments: 연속작업 120분 초과 구간 목록
          break_gaps: 작업 블록 사이 휴식 구간 정보
          longest_no_break_min: 휴식 없이 가장 길게 일한 시간(분)
          recovery_score: 0~1, 적절한 휴식 비율
        }
    """
    wdf = df[df[ProcessedColumns.WORKER_KEY] == worker_key].copy()
    wdf = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)

    empty_result: dict[str, Any] = {
        "risk_segments": [],
        "break_gaps": [],
        "longest_no_break_min": 0,
        "recovery_score": 1.0,
    }

    if wdf.empty:
        return empty_result

    blocks = calc_working_blocks(wdf)
    if blocks.empty:
        return empty_result

    risk_segments: list[dict] = []
    for _, b in blocks.iterrows():
        if b["duration_min"] > FATIGUE_THRESHOLD_MIN:
            block_df = wdf[
                (wdf[RawColumns.TIME] >= b["start_time"])
                & (wdf[RawColumns.TIME] <= b["end_time"])
            ]
            locs = block_df[ProcessedColumns.CORRECTED_PLACE].mode()
            risk_segments.append({
                "start":    b["start_time"],
                "end":      b["end_time"],
                "duration": int(b["duration_min"]),
                "location": locs.iloc[0] if not locs.empty else "Unknown",
            })

    break_gaps: list[dict] = []
    longest_no_break = int(blocks["duration_min"].max()) if not blocks.empty else 0
    proper_count = 0
    total_gaps = 0

    for i in range(len(blocks) - 1):
        gap_start = blocks.iloc[i]["end_time"]
        gap_end   = blocks.iloc[i + 1]["start_time"]
        gap_min   = int((gap_end - gap_start).total_seconds() / 60)

        if gap_min >= 15:
            quality = "proper"
            proper_count += 1
        elif gap_min >= 5:
            quality = "short"
        else:
            quality = "micro"

        total_gaps += 1
        break_gaps.append({
            "start":        gap_start,
            "end":          gap_end,
            "duration_min": gap_min,
            "quality":      quality,
        })

    recovery_score = round(proper_count / total_gaps, 2) if total_gaps > 0 else 1.0

    return {
        "risk_segments":       risk_segments,
        "break_gaps":          break_gaps,
        "longest_no_break_min": longest_no_break,
        "recovery_score":       recovery_score,
    }


def generate_worker_insight(
    df: pd.DataFrame,
    worker_key: str,
    productivity_summary: dict,
    safety_summary: dict,
) -> list[dict]:
    """
    작업자의 하루를 자동 해석하여 인사이트 리스트 생성 (rule-based, LLM 없음).

    Args:
        df: 전체 또는 단일 작업자 DataFrame
        worker_key: 대상 작업자 키
        productivity_summary: calc_productivity_summary 결과
        safety_summary: calc_safety_summary 결과

    Returns:
        인사이트 딕셔너리 목록:
          type (warning|info|positive), category, title, detail,
          time_range, location, metric_value
    """
    insights: list[dict] = []
    wdf = df[df[ProcessedColumns.WORKER_KEY] == worker_key].copy()
    wdf = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)

    if wdf.empty:
        return insights

    idle_eps    = analyze_idle_episodes(df, worker_key)
    work_blocks = analyze_work_blocks(df, worker_key)
    fatigue     = analyze_fatigue_pattern(df, worker_key)

    def _fmt_ts(ts) -> str:
        try:
            return pd.Timestamp(ts).strftime("%H:%M")
        except Exception:
            return str(ts)

    def _add(itype: str, category: str, title: str, detail: str,
             time_range: str = "", location: str = "", metric_value: Any = None) -> None:
        insights.append({
            "type":         itype,
            "category":     category,
            "title":        title,
            "detail":       detail,
            "time_range":   time_range,
            "location":     location,
            "metric_value": metric_value,
        })

    # 규칙 1: 연속 idle ≥ 60분
    long_idles = idle_eps[idle_eps["duration_min"] >= 60]
    for _, ep in long_idles.iterrows():
        tr = f"{_fmt_ts(ep['start_time'])}~{_fmt_ts(ep['end_time'])}"
        _add("warning", "productivity",
             "장시간 비활동 감지",
             f"{ep['duration_min']}분 동안 비활성 상태가 지속되었습니다. 원인: {_cause_label(ep['cause'])}",
             tr, str(ep["location"]), ep["duration_min"])

    # 규칙 2: helmet_off 원인 idle (완전 비활성 정지)
    helmet_off = idle_eps[idle_eps["cause"] == "helmet_off"]
    for _, ep in helmet_off.iterrows():
        tr = f"{_fmt_ts(ep['start_time'])}~{_fmt_ts(ep['end_time'])}"
        _add("warning", "safety",
             "완전 비활성 정지",
             f"{tr} 동안 활성비율이 {ep['active_ratio_avg']:.2f}로 매우 낮습니다. "
             f"현장 부재 또는 장시간 정지 상태로 추정됩니다.",
             tr, str(ep["location"]), ep["active_ratio_avg"])

    # 규칙 3: 작업 블록 ≥ 3시간 (180분)
    long_blocks = work_blocks[work_blocks["duration_min"] >= 180]
    for _, blk in long_blocks.iterrows():
        tr = f"{_fmt_ts(blk['start_time'])}~{_fmt_ts(blk['end_time'])}"
        _add("warning", "safety",
             "장시간 연속 작업 (피로 위험)",
             f"{blk['duration_min']}분({blk['duration_min']//60}시간{blk['duration_min']%60}분) "
             f"연속 작업이 감지되었습니다. 충분한 휴식이 필요합니다.",
             tr, str(blk["location_sequence"]), blk["duration_min"])

    # 규칙 4: fragmentation ≥ 5
    frag = productivity_summary.get("fragmentation_index", 0)
    if frag >= 5:
        _add("info", "productivity",
             "작업이 자주 끊김",
             f"작업 분절 지수 {frag:.1f} — 시간당 {frag:.1f}회 작업이 중단됩니다. "
             f"집중 작업 환경 개선이 도움이 될 수 있습니다.",
             "", "", frag)

    # 규칙 5: 높은 활성비율 (positive)
    ar = productivity_summary.get("active_ratio", 0)
    if ar >= 0.7:
        _add("positive", "productivity",
             "높은 작업 집중도",
             f"활성비율 {ar:.1%}로 오늘 매우 높은 작업 집중도를 보였습니다.",
             "", "", ar)

    # 규칙 6: 휴게실 미방문
    rest_min = productivity_summary.get("rest_time_min", 0)
    if rest_min == 0:
        _add("info", "pattern",
             "휴게실 미방문",
             "오늘 휴게실 또는 식당 방문 기록이 없습니다. "
             "적절한 휴식이 생산성과 안전에 도움이 됩니다.",
             "", "", 0)

    # 규칙 7: 점심시간 idle (12~13시에 idle 에피소드)
    if not idle_eps.empty:
        lunch_idle = idle_eps[
            idle_eps["start_time"].apply(
                lambda t: 12 <= pd.Timestamp(t).hour < 13
            )
        ]
        if not lunch_idle.empty:
            lmin = int(lunch_idle["duration_min"].sum())
            _add("info", "pattern",
                 "점심시간 비활동",
                 f"점심시간(12~13시)에 {lmin}분 비활동이 감지되었습니다. "
                 f"식당 또는 휴게실 방문이 확인되지 않았습니다.",
                 "12:00~13:00", "", lmin)

    # 규칙 8: 정규 근무시간 외 활동
    off_hours = wdf[
        (wdf[ProcessedColumns.HOUR] < WORK_HOURS_START)
        | (wdf[ProcessedColumns.HOUR] >= WORK_HOURS_END)
    ]
    off_active = off_hours[off_hours[ProcessedColumns.ACTIVE_RATIO] >= 0.3]
    if len(off_active) >= 10:
        _add("info", "pattern",
             "정규 근무시간 외 활동",
             f"근무시간(07~20시) 외에 {len(off_active)}분간 활성 상태가 감지되었습니다.",
             "", "", len(off_active))

    return insights


def _cause_label(cause: str) -> str:
    """원인 코드를 한국어 레이블로 변환."""
    return {
        "helmet_off":  "완전 비활성 (정지/부재)",
        "waiting":     "대기",
        "slow_work":   "저강도 작업",
        "transition":  "이동 중",
        "unknown":     "미분류",
    }.get(cause, cause)
