"""
Site Operations Intelligence Framework (SOIF) — 핵심 지표 계산 모듈.

Layer 3 (Spatial State): Zone-Time Table
Layer 4 (Flow State):    Flow Edge Table, Bottleneck Score
Layer 5 (Operational Intelligence): EWI, Zone Utilization, CRE, OFI
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns
from src.utils.constants import (
    WORK_INTENSITY_HIGH_THRESHOLD,
    WORK_INTENSITY_LOW_THRESHOLD,
    ACTIVE_RATIO_ZERO_THRESHOLD,
    WORK_HOURS_START,
    WORK_HOURS_END,
    FATIGUE_THRESHOLD_MIN,
    ALONE_RISK_RADIUS,
)

logger = logging.getLogger(__name__)

# ── 구역 고유 위험 가중치 (Static Space Risk) ─────────────────────────
_STATIC_RISK: dict[str, float] = {
    "CONFINED_SPACE": 2.0,
    "WORK_AREA":      1.2,
    "OUTDOOR":        1.1,
    "INDOOR":         1.0,
    "GATE":           0.8,
    "REST":           0.3,
    "HELMET_RACK":    0.2,
    "OFFICE":         0.3,
    "UNKNOWN":        1.0,
}


# ═══════════════════════════════════════════════════════════════════════
# Layer 5-0: 출퇴근 시점 감지 (Work Shift Detection)
# ═══════════════════════════════════════════════════════════════════════

def detect_work_shift(df: pd.DataFrame) -> dict:
    """
    하루 Journey에서 출퇴근 시점을 감지.

    철학:
      - 개별 시점이 아니라, 하루 전체 Journey(문장)를 보고
        "실제 근무 블록"이 어디인지 찾는다.
      - 활성/비활성 패턴과 긴 공백(야간 off-duty) 을 이용해
        전날 퇴근 꼬리와 당일 근무를 구분한다.

    로직 개요:
      1. 시간순 정렬
      2. ACTIVE_RATIO 기반으로 "활동 Run" 목록 생성
         - 후보: ACTIVE_RATIO ≥ WORK_INTENSITY_LOW_THRESHOLD 이고,
                 PLACE_TYPE != HELMET_RACK
      3. Run 들을 시간 순으로 정렬한 뒤,
         - Run 사이의 간격이 LONG_INACTIVE_GAP_MIN (기본 240분)보다 크면
           서로 다른 Shift로 분리
         - 각 Shift별로 고/저활성 시간을 합산
      4. 가장 높은 활동 시간을 가진 Shift를 "당일 근무"로 선택
      5. 해당 Shift의 첫 Run 시작 시점을 출근(clock_in),
         마지막 Run 끝 시점을 퇴근(clock_out)으로 사용

    Returns:
        {
            "clock_in_idx": int,      # 출근 시점 행 인덱스
            "clock_out_idx": int,     # 퇴근 시점 행 인덱스
            "clock_in_time": Timestamp,
            "clock_out_time": Timestamp,
            "work_duration_min": int, # 출근~퇴근 사이 분 (행 기준)
            "pre_work_min": int,      # 출근 전 행 수 (off-duty 분)
            "post_work_min": int,     # 퇴근 후 행 수 (off-duty 분)
        }
    """
    if df.empty:
        return {
            "clock_in_idx": 0, "clock_out_idx": 0,
            "clock_in_time": None, "clock_out_time": None,
            "work_duration_min": 0, "pre_work_min": 0, "post_work_min": 0,
        }

    sorted_df = df.sort_values(RawColumns.TIME).reset_index(drop=True)
    times = pd.to_datetime(sorted_df[RawColumns.TIME].values)
    n = len(sorted_df)

    # ── (옵션) LLM 기반 출퇴근 시점 해석 ────────────────────────────────────
    # 배포 안정성을 위해 현재는 rule-based 결과만 사용.
    USE_LLM_SHIFT = False
    if USE_LLM_SHIFT and is_llm_available():
        try:
            worker_name = str(sorted_df[RawColumns.WORKER].iloc[0]) if RawColumns.WORKER in sorted_df.columns else "작업자"
            date_str = str(sorted_df[ProcessedColumns.DATE].iloc[0]) if ProcessedColumns.DATE in sorted_df.columns else ""
            company = str(sorted_df[RawColumns.COMPANY].iloc[0]) if RawColumns.COMPANY in sorted_df.columns else ""

            j_ctx = build_journey_context(sorted_df, worker_name=worker_name, date_str=date_str, company=company)
            llm_res = interpret_journey_shift(j_ctx) if j_ctx else {}

            if llm_res:
                def _find_idx(hhmm: str, is_start: bool) -> Optional[int]:
                    try:
                        base = pd.to_datetime(f"{date_str} {hhmm}") if date_str else pd.to_datetime(hhmm)
                    except Exception:
                        return None
                    if is_start:
                        mask = times >= base
                        if not mask.any():
                            return 0
                        return int(mask.argmax())
                    else:
                        mask = times <= base
                        if not mask.any():
                            return n - 1
                        return int(mask.nonzero()[0][-1])

                ci_idx = _find_idx(llm_res["clock_in"], True)
                co_idx = _find_idx(llm_res["clock_out"], False)

                if ci_idx is not None and co_idx is not None and 0 <= ci_idx < n and 0 <= co_idx < n and ci_idx <= co_idx:
                    clock_in_time = times[ci_idx]
                    clock_out_time = times[co_idx]
                    work_duration_min = co_idx - ci_idx + 1
                    pre_work_min = ci_idx
                    post_work_min = n - co_idx - 1
                    return {
                        "clock_in_idx": ci_idx,
                        "clock_out_idx": co_idx,
                        "clock_in_time": clock_in_time,
                        "clock_out_time": clock_out_time,
                        "work_duration_min": work_duration_min,
                        "pre_work_min": pre_work_min,
                        "post_work_min": post_work_min,
                    }
        except Exception as e:
            logger.warning(f"LLM 기반 출퇴근 시점 해석 실패, rule-based로 대체: {e}")

    # ── 1) 활성/비활성 기반 분 단위 Run 생성 ──────────────────────────────
    pt = sorted_df[ProcessedColumns.PLACE_TYPE].fillna("UNKNOWN").values
    ratio = sorted_df[ProcessedColumns.ACTIVE_RATIO].fillna(0).values

    is_work_space = (pt != "HELMET_RACK") & (pt != "REST")

    strict_active = (ratio >= WORK_INTENSITY_LOW_THRESHOLD) & is_work_space
    strict_active_count = int(strict_active.sum())

    if strict_active_count >= 30:
        base_candidate = strict_active
    else:
        # 엄격 기준으로는 활동 분이 너무 적으면, ratio>0 인 모든 분을 후보로 사용
        base_candidate = (ratio > 0) & is_work_space

    # ── 1-1) 계층적 판정: off-duty 구간으로 하루를 여러 Segment로 나누고,
    #       각 Segment의 활동 점수를 비교해 가장 큰 Segment를 근무로 선택 ──
    is_candidate = base_candidate.copy()
    candidate_indices = np.where(base_candidate)[0]

    if candidate_indices.size > 0:
        LONG_OFF_MIN = 240      # 4시간
        OFF_RATIO_MIN = 0.9     # 창 안의 90% 이상이 완전 비활성이면 off-duty 창

        # off-duty 창의 중심 인덱스를 separator 후보로 수집
        separators: list[int] = []
        first_active_idx = int(candidate_indices[0])
        s_start = first_active_idx + 1
        for s in range(s_start, n):
            e = s + LONG_OFF_MIN - 1
            if e >= n:
                break
            window = ratio[s:e + 1]
            length = e - s + 1
            off_mask = window < ACTIVE_RATIO_ZERO_THRESHOLD
            off_ratio = off_mask.sum() / float(length)
            if off_ratio >= OFF_RATIO_MIN:
                mid = (s + e) // 2
                separators.append(mid)

        # separator 들을 연속 구간으로 압축 (하나의 off-duty 밴드)
        bands: list[tuple[int, int]] = []
        if separators:
            sep_sorted = sorted(separators)
            band_start = sep_sorted[0]
            prev = sep_sorted[0]
            for idx in sep_sorted[1:]:
                if idx == prev + 1:
                    prev = idx
                else:
                    bands.append((band_start, prev))
                    band_start = idx
                    prev = idx
            bands.append((band_start, prev))

        # bands 를 기준으로 Segment 구간 생성
        segments: list[tuple[int, int]] = []
        if bands:
            prev_end = -1
            for b_start, b_end in bands:
                seg_start = prev_end + 1
                seg_end = b_start - 1
                if seg_start <= seg_end:
                    segments.append((seg_start, seg_end))
                prev_end = b_end
            # 마지막 Segment
            if prev_end + 1 <= n - 1:
                segments.append((prev_end + 1, n - 1))
        else:
            # off-duty 밴드가 없으면 하루 전체를 하나의 Segment 로 본다.
            segments.append((0, n - 1))

        # 각 Segment 에 대해, base_candidate 가 True 인 분들의 ACTIVE_RATIO 합을 점수로 계산
        best_seg = None
        best_score = -1.0
        for seg_start, seg_end in segments:
            idxs = candidate_indices[(candidate_indices >= seg_start) & (candidate_indices <= seg_end)]
            if idxs.size == 0:
                continue
            score = float(ratio[idxs].clip(min=0).sum())
            if score > best_score:
                best_score = score
                best_seg = (seg_start, seg_end)

        if best_seg is not None:
            seg_start, seg_end = best_seg
            idxs = candidate_indices[(candidate_indices >= seg_start) & (candidate_indices <= seg_end)]
            if idxs.size > 0:
                is_candidate[:] = False
                is_candidate[idxs] = True

    runs: list[dict] = []
    current_start: Optional[int] = None

    for i in range(n):
        if is_candidate[i]:
            if current_start is None:
                current_start = i
        else:
            if current_start is not None:
                runs.append({
                    "start_idx": current_start,
                    "end_idx": i - 1,
                })
                current_start = None

    if current_start is not None:
        runs.append({
            "start_idx": current_start,
            "end_idx": n - 1,
        })

    if not runs:
        # 활동 Run 자체가 없으면, 전 구간을 하나의 블록으로 취급
        clock_in_idx = 0
        clock_out_idx = n - 1
    else:
        # ── 2) Run 들을 긴 비활성 공백 기준으로 Shift 로 클러스터 ────────────
        LONG_OFF_MIN = 240  # 4시간 이상 공백이면 다른 Shift

        shifts: list[dict] = []
        current_shift = {
            "runs": [],
            "score": 0.0,
        }
        last_end_time = None

        for run in runs:
            s_idx = run["start_idx"]
            e_idx = run["end_idx"]
            start_t = times[s_idx]
            end_t = times[e_idx]
            length_min = e_idx - s_idx + 1

            # Run 활동 점수: 길이 × 평균 활성비율
            avg_r = float(ratio[s_idx:e_idx + 1].mean() if e_idx >= s_idx else 0.0)
            run_score = length_min * max(avg_r, 0.0)

            if last_end_time is None:
                current_shift["runs"].append(run)
                current_shift["score"] += run_score
                last_end_time = end_t
            else:
                gap_min = (start_t - last_end_time).total_seconds() / 60.0
                if gap_min >= LONG_OFF_MIN:
                    shifts.append(current_shift)
                    current_shift = {
                        "runs": [run],
                        "score": run_score,
                    }
                else:
                    current_shift["runs"].append(run)
                    current_shift["score"] += run_score
                last_end_time = end_t

        if current_shift["runs"]:
            shifts.append(current_shift)

        # ── 3) 활동 점수가 가장 큰 Shift를 근무 Shift로 선택 ────────────────
        best_shift = max(shifts, key=lambda s: s["score"])
        shift_runs = best_shift["runs"]
        shift_runs.sort(key=lambda r: r["start_idx"])

        clock_in_idx = shift_runs[0]["start_idx"]
        clock_out_idx = shift_runs[-1]["end_idx"]

    clock_in_time = times[clock_in_idx]
    clock_out_time = times[clock_out_idx]

    pre_work_min = clock_in_idx
    post_work_min = n - clock_out_idx - 1
    work_duration_min = clock_out_idx - clock_in_idx + 1

    return {
        "clock_in_idx": clock_in_idx,
        "clock_out_idx": clock_out_idx,
        "clock_in_time": clock_in_time,
        "clock_out_time": clock_out_time,
        "work_duration_min": work_duration_min,
        "pre_work_min": pre_work_min,
        "post_work_min": post_work_min,
    }


# ═══════════════════════════════════════════════════════════════════════
# Layer 5-1: EWI (Effective Work Intensity)
# ═══════════════════════════════════════════════════════════════════════

def calc_ewi(df: pd.DataFrame, worker_key: Optional[str] = None) -> dict:
    """유효 작업 집중도 (EWI) 계산.

    ★ v6.5 변경: 실제 출퇴근 시간 기반 계산 (음영지역 고려)
    - 분모: 출근~퇴근 실제 시간 차이 (분) — 데이터 기록 수가 아닌 시간 차이
    - 분자: 고활성 작업 × 1.0 + 저활성 작업 × 0.5
    
    ★ 핵심: 음영지역(신호 미수집 구간)이 있어도 출퇴근 시간 기준으로 계산
    예) 출근 07:30, 퇴근 17:30 → 분모 = 600분 (기록된 400분이 아님)
    
    EWI = (High Work × 1.0  +  Low Work × 0.5) / (퇴근시간 - 출근시간)

    Returns:
        dict with ewi, high_work_min, low_work_min, standby_min,
        transit_min, rest_min, off_duty_min, onsite_min,
        work_duration_min, clock_in_time, clock_out_time, gap_min (음영지역 시간)
    """
    wdf = df.copy()
    if worker_key:
        wdf = wdf[wdf[ProcessedColumns.WORKER_KEY] == worker_key]

    if wdf.empty:
        return {"ewi": 0.0}

    # ── 출퇴근 시점 감지 ─────────────────────────────────────────────
    shift = detect_work_shift(wdf)
    clock_in_idx = shift["clock_in_idx"]
    clock_out_idx = shift["clock_out_idx"]
    clock_in_time = shift["clock_in_time"]
    clock_out_time = shift["clock_out_time"]
    
    # 시간순 정렬
    sorted_wdf = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)
    
    # 출근~퇴근 구간만 추출
    work_period_df = sorted_wdf.iloc[clock_in_idx:clock_out_idx + 1]
    
    if work_period_df.empty or clock_in_time is None or clock_out_time is None:
        return {
            "ewi": 0.0,
            "high_work_min": 0,
            "low_work_min": 0,
            "standby_min": 0,
            "transit_min": 0,
            "rest_min": 0,
            "off_duty_min": 0,
            "onsite_min": 0,
            "work_duration_min": 0,
            "recorded_min": 0,
            "gap_min": 0,
            "clock_in_time": clock_in_time,
            "clock_out_time": clock_out_time,
            "pre_work_min": shift["pre_work_min"],
            "post_work_min": shift["post_work_min"],
        }

    ratio = work_period_df[ProcessedColumns.ACTIVE_RATIO].fillna(0)
    pt = work_period_df[ProcessedColumns.PLACE_TYPE].fillna("UNKNOWN")
    hour = work_period_df[ProcessedColumns.HOUR].fillna(12)
    period = work_period_df[ProcessedColumns.PERIOD_TYPE].fillna("off")

    # 작업 구역 마스크 (출근~퇴근 내에서도 RACK/REST/GATE 제외)
    work_mask = (
        (pt != "HELMET_RACK")
        & (pt != "REST")
        & (pt != "GATE")
    )

    high_work = (work_mask & (ratio >= WORK_INTENSITY_HIGH_THRESHOLD)).sum()
    low_work = (work_mask & (ratio >= WORK_INTENSITY_LOW_THRESHOLD) & (ratio < WORK_INTENSITY_HIGH_THRESHOLD)).sum()
    standby = (work_mask & (ratio >= ACTIVE_RATIO_ZERO_THRESHOLD) & (ratio < WORK_INTENSITY_LOW_THRESHOLD)).sum()

    rest_min = (pt == "REST").sum() + ((period == "rest") & (pt != "REST")).sum()
    transit_min = (pt == "GATE").sum()
    rack_in_shift = (pt == "HELMET_RACK").sum()  # 근무 중 잠깐 RACK (점심 등)
    
    # ★ 핵심 변경: 실제 출퇴근 시간 차이 (분) — 음영지역 포함
    actual_work_duration_min = int((clock_out_time - clock_in_time).total_seconds() / 60)
    
    # 기록된 데이터 수 (음영지역 제외)
    recorded_min = len(work_period_df)
    
    # 음영지역 시간 (신호 미수집 구간)
    gap_min = actual_work_duration_min - recorded_min
    
    # EWI 계산: 분모는 실제 출퇴근 시간 차이 (음영지역 포함)
    # 휴게실/헬멧거치대 시간은 제외하지 않음 — 출퇴근 전체 시간 대비 생산성
    ewi = (high_work * 1.0 + low_work * 0.5) / actual_work_duration_min if actual_work_duration_min > 0 else 0.0

    # 전체 기록 대비 off_duty (출근 전 + 퇴근 후)
    total_records = len(wdf)
    off_duty = shift["pre_work_min"] + shift["post_work_min"]

    return {
        "ewi": round(ewi, 4),
        "high_work_min": int(high_work),
        "low_work_min": int(low_work),
        "standby_min": int(standby),
        "transit_min": int(transit_min),
        "rest_min": int(rest_min),
        "off_duty_min": int(off_duty),
        "onsite_min": int(total_records),
        "work_duration_min": int(actual_work_duration_min),  # 실제 출퇴근 시간 차이
        "recorded_min": int(recorded_min),  # 기록된 데이터 수
        "gap_min": int(gap_min),  # 음영지역 시간
        "effective_work_min": int(recorded_min - rack_in_shift),  # 기록된 시간 중 RACK 제외
        "clock_in_time": clock_in_time,
        "clock_out_time": clock_out_time,
        "pre_work_min": shift["pre_work_min"],
        "post_work_min": shift["post_work_min"],
    }


def calc_ewi_by_worker(df: pd.DataFrame) -> pd.DataFrame:
    """전체 작업자별 EWI 계산."""
    rows = []
    for wk in df[ProcessedColumns.WORKER_KEY].unique():
        wdf = df[df[ProcessedColumns.WORKER_KEY] == wk]
        name = wdf[RawColumns.WORKER].iloc[0]
        company = wdf[RawColumns.COMPANY].iloc[0]
        result = calc_ewi(df, worker_key=wk)
        result["worker_key"] = wk
        result["worker"] = name
        result["company"] = company
        rows.append(result)
    return pd.DataFrame(rows)


def calc_ewi_by_company(df: pd.DataFrame) -> pd.DataFrame:
    """업체별 평균 EWI 계산."""
    worker_ewi = calc_ewi_by_worker(df)
    if worker_ewi.empty:
        return pd.DataFrame()
    return (
        worker_ewi.groupby("company")
        .agg(
            ewi_avg=("ewi", "mean"),
            ewi_max=("ewi", "max"),
            ewi_min=("ewi", "min"),
            worker_count=("worker_key", "nunique"),
            total_high_work=("high_work_min", "sum"),
            total_low_work=("low_work_min", "sum"),
            total_standby=("standby_min", "sum"),
        )
        .reset_index()
    )


# ═══════════════════════════════════════════════════════════════════════
# Layer 3: Zone-Time Table (Spatial State)
# ═══════════════════════════════════════════════════════════════════════

def build_zone_time_table(df: pd.DataFrame, time_slot_min: int = 60) -> pd.DataFrame:
    """Zone × TimeSlot 공간 상태 테이블 생성.

    Args:
        df: 전처리 완료된 DataFrame
        time_slot_min: 시간 슬롯 크기 (분). 기본 60분(1시간).

    Returns:
        DataFrame: zone, time_slot, worker_count, high_work_count,
                   low_work_count, standby_count, transit_count,
                   rest_count, off_duty_count, avg_active_ratio,
                   zone_utilization
    """
    wdf = df.copy()

    place_col = ProcessedColumns.CORRECTED_PLACE if ProcessedColumns.CORRECTED_PLACE in wdf.columns else RawColumns.PLACE
    ratio_col = ProcessedColumns.ACTIVE_RATIO
    pt_col = ProcessedColumns.PLACE_TYPE
    hour_col = ProcessedColumns.HOUR

    if time_slot_min == 60:
        wdf["_time_slot"] = wdf[hour_col].astype(int)
    else:
        minute_of_day = wdf[hour_col].astype(int) * 60 + wdf.get(ProcessedColumns.MINUTE, pd.Series(0, index=wdf.index)).astype(int)
        wdf["_time_slot"] = (minute_of_day // time_slot_min) * time_slot_min

    ratio = wdf[ratio_col].fillna(0)
    pt = wdf[pt_col].fillna("UNKNOWN")
    hour = wdf[hour_col].fillna(12).astype(int)

    work_mask = (
        (pt != "HELMET_RACK") & (pt != "REST") & (pt != "GATE")
        & (hour >= WORK_HOURS_START) & (hour < WORK_HOURS_END)
    )

    wdf["_cat"] = "off_duty"
    wdf.loc[work_mask & (ratio >= WORK_INTENSITY_HIGH_THRESHOLD), "_cat"] = "high_work"
    wdf.loc[work_mask & (ratio >= WORK_INTENSITY_LOW_THRESHOLD) & (ratio < WORK_INTENSITY_HIGH_THRESHOLD), "_cat"] = "low_work"
    wdf.loc[work_mask & (ratio >= ACTIVE_RATIO_ZERO_THRESHOLD) & (ratio < WORK_INTENSITY_LOW_THRESHOLD), "_cat"] = "standby"
    wdf.loc[pt == "REST", "_cat"] = "rest"
    wdf.loc[pt == "GATE", "_cat"] = "transit"
    wdf.loc[pt == "HELMET_RACK", "_cat"] = "off_duty"

    grouped = wdf.groupby([place_col, "_time_slot"])

    results = []
    for (zone, ts), grp in grouped:
        cats = grp["_cat"].value_counts()
        total = len(grp)
        workers = grp[ProcessedColumns.WORKER_KEY].nunique()
        avg_ratio = grp[ratio_col].mean()

        high = cats.get("high_work", 0)
        low = cats.get("low_work", 0)
        productive = high + low
        utilization = productive / total if total > 0 else 0

        results.append({
            "zone": zone,
            "time_slot": int(ts),
            "worker_count": workers,
            "total_person_min": total,
            "high_work_count": int(high),
            "low_work_count": int(low),
            "standby_count": int(cats.get("standby", 0)),
            "transit_count": int(cats.get("transit", 0)),
            "rest_count": int(cats.get("rest", 0)),
            "off_duty_count": int(cats.get("off_duty", 0)),
            "avg_active_ratio": round(avg_ratio, 4),
            "zone_utilization": round(utilization, 4),
        })

    return pd.DataFrame(results) if results else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════
# Layer 4: Flow Edge Table (Flow State)
# ═══════════════════════════════════════════════════════════════════════

def build_flow_edge_table(df: pd.DataFrame) -> pd.DataFrame:
    """구역 간 전이(Flow Edge) 테이블 생성.

    각 작업자의 시계열에서 장소가 바뀌는 순간을 추출하여
    (from_zone, to_zone) 엣지를 생성.

    Returns:
        DataFrame: from_zone, to_zone, transition_count,
                   avg_transition_gap_min, unique_workers
    """
    place_col = ProcessedColumns.CORRECTED_PLACE if ProcessedColumns.CORRECTED_PLACE in df.columns else RawColumns.PLACE

    edges: list[dict] = []

    for wk, wdf in df.groupby(ProcessedColumns.WORKER_KEY):
        sorted_df = wdf.sort_values(RawColumns.TIME).reset_index(drop=True)
        places = sorted_df[place_col].values
        times = sorted_df[RawColumns.TIME].values

        for i in range(1, len(places)):
            if places[i] != places[i - 1]:
                gap = (pd.Timestamp(times[i]) - pd.Timestamp(times[i - 1])).total_seconds() / 60
                edges.append({
                    "from_zone": str(places[i - 1]),
                    "to_zone": str(places[i]),
                    "gap_min": gap,
                    "worker_key": wk,
                    "timestamp": pd.Timestamp(times[i]),
                })

    if not edges:
        return pd.DataFrame()

    edge_df = pd.DataFrame(edges)
    result = (
        edge_df.groupby(["from_zone", "to_zone"])
        .agg(
            transition_count=("gap_min", "count"),
            avg_transition_gap_min=("gap_min", "mean"),
            unique_workers=("worker_key", "nunique"),
        )
        .reset_index()
        .sort_values("transition_count", ascending=False)
    )
    result["avg_transition_gap_min"] = result["avg_transition_gap_min"].round(1)
    return result


# ═══════════════════════════════════════════════════════════════════════
# Layer 4: Bottleneck Score
# ═══════════════════════════════════════════════════════════════════════

def calc_bottleneck_scores(
    zone_time_df: pd.DataFrame,
    flow_edge_df: pd.DataFrame,
) -> pd.DataFrame:
    """구역별 병목 점수 (Bottleneck Score) 계산.

    BS = Norm(ΔInflow − ΔOutflow) + Standby Pressure
    """
    if zone_time_df.empty or flow_edge_df.empty:
        return pd.DataFrame()

    zones = zone_time_df.groupby("zone").agg(
        total_person_min=("total_person_min", "sum"),
        standby_total=("standby_count", "sum"),
        avg_utilization=("zone_utilization", "mean"),
    ).reset_index()

    inflow = flow_edge_df.groupby("to_zone")["transition_count"].sum().rename("inflow")
    outflow = flow_edge_df.groupby("from_zone")["transition_count"].sum().rename("outflow")

    zones = zones.merge(inflow, left_on="zone", right_index=True, how="left")
    zones = zones.merge(outflow, left_on="zone", right_index=True, how="left")
    zones["inflow"] = zones["inflow"].fillna(0)
    zones["outflow"] = zones["outflow"].fillna(0)

    zones["flow_imbalance"] = zones["inflow"] - zones["outflow"]

    fi_max = zones["flow_imbalance"].abs().max()
    zones["flow_imbalance_norm"] = (
        zones["flow_imbalance"].abs() / fi_max if fi_max > 0 else 0
    )

    sp_max = zones["standby_total"].max()
    zones["standby_pressure"] = (
        zones["standby_total"] / sp_max if sp_max > 0 else 0
    )

    zones["bottleneck_score"] = (
        zones["flow_imbalance_norm"] * 0.6
        + zones["standby_pressure"] * 0.4
    ).round(4)

    return zones.sort_values("bottleneck_score", ascending=False).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════
# Layer 5-2: Zone Utilization
# ═══════════════════════════════════════════════════════════════════════

def calc_zone_utilization(zone_time_df: pd.DataFrame) -> pd.DataFrame:
    """구역별 유효 활용도 계산.

    작업 구역이 실제로 생산적 작업에 사용되는 비율.
    """
    if zone_time_df.empty:
        return pd.DataFrame()

    zones = zone_time_df.groupby("zone").agg(
        total_person_min=("total_person_min", "sum"),
        high_work_total=("high_work_count", "sum"),
        low_work_total=("low_work_count", "sum"),
        standby_total=("standby_count", "sum"),
        avg_active_ratio=("avg_active_ratio", "mean"),
        worker_count=("worker_count", "max"),
        time_slots_active=("worker_count", lambda x: (x > 0).sum()),
    ).reset_index()

    zones["productive_min"] = zones["high_work_total"] + zones["low_work_total"]
    zones["utilization"] = (
        zones["productive_min"] / zones["total_person_min"]
    ).fillna(0).round(4)

    zones["waste_ratio"] = (
        zones["standby_total"] / zones["total_person_min"]
    ).fillna(0).round(4)

    return zones.sort_values("utilization", ascending=False).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════
# Layer 5-3: CRE (Combined Risk Exposure)
# ═══════════════════════════════════════════════════════════════════════

def calc_cre(df: pd.DataFrame, worker_key: Optional[str] = None) -> dict:
    """복합 위험 노출도 (CRE) 계산.

    Total Risk = Personal Risk × Static Space Risk × Dynamic Pressure

    Personal Risk: 피로도 (연속 작업 시간) + 단독 작업 비율
    Static Space Risk: 구역 고유 위험 가중치
    Dynamic Pressure: 구역 내 밀집도, 이상 정지 빈도
    """
    wdf = df.copy()
    if worker_key:
        wdf = wdf[wdf[ProcessedColumns.WORKER_KEY] == worker_key]

    if wdf.empty:
        return {"cre": 0.0, "personal_risk": 0.0, "static_risk": 0.0, "dynamic_pressure": 0.0}

    # ── Personal Risk ─────────────────────────────────────────────────
    # 피로 위험: 연속 작업 블록 최대 길이 / 기준
    sorted_wdf = wdf.sort_values(RawColumns.TIME)
    ratio = sorted_wdf[ProcessedColumns.ACTIVE_RATIO].fillna(0).values
    max_streak = _max_work_streak(ratio)
    fatigue_score = min(max_streak / FATIGUE_THRESHOLD_MIN, 2.0)

    # 단독 작업 추정: 전체에서 같은 시간대에 다른 작업자가 없는 비율
    alone_score = 0.0
    if worker_key:
        work_mask = wdf[ProcessedColumns.PERIOD_TYPE] == "work"
        work_df = wdf[work_mask]
        if not work_df.empty:
            alone_minutes = 0
            for _, row in work_df.iterrows():
                loc_key = row.get(ProcessedColumns.LOCATION_KEY)
                ts = row[RawColumns.TIME]
                if pd.isna(loc_key):
                    continue
                same_time = df[
                    (df[RawColumns.TIME] == ts)
                    & (df[ProcessedColumns.LOCATION_KEY] == loc_key)
                    & (df[ProcessedColumns.WORKER_KEY] != worker_key)
                ]
                if len(same_time) == 0:
                    alone_minutes += 1
            alone_score = alone_minutes / len(work_df) if len(work_df) > 0 else 0

    personal_risk = 0.5 * fatigue_score + 0.5 * alone_score
    personal_risk = min(personal_risk, 2.0)

    # ── Static Space Risk ─────────────────────────────────────────────
    place_types = wdf[ProcessedColumns.PLACE_TYPE].fillna("UNKNOWN")
    static_weights = place_types.map(_STATIC_RISK).fillna(1.0)
    work_mask_s = wdf[ProcessedColumns.PERIOD_TYPE] == "work"
    if work_mask_s.any():
        static_risk = static_weights[work_mask_s].mean()
    else:
        static_risk = static_weights.mean()

    # ── Dynamic Pressure ──────────────────────────────────────────────
    # 같은 시간대·장소의 인원 밀도 기반
    densities = []
    place_col = ProcessedColumns.CORRECTED_PLACE if ProcessedColumns.CORRECTED_PLACE in df.columns else RawColumns.PLACE
    for hour in wdf[ProcessedColumns.HOUR].unique():
        h_df = df[df[ProcessedColumns.HOUR] == hour]
        for place in wdf[wdf[ProcessedColumns.HOUR] == hour][place_col].unique():
            cnt = h_df[h_df[place_col] == place][ProcessedColumns.WORKER_KEY].nunique()
            densities.append(cnt)

    density_avg = np.mean(densities) if densities else 1.0
    dynamic_pressure = min(density_avg / 5.0, 2.0)

    # ── CRE 합산 ─────────────────────────────────────────────────────
    cre = personal_risk * static_risk * dynamic_pressure

    return {
        "cre": round(cre, 4),
        "personal_risk": round(personal_risk, 4),
        "fatigue_score": round(fatigue_score, 4),
        "alone_score": round(alone_score, 4),
        "static_risk": round(static_risk, 4),
        "dynamic_pressure": round(dynamic_pressure, 4),
    }


def calc_cre_by_worker(df: pd.DataFrame) -> pd.DataFrame:
    """전체 작업자별 CRE 계산."""
    rows = []
    for wk in df[ProcessedColumns.WORKER_KEY].unique():
        wdf = df[df[ProcessedColumns.WORKER_KEY] == wk]
        name = wdf[RawColumns.WORKER].iloc[0]
        company = wdf[RawColumns.COMPANY].iloc[0]
        result = calc_cre(df, worker_key=wk)
        result["worker_key"] = wk
        result["worker"] = name
        result["company"] = company
        rows.append(result)
    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════
# Layer 5-4: OFI (Operational Friction Index)
# ═══════════════════════════════════════════════════════════════════════

def calc_ofi(df: pd.DataFrame, company: Optional[str] = None) -> dict:
    """운영 마찰 지수 (OFI) 계산.

    OFI = (Standby Time + Excess Transit Time) / Total On-site Duration

    Excess Transit: 이동 시간이 전체의 10% 초과분
    낮을수록 효율적. 0.3 이상이면 심각한 운영 마찰.
    """
    wdf = df.copy()
    if company:
        wdf = wdf[wdf[RawColumns.COMPANY] == company]

    if wdf.empty:
        return {"ofi": 0.0}

    ratio = wdf[ProcessedColumns.ACTIVE_RATIO].fillna(0)
    pt = wdf[ProcessedColumns.PLACE_TYPE].fillna("UNKNOWN")
    hour = wdf[ProcessedColumns.HOUR].fillna(12).astype(int)

    onsite_mask = (hour >= WORK_HOURS_START) & (hour < WORK_HOURS_END)
    onsite = onsite_mask.sum()

    if onsite == 0:
        return {"ofi": 0.0, "standby_min": 0, "transit_min": 0, "onsite_min": 0}

    work_mask = onsite_mask & (pt != "HELMET_RACK") & (pt != "REST") & (pt != "GATE")
    standby = (work_mask & (ratio >= ACTIVE_RATIO_ZERO_THRESHOLD) & (ratio < WORK_INTENSITY_LOW_THRESHOLD)).sum()

    transit = (onsite_mask & (pt == "GATE")).sum()
    transit_pct = transit / onsite if onsite > 0 else 0
    excess_transit = max(0, transit - onsite * 0.10)

    ofi = (standby + excess_transit) / onsite

    return {
        "ofi": round(float(ofi), 4),
        "standby_min": int(standby),
        "transit_min": int(transit),
        "excess_transit_min": round(float(excess_transit), 1),
        "onsite_min": int(onsite),
        "transit_pct": round(float(transit_pct), 4),
    }


def calc_ofi_by_company(df: pd.DataFrame) -> pd.DataFrame:
    """업체별 OFI 계산."""
    rows = []
    for comp in df[RawColumns.COMPANY].unique():
        result = calc_ofi(df, company=comp)
        result["company"] = comp
        worker_count = df[df[RawColumns.COMPANY] == comp][ProcessedColumns.WORKER_KEY].nunique()
        result["worker_count"] = worker_count
        rows.append(result)
    return pd.DataFrame(rows).sort_values("ofi", ascending=False).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════
# Layer 5: 통합 SOIF Summary
# ═══════════════════════════════════════════════════════════════════════

def calc_soif_summary(df: pd.DataFrame) -> dict:
    """전체 현장 SOIF 통합 요약.

    Returns:
        dict with site-level EWI, OFI, avg CRE, top bottlenecks, zone stats
    """
    site_ewi = calc_ewi(df)

    zone_time = build_zone_time_table(df)
    flow_edge = build_flow_edge_table(df)

    bottleneck = calc_bottleneck_scores(zone_time, flow_edge)
    zone_util = calc_zone_utilization(zone_time)

    site_ofi = calc_ofi(df)
    cre_by_worker = calc_cre_by_worker(df)

    ewi_by_worker = calc_ewi_by_worker(df)
    ewi_by_company = calc_ewi_by_company(df)
    ofi_by_company = calc_ofi_by_company(df)

    top_bottlenecks = []
    if not bottleneck.empty:
        top_bottlenecks = bottleneck.head(5)[["zone", "bottleneck_score", "flow_imbalance", "standby_total"]].to_dict("records")

    return {
        "site_ewi": site_ewi,
        "site_ofi": site_ofi,
        "avg_cre": round(cre_by_worker["cre"].mean(), 4) if not cre_by_worker.empty else 0,
        "max_cre": round(cre_by_worker["cre"].max(), 4) if not cre_by_worker.empty else 0,
        "worker_count": df[ProcessedColumns.WORKER_KEY].nunique(),
        "zone_count": zone_time["zone"].nunique() if not zone_time.empty else 0,
        "flow_edge_count": len(flow_edge),
        "top_bottlenecks": top_bottlenecks,
        "zone_time_df": zone_time,
        "flow_edge_df": flow_edge,
        "bottleneck_df": bottleneck,
        "zone_util_df": zone_util,
        "ewi_by_worker": ewi_by_worker,
        "ewi_by_company": ewi_by_company,
        "cre_by_worker": cre_by_worker,
        "ofi_by_company": ofi_by_company,
    }


# ═══════════════════════════════════════════════════════════════════════
# 내부 헬퍼
# ═══════════════════════════════════════════════════════════════════════

def _max_work_streak(ratios: np.ndarray) -> int:
    """활성비율 배열에서 연속 작업(≥ 0.05) 최대 길이(분) 반환."""
    max_s = 0
    cur = 0
    for r in ratios:
        if r >= ACTIVE_RATIO_ZERO_THRESHOLD:
            cur += 1
            max_s = max(max_s, cur)
        else:
            cur = 0
    return max_s
