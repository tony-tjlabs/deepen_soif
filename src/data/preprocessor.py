"""
Journey 보정 전처리 모듈.
Raw CSV 데이터에서 작업자의 Journey를 보정하여 더 정확한 행동 패턴을 추출한다.

전처리 파이프라인 (preprocess):
  Step 1: 활성비율 계산 + coverage_gap / signal_confidence
  Step 2: 장소 분류 (SSMP 또는 키워드)
  Step 3: 작업자 키 생성
  Step 4: 시간 파생 컬럼
  Step 5: 작업자별 Journey 보정
    Phase 0: DBSCAN 좌표계별 클러스터링 (nearest-cluster 노이즈 보정)
    Phase 1: 헬멧 거치 패턴 보정 (REST 장소 제외)
    Phase 2: 좌표 이상치 보정
    Phase 2-post: 좌표↔장소명 정합성 검증
  Step 5-post: CORRECTED_PLACE 기반 PLACE_TYPE / LOCATION_KEY 재분류
  Step 6: 활동 유형 분류 (PERIOD_TYPE)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns
from src.utils.constants import (
    ACTIVE_RATIO_WORKING_THRESHOLD,
    ACTIVE_RATIO_ZERO_THRESHOLD,
    LOCATION_SMOOTHING_WINDOW,
    HELMET_RACK_MIN_DURATION_MIN,
    COORD_OUTLIER_THRESHOLD,
    DBSCAN_EPS_INDOOR,
    DBSCAN_EPS_OUTDOOR,
    DBSCAN_MIN_SAMPLES,
    SpaceFunction,
    DBSCAN_EPS_MULTIPLIER,
    STATE_OVERRIDE_SPACES,
    DWELL_NORMAL_MAX,
    TRANSITION_INTER_LOCATION_MIN,
    TRANSITION_SAME_LOCATION_MIN,
    TRANSITION_FROM_ENTRY_MIN,
    TRANSITION_FROM_REST_MIN,
    TRANSITION_MAX_RATIO,
    TRANSITION_TRAVEL_EXCLUDE_DEST,
    # v5: Intelligent Sequence Interpreter
    SPACE_DWELL_PROFILE,
    SEQUENCE_WINDOW_MIN,
    SEQUENCE_VARIETY_THRESH,
    # v6: 4증거 통합 Journey 보정
    ACTIVE_SIG_GHOST_MAX,
    ACTIVE_SIG_TRANSIT_MAX,
    ACTIVE_SIG_WORK_MIN,
    LOCATION_ENTROPY_WINDOW,
    LOCATION_UNSTABLE_THRESH,
    RUN_SHORT_MAX,
    RUN_CONTINUOUS_MIN,
    NIGHT_END_HOUR,
    PREDAWN_WORK_START,
    POST_WORK_HOUR,
    GHOST_SIGNAL_RACK_SEARCH_WINDOW,
    SPACE_FUNCTION_PRIORITY,
    TRANSIT_ONLY_FUNCTIONS,
    ANCHOR_SPACE_FUNCTIONS,
    ANCHOR_PLACE_KEYWORDS,
    WORK_HOURS_START,
    WORK_HOURS_END,
    LUNCH_START,
    LUNCH_END,
    # v6.1: Multi-Pass Refinement
    GHOST_MIN_BLOCK_LEN,
    GHOST_WORK_MIN_BLOCK_LEN,
    NARRATIVE_ANCHOR_MIN_DWELL,
    NARRATIVE_WORK_MIN_RATIO,
    IMPOSSIBLE_MOVE_SPEED,
    IMPOSSIBLE_BUILDING_JUMP_MIN,
    CONVERGENCE_CHANGE_THRESH,
    MULTI_PASS_MAX_ITERATIONS,
)
from src.utils.place_classifier import (
    add_place_columns,
    classify_place,
    classify_space_type,
    is_helmet_rack,
    make_location_key,
    classify_space_function,
    get_hazard_weight,
    classify_state_by_space,
)
from src.utils.time_utils import classify_activity_period, is_night_or_dawn, is_lunch_time
from src.utils.constants import REST_AREA_KEYWORDS, ANCHOR_SPACE_FUNCTIONS, ANCHOR_PLACE_KEYWORDS

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# v5.2 앵커 공간 보호 헬퍼 함수
# ═══════════════════════════════════════════════════════════════════════════

def _is_anchor_row(row_place_type: str, row_space_function: str) -> bool:
    """
    이 행이 앵커 공간(REST/RACK)인지 판별.
    앵커 행은 DBSCAN·노이즈보정·슬라이딩윈도우에서 보호됨.
    """
    # space_function 기반 (우선)
    if row_space_function in ANCHOR_SPACE_FUNCTIONS:
        return True
    # place_type 기반 (폴백)
    if row_place_type in ("REST", "HELMET_RACK"):
        return True
    return False


def _build_anchor_mask(df: pd.DataFrame) -> pd.Series:
    """
    DataFrame 전체에 대해 앵커 행 마스크(bool Series) 생성.
    space_function 컬럼이 없으면 place_type + 장소명 키워드로 폴백.
    """
    sf_col = ProcessedColumns.SPACE_FUNCTION
    pt_col = ProcessedColumns.PLACE_TYPE
    cp_col = ProcessedColumns.CORRECTED_PLACE

    mask = pd.Series(False, index=df.index)

    # 1순위: space_function
    if sf_col in df.columns:
        mask |= df[sf_col].isin(ANCHOR_SPACE_FUNCTIONS)

    # 2순위: place_type
    if pt_col in df.columns:
        mask |= df[pt_col].isin(["REST", "HELMET_RACK"])

    # 3순위: 장소명 키워드 (위 두 방법이 모두 없거나 미매칭인 경우)
    if cp_col in df.columns:
        keyword_mask = df[cp_col].fillna("").astype(str).str.contains(
            "|".join(ANCHOR_PLACE_KEYWORDS), na=False
        )
        mask |= keyword_mask

    return mask


def preprocess(df: pd.DataFrame, spatial_ctx=None) -> pd.DataFrame:
    """
    전체 전처리 파이프라인. 작업자별로 처리 후 병합.

    Args:
        df: 날짜 폴더의 모든 CSV를 병합한 Raw DataFrame
        spatial_ctx: SpatialContext 인스턴스 (선택).
                     제공 시 SSMP 기반 장소 분류 사용, 없으면 키워드 매칭 폴백.

    Returns:
        전처리된 DataFrame
    """
    if df.empty:
        logger.warning("빈 DataFrame 입력")
        return df

    logger.info(f"전처리 시작: {len(df)}행")

    # Step 1: 활성비율 계산
    df = _calc_active_ratio(df)

    # Step 2: 장소 분류 컬럼 추가 (SSMP 기반 또는 키워드 폴백)
    df = add_place_columns(df, spatial_ctx=spatial_ctx)

    # Step 3: 작업자 키 생성
    df = _add_worker_key(df)

    # Step 4: 시간 파생 컬럼 추가
    df = _add_time_columns(df)

    # Step 5: 작업자별 Journey 보정
    worker_keys = df[ProcessedColumns.WORKER_KEY].unique()
    corrected_parts = []
    for wk in worker_keys:
        worker_df = df[df[ProcessedColumns.WORKER_KEY] == wk].copy()
        worker_df = _correct_worker_journey(worker_df)
        corrected_parts.append(worker_df)

    result = pd.concat(corrected_parts, ignore_index=True)

    # Step 5-post: 보정된 장소 기반 PLACE_TYPE / LOCATION_KEY 재분류
    result = _reclassify_corrected_places(result, spatial_ctx)

    # Step 5-post2: 시퀀스 기반 맥락 해석 (Intelligent Journey v5)
    logger.info("Step 5-post2: 시퀀스 기반 맥락 해석 (Intelligent Journey v5)")
    post2_parts = []
    for wk in result[ProcessedColumns.WORKER_KEY].unique():
        wk_df = result[result[ProcessedColumns.WORKER_KEY] == wk].copy()
        wk_df = _interpret_sequence_context(wk_df)
        post2_parts.append(wk_df)
    result = pd.concat(post2_parts, ignore_index=True)

    # Step 5-post2-B: 번갈음 패턴 공간 우선순위 보정 (★ v5.3)
    logger.info("Step 5-post2-B: 번갈음 패턴 공간 우선순위 보정 (v5.3)")
    post2b_parts = []
    for wk in result[ProcessedColumns.WORKER_KEY].unique():
        wk_df = result[result[ProcessedColumns.WORKER_KEY] == wk].copy()
        wk_df = _detect_alternating_pattern(wk_df, window_size=SEQUENCE_WINDOW_MIN)
        post2b_parts.append(wk_df)
    result = pd.concat(post2b_parts, ignore_index=True)

    # Step 6: 활동 유형 분류
    result = _classify_activity_period(result)

    logger.info(f"전처리 완료: {len(result)}행")
    return result


def _calc_active_ratio(df: pd.DataFrame) -> pd.DataFrame:
    """
    활성비율 컬럼 추가.
    활성비율 = 활성신호갯수 / 신호갯수 (0이면 0.0 처리)

    추가 컬럼:
      coverage_gap      : signal_count == 0 (S-Ward 커버리지 밖 / 배터리 / 간섭)
      signal_confidence : 신호 수 기반 신뢰도 등급 (NONE/LOW/MED/HIGH)
    """
    sig = df[RawColumns.SIGNAL_COUNT].fillna(0).astype(int)
    active = df[RawColumns.ACTIVE_SIGNAL_COUNT].fillna(0).astype(int)

    active = active.clip(upper=sig)

    result = df.copy()
    result[ProcessedColumns.ACTIVE_RATIO] = np.where(
        sig > 0, active / sig, 0.0
    )
    result[ProcessedColumns.IS_ACTIVE] = (
        result[ProcessedColumns.ACTIVE_RATIO] >= ACTIVE_RATIO_WORKING_THRESHOLD
    )

    result["coverage_gap"] = sig == 0
    result["signal_confidence"] = pd.cut(
        sig,
        bins=[-1, 0, 3, 9, np.inf],
        labels=["NONE", "LOW", "MED", "HIGH"],
    )

    gap_count = int(result["coverage_gap"].sum())
    if gap_count > 0:
        logger.info(f"coverage_gap 감지: {gap_count}행 (signal_count=0)")

    return result


def _add_worker_key(df: pd.DataFrame) -> pd.DataFrame:
    """작업자 고유 키 생성 (이름_태그ID)."""
    result = df.copy()
    worker = result[RawColumns.WORKER].fillna("Unknown")
    tag = result[RawColumns.TAG].fillna("UNKNOWN")
    result[ProcessedColumns.WORKER_KEY] = worker + "_" + tag
    return result


def _add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """시간 파생 컬럼 추가 (날짜, 시, 분)."""
    result = df.copy()
    ts = result[RawColumns.TIME]
    result[ProcessedColumns.DATE] = ts.dt.date.astype(str)
    result[ProcessedColumns.HOUR] = ts.dt.hour
    result[ProcessedColumns.MINUTE] = ts.dt.minute
    return result


def _correct_worker_journey(df: pd.DataFrame) -> pd.DataFrame:
    """
    단일 작업자의 Journey 보정 — v6.1 Multi-Pass Refinement.

    이 함수는 BLE 위치 데이터의 노이즈를 제거하고, 작업자의 실제 이동 경로를
    추정하는 핵심 보정 파이프라인이다.

    Multi-Pass Refinement 철학:
    ┌────────────────────────────────────────────────────────────────────────┐
    │  "한 번에 모든 것을 판단하지 않고, 단계별로 확인하며 참값에 수렴"          │
    │                                                                         │
    │  기존 방식 (v3~v5):           Multi-Pass (v6.1):                        │
    │  ──────────────────          ─────────────────                          │
    │  각 Phase가 독립 판단          먼저 증거 수집                            │
    │       ↓                            ↓                                    │
    │  서로 결과를 덮어씌움            증거 기반 1회 통합 판단                  │
    │       ↓                            ↓                                    │
    │  과보정/미보정 반복             변경 < 5개가 될 때까지 반복               │
    │                                     ↓                                    │
    │                                 참값에 수렴                              │
    └────────────────────────────────────────────────────────────────────────┘

    파이프라인 구조:
        1. [초기화] 보정 컬럼 생성 (CORRECTED_PLACE, IS_CORRECTED 등)

        2. [증거 수집] _collect_evidence() + _segment_day()
           → E1/E3/E4 증거 컬럼 생성 (e1_active_level, segment_type 등)

        3. [1차 보정] DBSCAN 클러스터링
           → 좌표 기반 공간 그룹화, 앵커 공간 보호

        4. [Multi-Pass Loop] (최대 MULTI_PASS_MAX_ITERATIONS회)
           ┌──────────────────────────────────────────────────────────────┐
           │  Pass 1: Ghost Signal 제거                                   │
           │    - active_signal_count=0인 연속 구간 → RACK으로 통일       │
           │                                                               │
           │  Pass 2: 번갈음 패턴 해소                                     │
           │    - A↔B 번갈음 → 공간 우선순위 높은 쪽으로 흡수               │
           │                                                               │
           │  Pass 3: 맥락 검증                                            │
           │    - 하루 스토리라인 일관성 확인, 이례적 패턴 태깅              │
           │                                                               │
           │  Pass 4: 물리적 이상치 탐지                                   │
           │    - 텔레포트, 건물 점프, 노이즈 잔류 처리                     │
           │                                                               │
           │  [수렴 체크] 변경 < CONVERGENCE_CHANGE_THRESH → 종료          │
           │             아니면 → 증거 재수집 후 다음 반복                  │
           └──────────────────────────────────────────────────────────────┘

    4대 증거 체계:
        E1. 활성신호 (active_signal_count) — "사람이 움직였는가?"
        E2. 공간 속성 (space_function)     — "이 장소에서 머무는 게 정상인가?"
        E3. 시간 속성 (시간대)             — "지금 시각에 여기 있는 게 자연스러운가?"
        E4. 이동 패턴 (위치 연속성)        — "위치가 안정적인가, 점프하는가?"

    Args:
        df: 단일 작업자의 DataFrame (RawColumns 필수)

    Returns:
        보정 컬럼이 추가된 DataFrame:
        - CORRECTED_PLACE: 보정된 장소명
        - CORRECTED_X, CORRECTED_Y: 보정된 좌표
        - IS_CORRECTED: 보정 적용 여부
        - STATE_DETAIL: 보정 상세 사유
        - ANOMALY_FLAG: 이상치 플래그
    """
    result = df.copy()

    # ═══════════════════════════════════════════════════════════════════════
    # [1. 초기화] 보정 컬럼 생성
    # ═══════════════════════════════════════════════════════════════════════
    result[ProcessedColumns.CORRECTED_PLACE] = result[RawColumns.PLACE]
    result[ProcessedColumns.CORRECTED_X] = result[RawColumns.X]
    result[ProcessedColumns.CORRECTED_Y] = result[RawColumns.Y]
    result[ProcessedColumns.IS_CORRECTED] = False
    result[ProcessedColumns.ANOMALY_FLAG] = ""

    # ═══════════════════════════════════════════════════════════════════════
    # [2. 증거 수집] E1/E3/E4 증거 컬럼 생성
    # ═══════════════════════════════════════════════════════════════════════
    result = _collect_evidence(result)
    result = _segment_day(result)

    # ═══════════════════════════════════════════════════════════════════════
    # [3. 1차 보정] DBSCAN 좌표 클러스터링 (앵커 보호 포함)
    # ═══════════════════════════════════════════════════════════════════════
    try:
        result = _cluster_locations_by_key(result)
        result = _correct_noise_by_cluster(result)
    except ImportError:
        logger.warning(
            "scikit-learn 미설치 → 슬라이딩 윈도우 폴백. "
            "pip install scikit-learn 권장."
        )
        result = _correct_location_noise(result)
    except Exception as e:
        logger.warning(f"DBSCAN 실패 → 폴백: {e}")
        result = _correct_location_noise(result)

    # ═══════════════════════════════════════════════════════════════════════
    # [4. Multi-Pass Refinement Loop]
    # ═══════════════════════════════════════════════════════════════════════
    pass_stats: list[dict] = []

    for iteration in range(MULTI_PASS_MAX_ITERATIONS):
        prev_correction_count = _count_corrections(result)

        # ─── Pass 1: Ghost Signal 제거 ─────────────────────────────────
        result = _correct_ghost_signals(result)
        pass1_changes = _count_corrections(result) - prev_correction_count

        # ─── Pass 1.5: Journey 문장화 보정 (전체 맥락 기반) ────────────
        prev_sentence = _count_corrections(result)
        result = _correct_journey_as_sentence(result)
        pass1_5_changes = _count_corrections(result) - prev_sentence

        # ─── Pass 2: 번갈음 패턴 해소 ──────────────────────────────────
        result = _correct_helmet_rack_pattern(result)
        result = _correct_coord_outliers(result)
        result = _validate_place_coord_consistency(result)
        result = _correct_alternating_by_context(result)
        pass2_changes = _count_corrections(result) - prev_correction_count - pass1_changes - pass1_5_changes

        # ─── Pass 3: 전체 맥락 검증 ────────────────────────────────────
        result = _pass3_verify_narrative(result)

        # ─── Pass 4: 물리적 이상치 탐지 ────────────────────────────────
        prev_pass4 = _count_corrections(result)
        result = _pass4_detect_impossible_movement(result)
        pass4_changes = _count_corrections(result) - prev_pass4

        # ─── 통계 기록 ─────────────────────────────────────────────────
        total_changes = pass1_changes + pass1_5_changes + pass2_changes + pass4_changes
        pass_stats.append({
            "iteration": iteration + 1,
            "pass1_ghost": pass1_changes,
            "pass1_5_sentence": pass1_5_changes,
            "pass2_alternating": pass2_changes,
            "pass4_impossible": pass4_changes,
            "total": total_changes
        })

        logger.info(
            f"Multi-Pass [{iteration + 1}/{MULTI_PASS_MAX_ITERATIONS}]: "
            f"Ghost={pass1_changes}, Sentence={pass1_5_changes}, "
            f"Alternating={pass2_changes}, Impossible={pass4_changes}, Total={total_changes}"
        )

        # ─── 수렴 체크: 변경이 충분히 적으면 종료 ──────────────────────
        if total_changes < CONVERGENCE_CHANGE_THRESH:
            logger.info(
                f"Multi-Pass 수렴 완료: {iteration + 1}회 반복 후 "
                f"변경 {total_changes}개 < 임계값 {CONVERGENCE_CHANGE_THRESH}"
            )
            break

        # ─── 다음 반복을 위해 증거 재수집 ──────────────────────────────
        if iteration < MULTI_PASS_MAX_ITERATIONS - 1:
            result = _collect_evidence(result)
            result = _segment_day(result)

    # ─── 최종 통계 로깅 ────────────────────────────────────────────────
    total_all_passes = sum(s["total"] for s in pass_stats)
    logger.info(
        f"Multi-Pass 완료: {len(pass_stats)}회 반복, 총 {total_all_passes}행 보정"
    )

    return result


def _cluster_locations_by_key(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    LOCATION_KEY 별로 독립적으로 DBSCAN 클러스터링 수행.

    핵심 원칙:
      같은 LOCATION_KEY = 같은 좌표계 = 직접 비교 가능
      다른 LOCATION_KEY = 다른 좌표계 = 절대 같이 클러스터링 금지

    처리 흐름:
      ★ v5.2: 앵커 행(REST/RACK) 사전 처리 추가
      0. 앵커 행을 먼저 찾아 자기 장소명 기반 cluster_id 부여 (min_samples 제약 우회)
      1. df_worker를 LOCATION_KEY 별로 그룹화 (앵커 제외)
      2. 각 그룹 독립적으로 DBSCAN
      3. 클러스터 ID는 그룹별 오프셋 적용 (그룹 간 ID 충돌 방지)
      4. SPATIAL_CLUSTER, CLUSTER_PLACE 컬럼 추가하여 반환

    Returns:
        SPATIAL_CLUSTER (int, -1=노이즈), CLUSTER_PLACE (str) 컬럼이 추가된 df
    """
    from sklearn.cluster import DBSCAN
    import numpy as np

    result = df_worker.copy()
    result[ProcessedColumns.SPATIAL_CLUSTER] = -1
    result[ProcessedColumns.CLUSTER_PLACE]   = result[ProcessedColumns.CORRECTED_PLACE]

    cluster_id_offset = 0

    # ═══════════════════════════════════════════════════════════════════════
    # ★ v5.2: 앵커 행 사전 처리 (DBSCAN min_samples 제약 우회)
    # ═══════════════════════════════════════════════════════════════════════
    anchor_mask = _build_anchor_mask(result)

    # 앵커 행: 자기 장소명 그대로 클러스터로 지정
    # (같은 장소명끼리 같은 cluster_id를 공유)
    anchor_place_to_cluster: dict = {}
    for idx in result[anchor_mask].index:
        place = result.at[idx, ProcessedColumns.CORRECTED_PLACE]
        if place not in anchor_place_to_cluster:
            anchor_place_to_cluster[place] = cluster_id_offset
            cluster_id_offset += 1
        result.at[idx, ProcessedColumns.SPATIAL_CLUSTER] = anchor_place_to_cluster[place]
        result.at[idx, ProcessedColumns.CLUSTER_PLACE] = place

    logger.debug(
        f"v5.2 앵커 사전 처리: {anchor_mask.sum()}행 → {len(anchor_place_to_cluster)}개 클러스터"
    )

    # ═══════════════════════════════════════════════════════════════════════
    # 기존 DBSCAN 루프는 앵커가 아닌 행에만 실행
    # ═══════════════════════════════════════════════════════════════════════
    non_anchor_df = result[~anchor_mask]

    for loc_key, group_idx in non_anchor_df.groupby(ProcessedColumns.LOCATION_KEY).groups.items():
        group = non_anchor_df.loc[group_idx]

        # 유효한 좌표 행만 클러스터링
        valid_mask = (
            group[ProcessedColumns.CORRECTED_X].notna()
            & group[ProcessedColumns.CORRECTED_Y].notna()
        )
        valid_group = group[valid_mask]

        if len(valid_group) < 3:
            continue

        # 공간적응형 eps 결정: space_function별 배수 적용
        base_eps = DBSCAN_EPS_OUTDOOR if loc_key == "OUTDOOR" else DBSCAN_EPS_INDOOR
        eps = _get_adaptive_eps(valid_group, base_eps)

        # eps=0 이면 클러스터링 스킵 (STATE_OVERRIDE 공간)
        if eps <= 0:
            continue

        coords = valid_group[[ProcessedColumns.CORRECTED_X, ProcessedColumns.CORRECTED_Y]].values.astype(float)

        db     = DBSCAN(eps=eps, min_samples=DBSCAN_MIN_SAMPLES).fit(coords)
        labels = db.labels_  # -1 = 노이즈

        # 오프셋 적용하여 전체 df에서 유일한 클러스터 ID 보장
        adjusted = np.where(labels >= 0, labels + cluster_id_offset, -1)
        result.loc[valid_group.index, ProcessedColumns.SPATIAL_CLUSTER] = adjusted

        # 클러스터별 대표 장소명 결정 (signal_count 가중 최빈값)
        for raw_id in np.unique(labels):
            if raw_id < 0:
                continue
            cluster_rows = result.loc[valid_group.index[labels == raw_id]]
            rep_place = _weighted_mode_place(cluster_rows)
            if rep_place:
                result.loc[cluster_rows.index, ProcessedColumns.CLUSTER_PLACE] = rep_place

        max_label = int(labels.max()) if labels.max() >= 0 else -1
        if max_label >= 0:
            cluster_id_offset += max_label + 1

    return result


_CONTINUOUS_NOISE_TRANSIT_MIN = 5  # 연속 노이즈 이 이상이면 이동(transit) 간주


def _get_adaptive_eps(group: pd.DataFrame, base_eps: float) -> float:
    """
    그룹의 주요 space_function에 따라 적응형 eps 반환.

    STATE_OVERRIDE 공간(REST, RACK, TRANSIT_GATE, TRANSIT_CORRIDOR)은
    클러스터링이 불필요하므로 0.0 반환하여 스킵.

    Args:
        group: 단일 LOCATION_KEY 그룹의 DataFrame
        base_eps: 기본 eps 값 (INDOOR/OUTDOOR 기준)

    Returns:
        조정된 eps 값 (0.0이면 클러스터링 스킵)
    """
    if ProcessedColumns.SPACE_FUNCTION not in group.columns:
        return base_eps

    # 가장 빈번한 space_function 찾기
    sf_counts = group[ProcessedColumns.SPACE_FUNCTION].value_counts()
    if sf_counts.empty:
        return base_eps

    dominant_sf = sf_counts.index[0]

    # STATE_OVERRIDE 공간은 클러스터링 불필요
    if dominant_sf in STATE_OVERRIDE_SPACES:
        return 0.0

    multiplier = DBSCAN_EPS_MULTIPLIER.get(dominant_sf, 1.0)
    if multiplier <= 0:
        return 0.0

    return base_eps * multiplier


def _correct_noise_by_cluster(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    DBSCAN 노이즈 행을 **nearest-cluster** 방식으로 보정.

    ffill(직전 값 채움)의 구조적 한계 해결:
      A→B 이동 시작점이 노이즈로 처리되면 ffill은 A로 채워 B 도착이 늦게 기록됨.
      nearest-cluster는 앞뒤의 유효 클러스터 거리를 비교하여 더 가까운 쪽으로 채움.

    연속 노이즈 처리:
      _CONTINUOUS_NOISE_TRANSIT_MIN(5분) 이상 연속 노이즈 → "[이동]" 으로 표기.
      실제 이동 구간일 가능성이 높음.

    ★ v5.2 앵커 공간 보호:
      REST, RACK 등 앵커 공간은 짧은 체류가 정상이므로
      SPATIAL_CLUSTER == -1 이어도 절대 덮어쓰지 않는다.
      앵커이면서 노이즈인 행: SPATIAL_CLUSTER를 -2로 설정 (앵커 보호 마커)
    """
    if ProcessedColumns.CLUSTER_PLACE not in df_worker.columns:
        return df_worker

    result = df_worker.copy()
    is_noise = result[ProcessedColumns.SPATIAL_CLUSTER] == -1
    not_corrected = ~result[ProcessedColumns.IS_CORRECTED]
    
    # ═══════════════════════════════════════════════════════════════════════
    # ★ v5.2: 앵커 행 보호
    # ═══════════════════════════════════════════════════════════════════════
    anchor_mask = _build_anchor_mask(result)
    
    # 실제 보정 대상 = 노이즈 AND NOT 앵커
    correction_target = is_noise & not_corrected & ~anchor_mask
    
    # 앵커이면서 노이즈인 행: SPATIAL_CLUSTER를 -2로 설정 (앵커 보호 마커)
    # → 이후 코드에서 -1로 오인하여 덮어쓰는 것을 방지
    anchor_noise_mask = is_noise & anchor_mask
    if anchor_noise_mask.any():
        result.loc[anchor_noise_mask, ProcessedColumns.SPATIAL_CLUSTER] = -2
        logger.debug(f"v5.2 앵커 노이즈 보호: {anchor_noise_mask.sum()}행 → SPATIAL_CLUSTER=-2")

    if not correction_target.any():
        return result

    # ── 1) 연속 노이즈 구간 처리 ──
    # v6.x: 더 이상 "[이동] A → B" 형태의 가상 장소를 생성하지 않는다.
    #       연속 노이즈 구간도 모두 nearest-cluster 보정(2단계)으로 넘겨서
    #       CORRECTED_PLACE에는 항상 실제 장소명만 남도록 한다.
    noise_groups = _get_consecutive_groups(correction_target)
    transit_indices: set[int] = set()
    # (이전 버전에서는 length >= _CONTINUOUS_NOISE_TRANSIT_MIN 인 구간에
    #  "[이동] ..." 라벨을 부여했지만, 현재는 사용하지 않음.)

    # ── 2) 개별 노이즈 행 → nearest-cluster 보정 ──
    # ★ v5.2: correction_target 기준 (앵커 행 제외)
    remaining_noise = correction_target & ~result.index.isin(
        [result.index[i] for i in transit_indices if i < len(result)]
    )

    if remaining_noise.any():
        cluster_place_valid = result[ProcessedColumns.CLUSTER_PLACE].copy()
        # ★ v5.2: 노이즈 or 앵커 보호(-2) 모두 제외
        invalid_cluster = result[ProcessedColumns.SPATIAL_CLUSTER].isin([-1, -2])
        cluster_place_valid[invalid_cluster] = None

        fwd = cluster_place_valid.ffill()
        bwd = cluster_place_valid.bfill()

        for idx in result.index[remaining_noise]:
            pos = result.index.get_loc(idx)
            f_val = fwd.get(idx)
            b_val = bwd.get(idx)

            if pd.notna(f_val) and pd.notna(b_val):
                f_dist = _distance_to_nearest_valid(result, pos, direction="backward")
                b_dist = _distance_to_nearest_valid(result, pos, direction="forward")
                chosen = f_val if f_dist <= b_dist else b_val
            elif pd.notna(f_val):
                chosen = f_val
            elif pd.notna(b_val):
                chosen = b_val
            else:
                continue

            if chosen != result.loc[idx, ProcessedColumns.CORRECTED_PLACE]:
                result.loc[idx, ProcessedColumns.CORRECTED_PLACE] = chosen
                result.loc[idx, ProcessedColumns.IS_CORRECTED] = True

    return result


def _find_nearest_valid_place(
    df: pd.DataFrame, pos: int, direction: str
) -> Optional[str]:
    """
    pos 위치에서 direction 방향의 첫 유효(비노이즈) CLUSTER_PLACE 반환.
    
    ★ v5.2: SPATIAL_CLUSTER >= -2 를 유효로 간주 (-2는 앵커 보호 마커)
    """
    if direction == "backward":
        rng = range(pos - 1, -1, -1)
    else:
        rng = range(pos + 1, len(df))

    for i in rng:
        # ★ v5.2: -1만 노이즈, -2는 앵커 보호로 유효
        cluster_val = df.iloc[i][ProcessedColumns.SPATIAL_CLUSTER]
        if cluster_val != -1:  # -2(앵커 보호) 및 양수 클러스터 모두 유효
            val = df.iloc[i][ProcessedColumns.CLUSTER_PLACE]
            if pd.notna(val):
                return val
    return None


def _distance_to_nearest_valid(
    df: pd.DataFrame, pos: int, direction: str
) -> int:
    """
    pos에서 direction 방향 첫 유효 행까지의 행 수(시간 거리).
    
    ★ v5.2: SPATIAL_CLUSTER != -1 를 유효로 간주 (-2는 앵커 보호 마커)
    """
    if direction == "backward":
        rng = range(pos - 1, -1, -1)
    else:
        rng = range(pos + 1, len(df))
    for i in rng:
        # ★ v5.2: -1만 노이즈, -2는 앵커 보호로 유효
        if df.iloc[i][ProcessedColumns.SPATIAL_CLUSTER] != -1:
            return abs(i - pos)
    return len(df)


def _weighted_mode_place(cluster_rows: pd.DataFrame) -> Optional[str]:
    """
    클러스터의 대표 장소명 결정 — signal_count 가중 최빈값.
    
    ★ v5.4: v5.3의 space_function priority 적용 제거 (과보정 원인)
    ★ 앵커 행(REST/RACK)은 이미 _cluster_locations_by_key에서 클러스터 분리됨
       따라서 여기서 priority를 적용할 필요 없음.
       Space Priority는 시퀀스 해석(_detect_alternating_pattern)에서만 적용.
    """
    place_col = ProcessedColumns.CORRECTED_PLACE
    sig_col = RawColumns.SIGNAL_COUNT

    if place_col not in cluster_rows.columns:
        return None
    places = cluster_rows[place_col].dropna()
    if places.empty:
        return None

    # signal_count 가중 최빈값
    if sig_col in cluster_rows.columns:
        place_signal = cluster_rows.groupby(place_col)[sig_col].sum()
        return place_signal.idxmax() if not place_signal.empty else places.mode().iloc[0]
    else:
        mode_val = places.mode()
        return mode_val.iloc[0] if not mode_val.empty else None


def _correct_helmet_rack_pattern(df: pd.DataFrame) -> pd.DataFrame:
    """
    헬멧 거치 패턴 보정.

    조건: 야간/새벽 또는 점심시간 + 활성신호=0 + 근처에 헬멧 거치 장소 출현
    보정: 해당 구간을 헬멧 거치 장소로 통일

    대표 케이스:
    - 야간에 "본진 타각기 출구"↔"본진 타각기 앞 보호구 걸이대" 번갈아 나타남
    - 활성신호갯수 = 0
    → 모두 "본진 타각기 앞 보호구 걸이대"로 보정
    """
    result = df.copy()
    result = result.sort_values(RawColumns.TIME).reset_index(drop=True)

    hours = result[ProcessedColumns.HOUR]
    active_ratio = result[ProcessedColumns.ACTIVE_RATIO]

    # 야간/새벽 비활성 구간 마스크
    night_inactive = (
        hours.apply(is_night_or_dawn) & (active_ratio <= ACTIVE_RATIO_ZERO_THRESHOLD)
    )

    # 점심 비활성 구간 마스크
    lunch_inactive = (
        hours.apply(is_lunch_time) & (active_ratio <= ACTIVE_RATIO_ZERO_THRESHOLD)
    )

    # ★ v5.2: 앵커 공간(REST, RACK) 제외
    # 휴게실에서의 비활성은 정상적인 휴식이므로 헬멧 거치 보정 대상이 아님
    anchor_mask = _build_anchor_mask(result)
    not_anchor = ~anchor_mask

    target_mask = (night_inactive | lunch_inactive) & not_anchor

    if not target_mask.any():
        return result

    # 연속 구간 그룹화
    groups = _get_consecutive_groups(target_mask)

    for start_idx, end_idx in groups:
        group_slice = result.iloc[start_idx:end_idx + 1]
        duration_min = len(group_slice)  # 1행 = 1분

        # 최소 지속시간 미달 → 스킵
        if duration_min < HELMET_RACK_MIN_DURATION_MIN:
            continue

        # 그룹 내 헬멧 거치 장소 찾기
        rack_rows = group_slice[
            group_slice[RawColumns.PLACE].apply(is_helmet_rack)
        ]

        if rack_rows.empty:
            # 인접 행에서 헬멧 거치 장소 탐색
            search_start = max(0, start_idx - 10)
            search_end = min(len(result), end_idx + 10)
            nearby = result.iloc[search_start:search_end]
            rack_rows = nearby[nearby[RawColumns.PLACE].apply(is_helmet_rack)]

        if rack_rows.empty:
            continue

        # 가장 많이 등장한 헬멧 거치 장소 선택
        best_rack = rack_rows[RawColumns.PLACE].mode()
        if best_rack.empty:
            continue
        rack_place = best_rack.iloc[0]
        rack_row = rack_rows[rack_rows[RawColumns.PLACE] == rack_place].iloc[0]

        # 보정 적용
        idx_slice = result.index[start_idx:end_idx + 1]
        result.loc[idx_slice, ProcessedColumns.CORRECTED_PLACE] = rack_place
        result.loc[idx_slice, ProcessedColumns.CORRECTED_X] = rack_row[RawColumns.X]
        result.loc[idx_slice, ProcessedColumns.CORRECTED_Y] = rack_row[RawColumns.Y]
        result.loc[idx_slice, ProcessedColumns.IS_CORRECTED] = True

    return result


def _correct_location_noise(df: pd.DataFrame) -> pd.DataFrame:
    """
    이동 노이즈 제거.
    슬라이딩 윈도우 최빈값(mode) 필터를 적용하여 A,A,A,B,A,B,B,B → A→B 이동으로 보정.

    이미 헬멧 거치 보정이 적용된 행(IS_CORRECTED=True)은 제외.
    
    ★ v5.2: 앵커 공간(REST/RACK)도 슬라이딩 윈도우 대상에서 제외.
    """
    result = df.copy()
    window = LOCATION_SMOOTHING_WINDOW
    places = result[ProcessedColumns.CORRECTED_PLACE].tolist()
    is_corrected = result[ProcessedColumns.IS_CORRECTED].tolist()
    
    # ★ v5.2: 앵커 행 체크용 배열 준비
    place_types = result[ProcessedColumns.PLACE_TYPE].values if ProcessedColumns.PLACE_TYPE in result.columns else [""] * len(result)
    space_funcs = result[ProcessedColumns.SPACE_FUNCTION].values if ProcessedColumns.SPACE_FUNCTION in result.columns else ["UNKNOWN"] * len(result)

    smoothed = list(places)

    for i in range(len(places)):
        if is_corrected[i]:
            continue  # 이미 보정된 행 스킵
        
        # ★ v5.2: 앵커 행 체크 (인덱스 기반)
        pt = str(place_types[i]) if i < len(place_types) else ""
        sf = str(space_funcs[i]) if i < len(space_funcs) else ""
        if _is_anchor_row(pt, sf):
            continue  # 앵커 행은 스킵

        # 윈도우 내 값 수집
        start = max(0, i - window // 2)
        end = min(len(places), i + window // 2 + 1)
        window_vals = [
            places[j] for j in range(start, end) if not is_corrected[j]
        ]

        if not window_vals:
            continue

        # 최빈값 계산 (문자열 지원)
        from collections import Counter
        counter = Counter(window_vals)
        mode_val = counter.most_common(1)[0][0]

        if mode_val != places[i]:
            smoothed[i] = mode_val
            result.iloc[i, result.columns.get_loc(ProcessedColumns.IS_CORRECTED)] = True

    result[ProcessedColumns.CORRECTED_PLACE] = smoothed
    return result


def _correct_coord_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    좌표 이상치 보정.
    같은 location_key(건물+층) 내에서 급격한 좌표 변화를 인접 값으로 보간.
    """
    result = df.copy()

    for loc_key in result[ProcessedColumns.LOCATION_KEY].unique():
        mask = result[ProcessedColumns.LOCATION_KEY] == loc_key
        loc_df = result[mask].copy()

        if len(loc_df) < 3:
            continue

        for coord_col, corr_col in [
            (RawColumns.X, ProcessedColumns.CORRECTED_X),
            (RawColumns.Y, ProcessedColumns.CORRECTED_Y),
        ]:
            coords = loc_df[corr_col].values.astype(float)
            # 이미 nan이 아닌 값들 사이의 급격한 변화 탐지
            diffs = np.abs(np.diff(coords))
            outlier_mask = np.zeros(len(coords), dtype=bool)
            outlier_mask[1:] = diffs > COORD_OUTLIER_THRESHOLD

            if outlier_mask.any():
                # 선형 보간으로 이상치 대체
                coords_series = pd.Series(coords)
                coords_series[outlier_mask] = np.nan
                coords_interp = coords_series.interpolate(method="linear").values.astype(float)
                result[corr_col] = result[corr_col].astype(float)
                result.loc[mask, corr_col] = coords_interp

    return result


def _validate_place_coord_consistency(df: pd.DataFrame) -> pd.DataFrame:
    """
    [Phase 2-post] 좌표 이상치 보정으로 좌표가 크게 이동했는데
    장소명이 Phase 0 기준 그대로인 불일치를 감지/보정.

    같은 LOCATION_KEY 내 같은 CORRECTED_PLACE의 좌표 centroid를 구하고,
    centroid에서 COORD_OUTLIER_THRESHOLD 이상 떨어진 행은
    같은 LOCATION_KEY 내에서 가장 가까운 다른 클러스터의 대표 장소명으로 변경.
    """
    if ProcessedColumns.SPATIAL_CLUSTER not in df.columns:
        return df

    result = df.copy()
    inconsistent_count = 0

    for loc_key in result[ProcessedColumns.LOCATION_KEY].unique():
        loc_mask = result[ProcessedColumns.LOCATION_KEY] == loc_key
        loc_df = result[loc_mask]

        # 클러스터별 centroid 계산
        clusters = loc_df[loc_df[ProcessedColumns.SPATIAL_CLUSTER] >= 0].groupby(ProcessedColumns.SPATIAL_CLUSTER)
        centroids = {}
        for cid, group in clusters:
            cx = group[ProcessedColumns.CORRECTED_X].mean()
            cy = group[ProcessedColumns.CORRECTED_Y].mean()
            rep = group[ProcessedColumns.CLUSTER_PLACE].mode()
            centroids[cid] = {
                "cx": cx, "cy": cy,
                "place": rep.iloc[0] if not rep.empty else None,
            }

        if len(centroids) < 2:
            continue

        # 각 행이 자기 클러스터 centroid에서 크게 벗어났는지 확인
        for idx in loc_df.index:
            cid = result.loc[idx, ProcessedColumns.SPATIAL_CLUSTER]
            if cid < 0 or cid not in centroids:
                continue

            x = result.loc[idx, ProcessedColumns.CORRECTED_X]
            y = result.loc[idx, ProcessedColumns.CORRECTED_Y]
            if pd.isna(x) or pd.isna(y):
                continue

            c = centroids[cid]
            dist = np.sqrt((x - c["cx"]) ** 2 + (y - c["cy"]) ** 2)

            if dist > COORD_OUTLIER_THRESHOLD:
                # 가장 가까운 다른 클러스터 찾기
                best_cid, best_dist = None, float("inf")
                for other_cid, other_c in centroids.items():
                    if other_cid == cid:
                        continue
                    d = np.sqrt((x - other_c["cx"]) ** 2 + (y - other_c["cy"]) ** 2)
                    if d < best_dist:
                        best_dist = d
                        best_cid = other_cid

                if best_cid is not None and centroids[best_cid]["place"]:
                    result.loc[idx, ProcessedColumns.CORRECTED_PLACE] = centroids[best_cid]["place"]
                    result.loc[idx, ProcessedColumns.SPATIAL_CLUSTER] = best_cid
                    result.loc[idx, ProcessedColumns.CLUSTER_PLACE] = centroids[best_cid]["place"]
                    result.loc[idx, ProcessedColumns.IS_CORRECTED] = True
                    inconsistent_count += 1

    if inconsistent_count > 0:
        logger.info(
            f"Phase 2-post: {inconsistent_count}행 좌표↔장소명 불일치 보정"
        )
    return result


def _reclassify_corrected_places(
    df: pd.DataFrame, spatial_ctx=None
) -> pd.DataFrame:
    """
    [Step 5-post] CORRECTED_PLACE가 원본 PLACE와 다른 행에 대해
    PLACE_TYPE, IS_HELMET_RACK, LOCATION_KEY, SPACE_FUNCTION, HAZARD_WEIGHT를 재분류.

    DBSCAN이 장소를 변경해도 PLACE_TYPE이 업데이트되지 않으면
    휴게실 보정된 행이 여전히 INDOOR로 남아 work로 잘못 집계되는 P0 버그 발생.
    """
    result = df.copy()

    changed = result[ProcessedColumns.CORRECTED_PLACE] != result[RawColumns.PLACE]
    if not changed.any():
        return result

    changed_idx = result.index[changed]
    logger.info(
        f"Step 5-post: {len(changed_idx)}행 CORRECTED_PLACE 변경 → "
        f"PLACE_TYPE/LOCATION_KEY/SPACE_FUNCTION 재분류"
    )

    for idx in changed_idx:
        row = result.loc[idx]
        corrected = str(row.get(ProcessedColumns.CORRECTED_PLACE, "") or "")
        building = row.get(RawColumns.BUILDING)
        floor = row.get(RawColumns.FLOOR)

        if spatial_ctx is not None:
            new_type = spatial_ctx.classify_place(corrected, building, floor)
            new_key = spatial_ctx.get_location_key(
                place_name=corrected, building=building, floor=floor
            )
            zone_type = spatial_ctx.get_zone_type(corrected)
            risk_level = spatial_ctx.get_risk_level(corrected)
        else:
            new_type = classify_place(corrected, building, floor)
            new_key = make_location_key(building, floor)
            zone_type = None
            risk_level = None

        new_sf = classify_space_function(corrected, zone_type, building, floor)
        new_hw = get_hazard_weight(new_sf, risk_level)

        result.loc[idx, ProcessedColumns.PLACE_TYPE] = new_type
        result.loc[idx, ProcessedColumns.IS_HELMET_RACK] = is_helmet_rack(corrected)
        result.loc[idx, ProcessedColumns.LOCATION_KEY] = new_key
        result.loc[idx, ProcessedColumns.SPACE_FUNCTION] = new_sf
        result.loc[idx, ProcessedColumns.HAZARD_WEIGHT] = new_hw

    return result


def classify_activity_period_col(df: pd.DataFrame) -> pd.DataFrame:
    """공개 API: 활동 유형 분류 컬럼 추가 (래퍼 함수)."""
    return _classify_activity_period(df)


def _classify_activity_period(df: pd.DataFrame) -> pd.DataFrame:
    """
    각 행에 활동 유형 분류 컬럼 추가.

    v4 변경: space_function 기반 맥락 분류.
    - STATE_OVERRIDE 공간(REST, RACK, TRANSIT_GATE 등)은 active_ratio와 무관
    - WORK 계열 공간만 active_ratio 기반 분류

    추가 컬럼:
      PERIOD_TYPE  : work/rest/transit/off (기존 호환)
      STATE_DETAIL : 세분화 상태 (high_work, low_work, standby, transit_queue 등)
    """
    result = df.copy()

    # STATE_DETAIL 컬럼 초기화
    if ProcessedColumns.STATE_DETAIL not in result.columns:
        result[ProcessedColumns.STATE_DETAIL] = None

    # ANOMALY_FLAG 컬럼 초기화
    if ProcessedColumns.ANOMALY_FLAG not in result.columns:
        result[ProcessedColumns.ANOMALY_FLAG] = None

    # DWELL_EXCEEDED 컬럼 초기화
    if ProcessedColumns.DWELL_EXCEEDED not in result.columns:
        result[ProcessedColumns.DWELL_EXCEEDED] = False

    # 체류시간 계산을 위해 정렬
    result = result.sort_values([ProcessedColumns.WORKER_KEY, RawColumns.TIME]).reset_index(drop=True)

    # 작업자별 처리
    for wk in result[ProcessedColumns.WORKER_KEY].unique():
        wk_mask = result[ProcessedColumns.WORKER_KEY] == wk
        wk_df = result[wk_mask].copy()

        # 연속 동일 장소 체류시간 계산
        dwell_min = _calc_dwell_minutes(wk_df)

        for i, (idx, row) in enumerate(wk_df.iterrows()):
            hour = int(row.get(ProcessedColumns.HOUR, 0))
            active_ratio = float(row.get(ProcessedColumns.ACTIVE_RATIO, 0.0) or 0.0)
            place_type = row.get(ProcessedColumns.PLACE_TYPE, "UNKNOWN")
            corrected_place = str(row.get(ProcessedColumns.CORRECTED_PLACE, "") or "")
            space_func = row.get(ProcessedColumns.SPACE_FUNCTION, SpaceFunction.UNKNOWN)

            # space_function이 없으면 레거시 폴백
            if pd.isna(space_func) or space_func == "" or space_func is None:
                space_func = SpaceFunction.UNKNOWN

            # 체류시간
            dwell = dwell_min[i] if i < len(dwell_min) else 1

            # space_function 기반 상태 분류
            state_detail = classify_state_by_space(space_func, active_ratio, hour, dwell)

            # dwell_exceeded 판단
            normal_max = DWELL_NORMAL_MAX.get(space_func, 999)
            dwell_exceeded = dwell > normal_max

            # anomaly_flag (WORK_HAZARD abnormal_stop 등)
            anomaly_flag = None
            if state_detail == "abnormal_stop":
                anomaly_flag = "abnormal_stop"
            elif state_detail == "gate_congestion":
                anomaly_flag = "gate_congestion"
            elif state_detail == "corridor_block":
                anomaly_flag = "corridor_block"

            # PERIOD_TYPE (기존 호환): state_detail → 상위 그룹 매핑
            period_type = _state_detail_to_period_type(state_detail)

            # 레거시 폴백: 휴게 키워드 체크
            if period_type not in ("rest", "off"):
                if place_type == "REST" or any(kw in corrected_place for kw in REST_AREA_KEYWORDS):
                    period_type = "rest"
                    state_detail = "rest_facility"

            result.loc[idx, ProcessedColumns.PERIOD_TYPE] = period_type
            result.loc[idx, ProcessedColumns.STATE_DETAIL] = state_detail
            result.loc[idx, ProcessedColumns.DWELL_EXCEEDED] = dwell_exceeded
            result.loc[idx, ProcessedColumns.ANOMALY_FLAG] = anomaly_flag

    # Step 6-post: 장소 전환 이동 태깅
    result = _tag_transition_travel(result)

    return result


def _calc_dwell_minutes(wk_df: pd.DataFrame) -> list[int]:
    """
    작업자 DataFrame에서 각 행의 연속 동일 장소 체류시간(분) 계산.

    같은 CORRECTED_PLACE + LOCATION_KEY가 연속될 때 체류 중으로 간주.
    """
    if wk_df.empty:
        return []

    places = wk_df[ProcessedColumns.CORRECTED_PLACE].fillna("").tolist()
    loc_keys = wk_df[ProcessedColumns.LOCATION_KEY].fillna("").tolist()

    dwell = []
    current_place = None
    current_loc = None
    current_count = 0

    indices_to_update: list[int] = []

    for i in range(len(places)):
        p = places[i]
        l = loc_keys[i]

        if p == current_place and l == current_loc:
            current_count += 1
            indices_to_update.append(i)
        else:
            # 이전 그룹 업데이트
            for j in indices_to_update:
                dwell.append(current_count)
            # 새 그룹 시작
            current_place = p
            current_loc = l
            current_count = 1
            indices_to_update = [i]

    # 마지막 그룹
    for j in indices_to_update:
        dwell.append(current_count)

    return dwell


def _calc_consecutive_dwell(places: np.ndarray) -> np.ndarray:
    """
    각 행에서 시작하는 연속 체류 길이 계산.
    
    예) [A, A, A, B, B, A] → [3, 2, 1, 2, 1, 1]
    
    태깅 상한 계산에 사용: 3분짜리 체류에 10분 태깅하면 전체가 이동이 됨.
    """
    n = len(places)
    if n == 0:
        return np.array([], dtype=int)
    
    dwell = np.ones(n, dtype=int)
    for i in range(n - 2, -1, -1):
        if places[i] == places[i + 1]:
            dwell[i] = dwell[i + 1] + 1
    return dwell


def _estimate_travel_mins(prev_sf: str, prev_loc_key: str, curr_loc_key: str) -> int:
    """출발 공간 유형과 LOCATION_KEY 변화로 이동 시간 추정."""
    
    # 출발지 유형 우선 체크
    if prev_sf in (SpaceFunction.RACK, SpaceFunction.TRANSIT_GATE):
        return TRANSITION_FROM_ENTRY_MIN   # 10분
    
    if prev_sf == SpaceFunction.REST:
        return TRANSITION_FROM_REST_MIN    # 5분
    
    # LOCATION_KEY 변화로 판단
    if prev_loc_key != curr_loc_key and prev_loc_key and curr_loc_key:
        return TRANSITION_INTER_LOCATION_MIN   # 10분 (건물/층 간)
    
    return TRANSITION_SAME_LOCATION_MIN        # 3분 (같은 층 내)


# ═══════════════════════════════════════════════════════════════════════════
# Intelligent Journey Correction v5 — 시퀀스 기반 맥락 해석
# ═══════════════════════════════════════════════════════════════════════════

def _get_runs(values: np.ndarray) -> list:
    """
    연속 동일값 구간(run) 목록 반환.
    
    Args:
        values: 값 배열
        
    Returns:
        [(start_idx, end_idx, value, length), ...] 리스트
        
    예) [A,A,B,B,B,A] → [(0,1,A,2), (2,4,B,3), (5,5,A,1)]
    """
    if len(values) == 0:
        return []
    
    runs = []
    start = 0
    for i in range(1, len(values)):
        if values[i] != values[i - 1]:
            runs.append((start, i - 1, values[start], i - start))
            start = i
    runs.append((start, len(values) - 1, values[start], len(values) - start))
    return runs


def _get_run_length_at(values: np.ndarray, idx: int) -> int:
    """
    idx 위치에서 같은 값이 연속되는 구간 길이 반환.
    
    예) [A,A,B,B,B,A], idx=2 → 3 (B가 3칸 연속)
    """
    if len(values) == 0 or idx < 0 or idx >= len(values):
        return 0
    
    val = values[idx]
    # 앞으로
    start = idx
    while start > 0 and values[start - 1] == val:
        start -= 1
    # 뒤로
    end = idx
    while end < len(values) - 1 and values[end + 1] == val:
        end += 1
    return end - start + 1


# ═══════════════════════════════════════════════════════════════════════════
# ★ v6: 4증거 통합 Journey 보정 — 증거 수집 레이어
# ═══════════════════════════════════════════════════════════════════════════

def _calc_run_lengths(places: np.ndarray) -> np.ndarray:
    """
    각 행의 현재 연속 구간 길이를 계산.
    
    예: [A, A, A, B, A, A] → [3, 3, 3, 1, 2, 2]
    """
    n = len(places)
    run_lengths = np.ones(n, dtype=int)

    i = 0
    while i < n:
        j = i
        while j < n and places[j] == places[i]:
            j += 1
        run_len = j - i
        for k in range(i, j):
            run_lengths[k] = run_len
        i = j

    return run_lengths


def _collect_evidence(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    [v6] 4대 증거를 행(分)별로 수집하여 컬럼으로 추가.

    이 함수는 보정을 수행하지 않고, 후속 보정 단계에서 사용할 판단 근거를 생성한다.
    Multi-Pass Refinement의 첫 번째 단계로, 매 반복마다 재수집된다.

    4대 증거 체계:
    ┌────────────────────────────────────────────────────────────────────────┐
    │  E1. 활성신호 (active_signal_count)                                     │
    │      → "사람이 실제로 움직였는가?"                                        │
    │      → none/low/mid/high 4단계 분류                                     │
    │                                                                         │
    │  E2. 공간 속성 (space_function) — 이 함수에서는 수집하지 않음            │
    │      → 이미 SPACE_FUNCTION 컬럼으로 존재                                 │
    │                                                                         │
    │  E3. 시간 속성 — _segment_day()에서 처리                                 │
    │      → segment_type (pre_work/work/lunch/post_work)                    │
    │                                                                         │
    │  E4. 이동 패턴                                                           │
    │      → e4_location_stable: 위치 안정성 (True=안정, False=점프)           │
    │      → e4_run_length: 현재 연속 구간 길이 (분)                           │
    └────────────────────────────────────────────────────────────────────────┘

    추가되는 컬럼:
        e1_active_level (str):
            - "none": active_signal_count = 0 (무활성, Ghost 후보)
            - "low":  active_signal_count = 1~2 (이동 중 간헐)
            - "mid":  active_signal_count = 3~5 (경미한 활동)
            - "high": active_signal_count >= 6 (실제 작업)

        e4_location_stable (bool):
            - True:  윈도우(5분) 내 고유 장소 1개 (안정적 체류)
            - False: 윈도우 내 고유 장소 2개 이상 (불안정/이동/BLE 점프)

        e4_run_length (int):
            - 현재 행이 속한 연속 체류 구간의 길이 (분)
            - 예: AAA면 각 A의 run_length = 3

        e_ghost_candidate (bool):
            - True: E1="none" AND E4=불안정
            - BLE 다중반사로 인한 가짜 신호 후보

        e_transition (bool):
            - True: E1="low"/"mid" AND E4=불안정
            - 이동 중인 상태 후보

    Args:
        df_worker: 작업자 단위 DataFrame (CORRECTED_PLACE 컬럼 필수)

    Returns:
        5개의 증거 컬럼이 추가된 DataFrame
    """
    df = df_worker.copy()
    n = len(df)

    # ─── 빈 DataFrame 처리 ───────────────────────────────────────────────
    if n == 0:
        df[ProcessedColumns.E1_ACTIVE_LEVEL] = []
        df[ProcessedColumns.E4_LOCATION_STABLE] = []
        df[ProcessedColumns.E4_RUN_LENGTH] = []
        df[ProcessedColumns.E_GHOST_CANDIDATE] = []
        df[ProcessedColumns.E_TRANSITION] = []
        return df

    # ─── 원본 데이터 추출 ────────────────────────────────────────────────
    active_sig_col = RawColumns.ACTIVE_SIGNAL_COUNT
    if active_sig_col not in df.columns:
        active_sig = np.zeros(n, dtype=int)
    else:
        active_sig = df[active_sig_col].fillna(0).astype(int).values

    places = df[ProcessedColumns.CORRECTED_PLACE].fillna("").astype(str).values

    # ═══════════════════════════════════════════════════════════════════════
    # E1: 활성신호 레벨 분류
    # ═══════════════════════════════════════════════════════════════════════
    def _classify_active_level(active_count: int) -> str:
        """활성신호갯수를 4단계 레벨로 분류."""
        if active_count <= ACTIVE_SIG_GHOST_MAX:  # 0
            return "none"
        if active_count <= ACTIVE_SIG_TRANSIT_MAX:  # 1~2
            return "low"
        if active_count < ACTIVE_SIG_WORK_MIN + 3:  # 3~5
            return "mid"
        return "high"  # 6+

    e1_levels = [_classify_active_level(v) for v in active_sig]

    # ═══════════════════════════════════════════════════════════════════════
    # E4-1: 연속 구간 길이 계산
    # ═══════════════════════════════════════════════════════════════════════
    run_lengths = _calc_run_lengths(places)

    # ═══════════════════════════════════════════════════════════════════════
    # E4-2: 위치 안정성 계산
    # ═══════════════════════════════════════════════════════════════════════
    window_size = LOCATION_ENTROPY_WINDOW
    e4_stable_flags = []

    for i in range(n):
        window_start = max(0, i - window_size // 2)
        window_end = min(n, i + window_size // 2 + 1)
        unique_place_count = len(set(places[window_start:window_end]))
        is_stable = unique_place_count < LOCATION_UNSTABLE_THRESH
        e4_stable_flags.append(is_stable)

    # ═══════════════════════════════════════════════════════════════════════
    # 복합 증거 플래그 생성
    # ═══════════════════════════════════════════════════════════════════════
    e_ghost_flags = [
        e1_levels[i] == "none" and not e4_stable_flags[i]
        for i in range(n)
    ]
    e_transition_flags = [
        e1_levels[i] in ("low", "mid") and not e4_stable_flags[i]
        for i in range(n)
    ]

    # ─── 컬럼 추가 ───────────────────────────────────────────────────────
    df[ProcessedColumns.E1_ACTIVE_LEVEL] = e1_levels
    df[ProcessedColumns.E4_LOCATION_STABLE] = e4_stable_flags
    df[ProcessedColumns.E4_RUN_LENGTH] = run_lengths
    df[ProcessedColumns.E_GHOST_CANDIDATE] = e_ghost_flags
    df[ProcessedColumns.E_TRANSITION] = e_transition_flags

    ghost_count = sum(e_ghost_flags)
    transition_count = sum(e_transition_flags)
    logger.debug(
        f"v6 증거 수집 완료: ghost_candidate={ghost_count}, "
        f"transition={transition_count}, total={n}행"
    )

    return df


def _segment_day(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    [v6] 하루를 논리적 구간으로 분절하여 시간적 맥락 생성.

    이 함수는 보정을 수행하지 않고, 각 행이 어떤 시간대에 속하는지만 분류한다.
    후속 보정 단계에서 시간적 맥락(E3)으로 활용된다.

    하루의 논리적 구간:
    ┌────────────────────────────────────────────────────────────────────────┐
    │  시간대 구분                                                            │
    │                                                                         │
    │  00:00 ─────────────── 05:00 ─────────── 07:00 ─────────── 12:00 ─────  │
    │    │     야간/새벽       │    출근 준비    │    오전 근무    │    점심   │
    │    │    (pre_work)      │   (pre_work)   │    (work)      │  (lunch)  │
    │    └─────────────────────┴────────────────┴────────────────┴──────────  │
    │                                                                         │
    │  ─── 13:00 ─────────────── 20:00 ────────────── 24:00                   │
    │       │     오후 근무       │    퇴근 후      │                         │
    │       │     (work)         │   (post_work)   │                         │
    │       └────────────────────┴─────────────────┘                         │
    └────────────────────────────────────────────────────────────────────────┘

    segment_type 값:
        "pre_work":  출근 전 (새벽~첫 번째 실제 활동 이전)
                     - hour < PREDAWN_WORK_START (5시)
                     - 또는 첫 활동 이전의 모든 무활성 구간

        "work":      근무 시간
                     - 첫 활동 ~ 마지막 활동 사이
                     - 점심시간 비활성 제외

        "lunch":     점심시간 (LUNCH_START ~ LUNCH_END)
                     - 시간 조건 + 비활성/저활성 (e1 = none/low)

        "post_work": 퇴근 후 (마지막 활동 이후)
                     - hour >= POST_WORK_HOUR (20시) 또는
                     - 마지막 활동 이후 모든 무활성 구간

    Args:
        df_worker: 작업자 단위 DataFrame (E1_ACTIVE_LEVEL, HOUR 컬럼 필요)

    Returns:
        SEGMENT_TYPE 컬럼이 추가된 DataFrame
    """
    df = df_worker.copy()
    n = len(df)

    # ─── 빈 DataFrame 처리 ───────────────────────────────────────────────
    if n == 0:
        df[ProcessedColumns.SEGMENT_TYPE] = []
        return df

    # ─── 원본 데이터 추출 ────────────────────────────────────────────────
    hours = (
        df[ProcessedColumns.HOUR].values
        if ProcessedColumns.HOUR in df.columns
        else np.zeros(n)
    )
    e1_vals = (
        df[ProcessedColumns.E1_ACTIVE_LEVEL].values
        if ProcessedColumns.E1_ACTIVE_LEVEL in df.columns
        else ["none"] * n
    )

    # ═══════════════════════════════════════════════════════════════════════
    # 핵심 경계점 탐지: 첫 번째/마지막 실제 활동
    # ═══════════════════════════════════════════════════════════════════════

    # 첫 번째 실제 활동: e1 != "none" AND hour >= PREDAWN_WORK_START
    first_active_idx = n  # 기본값: 전체가 pre_work
    for i in range(n):
        if e1_vals[i] != "none" and hours[i] >= PREDAWN_WORK_START:
            first_active_idx = i
            break

    # 마지막 실제 활동: e1 != "none"인 마지막 행
    last_active_idx = 0  # 기본값: 첫 번째 행
    for i in range(n - 1, -1, -1):
        if e1_vals[i] != "none":
            last_active_idx = i
            break

    # ═══════════════════════════════════════════════════════════════════════
    # 각 행의 segment_type 결정
    # ═══════════════════════════════════════════════════════════════════════
    segment_types = []

    for i in range(n):
        hour = hours[i]
        e1_level = e1_vals[i]

        # 1) 첫 활동 이전 → pre_work
        if i < first_active_idx:
            segment_types.append("pre_work")

        # 2) 마지막 활동 이후 + 무활성 → post_work
        elif i > last_active_idx and e1_level == "none":
            segment_types.append("post_work")

        # 3) 점심시간 + 비/저활성 → lunch
        elif LUNCH_START <= hour < LUNCH_END and e1_level in ("none", "low"):
            segment_types.append("lunch")

        # 4) 그 외 → work
        else:
            segment_types.append("work")

    df[ProcessedColumns.SEGMENT_TYPE] = segment_types

    # ─── 디버그 로깅 ─────────────────────────────────────────────────────
    seg_counts = pd.Series(segment_types).value_counts().to_dict()
    logger.debug(f"v6 하루 구간 분절: {seg_counts}")

    return df


def _correct_ghost_signals(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    [v6.2 / Pass 1] Ghost Signal 보정 — 무활성 구간의 합리적 장소 추론.

    핵심 원칙:
    ┌────────────────────────────────────────────────────────────────────────┐
    │  활성신호 = 0 → 움직임이 없다                                           │
    │  → 여러 장소가 번갈아 나와도 실제로는 "한 곳"에 고정                       │
    │  → 원본 데이터 내 장소들 중에서 "합리적인 장소"를 추론                     │
    │  → 공간 특성 + 시간 맥락 + 빈도를 종합해서 판단                          │
    └────────────────────────────────────────────────────────────────────────┘

    추론 로직 (원본 데이터 내에서만):
        1. 블록 내 등장한 모든 장소와 빈도 수집
        2. 공간 특성별 점수 부여:
           - RACK (걸이대): 야간/새벽에 최고 점수 (헬멧 걸어둔 상태)
           - REST (휴게실): 점심시간에 높은 점수
           - TRANSIT_GATE: 통과 공간, 체류 불가 → 최저 점수
           - WORK: 근무시간에 기본 점수
        3. 빈도 × 공간점수 = 최종 점수
        4. 최고 점수 장소로 블록 통일

    Args:
        df_worker: 작업자 단위 DataFrame

    Returns:
        Ghost Signal이 보정된 DataFrame
    """
    df = df_worker.copy()
    n = len(df)

    if n == 0:
        return df

    # ─── 원본 데이터 추출 ────────────────────────────────────────────────
    places = df[ProcessedColumns.CORRECTED_PLACE].values.copy()
    is_rack = (
        df[ProcessedColumns.IS_HELMET_RACK].values
        if ProcessedColumns.IS_HELMET_RACK in df.columns
        else [False] * n
    )
    e1_vals = (
        df[ProcessedColumns.E1_ACTIVE_LEVEL].values
        if ProcessedColumns.E1_ACTIVE_LEVEL in df.columns
        else ["mid"] * n
    )
    segments = (
        df[ProcessedColumns.SEGMENT_TYPE].values
        if ProcessedColumns.SEGMENT_TYPE in df.columns
        else ["work"] * n
    )
    sf_vals = (
        df[ProcessedColumns.SPACE_FUNCTION].values
        if ProcessedColumns.SPACE_FUNCTION in df.columns
        else ["UNKNOWN"] * n
    )

    # ─── 공간 특성 판별 함수들 ──────────────────────────────────────────
    rack_keywords = ["걸이대", "거치대", "보호구"]
    rest_keywords = ["휴게", "흡연", "식당", "탈의", "화장실"]
    transit_keywords = ["타각기", "출구", "입구", "게이트", "GATE"]

    def _get_space_type(place_name: str, rack_flag: bool, space_func: str) -> str:
        """장소의 공간 유형 판별: RACK > REST > WORK > TRANSIT"""
        if rack_flag or (place_name and any(kw in str(place_name) for kw in rack_keywords)):
            return "RACK"
        if space_func == "REST" or (place_name and any(kw in str(place_name) for kw in rest_keywords)):
            return "REST"
        if space_func in ("TRANSIT_GATE", "TRANSIT_CORRIDOR") or \
           (place_name and any(kw in str(place_name) for kw in transit_keywords)):
            return "TRANSIT"
        return "WORK"

    def _calc_space_score(space_type: str, segment: str) -> float:
        """공간 유형과 시간대에 따른 점수 계산."""
        # 기본 점수 (체류 가능성 기준)
        base_scores = {
            "RACK": 10.0,      # 가장 확실한 체류 공간 (헬멧 고정)
            "REST": 8.0,       # 휴게 공간
            "WORK": 5.0,       # 작업 공간
            "TRANSIT": 1.0,    # 통과 공간, 체류 불가
        }
        score = base_scores.get(space_type, 3.0)
        
        # 시간대별 보정
        if segment in ("pre_work", "post_work"):
            # 야간/새벽/퇴근 후: RACK 강화, WORK 약화
            if space_type == "RACK":
                score *= 2.0
            elif space_type == "WORK":
                score *= 0.3  # 야간에 작업 공간은 비합리적
        elif segment == "lunch":
            # 점심시간: REST 강화
            if space_type == "REST":
                score *= 1.5
        
        return score

    # ═══════════════════════════════════════════════════════════════════════
    # Step 1: Ghost Block 탐지 (e1_active_level = "none"인 연속 구간)
    # ═══════════════════════════════════════════════════════════════════════
    ghost_blocks: list[tuple[int, int]] = []
    i = 0

    while i < n:
        if e1_vals[i] == "none":
            block_start = i
            while i < n and e1_vals[i] == "none":
                i += 1
            block_end = i - 1
            ghost_blocks.append((block_start, block_end))
        else:
            i += 1

    corrected_count = 0

    # ═══════════════════════════════════════════════════════════════════════
    # Step 2: 각 Ghost Block 처리 — 전체 맥락 기반 추론
    # ═══════════════════════════════════════════════════════════════════════
    for block_start, block_end in ghost_blocks:
        block_len = block_end - block_start + 1

        # 조건 1: 너무 짧은 블록은 일시 정지일 수 있음 → 스킵
        if block_len < GHOST_MIN_BLOCK_LEN:
            continue

        # 조건 2: 시간대 확인 (pre_work/post_work vs work)
        block_segments = [segments[j] for j in range(block_start, block_end + 1)]
        is_non_work_segment = any(
            seg in ("pre_work", "post_work") for seg in block_segments
        )

        # 근무시간대 Ghost는 더 긴 시간 (20분+) 지속해야 처리
        if not is_non_work_segment and block_len < GHOST_WORK_MIN_BLOCK_LEN:
            continue

        # ═══════════════════════════════════════════════════════════════════
        # ★★★ v6.2: 원본 내 장소들의 공간 특성 + 빈도 + 시간 맥락 종합 ★★★
        # ═══════════════════════════════════════════════════════════════════
        
        # ─── Step 2-1: 블록 내 장소별 빈도와 공간 점수 계산 ───────────────
        place_scores: dict[str, float] = {}
        dominant_segment = max(set(block_segments), key=block_segments.count)
        
        for j in range(block_start, block_end + 1):
            place_name = places[j]
            if not place_name:
                continue
            
            space_type = _get_space_type(place_name, is_rack[j], sf_vals[j])
            space_score = _calc_space_score(space_type, dominant_segment)
            
            # 빈도 × 공간점수 누적
            if place_name not in place_scores:
                place_scores[place_name] = 0.0
            place_scores[place_name] += space_score

        if not place_scores:
            continue

        # ─── Step 2-2: 최고 점수 장소 선택 (원본 내에서만) ─────────────────
        best_place = max(place_scores.keys(), key=lambda p: place_scores[p])
        
        # 디버그 로깅
        logger.debug(
            f"Ghost Block [{block_start}~{block_end}] ({block_len}분, {dominant_segment}): "
            f"점수 = {place_scores}, 선택 = {best_place}"
        )

        # ─── Step 2-3: 보정 적용 ──────────────────────────────────────────
        for j in range(block_start, block_end + 1):
            if places[j] != best_place:
                df.at[df.index[j], ProcessedColumns.CORRECTED_PLACE] = best_place
                df.at[df.index[j], ProcessedColumns.IS_CORRECTED] = True
                df.at[df.index[j], ProcessedColumns.STATE_DETAIL] = "ghost_unified"
                corrected_count += 1

    logger.debug(
        f"Pass 1 Ghost Signal 보정: {len(ghost_blocks)}개 블록, "
        f"{corrected_count}행 통일 (공간+시간+빈도 추론)"
    )

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# v6.2 Phase 2: Journey 문장화 보정 — 전체 맥락 기반
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class JourneyRun:
    """Journey의 연속 구간 (= 문장의 단어)."""
    place: str                    # 장소명
    start_idx: int               # 시작 인덱스
    end_idx: int                 # 끝 인덱스 (inclusive)
    length: int                  # 구간 길이 (분)
    space_function: str          # 공간 기능 (RACK/REST/WORK/TRANSIT 등)
    avg_active_level: str        # 평균 활성 레벨 (none/low/mid/high)
    segment_type: str            # 시간대 유형 (pre_work/work/lunch/post_work)
    is_anchor: bool              # 앵커 공간 여부 (RACK/REST)


def _build_journey_runs(df_worker: pd.DataFrame) -> list[JourneyRun]:
    """
    DataFrame을 Run 시퀀스로 압축 — Journey를 "문장"으로 변환.
    
    원리:
      1440분 데이터 → N개의 Run (연속 동일 장소 구간)
      각 Run = (장소, 시작, 끝, 길이, 공간속성, 활성레벨, 시간대)
    
    예시:
      분단위: [걸이대, 걸이대, ..., FAB, FAB, ..., 휴게실, FAB, ...]
      → Run 시퀀스: [(걸이대,280분), (FAB,120분), (휴게실,15분), (FAB,180분), ...]
    """
    n = len(df_worker)
    if n == 0:
        return []
    
    places = df_worker[ProcessedColumns.CORRECTED_PLACE].fillna("").astype(str).values
    sf_vals = (
        df_worker[ProcessedColumns.SPACE_FUNCTION].values
        if ProcessedColumns.SPACE_FUNCTION in df_worker.columns
        else ["UNKNOWN"] * n
    )
    e1_vals = (
        df_worker[ProcessedColumns.E1_ACTIVE_LEVEL].values
        if ProcessedColumns.E1_ACTIVE_LEVEL in df_worker.columns
        else ["mid"] * n
    )
    seg_vals = (
        df_worker[ProcessedColumns.SEGMENT_TYPE].values
        if ProcessedColumns.SEGMENT_TYPE in df_worker.columns
        else ["work"] * n
    )
    
    runs: list[JourneyRun] = []
    i = 0
    
    while i < n:
        place = places[i]
        start_idx = i
        
        # 동일 장소 연속 구간 찾기
        while i < n and places[i] == place:
            i += 1
        end_idx = i - 1
        length = end_idx - start_idx + 1
        
        # 구간 내 대표값 계산
        space_funcs = [sf_vals[j] for j in range(start_idx, end_idx + 1)]
        active_levels = [e1_vals[j] for j in range(start_idx, end_idx + 1)]
        segments = [seg_vals[j] for j in range(start_idx, end_idx + 1)]
        
        # 최빈값으로 대표값 선택
        dominant_sf = max(set(space_funcs), key=space_funcs.count) if space_funcs else "UNKNOWN"
        dominant_active = max(set(active_levels), key=active_levels.count) if active_levels else "mid"
        dominant_segment = max(set(segments), key=segments.count) if segments else "work"
        
        is_anchor = dominant_sf in ANCHOR_SPACE_FUNCTIONS
        
        runs.append(JourneyRun(
            place=place,
            start_idx=start_idx,
            end_idx=end_idx,
            length=length,
            space_function=dominant_sf,
            avg_active_level=dominant_active,
            segment_type=dominant_segment,
            is_anchor=is_anchor,
        ))
    
    return runs


def _analyze_journey_context(runs: list[JourneyRun]) -> dict:
    """
    전체 Journey의 맥락 분석 — "문장의 문법 파악".
    
    분석 항목:
      1. 주요 장소 (가장 많이 등장하고 오래 체류한 장소들)
      2. 출근/퇴근 패턴 (첫 앵커 이탈, 마지막 앵커 복귀)
      3. Run 길이 분포 (짧은 Run = 노이즈 가능성)
      4. 장소별 총 체류시간
    """
    if not runs:
        return {}
    
    # 장소별 통계
    place_stats: dict[str, dict] = {}
    for run in runs:
        if run.place not in place_stats:
            place_stats[run.place] = {
                "total_minutes": 0,
                "visit_count": 0,
                "space_function": run.space_function,
                "is_anchor": run.is_anchor,
                "run_lengths": [],
            }
        place_stats[run.place]["total_minutes"] += run.length
        place_stats[run.place]["visit_count"] += 1
        place_stats[run.place]["run_lengths"].append(run.length)
    
    # 주요 장소 판별 (총 체류시간 기준 상위)
    sorted_places = sorted(
        place_stats.items(),
        key=lambda x: x[1]["total_minutes"],
        reverse=True
    )
    major_places = [p[0] for p in sorted_places[:5]]  # 상위 5개
    
    # 출근/퇴근 패턴 파악
    first_non_anchor_idx = next(
        (i for i, r in enumerate(runs) if not r.is_anchor and r.avg_active_level != "none"),
        None
    )
    last_non_anchor_idx = next(
        (len(runs) - 1 - i for i, r in enumerate(reversed(runs)) 
         if not r.is_anchor and r.avg_active_level != "none"),
        None
    )
    
    # 짧은 Run 통계
    short_runs = [r for r in runs if r.length <= 3]
    
    return {
        "place_stats": place_stats,
        "major_places": major_places,
        "total_runs": len(runs),
        "short_run_count": len(short_runs),
        "first_work_run_idx": first_non_anchor_idx,
        "last_work_run_idx": last_non_anchor_idx,
    }


def _should_absorb_run(
    run: JourneyRun,
    prev_run: Optional[JourneyRun],
    next_run: Optional[JourneyRun],
    context: dict,
) -> tuple[bool, Optional[str], str]:
    """
    특정 Run이 앞/뒤 Run에 흡수되어야 하는지 판단.
    
    흡수 조건:
      1. 너무 짧은 Run (1~2분) + 앞뒤가 같은 장소
      2. TRANSIT 전용 공간 + 무활성 + 짧은 체류
      3. 맥락에 맞지 않는 장소 (야간에 WORK 1분 등)
      4. 앞뒤가 같고, 중간만 다른 "노이즈" 패턴
    
    Returns:
        (should_absorb, target_place, reason)
    """
    # 앵커 공간은 흡수하지 않음 (휴게실 1분도 정상 행동)
    if run.is_anchor:
        return False, None, ""
    
    # 조건 1: 앞뒤가 같고 중간만 다름 (A-X-A 패턴)
    if prev_run and next_run:
        if prev_run.place == next_run.place and run.place != prev_run.place:
            # 중간 Run이 매우 짧으면 → 노이즈로 판단
            if run.length <= 2:
                return True, prev_run.place, "noise_aba_pattern"
            
            # TRANSIT 공간이고 무활성이면 → 노이즈
            if run.space_function in TRANSIT_ONLY_FUNCTIONS and run.avg_active_level == "none":
                return True, prev_run.place, "transit_noise"
    
    # 조건 2: 매우 짧은 Run + 앞/뒤 중 하나와 연결 가능
    if run.length <= 2:
        # 앞이 더 길면 앞으로 흡수
        if prev_run and (not next_run or prev_run.length >= next_run.length):
            # TRANSIT이면 앞으로 흡수
            if run.space_function in TRANSIT_ONLY_FUNCTIONS:
                return True, prev_run.place, "short_transit"
        # 뒤가 더 길면 뒤로 흡수
        elif next_run:
            if run.space_function in TRANSIT_ONLY_FUNCTIONS:
                return True, next_run.place, "short_transit"
    
    # 조건 3: 맥락 이상 — 비근무시간에 WORK 공간 짧은 체류
    if run.segment_type in ("pre_work", "post_work"):
        if run.space_function in ("WORK", "WORK_HAZARD") and run.avg_active_level == "none":
            if run.length <= 5:
                # 앞뒤 중 앵커가 있으면 그쪽으로 흡수
                if prev_run and prev_run.is_anchor:
                    return True, prev_run.place, "non_work_time_noise"
                if next_run and next_run.is_anchor:
                    return True, next_run.place, "non_work_time_noise"
    
    return False, None, ""


def _correct_journey_as_sentence(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    [v6.2 Phase 2] 전체 Journey를 "문장"으로 보고 보정.
    
    핵심 철학:
    ┌────────────────────────────────────────────────────────────────────────┐
    │  Journey = 문장 (Sentence)                                             │
    │  장소 = 단어 (Word)                                                    │
    │  연속 체류 = Run (단어 토큰)                                            │
    │                                                                        │
    │  부분(개별 분)을 보는 것이 아니라,                                       │
    │  전체 문장(하루 Journey)의 흐름을 보고 어색한 단어(장소)를 찾아 보정      │
    └────────────────────────────────────────────────────────────────────────┘
    
    처리 흐름:
      1. Run 시퀀스 생성 (1440분 → N개 Run으로 압축)
      2. 전체 맥락 분석 (주요 장소, 출퇴근 패턴, 짧은 Run 등)
      3. 각 Run을 앞뒤 문맥과 함께 평가
      4. 흡수해야 할 Run → 앞/뒤 장소로 통일
      5. DataFrame에 보정 적용
    
    복잡한 이동 패턴 처리:
      - 같은 장소 반복 방문: 주요 장소로 인식, 보호
      - 짧은 이탈 후 복귀: A→X(1분)→A 패턴은 X가 노이즈
      - 여러 작업구역 오가기: 각각 충분한 길이면 유지
      - 예외 상황: 앵커 공간(휴게실/걸이대)은 짧아도 보호
    """
    df = df_worker.copy()
    n = len(df)
    
    if n < 3:
        return df
    
    # ─── Step 1: Run 시퀀스 생성 ─────────────────────────────────────────
    runs = _build_journey_runs(df)
    
    if len(runs) < 3:
        return df
    
    # ─── Step 2: 전체 맥락 분석 ─────────────────────────────────────────
    context = _analyze_journey_context(runs)
    
    # ─── Step 3 & 4: 각 Run 평가 및 흡수 결정 ───────────────────────────
    absorption_plan: list[tuple[int, str, str]] = []  # (run_idx, target_place, reason)
    
    for i, run in enumerate(runs):
        prev_run = runs[i - 1] if i > 0 else None
        next_run = runs[i + 1] if i < len(runs) - 1 else None
        
        should_absorb, target_place, reason = _should_absorb_run(
            run, prev_run, next_run, context
        )
        
        if should_absorb and target_place:
            absorption_plan.append((i, target_place, reason))
    
    # ─── Step 5: DataFrame에 보정 적용 ─────────────────────────────────
    corrected_count = 0
    
    for run_idx, target_place, reason in absorption_plan:
        run = runs[run_idx]
        
        for j in range(run.start_idx, run.end_idx + 1):
            idx = df.index[j]
            current_place = df.at[idx, ProcessedColumns.CORRECTED_PLACE]
            
            if current_place != target_place:
                df.at[idx, ProcessedColumns.CORRECTED_PLACE] = target_place
                df.at[idx, ProcessedColumns.IS_CORRECTED] = True
                df.at[idx, ProcessedColumns.STATE_DETAIL] = f"sentence_{reason}"
                corrected_count += 1
    
    logger.debug(
        f"Journey 문장화 보정: {len(runs)}개 Run 중 {len(absorption_plan)}개 흡수, "
        f"{corrected_count}행 보정"
    )
    
    return df


def _correct_alternating_by_context(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    [v6 / Pass 2] 번갈음 패턴 해소 — 공간 우선순위 기반 통합.

    Multi-Pass Refinement의 두 번째 단계.
    두 장소가 번갈아 나타나는 패턴을 탐지하고, 공간 우선순위에 따라 하나로 통합한다.

    번갈음 패턴이란?
    ┌────────────────────────────────────────────────────────────────────────┐
    │  예시: FAB↔휴게실 번갈음                                                │
    │                                                                         │
    │  분단위: FAB → FAB → 휴게실 → FAB → 휴게실 → FAB → FAB                  │
    │          ─────────────────────────────────────────────────             │
    │                      ↑                                                  │
    │              10분 윈도우 내 2개 장소만 번갈아 나타남                       │
    │                                                                         │
    │  원인: BLE 신호가 두 공간 사이에서 흔들림                                 │
    │  해결: 공간 우선순위가 높은 쪽(휴게실)으로 통일                            │
    └────────────────────────────────────────────────────────────────────────┘

    공간 우선순위 (SPACE_FUNCTION_PRIORITY):
        1. RACK (헬멧 거치대) — 확실한 체류 지점
        2. REST (휴게실/흡연장) — 들어가서 머무는 공간
        3. WORK (작업장) — 작업 + 대기 + 통과 혼재
        4~6. TRANSIT 계열 — 통과만 하는 공간
        7+. UNKNOWN

    적용 조건 (모두 만족 필요):
        1. 윈도우 내 고유 장소가 정확히 2개 (번갈음)
        2. 두 장소 모두 연속 구간 ≤ RUN_SHORT_MAX (5분)
        3. 윈도우 내 최장 연속 < RUN_CONTINUOUS_MIN (10분)
        4. 두 장소의 우선순위가 다름

    강화 조건 (하나라도 만족시 강제 적용):
        A. TRANSIT_GATE 포함 → 체류 불가 공간이므로 반드시 제거
        B. 점심/야간 + REST/RACK → 해당 시간대에 앵커 공간 우세
        C. 무활성(e1="none") + RACK → Ghost 잔류 처리

    보호 조건 (적용 안 함):
        X. 어느 한쪽이 10분 이상 연속 → 실제 체류로 인정
        Y. 두 장소 우선순위 동일 → 판단 불가

    Args:
        df_worker: 작업자 단위 DataFrame

    Returns:
        번갈음 패턴이 보정된 DataFrame
    """
    df = df_worker.copy()
    n = len(df)
    
    if n < 3:
        return df

    sf_col = ProcessedColumns.SPACE_FUNCTION
    cp_col = ProcessedColumns.CORRECTED_PLACE
    sd_col = ProcessedColumns.STATE_DETAIL

    if sf_col not in df.columns:
        return df

    places = df[cp_col].fillna("").astype(str).values
    sf_vals = df[sf_col].fillna(SpaceFunction.UNKNOWN).values
    run_lens = df[ProcessedColumns.E4_RUN_LENGTH].values if ProcessedColumns.E4_RUN_LENGTH in df.columns else _calc_run_lengths(places)
    e1_vals = df[ProcessedColumns.E1_ACTIVE_LEVEL].values if ProcessedColumns.E1_ACTIVE_LEVEL in df.columns else ["mid"] * n
    hours = df[ProcessedColumns.HOUR].values if ProcessedColumns.HOUR in df.columns else np.zeros(n)
    segments = df[ProcessedColumns.SEGMENT_TYPE].values if ProcessedColumns.SEGMENT_TYPE in df.columns else ["work"] * n

    # 장소 → space_function 매핑 캐시
    place_sf_cache: dict[str, str] = {}
    for i in range(n):
        p = places[i]
        if p and p not in place_sf_cache:
            place_sf_cache[p] = sf_vals[i]

    W = 10  # 슬라이딩 윈도우 크기
    absorbed_count = 0

    for i in range(n):
        # ── 보호 조건 X: 현재 행이 긴 연속 구간이면 스킵 ─────────────────
        if run_lens[i] >= RUN_CONTINUOUS_MIN:
            continue

        start = max(0, i - W // 2)
        end = min(n, i + W // 2 + 1)
        w_places = places[start:end]
        w_runs = run_lens[start:end]

        # ── 조건 3: 윈도우 내 최장 run 체크 ─────────────────────────────
        if max(w_runs) >= RUN_CONTINUOUS_MIN:
            continue

        # ── 조건 1: 고유 장소 2개 ────────────────────────────────────────
        unique_places = set(p for p in w_places if p)
        if len(unique_places) != 2:
            continue

        place_a, place_b = tuple(unique_places)
        sf_a = place_sf_cache.get(place_a, SpaceFunction.UNKNOWN)
        sf_b = place_sf_cache.get(place_b, SpaceFunction.UNKNOWN)
        prio_a = SPACE_FUNCTION_PRIORITY.get(sf_a, 9)
        prio_b = SPACE_FUNCTION_PRIORITY.get(sf_b, 9)

        # ── 조건 4: 우선순위 차이 있어야 함 ─────────────────────────────
        if prio_a == prio_b:
            continue

        winner_place = place_a if prio_a < prio_b else place_b
        loser_sf = sf_b if prio_a < prio_b else sf_a
        winner_sf = sf_a if prio_a < prio_b else sf_b

        # ── 강화 조건 판단 ────────────────────────────────────────────────
        h = hours[i]
        seg = segments[i]
        e1 = e1_vals[i]

        force_apply = False

        # A: TRANSIT_GATE 포함 → 강제 적용
        if loser_sf in TRANSIT_ONLY_FUNCTIONS:
            force_apply = True

        # B: 점심/야간 + REST/RACK → 강제 적용
        if winner_sf in ANCHOR_SPACE_FUNCTIONS:
            if LUNCH_START <= h < LUNCH_END:
                force_apply = True
            if seg in ("pre_work", "post_work"):
                force_apply = True

        # C: 무활성 + RACK → 강제 적용
        if e1 == "none" and winner_sf == SpaceFunction.RACK:
            force_apply = True

        # 일반 조건: 우선순위 2단계 이상 차이
        if not force_apply and abs(prio_a - prio_b) < 2:
            continue

        # ── 현재 행이 loser 장소이면 winner로 교체 ─────────────────────
        if places[i] != winner_place:
            current_sd = df.at[df.index[i], sd_col] if sd_col in df.columns else ""
            if current_sd == "absorbed_by_context":
                continue
            df.at[df.index[i], cp_col] = winner_place
            df.at[df.index[i], sd_col] = "absorbed_by_context"
            df.at[df.index[i], ProcessedColumns.IS_CORRECTED] = True
            absorbed_count += 1

    logger.debug(f"Pass 2 번갈음 보정: absorbed_by_context {absorbed_count}행")

    return df


# ═══════════════════════════════════════════════════════════════════════════
# ★ v6.1: Multi-Pass Refinement — Pass 3, Pass 4
# ═══════════════════════════════════════════════════════════════════════════

def _pass3_verify_narrative(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    [Pass 3] 전체 맥락 검증 — 하루 스토리라인 일관성 확인.

    Multi-Pass Refinement의 세 번째 단계.
    Pass 1, 2에서 보정된 결과를 "하루 전체" 관점에서 검증하여 논리적 불일치를 탐지한다.

    스토리라인 일관성이란?
    ┌────────────────────────────────────────────────────────────────────────┐
    │  하루의 자연스러운 흐름:                                                 │
    │                                                                         │
    │  [출근 전]     [근무]      [점심]     [근무]      [퇴근 후]              │
    │  ─────────────────────────────────────────────────────────────         │
    │  RACK/이동    WORK        REST      WORK        RACK/이동              │
    │                                                                         │
    │  이 흐름에 맞지 않는 패턴은 "이례적"으로 태깅:                            │
    │  - 새벽 3시에 WORK 공간에서 무활성? → 실제 작업이 아님                    │
    │  - 점심시간에 WORK 공간에서 무활성? → 쉬고 있거나 데이터 오류             │
    │  - 휴게실 1분만 체류? → 데이터 노이즈일 가능성                            │
    └────────────────────────────────────────────────────────────────────────┘

    검증 규칙:
        규칙 1-2: 비근무시간 WORK 구간 재검증
            - pre_work/post_work 구간에서 WORK + 무활성 → "anomaly_work_in_off_hours"

        규칙 3: 점심시간 WORK 구간 검증
            - 점심 + WORK + 무활성 → "anomaly_work_in_lunch_inactive"
            - (활성신호 있으면 잔업으로 인정)

        규칙 4: 앵커 공간 체류 시간 검증
            - REST/RACK 체류가 NARRATIVE_ANCHOR_MIN_DWELL 미만
            - → "short_anchor_dwell" (데이터 노이즈 가능성)

    Args:
        df_worker: 작업자 단위 DataFrame

    Returns:
        이례적 패턴이 태깅된 DataFrame
    """
    df = df_worker.copy()
    n = len(df)
    
    if n == 0:
        return df

    places = df[ProcessedColumns.CORRECTED_PLACE].values
    segments = df[ProcessedColumns.SEGMENT_TYPE].values if ProcessedColumns.SEGMENT_TYPE in df.columns else ["work"] * n
    hours = df[ProcessedColumns.HOUR].values if ProcessedColumns.HOUR in df.columns else np.zeros(n)
    e1_vals = df[ProcessedColumns.E1_ACTIVE_LEVEL].values if ProcessedColumns.E1_ACTIVE_LEVEL in df.columns else ["mid"] * n
    sf_vals = df[ProcessedColumns.SPACE_FUNCTION].values if ProcessedColumns.SPACE_FUNCTION in df.columns else [SpaceFunction.UNKNOWN] * n

    corrected_count = 0

    # ── 규칙 1, 2: 비근무시간에 WORK 구간 재검증 ─────────────────────────
    for i in range(n):
        seg = segments[i]
        sf = sf_vals[i]
        e1 = e1_vals[i]
        
        # pre_work/post_work 구간인데 WORK 공간이고 무활성이면 → 이례적
        if seg in ("pre_work", "post_work") and sf == SpaceFunction.WORK and e1 == "none":
            df.at[df.index[i], ProcessedColumns.STATE_DETAIL] = "anomaly_work_in_off_hours"
            corrected_count += 1

    # ── 규칙 3: 점심시간 연속 WORK 검증 ─────────────────────────────────
    for i in range(n):
        seg = segments[i]
        sf = sf_vals[i]
        e1 = e1_vals[i]
        
        # 점심인데 WORK이고 활성신호가 있으면 → 잔업으로 인정 (정상)
        # 점심인데 WORK이고 무활성이면 → 이례적
        if seg == "lunch" and sf == SpaceFunction.WORK and e1 == "none":
            df.at[df.index[i], ProcessedColumns.STATE_DETAIL] = "anomaly_work_in_lunch_inactive"
            corrected_count += 1

    # ── 규칙 4: 앵커 공간 체류 시간 검증 ─────────────────────────────────
    # (연속 체류가 NARRATIVE_ANCHOR_MIN_DWELL 미만이면 이례적 태깅)
    run_lens = df[ProcessedColumns.E4_RUN_LENGTH].values if ProcessedColumns.E4_RUN_LENGTH in df.columns else _calc_run_lengths(places)
    
    for i in range(n):
        sf = sf_vals[i]
        run_len = run_lens[i]
        
        if sf in (SpaceFunction.REST, SpaceFunction.RACK):
            if run_len < NARRATIVE_ANCHOR_MIN_DWELL and run_len > 0:
                current_detail = df.at[df.index[i], ProcessedColumns.STATE_DETAIL] if ProcessedColumns.STATE_DETAIL in df.columns else None
                if current_detail not in ("ghost_corrected", "absorbed_by_context"):
                    df.at[df.index[i], ProcessedColumns.STATE_DETAIL] = "short_anchor_dwell"

    logger.debug(f"Pass 3 맥락 검증: {corrected_count}개 이례적 구간 탐지")
    return df


def _pass4_detect_impossible_movement(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    [Pass 4] 물리적 이상치 탐지 — 불가능한 이동 제거.

    Multi-Pass Refinement의 네 번째(마지막) 단계.
    Pass 1~3에서 보정된 결과에서 물리적으로 불가능한 이동을 탐지한다.

    물리적 이상치란?
    ┌────────────────────────────────────────────────────────────────────────┐
    │  규칙 1: 텔레포트 (Teleportation)                                       │
    │  ─────────────────────────────────────────────────────────────────      │
    │  1분 내 좌표 이동 거리 > IMPOSSIBLE_MOVE_SPEED (500)                    │
    │  → 물리적으로 불가능한 순간이동                                          │
    │  → anomaly_flag = "impossible_teleport"                                │
    │                                                                         │
    │  규칙 2: 건물 간 점프                                                    │
    │  ─────────────────────────────────────────────────────────────────      │
    │  2분 내 다른 LOCATION_KEY로 이동 (중간 OUTDOOR/TRANSIT 없이)            │
    │  → 건물 사이 이동 시간 없이 바로 점프                                     │
    │  → anomaly_flag = "impossible_building_jump"                           │
    │                                                                         │
    │  규칙 3: 노이즈 잔류 (A-B-A 패턴)                                        │
    │  ─────────────────────────────────────────────────────────────────      │
    │  ...AAA B AAA... (A 연속 중 B 단발)                                     │
    │  → Pass 1-2에서 처리되지 않은 잔류 노이즈                                 │
    │  → B를 A로 흡수                                                         │
    └────────────────────────────────────────────────────────────────────────┘

    처리 방식:
        - 텔레포트/점프: anomaly_flag 태깅 (시각화에서 경고 표시용)
        - 노이즈 잔류: 실제로 장소를 보정하여 흡수

    Args:
        df_worker: 작업자 단위 DataFrame

    Returns:
        물리적 이상치가 처리된 DataFrame
    """
    df = df_worker.copy()
    n = len(df)
    
    if n < 3:
        return df

    places = df[ProcessedColumns.CORRECTED_PLACE].values.copy()
    x_vals = df[ProcessedColumns.CORRECTED_X].values if ProcessedColumns.CORRECTED_X in df.columns else df[RawColumns.X].values
    y_vals = df[ProcessedColumns.CORRECTED_Y].values if ProcessedColumns.CORRECTED_Y in df.columns else df[RawColumns.Y].values
    loc_keys = df[ProcessedColumns.LOCATION_KEY].values if ProcessedColumns.LOCATION_KEY in df.columns else ["OUTDOOR"] * n

    corrected_count = 0
    anomaly_count = 0

    # ── 규칙 1: 텔레포트 탐지 (1분 내 급격한 좌표 이동) ─────────────────
    for i in range(1, n):
        dx = abs(x_vals[i] - x_vals[i-1])
        dy = abs(y_vals[i] - y_vals[i-1])
        dist = (dx**2 + dy**2) ** 0.5
        
        if dist > IMPOSSIBLE_MOVE_SPEED:
            df.at[df.index[i], ProcessedColumns.ANOMALY_FLAG] = "impossible_teleport"
            anomaly_count += 1

    # ── 규칙 2: 건물 간 점프 탐지 ─────────────────────────────────────────
    for i in range(IMPOSSIBLE_BUILDING_JUMP_MIN, n):
        curr_loc = loc_keys[i]
        prev_loc = loc_keys[i - IMPOSSIBLE_BUILDING_JUMP_MIN]
        
        # OUTDOOR는 건물 간 이동으로 간주하지 않음
        if curr_loc == "OUTDOOR" or prev_loc == "OUTDOOR":
            continue
            
        if curr_loc != prev_loc:
            # 건물이 다르면 중간에 OUTDOOR나 TRANSIT이 있어야 정상
            has_transition = False
            for j in range(i - IMPOSSIBLE_BUILDING_JUMP_MIN + 1, i):
                if loc_keys[j] == "OUTDOOR" or "TRANSIT" in str(loc_keys[j]):
                    has_transition = True
                    break
            
            if not has_transition:
                df.at[df.index[i], ProcessedColumns.ANOMALY_FLAG] = "impossible_building_jump"
                anomaly_count += 1

    # ── 규칙 3: 노이즈 잔류 탐지 (A-B-A 패턴에서 단발 B) ─────────────────
    for i in range(1, n - 1):
        if places[i-1] == places[i+1] and places[i] != places[i-1]:
            # 앞뒤가 같고 중간만 다름 → 노이즈 잔류
            # 이미 보정되지 않은 경우에만 처리
            current_detail = df.at[df.index[i], ProcessedColumns.STATE_DETAIL] if ProcessedColumns.STATE_DETAIL in df.columns else None
            if current_detail not in ("ghost_corrected", "absorbed_by_context", "absorbed_by_priority"):
                df.at[df.index[i], ProcessedColumns.CORRECTED_PLACE] = places[i-1]
                df.at[df.index[i], ProcessedColumns.STATE_DETAIL] = "noise_residual_absorbed"
                df.at[df.index[i], ProcessedColumns.IS_CORRECTED] = True
                corrected_count += 1

    logger.debug(f"Pass 4 이상치 탐지: {anomaly_count}개 불가능 이동, {corrected_count}개 노이즈 잔류 보정")
    return df


def _count_corrections(df: pd.DataFrame) -> int:
    """이번 Pass에서 IS_CORRECTED=True인 행 수 반환."""
    if ProcessedColumns.IS_CORRECTED not in df.columns:
        return 0
    return df[ProcessedColumns.IS_CORRECTED].sum()


def _detect_alternating_pattern(df_worker: pd.DataFrame, window_size: int = 10) -> pd.DataFrame:
    """
    번갈음 패턴 감지 및 Space Priority 기반 보정.
    
    ★ v5.4 핵심 조건 (과보정 방지):
      - 양쪽 장소 모두 연속 구간이 PRIORITY_MAX_RUN_MIN 이하일 때만 적용
      - 윈도우 내 최장 연속 구간이 PRIORITY_CONTINUOUS_THRESHOLD 이상이면 건드리지 않음
    
    적용 예:
      OK  — 휴게실(2분) ↔ FAB(1분) ↔ 휴게실(3분): 양쪽 모두 단발 → 적용
      OK  — 타각기출구(2분) ↔ 걸이대(1분): 단발 번갈음 + TRANSIT_GATE → 적용
      NO  — FAB(30분 연속) 중 휴게실 1분: FAB 연속 구간 건드리지 않음
    
    Args:
        df_worker: 작업자 단위 DataFrame
        window_size: 번갈음 패턴 감지 윈도우 크기 (기본 10분)
    
    Returns:
        IS_CORRECTED, CORRECTED_PLACE, STATE_DETAIL 업데이트된 DataFrame
    """
    from src.utils.constants import (
        SPACE_FUNCTION_PRIORITY, TRANSIT_ONLY_FUNCTIONS,
        PRIORITY_MAX_RUN_MIN, PRIORITY_CONTINUOUS_THRESHOLD
    )

    if len(df_worker) < 3:
        return df_worker

    df = df_worker.copy()
    sf_col = ProcessedColumns.SPACE_FUNCTION
    cp_col = ProcessedColumns.CORRECTED_PLACE
    sd_col = ProcessedColumns.STATE_DETAIL

    if sf_col not in df.columns:
        return df

    places = df[cp_col].fillna("").astype(str).values
    sf_vals = df[sf_col].fillna(SpaceFunction.UNKNOWN).values
    n = len(df)

    # ── 사전 계산: 각 행의 연속 구간 길이 ─────────────────────────────────
    run_lengths = _calc_run_lengths(places)

    absorbed_count = 0

    for i in range(n):
        # ── 조건 1: 현재 행이 짧은 연속 구간에 속할 때만 처리 ──────────────
        if run_lengths[i] > PRIORITY_MAX_RUN_MIN:
            continue  # 긴 연속 구간 → 건드리지 않음

        # ── 윈도우 내 장소 목록 ─────────────────────────────────────────────
        start = max(0, i - window_size // 2)
        end = min(n, i + window_size // 2 + 1)
        window_places = places[start:end]
        window_runs = run_lengths[start:end]

        # ── 조건 2: 윈도우 내 고유 장소가 정확히 2개 ─────────────────────────
        if len(set(window_places)) != 2:
            continue

        unique_in_window = list(dict.fromkeys(window_places))
        place_a, place_b = unique_in_window[0], unique_in_window[1]

        # ── 조건 3: 윈도우 내에 긴 연속 구간이 없어야 함 ─────────────────────
        max_run_in_window = int(max(window_runs))
        if max_run_in_window >= PRIORITY_CONTINUOUS_THRESHOLD:
            continue  # 긴 연속 구간 포함 → 번갈음 보정 안 함

        # ── 조건 4: 번갈음 패턴 확인 (A-B-A 또는 B-A-B) ──────────────────────
        is_alternating = any(
            window_places[j-1] != window_places[j] and
            window_places[j] != window_places[j+1] and
            window_places[j-1] == window_places[j+1]
            for j in range(1, len(window_places) - 1)
        )
        if not is_alternating:
            continue

        # ── 우선순위 결정 ────────────────────────────────────────────────────
        def get_sf(place_name):
            idxs = [k for k in range(n) if places[k] == place_name]
            return sf_vals[idxs[0]] if idxs else SpaceFunction.UNKNOWN

        sf_a = get_sf(place_a)
        sf_b = get_sf(place_b)
        prio_a = SPACE_FUNCTION_PRIORITY.get(sf_a, 9)
        prio_b = SPACE_FUNCTION_PRIORITY.get(sf_b, 9)

        if prio_a == prio_b:
            continue

        # ── 조건 5: TRANSIT 전용 공간이 포함된 경우, 또는 우선순위 차이 ──────
        loser_sf = sf_b if prio_a < prio_b else sf_a
        is_transit_involved = loser_sf in TRANSIT_ONLY_FUNCTIONS

        # REST vs WORK (차이=1)도 적용, TRANSIT_GATE는 항상 적용
        if not is_transit_involved and abs(prio_a - prio_b) < 1:
            continue

        winner_place = place_a if prio_a < prio_b else place_b
        winner_sf = sf_a if prio_a < prio_b else sf_b

        # ── 현재 행이 loser 장소이면 winner로 교체 ─────────────────────────
        if places[i] != winner_place:
            idx = df.index[i]
            # 이미 absorbed_by_priority로 처리된 행은 스킵
            current_sd = df.at[idx, sd_col] if sd_col in df.columns else ""
            if current_sd == "absorbed_by_priority":
                continue
            df.at[idx, cp_col] = winner_place
            df.at[idx, sf_col] = winner_sf
            df.at[idx, sd_col] = "absorbed_by_priority"
            df.at[idx, ProcessedColumns.IS_CORRECTED] = True
            absorbed_count += 1

    logger.debug(f"v5.4 번갈음 패턴 보정: absorbed_by_priority {absorbed_count}행")

    return df


def _interpret_sequence_context(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    연속된 위치 기록 시퀀스를 공간·시간·이동 맥락으로 통합 해석.
    
    4개 판정 규칙 (우선순위 순):
      1. 단독 의심 체류 (Solo Suspicious Dwell)
      2. 시퀀스 다양성 (Sequence Variety)
      3. 앵커 복귀 (Anchor Return)
      4. 시간 맥락 (Temporal Context)
    
    Pipeline 위치: Step 5-post2 (장소 보정 완료 후, 상태 분류 전)
    """
    if len(df_worker) < 2:
        return df_worker
    
    df = df_worker.copy()
    
    # 필요 컬럼 추출
    places = df[ProcessedColumns.CORRECTED_PLACE].fillna("").astype(str).values
    
    sf_col = ProcessedColumns.SPACE_FUNCTION
    if sf_col in df.columns:
        sf_vals = df[sf_col].fillna(SpaceFunction.UNKNOWN).values
    else:
        sf_vals = np.array([SpaceFunction.UNKNOWN] * len(df))
    
    hour_col = ProcessedColumns.HOUR
    if hour_col in df.columns:
        hours = df[hour_col].fillna(12).astype(int).values
    else:
        hours = np.array([12] * len(df))
    
    n = len(df)
    
    # state_detail 초기화
    if ProcessedColumns.STATE_DETAIL not in df.columns:
        df[ProcessedColumns.STATE_DETAIL] = ""
    
    # 판정 결과 배열: None이면 기존 판정 유지
    interpretations = [None] * n
    
    # ── 규칙 1: 단독 의심 체류 (Solo Suspicious Dwell) ────────────────────
    # 특정 장소가 transit_tolerance 이하로만 등장하고 앵커가 아니면
    # → 이동 중 순간 태깅으로 판정
    
    place_runs = _get_runs(places)  # (start_idx, end_idx, place, length)
    
    for run_start, run_end, place, run_len in place_runs:
        sf = sf_vals[run_start] if run_start < len(sf_vals) else SpaceFunction.UNKNOWN
        profile = SPACE_DWELL_PROFILE.get(sf, SPACE_DWELL_PROFILE.get(SpaceFunction.UNKNOWN, {}))
        
        transit_tol = profile.get("transit_tolerance", 2)
        is_anchor = profile.get("is_anchor", False)
        
        if run_len <= transit_tol and not is_anchor:
            # 이 장소가 transit_tolerance 이하로만 등장 → 이동 중 순간 태깅
            for i in range(run_start, run_end + 1):
                interpretations[i] = "transit_passing"
    
    # ── 규칙 2: 시퀀스 다양성 (Sequence Variety) ──────────────────────────
    # 슬라이딩 윈도우 내 장소 종류가 SEQUENCE_VARIETY_THRESH 이상이면
    # 그 구간은 이동으로 판정
    # 단, 앵커 공간이 윈도우의 절반 이상을 차지하면 이동 판정 억제
    
    for win_start in range(n - SEQUENCE_WINDOW_MIN + 1):
        win_end = win_start + SEQUENCE_WINDOW_MIN
        win_places = places[win_start:win_end]
        win_sf = sf_vals[win_start:win_end]
        
        unique_places = len(set(win_places))
        
        # 앵커 공간 점유율 계산
        anchor_count = sum(
            1 for sf in win_sf
            if SPACE_DWELL_PROFILE.get(sf, {}).get("is_anchor", False)
        )
        anchor_ratio = anchor_count / SEQUENCE_WINDOW_MIN
        
        if unique_places >= SEQUENCE_VARIETY_THRESH and anchor_ratio < 0.5:
            # 이 윈도우는 이동 구간 → 아직 미판정인 행들만 transit_sequence로 마킹
            for i in range(win_start, win_end):
                if interpretations[i] is None:
                    interpretations[i] = "transit_sequence"
    
    # ── 규칙 3: 앵커 복귀 (Anchor Return) ─────────────────────────────────
    # [REST → X → REST] 패턴에서 X가 짧으면 → REST 체류 유지
    
    for i in range(1, n - 1):
        if interpretations[i] is not None:
            continue
        
        curr_sf = sf_vals[i]
        prev_sf = sf_vals[i - 1]
        next_sf = sf_vals[i + 1] if i + 1 < n else None
        
        if next_sf is None:
            continue
        
        curr_run = _get_run_length_at(places, i)
        
        # 앞뒤가 앵커이고 현재가 짧은 비앵커 공간이면 → 앵커로 흡수
        prev_is_anchor = SPACE_DWELL_PROFILE.get(prev_sf, {}).get("is_anchor", False)
        next_is_anchor = SPACE_DWELL_PROFILE.get(next_sf, {}).get("is_anchor", False)
        curr_transit_tol = SPACE_DWELL_PROFILE.get(curr_sf, {}).get("transit_tolerance", 2)
        
        both_sides_anchor = prev_is_anchor and next_is_anchor
        curr_suspicious = curr_run <= curr_transit_tol
        
        if both_sides_anchor and curr_suspicious:
            interpretations[i] = "rest_absorbed"
            # CORRECTED_PLACE를 앞 앵커 장소로 교체
            df.at[df.index[i], ProcessedColumns.CORRECTED_PLACE] = places[i - 1]
            df.at[df.index[i], ProcessedColumns.SPACE_FUNCTION] = prev_sf
            df.at[df.index[i], ProcessedColumns.IS_CORRECTED] = True
    
    # ── 규칙 4: 시간 맥락 보정 (Temporal Context) ─────────────────────────
    # 점심 직후(13:00~14:00) + REST 앵커 근처 → 더 강하게 REST로 유지
    # 출근 직후(07:00~08:00) + RACK 다음 → 이동 우선 해석
    
    for i in range(n):
        if interpretations[i] is not None:
            continue
        
        sf = sf_vals[i]
        hour = int(hours[i])
        
        # 점심 직후 REST 강화
        if sf == SpaceFunction.REST and 13 <= hour < 14:
            interpretations[i] = "rest_postlunch"
        
        # 출근 직후 (RACK에서 이동하는 중)
        if sf in (SpaceFunction.WORK, SpaceFunction.WORK_HAZARD) and hour == 7:
            run_len = _get_run_length_at(places, i)
            if run_len <= 5:
                # 출근 직후 짧은 WORK 체류 → 이동 중 태깅
                interpretations[i] = "transit_arrival"
    
    # ── 판정 결과를 state_detail 컬럼에 반영 ──────────────────────────────
    TRANSIT_INTERPRETATIONS = {
        "transit_passing",
        "transit_sequence",
        "transit_arrival",
    }
    
    for i, interp in enumerate(interpretations):
        if interp is None:
            continue
        
        idx = df.index[i]
        
        if interp in TRANSIT_INTERPRETATIONS:
            df.at[idx, ProcessedColumns.STATE_DETAIL] = interp
            # PERIOD_TYPE은 "work"로 유지 (이동도 현장 투입 시간)
        
        elif interp == "rest_absorbed":
            df.at[idx, ProcessedColumns.STATE_DETAIL] = "rest_facility"
            if ProcessedColumns.PERIOD_TYPE in df.columns:
                df.at[idx, ProcessedColumns.PERIOD_TYPE] = "rest"
        
        elif interp == "rest_postlunch":
            df.at[idx, ProcessedColumns.STATE_DETAIL] = "rest_facility"
            if ProcessedColumns.PERIOD_TYPE in df.columns:
                df.at[idx, ProcessedColumns.PERIOD_TYPE] = "rest"
    
    return df


def _tag_transition_travel(df: pd.DataFrame) -> pd.DataFrame:
    """
    장소 전환 시 이동 도착 시간을 transit_arrival로 태깅.
    
    흔적 없는 순간 전환(abrupt transition)을 감지하여
    새 장소 도착 초기 N분을 state_detail='transit_arrival'로 마킹.
    
    핵심 설계:
      1. 흔적 있는 이동 스킵: [이동] 접두사, TRANSIT_GATE, 기존 transit
      2. 이동 규모 판단: LOCATION_KEY 변화 여부 + 출발지 유형
      3. 태깅 상한: 체류 시간의 50%까지만 (TRANSITION_MAX_RATIO)
    
    Pipeline 위치: Step 6-post (_classify_activity_period 말미)
    """
    if df.empty:
        return df
    
    result = df.copy()
    
    for wk in result[ProcessedColumns.WORKER_KEY].unique():
        wk_mask = result[ProcessedColumns.WORKER_KEY] == wk
        wk_df = result[wk_mask].copy()
        wk_indices = wk_df.index.tolist()
        
        if len(wk_indices) < 2:
            continue
        
        # 장소/위치키 배열 추출
        places = wk_df[ProcessedColumns.CORRECTED_PLACE].fillna("").astype(str).values
        loc_keys = wk_df[ProcessedColumns.LOCATION_KEY].fillna("").astype(str).values
        
        sf_col = ProcessedColumns.SPACE_FUNCTION
        if sf_col in wk_df.columns:
            sf_vals = wk_df[sf_col].fillna(SpaceFunction.UNKNOWN).values
        else:
            sf_vals = np.array([SpaceFunction.UNKNOWN] * len(wk_df))
        
        state_details = wk_df[ProcessedColumns.STATE_DETAIL].fillna("").astype(str).values
        
        # 이미 처리된 transit 행 마스크
        already_transit = np.array([
            (sd.startswith("transit") if sd else False) or 
            (places[i].startswith("[이동]") if places[i] else False) or
            (sf_vals[i] == SpaceFunction.TRANSIT_GATE)
            for i, sd in enumerate(state_details)
        ])
        
        # 연속 체류 시간 계산 (태깅 상한용)
        dwell_counts = _calc_consecutive_dwell(places)
        
        i = 0
        while i < len(wk_indices) - 1:
            curr_place = places[i + 1]
            prev_place = places[i]
            curr_loc_key = loc_keys[i + 1]
            prev_loc_key = loc_keys[i]
            prev_sf = sf_vals[i]
            curr_sf = sf_vals[i + 1]
            
            # 장소 전환 감지
            place_changed = (curr_place != prev_place) and curr_place
            
            # 이미 transit으로 표시된 전환이면 스킵
            already_handled = already_transit[i + 1]
            
            # 목적지가 제외 대상이면 스킵
            dest_excluded = curr_sf in TRANSITION_TRAVEL_EXCLUDE_DEST
            
            if place_changed and not already_handled and not dest_excluded:
                # 이동 시간 추정
                travel_mins = _estimate_travel_mins(prev_sf, prev_loc_key, curr_loc_key)
                
                # 태깅 상한 적용: 체류 시간의 50%까지만
                dwell_at_new_place = dwell_counts[i + 1]
                max_tag = max(1, int(dwell_at_new_place * TRANSITION_MAX_RATIO))
                travel_mins = min(travel_mins, max_tag)
                
                # i+1 ~ i+travel_mins 행을 transit_arrival로 태깅
                tag_end = min(i + 1 + travel_mins, len(wk_indices))
                for j in range(i + 1, tag_end):
                    # 같은 장소 연속 체류 중에만 태깅 (다음 전환 전까지)
                    if places[j] == curr_place and not already_transit[j]:
                        idx = wk_indices[j]
                        result.at[idx, ProcessedColumns.STATE_DETAIL] = "transit_arrival"
                
                # 태깅한 구간만큼 건너뜀
                i += travel_mins
            else:
                i += 1
    
    return result


def _state_detail_to_period_type(state_detail: str) -> str:
    """state_detail → PERIOD_TYPE 상위 그룹 매핑."""
    if state_detail in ("high_work", "low_work", "standby", "transit_slow", "transit_queue", "transit_idle"):
        return "work"
    if state_detail in ("transit", "gate_congestion", "corridor_block", "transit_arrival"):
        return "work"  # 이동도 현장 투입 시간
    if state_detail == "rest_facility":
        return "rest"
    if state_detail in ("off_duty", "abnormal_stop"):
        return "off"
    return "off"


def _get_consecutive_groups(mask: pd.Series) -> list[tuple[int, int]]:
    """
    boolean Series에서 True인 연속 구간의 (시작 인덱스, 끝 인덱스) 목록 반환.

    Args:
        mask: boolean Series

    Returns:
        (시작 위치 인덱스, 끝 위치 인덱스) 튜플 목록
    """
    groups = []
    in_group = False
    start = 0

    mask_list = mask.tolist()
    for i, val in enumerate(mask_list):
        if val and not in_group:
            in_group = True
            start = i
        elif not val and in_group:
            in_group = False
            groups.append((start, i - 1))
    if in_group:
        groups.append((start, len(mask_list) - 1))

    return groups


# ═══════════════════════════════════════════════════════════════════════════
# ★ v5.3: 체류시간 집계 유틸리티 (UI에서 사용)
# ═══════════════════════════════════════════════════════════════════════════

def calc_place_dwell_stats(df_worker: pd.DataFrame, place: str) -> dict:
    """
    특정 장소의 체류 통계 계산.
    
    Args:
        df_worker: 작업자 단위 DataFrame
        place: 장소명 (CORRECTED_PLACE 기준)
    
    Returns:
        {
            "total_min": 총 체류시간 (분),
            "max_run": 최장 연속 체류 (분),
            "visit_count": 방문 횟수,
            "runs": [각 연속 체류 시간 목록]
        }
    """
    cp_col = ProcessedColumns.CORRECTED_PLACE
    
    place_rows = df_worker[df_worker[cp_col] == place].copy()
    if place_rows.empty:
        return {"total_min": 0, "max_run": 0, "visit_count": 0, "runs": []}
    
    # 인덱스 기반 연속 구간 탐지
    indices = place_rows.index.tolist()
    all_indices = df_worker.index.tolist()
    pos_map = {idx: pos for pos, idx in enumerate(all_indices)}
    
    runs = []
    current_run = [indices[0]]
    for i in range(1, len(indices)):
        prev_pos = pos_map[indices[i-1]]
        curr_pos = pos_map[indices[i]]
        if curr_pos - prev_pos == 1:
            current_run.append(indices[i])
        else:
            runs.append(len(current_run))
            current_run = [indices[i]]
    runs.append(len(current_run))
    
    return {
        "total_min": sum(runs),
        "max_run": max(runs) if runs else 0,
        "visit_count": len(runs),
        "runs": runs,
    }


def calc_all_place_dwell_summary(df_worker: pd.DataFrame) -> pd.DataFrame:
    """
    작업자의 모든 장소별 체류 통계 요약.
    
    Returns:
        DataFrame with columns: 장소, 총체류(분), 최장연속(분), 방문횟수
    """
    cp_col = ProcessedColumns.CORRECTED_PLACE
    
    places = df_worker[cp_col].dropna().unique()
    summary_data = []
    
    for place in places:
        stats = calc_place_dwell_stats(df_worker, place)
        summary_data.append({
            "장소": place,
            "총체류(분)": stats["total_min"],
            "최장연속(분)": stats["max_run"],
            "방문횟수": stats["visit_count"],
        })
    
    return pd.DataFrame(summary_data).sort_values("총체류(분)", ascending=False)
