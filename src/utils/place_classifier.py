"""
장소 분류 유틸리티.
장소명 문자열을 기반으로 유형을 분류하고, 실내/실외를 구분한다.
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from src.utils.constants import (
    HELMET_RACK_KEYWORDS,
    REST_AREA_KEYWORDS,
    OFFICE_KEYWORDS,
    GATE_KEYWORDS,
    SpaceFunction,
    SPACE_KEYWORDS,
    SPACE_KEYWORD_PRIORITY,
    SSMP_ZONE_TYPE_MAPPING,
    HAZARD_WEIGHT_DEFAULT,
    HAZARD_WEIGHT_BY_RISK_LEVEL,
)

logger = logging.getLogger(__name__)


def classify_place(
    place: Optional[str],
    building: Optional[str] = None,
    floor: Optional[str] = None,
) -> str:
    """
    장소명, 건물, 층 정보를 기반으로 장소 유형을 분류.

    Args:
        place: 장소명
        building: 건물명 (없으면 실외)
        floor: 층 정보 (없으면 실외)

    Returns:
        장소 유형 문자열:
        - "HELMET_RACK": 헬멧 걸이대
        - "REST": 휴게 구역
        - "OFFICE": 사무실
        - "GATE": 출입구/게이트
        - "INDOOR": 실내 작업 구역
        - "OUTDOOR": 실외 작업 구역
        - "UNKNOWN": 분류 불가
    """
    if not place:
        return "UNKNOWN"

    place_str = str(place)

    # 헬멧 걸이대 판별 (최우선)
    for kw in HELMET_RACK_KEYWORDS:
        if kw in place_str:
            return "HELMET_RACK"

    # 휴게 구역
    for kw in REST_AREA_KEYWORDS:
        if kw in place_str:
            return "REST"

    # 사무실
    for kw in OFFICE_KEYWORDS:
        if kw in place_str:
            return "OFFICE"

    # 게이트/출입구
    for kw in GATE_KEYWORDS:
        if kw in place_str:
            return "GATE"

    # 실내/실외 구분 (건물+층 정보 기반)
    has_building = bool(building and str(building).strip())
    has_floor = bool(floor and str(floor).strip())

    if has_building and has_floor:
        return "INDOOR"
    else:
        return "OUTDOOR"


def classify_space_type(
    building: Optional[str],
    floor: Optional[str],
) -> str:
    """
    건물, 층 정보를 기반으로 공간 유형(실내/실외) 분류.

    Args:
        building: 건물명
        floor: 층 정보

    Returns:
        "INDOOR" 또는 "OUTDOOR"
    """
    has_building = bool(building and str(building).strip())
    has_floor = bool(floor and str(floor).strip())
    return "INDOOR" if (has_building and has_floor) else "OUTDOOR"


def make_location_key(
    building: Optional[str],
    floor: Optional[str],
) -> str:
    """
    건물+층 조합으로 위치 키 생성.
    실내/실외 좌표계 구분에 사용.

    Args:
        building: 건물명
        floor: 층 정보

    Returns:
        위치 키 문자열 (예: "WWT_1F", "OUTDOOR")
    """
    has_building = bool(building and str(building).strip())
    has_floor = bool(floor and str(floor).strip())

    if has_building and has_floor:
        b = str(building).strip().replace(" ", "_")
        f = str(floor).strip().replace(" ", "_")
        return f"{b}_{f}"
    return "OUTDOOR"


def is_helmet_rack(place: Optional[str]) -> bool:
    """
    헬멧 걸이대 장소 여부 확인.

    Args:
        place: 장소명

    Returns:
        헬멧 걸이대이면 True
    """
    if not place:
        return False
    place_str = str(place)
    return any(kw in place_str for kw in HELMET_RACK_KEYWORDS)


# ═══════════════════════════════════════════════════════════════════════════
# Space-Aware Journey Interpretation (v4) 함수
# ═══════════════════════════════════════════════════════════════════════════

def classify_space_function(
    place: Optional[str],
    zone_type: Optional[str] = None,
    building: Optional[str] = None,
    floor: Optional[str] = None,
) -> str:
    """
    장소명과 SSMP zone_type을 기반으로 공간 기능(space_function) 분류.

    키워드 우선순위: RACK > GATE > REST > HAZARD > CORRIDOR > TRANSIT_WORK > WORK
    SSMP zone_type이 있으면 최종 권위로 덮어씀.

    Args:
        place: 장소명
        zone_type: SSMP zone_type (선택)
        building: 건물명 (선택, 실내/실외 판단용)
        floor: 층 정보 (선택, 실내/실외 판단용)

    Returns:
        SpaceFunction 상수 (WORK, WORK_HAZARD, TRANSIT_GATE, REST 등)
    """
    if not place:
        return SpaceFunction.UNKNOWN

    place_str = str(place).upper()
    matched_sf: Optional[str] = None

    # 1. 키워드 우선순위 순서대로 매칭
    for sf in SPACE_KEYWORD_PRIORITY:
        keywords = SPACE_KEYWORDS.get(sf, [])
        for kw in keywords:
            if kw.upper() in place_str:
                matched_sf = sf
                break
        if matched_sf:
            break

    # 2. 키워드 매칭 실패 시 실내/실외 기반 폴백
    if not matched_sf:
        has_building = bool(building and str(building).strip())
        has_floor = bool(floor and str(floor).strip())
        if has_building and has_floor:
            matched_sf = SpaceFunction.WORK
        else:
            matched_sf = SpaceFunction.OUTDOOR_MISC

    # 3. SSMP zone_type이 있으면 덮어씀 (최종 권위)
    if zone_type:
        zone_type_lower = str(zone_type).lower().strip()
        if zone_type_lower in SSMP_ZONE_TYPE_MAPPING:
            ssmp_sf = SSMP_ZONE_TYPE_MAPPING[zone_type_lower]
            if ssmp_sf != SpaceFunction.UNKNOWN:
                matched_sf = ssmp_sf

    return matched_sf or SpaceFunction.UNKNOWN


def get_hazard_weight(
    space_function: str,
    risk_level: Optional[str] = None,
) -> float:
    """
    공간 기능과 SSMP risk_level을 기반으로 위험 가중치 반환.

    Args:
        space_function: 공간 기능 (SpaceFunction 상수)
        risk_level: SSMP risk_level (LOW/MEDIUM/HIGH/CRITICAL, 선택)

    Returns:
        0.0 ~ 1.0 사이의 위험 가중치
    """
    base_weight = HAZARD_WEIGHT_DEFAULT.get(space_function, 0.3)

    # WORK_HAZARD는 항상 1.0 고정
    if space_function == SpaceFunction.WORK_HAZARD:
        return 1.0

    # REST/RACK는 항상 0.0 고정
    if space_function in (SpaceFunction.REST, SpaceFunction.RACK):
        return 0.0

    # risk_level이 있으면 해당 값으로 대체
    if risk_level:
        level_upper = str(risk_level).upper().strip()
        if level_upper in HAZARD_WEIGHT_BY_RISK_LEVEL:
            return HAZARD_WEIGHT_BY_RISK_LEVEL[level_upper]

    return base_weight


def classify_state_by_space(
    space_function: str,
    active_ratio: float,
    hour: int,
    dwell_min: int = 0,
) -> str:
    """
    공간 기능 × 활성비율 행렬 기반 상태 분류.

    state_override 공간(REST, RACK, TRANSIT_GATE, TRANSIT_CORRIDOR)은
    active_ratio와 무관하게 상태가 결정됨.

    Args:
        space_function: 공간 기능
        active_ratio: 활성비율 (0.0~1.0)
        hour: 시간 (0~23)
        dwell_min: 해당 공간 체류시간 (분)

    Returns:
        state_detail 문자열:
        - high_work, low_work, standby (WORK 계열)
        - transit, transit_queue, transit_slow, transit_idle
        - rest_facility
        - off_duty, abnormal_stop
    """
    from src.utils.constants import (
        WORK_INTENSITY_HIGH_THRESHOLD,
        WORK_INTENSITY_LOW_THRESHOLD,
        ACTIVE_RATIO_ZERO_THRESHOLD,
        WORK_HOURS_START,
        WORK_HOURS_END,
        DWELL_NORMAL_MAX,
        ABNORMAL_STOP_THRESHOLD,
    )

    # state_override 공간: active_ratio 무관
    if space_function == SpaceFunction.REST:
        return "rest_facility"

    if space_function == SpaceFunction.RACK:
        return "off_duty"

    if space_function == SpaceFunction.TRANSIT_GATE:
        if active_ratio < ACTIVE_RATIO_ZERO_THRESHOLD:
            return "gate_congestion" if dwell_min >= DWELL_NORMAL_MAX.get(SpaceFunction.TRANSIT_GATE, 5) else "transit_queue"
        return "transit"

    if space_function == SpaceFunction.TRANSIT_CORRIDOR:
        if active_ratio < ACTIVE_RATIO_ZERO_THRESHOLD:
            return "corridor_block" if dwell_min >= DWELL_NORMAL_MAX.get(SpaceFunction.TRANSIT_CORRIDOR, 10) else "transit_slow"
        return "transit"

    # 근무시간 외
    if hour < WORK_HOURS_START or hour >= WORK_HOURS_END:
        return "off_duty"

    # active_ratio 기반 분류 (WORK, WORK_HAZARD, TRANSIT_WORK, OUTDOOR_MISC)
    if active_ratio >= WORK_INTENSITY_HIGH_THRESHOLD:
        base = "high_work"
    elif active_ratio >= WORK_INTENSITY_LOW_THRESHOLD:
        base = "low_work"
    elif active_ratio >= ACTIVE_RATIO_ZERO_THRESHOLD:
        base = "standby"
    else:
        base = "off_duty"

    # TRANSIT_WORK / OUTDOOR_MISC: 낮은 활성비율은 이동 관련 상태로 전환
    if space_function in (SpaceFunction.TRANSIT_WORK, SpaceFunction.OUTDOOR_MISC):
        if base == "standby":
            base = "transit_slow"
        elif base == "off_duty":
            base = "transit_idle"

    # WORK_HAZARD: 장시간 비활성 → abnormal_stop
    if space_function == SpaceFunction.WORK_HAZARD:
        threshold = ABNORMAL_STOP_THRESHOLD.get(SpaceFunction.WORK_HAZARD, 5)
        if base == "off_duty" or (base == "standby" and dwell_min >= threshold):
            base = "abnormal_stop"

    return base


def add_place_columns(df: pd.DataFrame, spatial_ctx=None) -> pd.DataFrame:
    """
    DataFrame에 장소 분류 관련 컬럼을 일괄 추가.

    SpatialContext가 제공되면 SSMP 기반 분류를 우선 적용하고,
    없거나 매칭 실패 시 키워드 매칭으로 폴백한다.

    추가 컬럼:
      PLACE_TYPE    : SSMP 기반 또는 키워드 기반 장소 유형
      SPACE_TYPE    : INDOOR / OUTDOOR
      LOCATION_KEY  : 좌표계 구분 위치 키
      IS_HELMET_RACK: 헬멧 걸이대 여부
      SSMP_MATCHED  : SSMP에서 매칭됐으면 True (spatial_ctx 없으면 항상 False)
      SPACE_FUNCTION: 공간 기능 (WORK/WORK_HAZARD/TRANSIT_GATE/REST 등) ★ v4 신규
      HAZARD_WEIGHT : 공간 위험 가중치 (0.0~1.0) ★ v4 신규

    Args:
        df: 원본 DataFrame (건물, 층, 장소 컬럼 포함)
        spatial_ctx: SpatialContext 인스턴스 (선택, 없으면 키워드 매칭만 사용)

    Returns:
        장소 분류 컬럼이 추가된 DataFrame
    """
    from src.data.schema import RawColumns, ProcessedColumns

    result = df.copy()

    if spatial_ctx is not None:
        # SSMP 기반 분류 (place = 보정 전 원본 장소)
        result[ProcessedColumns.PLACE_TYPE] = result.apply(
            lambda row: spatial_ctx.classify_place(
                row.get(RawColumns.PLACE),
                row.get(RawColumns.BUILDING),
                row.get(RawColumns.FLOOR),
            ),
            axis=1,
        )
        result[ProcessedColumns.LOCATION_KEY] = result.apply(
            lambda row: spatial_ctx.get_location_key(
                place_name=row.get(RawColumns.PLACE),
                building=row.get(RawColumns.BUILDING),
                floor=row.get(RawColumns.FLOOR),
            ),
            axis=1,
        )
        result["ssmp_matched"] = result[RawColumns.PLACE].apply(
            lambda p: spatial_ctx.is_ssmp_matched(p) if p else False
        )

        # space_function with zone_type (SSMP 지원)
        def _get_space_func_ssmp(row):
            zone_type = None
            if spatial_ctx:
                zone_type = spatial_ctx.get_zone_type(row.get(RawColumns.PLACE))
            return classify_space_function(
                row.get(RawColumns.PLACE),
                zone_type=zone_type,
                building=row.get(RawColumns.BUILDING),
                floor=row.get(RawColumns.FLOOR),
            )

        result[ProcessedColumns.SPACE_FUNCTION] = result.apply(_get_space_func_ssmp, axis=1)

        # hazard_weight with risk_level (SSMP 지원)
        def _get_hazard_ssmp(row):
            risk_level = None
            if spatial_ctx:
                risk_level = spatial_ctx.get_risk_level(row.get(RawColumns.PLACE))
            return get_hazard_weight(
                row[ProcessedColumns.SPACE_FUNCTION],
                risk_level=risk_level,
            )

        result[ProcessedColumns.HAZARD_WEIGHT] = result.apply(_get_hazard_ssmp, axis=1)
    else:
        # 키워드 매칭 폴백
        result[ProcessedColumns.PLACE_TYPE] = result.apply(
            lambda row: classify_place(
                row.get(RawColumns.PLACE),
                row.get(RawColumns.BUILDING),
                row.get(RawColumns.FLOOR),
            ),
            axis=1,
        )
        result[ProcessedColumns.LOCATION_KEY] = result.apply(
            lambda row: make_location_key(
                row.get(RawColumns.BUILDING),
                row.get(RawColumns.FLOOR),
            ),
            axis=1,
        )
        result["ssmp_matched"] = False

        # space_function (키워드만)
        result[ProcessedColumns.SPACE_FUNCTION] = result.apply(
            lambda row: classify_space_function(
                row.get(RawColumns.PLACE),
                zone_type=None,
                building=row.get(RawColumns.BUILDING),
                floor=row.get(RawColumns.FLOOR),
            ),
            axis=1,
        )

        # hazard_weight (기본값)
        result[ProcessedColumns.HAZARD_WEIGHT] = result[ProcessedColumns.SPACE_FUNCTION].apply(
            lambda sf: get_hazard_weight(sf, risk_level=None)
        )

    result[ProcessedColumns.SPACE_TYPE] = result.apply(
        lambda row: classify_space_type(
            row.get(RawColumns.BUILDING),
            row.get(RawColumns.FLOOR),
        ),
        axis=1,
    )

    result[ProcessedColumns.IS_HELMET_RACK] = result[RawColumns.PLACE].apply(
        is_helmet_rack
    )

    return result


# ─── 블록 활동 상태 분류 (Gantt 차트용) ───────────────────────────────────

def classify_block_activity(place_type: str, avg_ratio: float, hour: int) -> str:
    """
    블록의 평균 활성비율, 장소유형, 시간대로 6카테고리 활동 상태 결정.
    
    Args:
        place_type: 장소유형 (HELMET_RACK, REST, GATE, INDOOR 등)
        avg_ratio: 평균 활성비율 (0.0 ~ 1.0)
        hour: 시간대 (0~23)
        
    Returns:
        활동 상태: high_work, low_work, standby, transit, rest_facility, off_duty
    """
    from src.utils.constants import (
        WORK_HOURS_START, WORK_HOURS_END,
        WORK_INTENSITY_HIGH_THRESHOLD, WORK_INTENSITY_LOW_THRESHOLD,
        ACTIVE_RATIO_ZERO_THRESHOLD,
    )

    if place_type in ("HELMET_RACK", "RACK"):
        return "off_duty"
    if place_type in ("REST", "REST_FACILITY"):
        return "rest"
    if place_type in ("GATE", "TRANSIT_GATE", "TRANSIT_CORRIDOR"):
        return "transit"
    if hour < WORK_HOURS_START or hour >= WORK_HOURS_END:
        return "off_duty"
    if avg_ratio >= WORK_INTENSITY_HIGH_THRESHOLD:
        return "high_work"
    if avg_ratio >= WORK_INTENSITY_LOW_THRESHOLD:
        return "low_work"
    if avg_ratio >= ACTIVE_RATIO_ZERO_THRESHOLD:
        return "standby"
    return "off_duty"
