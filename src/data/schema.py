"""
데이터 스키마 정의.
Raw CSV 컬럼 스키마, 전처리된 DataFrame 스키마, 캐시 스키마를 정의한다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
import pandas as pd


# ─── Raw CSV 컬럼 이름 정의 ─────────────────────────────────────────
class RawColumns:
    """Raw CSV 컬럼명 상수."""
    TIME = "시간(분)"
    WORKER = "작업자"
    ZONE = "구역"
    BUILDING = "건물"
    FLOOR = "층"
    PLACE = "장소"
    X = "X"
    Y = "Y"
    TAG = "태그"
    TAG_TYPE = "태그구분"
    COMPANY = "업체"
    EQUIPMENT = "장비"
    SIGNAL_COUNT = "신호갯수"
    ACTIVE_SIGNAL_COUNT = "활성신호갯수"


# ─── 전처리 후 추가되는 컬럼 정의 ──────────────────────────────────
class ProcessedColumns:
    """전처리 후 추가 컬럼명 상수."""
    DATE = "날짜"
    HOUR = "시"
    MINUTE = "분"
    ACTIVE_RATIO = "활성비율"            # 활성신호갯수 / 신호갯수
    PLACE_TYPE = "장소유형"              # HELMET_RACK, REST, OFFICE 등
    SPACE_TYPE = "공간유형"              # INDOOR, OUTDOOR
    LOCATION_KEY = "위치키"              # 건물+층 또는 'OUTDOOR'
    IS_HELMET_RACK = "헬멧거치여부"       # 헬멧 걸이대 여부
    IS_ACTIVE = "활동여부"               # 활성비율 기준 활동 중 여부
    PERIOD_TYPE = "시간대유형"           # work/rest/transit/off
    CORRECTED_PLACE = "보정장소"         # Journey 보정 후 장소
    CORRECTED_X = "보정X"
    CORRECTED_Y = "보정Y"
    IS_CORRECTED = "보정여부"            # 보정 적용 여부
    WORKER_KEY = "작업자키"              # 작업자이름_태그 조합
    COVERAGE_GAP = "coverage_gap"       # signal_count=0 플래그
    SIGNAL_CONFIDENCE = "signal_confidence"  # NONE/LOW/MED/HIGH

    # ─── DBSCAN 클러스터링 컬럼 ────────────────────────────────────────
    SPATIAL_CLUSTER = "SPATIAL_CLUSTER"  # DBSCAN 클러스터 ID
    CLUSTER_PLACE = "CLUSTER_PLACE"      # 클러스터 대표 장소

    # ─── Space-Aware Journey Interpretation 신규 컬럼 (v4) ────────────
    SPACE_FUNCTION = "space_function"   # WORK/WORK_HAZARD/TRANSIT_GATE/REST 등
    HAZARD_WEIGHT = "hazard_weight"     # 0.0~1.0 공간 위험 가중치
    STATE_DETAIL = "state_detail"       # transit_queue/transit_slow/standby 등
    DWELL_EXCEEDED = "dwell_exceeded"   # 정상 체류시간 초과 여부
    JOURNEY_PATTERN = "journey_pattern" # zone_fixed/zone_cycle/explorer (선독 결과)

    # ─── v6 4증거 통합 Journey 보정 컬럼 (2026-03-02) ────────────────────
    E1_ACTIVE_LEVEL = "e1_active_level"       # none/low/mid/high (활성신호 레벨)
    E4_LOCATION_STABLE = "e4_location_stable" # bool (위치 안정성)
    E4_RUN_LENGTH = "e4_run_length"           # int (현재 연속 구간 길이, 분)
    E_GHOST_CANDIDATE = "e_ghost_candidate"   # bool (Ghost Signal 후보)
    E_TRANSITION = "e_transition"             # bool (이동 중 후보)
    SEGMENT_TYPE = "segment_type"             # pre_work/work/lunch/post_work
    
    # ─── v6.1 Multi-Pass Refinement 컬럼 ────────────────────────────────
    # ANOMALY_FLAG: 이상치 유형 플래그
    #   - abnormal_stop: 비정상 정지 (v4)
    #   - gate_congestion: 출입구 혼잡 (v4)
    #   - lone_hazard: 단독 위험작업 (v4)
    #   - impossible_teleport: 불가능한 텔레포트 (v6.1)
    #   - impossible_building_jump: 불가능한 건물 간 이동 (v6.1)
    ANOMALY_FLAG = "anomaly_flag"


# ─── Raw CSV pandas dtype 매핑 ──────────────────────────────────────
RAW_CSV_DTYPES: dict = {
    RawColumns.WORKER:              "string",
    RawColumns.ZONE:                "string",
    RawColumns.BUILDING:            "string",
    RawColumns.FLOOR:               "string",
    RawColumns.PLACE:               "string",
    RawColumns.X:                   "float64",
    RawColumns.Y:                   "float64",
    RawColumns.TAG:                 "string",
    RawColumns.TAG_TYPE:            "Int64",
    RawColumns.COMPANY:             "string",
    RawColumns.EQUIPMENT:           "string",
    RawColumns.SIGNAL_COUNT:        "Int64",
    RawColumns.ACTIVE_SIGNAL_COUNT: "Int64",
}


# ─── 워커 정보 dataclass ────────────────────────────────────────────
@dataclass
class WorkerInfo:
    """작업자 식별 정보."""
    name: str
    tag_id: str
    company: str
    worker_key: str = field(init=False)

    def __post_init__(self) -> None:
        self.worker_key = f"{self.name}_{self.tag_id}"


# ─── Journey 레코드 dataclass ───────────────────────────────────────
@dataclass
class JourneyRecord:
    """단일 Journey 기록 (1분 단위)."""
    timestamp: pd.Timestamp
    worker: str
    tag_id: str
    company: str
    building: Optional[str]
    floor: Optional[str]
    place: str
    x: float
    y: float
    signal_count: int
    active_signal_count: int
    active_ratio: float = field(init=False)

    def __post_init__(self) -> None:
        if self.signal_count > 0:
            self.active_ratio = self.active_signal_count / self.signal_count
        else:
            self.active_ratio = 0.0


# ─── 캐시 파일 스키마 ────────────────────────────────────────────────
# processed_YYYYMMDD.parquet 주요 컬럼
CACHE_COLUMNS = [
    RawColumns.TIME,
    RawColumns.WORKER,
    RawColumns.ZONE,
    RawColumns.BUILDING,
    RawColumns.FLOOR,
    RawColumns.PLACE,
    RawColumns.X,
    RawColumns.Y,
    RawColumns.TAG,
    RawColumns.COMPANY,
    RawColumns.SIGNAL_COUNT,
    RawColumns.ACTIVE_SIGNAL_COUNT,
    ProcessedColumns.DATE,
    ProcessedColumns.HOUR,
    ProcessedColumns.ACTIVE_RATIO,
    ProcessedColumns.PLACE_TYPE,
    ProcessedColumns.SPACE_TYPE,
    ProcessedColumns.LOCATION_KEY,
    ProcessedColumns.IS_HELMET_RACK,
    ProcessedColumns.IS_ACTIVE,
    ProcessedColumns.PERIOD_TYPE,
    ProcessedColumns.CORRECTED_PLACE,
    ProcessedColumns.CORRECTED_X,
    ProcessedColumns.CORRECTED_Y,
    ProcessedColumns.IS_CORRECTED,
    ProcessedColumns.WORKER_KEY,
    ProcessedColumns.COVERAGE_GAP,
    ProcessedColumns.SIGNAL_CONFIDENCE,
    ProcessedColumns.SPATIAL_CLUSTER,
    ProcessedColumns.CLUSTER_PLACE,
    # v4 신규 컬럼 (Space-Aware Journey Interpretation)
    ProcessedColumns.SPACE_FUNCTION,
    ProcessedColumns.HAZARD_WEIGHT,
    ProcessedColumns.STATE_DETAIL,
    ProcessedColumns.ANOMALY_FLAG,
    ProcessedColumns.DWELL_EXCEEDED,
    ProcessedColumns.JOURNEY_PATTERN,
]


def validate_raw_df(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    Raw DataFrame의 스키마 유효성 검증.

    Args:
        df: 검증할 DataFrame

    Returns:
        (is_valid, error_messages) 튜플
    """
    errors = []
    required_cols = [
        RawColumns.TIME, RawColumns.WORKER, RawColumns.PLACE,
        RawColumns.TAG, RawColumns.COMPANY,
        RawColumns.SIGNAL_COUNT, RawColumns.ACTIVE_SIGNAL_COUNT,
    ]
    for col in required_cols:
        if col not in df.columns:
            errors.append(f"필수 컬럼 누락: '{col}'")

    if not errors:
        # 신호갯수 음수 체크
        neg_mask = df[RawColumns.SIGNAL_COUNT].fillna(0) < 0
        if neg_mask.any():
            errors.append(f"신호갯수 음수 값 존재: {neg_mask.sum()}건")

        # 활성신호갯수 > 신호갯수 체크
        invalid = (
            df[RawColumns.ACTIVE_SIGNAL_COUNT].fillna(0)
            > df[RawColumns.SIGNAL_COUNT].fillna(0)
        )
        if invalid.any():
            errors.append(f"활성신호갯수 > 신호갯수 이상 데이터: {invalid.sum()}건")

    return len(errors) == 0, errors
