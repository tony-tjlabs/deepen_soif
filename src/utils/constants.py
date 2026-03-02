"""
프로젝트 전역 상수 정의.
T-Ward 동작 특성, 시간대 분류, 장소 분류 기준 등을 정의한다.
"""

# ─── T-Ward 동작 상수 (도메인 참조용) ─────────────────────────────────
# T-Ward BLE 태그의 신호 특성. 활성비율 계산 근거.
# T_WARD_ACTIVE_INTERVAL_SEC = 10   # 활성 상태 BLE advertising 주기 (초)
# T_WARD_INACTIVE_INTERVAL_SEC = 60 # 비활성 상태 BLE advertising 주기 (초)
# T_WARD_ACTIVE_PER_MIN = 6         # 활성 상태 1분당 최대 advertising 횟수

# ─── 활성비율 임계값 ─────────────────────────────────────────────────
ACTIVE_RATIO_WORKING_THRESHOLD = 0.3  # 이 이상이면 활동 중으로 판단
ACTIVE_RATIO_ZERO_THRESHOLD = 0.05    # 이 이하면 비활동

# 작업 강도 임계값 (6-카테고리 시간 분류 기준)
WORK_INTENSITY_HIGH_THRESHOLD = 0.6   # 고활성 작업 기준 (이상: 이동, 운반 등 몸을 많이 쓰는 작업)
WORK_INTENSITY_LOW_THRESHOLD  = 0.15  # 저활성 작업 하한 (미만: 현장 대기로 분류)
# 0.6 이상       → 고활성 작업 (High-Intensity Work)
# 0.15 ~ 0.6    → 저활성 작업 (Low-Intensity Work)
# 0.15 미만      → 현장 대기 (On-site Standby)

# ─── 시간대 분류 ─────────────────────────────────────────────────────
TIME_PERIODS = {
    "새벽":      (0,  6),   # 00:00 ~ 05:59
    "오전출근":  (6,  8),   # 06:00 ~ 07:59
    "오전작업":  (8, 12),   # 08:00 ~ 11:59
    "점심":      (12, 13),  # 12:00 ~ 12:59
    "오후작업":  (13, 17),  # 13:00 ~ 16:59
    "퇴근":      (17, 20),  # 17:00 ~ 19:59
    "야간":      (20, 24),  # 20:00 ~ 23:59
}

# 작업 시간대 (생산성 계산 기준)
WORK_HOURS_START = 7    # 오전 7시
WORK_HOURS_END = 20     # 오후 8시

# 점심시간
LUNCH_START = 12
LUNCH_END = 13

# 야간/새벽 (헬멧 거치 감지 기준)
NIGHT_HOURS_START = 20
DAWN_HOURS_END = 6

# ─── 장소 분류 키워드 (레거시 — SPACE_KEYWORDS 권장) ─────────────────
HELMET_RACK_KEYWORDS = [
    "보호구 걸이대", "보호구걸이대", "헬멧 걸이", "안전모 걸이"
]
REST_AREA_KEYWORDS = [
    "휴게", "식당", "탈의실", "탈의", "화장실", "휴식", "로비"
]
OFFICE_KEYWORDS = [
    "사무소", "사무실", "현장사무소", "현장 사무", "관리동"
]
GATE_KEYWORDS = [
    "게이트", "출입구", "입구", "정문", "후문"
]

# ─── Space Function 상수 (공간 기능 분류) ─────────────────────────────
# 설계 문서: PROJECT_MEMORY.md 섹션 21-B 참조

class SpaceFunction:
    """공간 기능 분류 상수."""
    WORK = "WORK"                      # 실내 작업공간 (FAB, CUB, WWT 등)
    WORK_HAZARD = "WORK_HAZARD"        # 고위험 작업공간 (밀폐, 맨홀, 고소)
    TRANSIT_WORK = "TRANSIT_WORK"      # 실외 공사/이동+작업 혼재
    TRANSIT_GATE = "TRANSIT_GATE"      # 출입구/타각기
    TRANSIT_CORRIDOR = "TRANSIT_CORRIDOR"  # 건물 내 이동통로
    REST = "REST"                      # 휴게 시설
    RACK = "RACK"                      # 헬멧 거치대
    OUTDOOR_MISC = "OUTDOOR_MISC"      # 실외 기타
    UNKNOWN = "UNKNOWN"                # 미분류

# 키워드 매핑 (우선순위 순서: RACK > GATE > REST > HAZARD > CORRIDOR > TRANSIT_WORK > WORK)
SPACE_KEYWORDS: dict[str, list[str]] = {
    SpaceFunction.RACK: [
        "걸이대", "거치대", "보호구", "안전모 걸이", "헬멧 걸이",
    ],
    SpaceFunction.TRANSIT_GATE: [
        "게이트", "GATE", "타각기", "정문", "입구", "출구", "후문",
    ],
    SpaceFunction.REST: [
        "휴게", "식당", "탈의실", "탈의", "흡연", "화장실", "휴식",
    ],
    SpaceFunction.WORK_HAZARD: [
        "밀폐", "맨홀", "탱크", "고소", "중장비", "confined", "CONFINED",
    ],
    SpaceFunction.TRANSIT_CORRIDOR: [
        "통로", "복도", "계단", "엘리베이터", "EV홀", "EV", "로비",
    ],
    SpaceFunction.TRANSIT_WORK: [
        "공사현장", "야적장", "옥외", "외부", "마당", "광장",
    ],
    SpaceFunction.WORK: [
        "FAB", "CUB", "WWT", "MDF", "1F", "2F", "3F", "B1", "B2",
        "작업구역", "공장", "작업장",
    ],
}

# 키워드 매칭 우선순위 (낮은 인덱스 = 높은 우선순위)
SPACE_KEYWORD_PRIORITY = [
    SpaceFunction.RACK,
    SpaceFunction.TRANSIT_GATE,
    SpaceFunction.REST,
    SpaceFunction.WORK_HAZARD,
    SpaceFunction.TRANSIT_CORRIDOR,
    SpaceFunction.TRANSIT_WORK,
    SpaceFunction.WORK,
]

# SSMP zone_type → space_function 매핑
SSMP_ZONE_TYPE_MAPPING: dict[str, str] = {
    "amenity_rest": SpaceFunction.REST,
    "amenity_smoking": SpaceFunction.REST,
    "checkpoint_gate": SpaceFunction.TRANSIT_GATE,
    "checkpoint_timeclock": SpaceFunction.TRANSIT_GATE,
    "work_area": SpaceFunction.WORK,
    "confined_space": SpaceFunction.WORK_HAZARD,
    "target_area": SpaceFunction.WORK,
    "parking": SpaceFunction.OUTDOOR_MISC,
    "other": SpaceFunction.UNKNOWN,
}

# ─── Hazard Weight (공간 위험 가중치) ─────────────────────────────────
# 위험 수준별 기본 가중치
HAZARD_WEIGHT_DEFAULT: dict[str, float] = {
    SpaceFunction.WORK: 0.3,           # LOW
    SpaceFunction.WORK_HAZARD: 1.0,    # HIGH (고정)
    SpaceFunction.TRANSIT_WORK: 0.5,   # MEDIUM
    SpaceFunction.TRANSIT_GATE: 0.2,   # LOW
    SpaceFunction.TRANSIT_CORRIDOR: 0.3,
    SpaceFunction.REST: 0.0,           # 없음
    SpaceFunction.RACK: 0.0,           # 없음
    SpaceFunction.OUTDOOR_MISC: 0.5,   # MEDIUM
    SpaceFunction.UNKNOWN: 0.3,
}

# SSMP risk_level에 따른 가중치 조정
HAZARD_WEIGHT_BY_RISK_LEVEL: dict[str, float] = {
    "LOW": 0.3,
    "MEDIUM": 0.5,
    "HIGH": 0.8,
    "CRITICAL": 1.0,
}

# Alone risk multiplier (단독 작업 위험 배수)
ALONE_RISK_MULTIPLIER: dict[str, float] = {
    SpaceFunction.WORK: 1.0,
    SpaceFunction.WORK_HAZARD: 3.0,    # 고위험 구역 단독 작업 = 최고 위험
    SpaceFunction.TRANSIT_WORK: 1.5,
    SpaceFunction.TRANSIT_GATE: 0.5,
    SpaceFunction.TRANSIT_CORRIDOR: 0.5,
    SpaceFunction.REST: 0.0,
    SpaceFunction.RACK: 0.0,
    SpaceFunction.OUTDOOR_MISC: 1.5,
    SpaceFunction.UNKNOWN: 1.0,
}

# ─── Dwell Expectation (정상 체류 시간 기준, 분) ─────────────────────
# 이 시간 초과 시 dwell_exceeded = True
DWELL_NORMAL_MAX: dict[str, int] = {
    SpaceFunction.WORK: 999,           # 무제한 (작업 구역)
    SpaceFunction.WORK_HAZARD: 60,     # 1시간 (고위험 구역)
    SpaceFunction.TRANSIT_WORK: 30,
    SpaceFunction.TRANSIT_GATE: 5,     # 5분 초과 = 병목
    SpaceFunction.TRANSIT_CORRIDOR: 10,  # 10분 초과 = 정체
    SpaceFunction.REST: 60,            # 1시간 (점심 + α)
    SpaceFunction.RACK: 999,           # 무제한 (야간 거치)
    SpaceFunction.OUTDOOR_MISC: 30,
    SpaceFunction.UNKNOWN: 30,
}

# Abnormal stop threshold (이상 정지 감지 기준, 분)
ABNORMAL_STOP_THRESHOLD: dict[str, int] = {
    SpaceFunction.WORK: 60,
    SpaceFunction.WORK_HAZARD: 5,      # 🚨 5분만 지나도 즉시 확인
    SpaceFunction.TRANSIT_WORK: 45,
    SpaceFunction.TRANSIT_GATE: 10,
    SpaceFunction.TRANSIT_CORRIDOR: 15,
    SpaceFunction.REST: 999,           # 휴게 시설은 해당 없음
    SpaceFunction.RACK: 999,
    SpaceFunction.OUTDOOR_MISC: 45,
    SpaceFunction.UNKNOWN: 60,
}

# ─── 공간적응형 DBSCAN 파라미터 ───────────────────────────────────────
# space_function별 eps 배수 (기본 DBSCAN_EPS_INDOOR/OUTDOOR에 곱함)
DBSCAN_EPS_MULTIPLIER: dict[str, float] = {
    SpaceFunction.WORK: 1.0,           # 기본
    SpaceFunction.WORK_HAZARD: 0.7,    # 더 공격적 (작은 eps)
    SpaceFunction.TRANSIT_WORK: 1.5,   # 완화 (큰 eps)
    SpaceFunction.TRANSIT_GATE: 0.0,   # 클러스터링 불필요
    SpaceFunction.TRANSIT_CORRIDOR: 0.0,
    SpaceFunction.REST: 0.0,           # 클러스터링 불필요
    SpaceFunction.RACK: 0.0,
    SpaceFunction.OUTDOOR_MISC: 1.5,
    SpaceFunction.UNKNOWN: 1.0,
}

# state_override 공간 (active_ratio와 무관하게 상태 결정)
STATE_OVERRIDE_SPACES = {
    SpaceFunction.REST,
    SpaceFunction.RACK,
    SpaceFunction.TRANSIT_GATE,
    SpaceFunction.TRANSIT_CORRIDOR,
}

# ─── 장소 전환 이동 감지 (Transition Travel Detection) ★v5 ────────────
# 장소 전환 시 도착 후 초기 N분을 transit_arrival로 태깅
# 이유: 헬멧 거치대 → FAB 이동처럼 km 단위 이동은 순간 불가능.
#       도착 직후를 작업으로 분류하면 실제 작업시간이 과대계상됨.

# LOCATION_KEY가 변경된 경우 (다른 건물/층으로 이동)
TRANSITION_INTER_LOCATION_MIN = 10  # 분

# 같은 LOCATION_KEY 내 장소명만 변경 (같은 층 내 이동)
TRANSITION_SAME_LOCATION_MIN = 3    # 분

# 특수 출발지: RACK(거치대), GATE에서 출발 시
TRANSITION_FROM_ENTRY_MIN = 10      # 분

# 특수 출발지: REST(휴게시설)에서 출발 시
TRANSITION_FROM_REST_MIN = 5        # 분

# 태깅 상한: 해당 장소 연속 체류 시간의 이 비율까지만 태깅
# (FAB에 3분만 있다가 떠나는 경우, 10분치 태깅하면 전체가 이동이 되어버림)
TRANSITION_MAX_RATIO = 0.5          # 체류 시간의 최대 50%까지만

# 전환 이동 태깅에서 제외할 목적지 space_function
# (이미 transit 계열이거나 작업과 무관한 공간)
TRANSITION_TRAVEL_EXCLUDE_DEST = {
    SpaceFunction.RACK,
    SpaceFunction.REST,
    SpaceFunction.TRANSIT_GATE,
    SpaceFunction.TRANSIT_CORRIDOR,
}

# 레거시 호환용 별칭
TRANSITION_TRAVEL_INTER_LOCATION = TRANSITION_INTER_LOCATION_MIN
TRANSITION_TRAVEL_SAME_LOCATION = TRANSITION_SAME_LOCATION_MIN
TRANSITION_TRAVEL_FROM_ENTRY = TRANSITION_FROM_ENTRY_MIN

# ─── Journey 보정 상수 ──────────────────────────────────────────────
# 슬라이딩 윈도우 최빈값 필터 윈도우 크기 (분 단위)
LOCATION_SMOOTHING_WINDOW = 5

# 연속 비활성 신호로 헬멧 거치 판단하는 최소 시간 (분)
HELMET_RACK_MIN_DURATION_MIN = 30

# 좌표 이상치 판단 임계값 (같은 건물 내 최대 허용 좌표 변화량)
# SSMP 좌표계: S-Ward 측위 서버가 출력하는 좌표 단위.
#   실내: 건물 도면 기반 (보통 1unit ≈ 10~30cm, 현장별 확인 필요)
#   실외: GPS 기반 → 미터 단위에 가까움
# ⚠️ 정확한 스케일 팩터는 현장의 ssmp_buildings.csv와 도면을 교차 검증해야 함
COORD_OUTLIER_THRESHOLD = 200

# ─── DBSCAN 클러스터링 상수 ──────────────────────────────────────────
# eps: 같은 클러스터로 묶을 최대 좌표 거리 (SSMP 좌표 단위)
#   실내: 한 작업 구역 ≈ 15 좌표단위 (도면상 약 1.5~4.5m, 현장별 차이)
#   실외: GPS → 30 좌표단위 (약 30m)
DBSCAN_EPS_INDOOR = 15
DBSCAN_EPS_OUTDOOR = 30
DBSCAN_MIN_SAMPLES = 3

# ── 앵커 공간 보호 (★ v5.2 신규) ─────────────────────────────────────────
# 이 집합에 속하는 행은 DBSCAN·노이즈보정·슬라이딩윈도우에서 덮어쓰기 금지.
# 1~2분 체류도 정상적인 행동 (흡연장, 휴게실, 헬멧 거치대)
ANCHOR_SPACE_FUNCTIONS: frozenset = frozenset({SpaceFunction.REST, SpaceFunction.RACK})

# 장소명 키워드 기반 앵커 판별 (SpaceFunction 컬럼 없을 때 폴백)
ANCHOR_PLACE_KEYWORDS: list = [
    "휴게", "흡연", "식당", "탈의", "화장실", "휴식",
    "걸이대", "거치대", "보호구"
]

# ── 공간 우선순위 (Space Priority) — 번갈음 패턴 해석 시 사용 ★ v5.3
# 낮은 숫자 = 더 신뢰할 수 있는 실제 체류 공간
# "신호 수(다수결)보다 공간의 물리적 특성이 더 강한 증거"
SPACE_FUNCTION_PRIORITY: dict = {
    SpaceFunction.RACK:             1,   # 헬멧 거치대 — 퇴근/출근 확실한 체류
    SpaceFunction.REST:             2,   # 휴게실·흡연장 — 들어가서 머무는 공간
    SpaceFunction.WORK:             3,   # 일반 작업공간
    SpaceFunction.WORK_HAZARD:      4,   # 고위험 작업공간
    SpaceFunction.TRANSIT_WORK:     5,   # 실외 공사구역 (이동+작업 혼재)
    SpaceFunction.TRANSIT_CORRIDOR: 6,   # 복도·계단 (이동 통로)
    SpaceFunction.TRANSIT_GATE:     7,   # 출입구·타각기 (통과 전용, 체류 불가)
    SpaceFunction.OUTDOOR_MISC:     8,
    SpaceFunction.UNKNOWN:          9,
}

# TRANSIT_GATE 체류 불가 원칙:
# TRANSIT_GATE가 RACK·REST와 같은 클러스터에 묶이면 → RACK·REST 장소를 대표로 선택
TRANSIT_ONLY_FUNCTIONS: frozenset = frozenset({
    SpaceFunction.TRANSIT_GATE,
    SpaceFunction.TRANSIT_CORRIDOR
})

# ═══════════════════════════════════════════════════════════════════════════
# v6: 4증거 통합 Journey 보정 파라미터 (2026-03-02)
# ═══════════════════════════════════════════════════════════════════════════
#
# Journey 보정의 핵심 증거 4가지:
#   E1. 활성신호 (active_signal_count) — "사람이 실제로 움직였는가?"
#   E2. 공간 속성 (space_function)     — "이 장소에서 머무는 게 정상인가?"
#   E3. 시간 속성 (시간대)             — "지금 시각에 여기 있는 게 자연스러운가?"
#   E4. 이동 패턴 (위치 연속성)        — "위치가 안정적인가, 점프하는가?"
#
# 이 4가지 증거를 종합하여 BLE 신호 노이즈와 실제 이동을 구분한다.

# ─── [E1] 활성신호 기반 상태 판단 ──────────────────────────────────────────
# active_signal_count(활성신호갯수) = 태그의 실제 움직임을 나타내는 가장 중요한 지표
# 값이 0이면 태그가 전혀 움직이지 않음 (헬멧이 걸이대에 걸려 있을 가능성 높음)
ACTIVE_SIG_GHOST_MAX: int = 0      # 이 값 이하 = 무활성 (Ghost Signal 후보)
ACTIVE_SIG_TRANSIT_MAX: int = 2    # 이 값 이하 + 위치 변화 = 이동 중
ACTIVE_SIG_WORK_MIN: int = 3       # 이 값 이상 = 실제 작업 활동

# ─── [E4] 위치 안정성 판단 ──────────────────────────────────────────────────
# 일정 시간 윈도우 내에서 장소가 얼마나 변하는지로 안정성 판단
LOCATION_ENTROPY_WINDOW: int = 5   # 위치 안정성 계산 윈도우 (분)
LOCATION_UNSTABLE_THRESH: int = 2  # 윈도우 내 고유 장소 수 ≥ 이 값 = 불안정 (BLE 점프)
RUN_SHORT_MAX: int = 5             # 단발 체류 최대 길이 (분) — 이하면 번갈음 보정 대상
RUN_CONTINUOUS_MIN: int = 10       # 연속 체류 최소 길이 (분) — 이상이면 보정 안 함

# ─── [E3] 시간대 구간 정의 ──────────────────────────────────────────────────
# 하루를 논리적 구간으로 분절하여 맥락 판단에 활용
NIGHT_END_HOUR: int = 5            # 0~5시 = 야간 (비근무 시간대)
PREDAWN_WORK_START: int = 5        # 5시부터 출근 가능
POST_WORK_HOUR: int = 20           # 20시 이후 = 퇴근 후

# ─── Ghost Signal 복합 조건 ─────────────────────────────────────────────────
# Ghost Signal = BLE 다중반사로 인한 가짜 위치 신호
# 조건: active_signal_count=0 AND 위치 불안정 AND (야간 OR 주변에 RACK 존재)
GHOST_SIGNAL_RACK_SEARCH_WINDOW: int = 30  # Ghost 구간에서 RACK 탐색 범위 (분)

# ─── v5.4 호환 (deprecated in v6) ────────────────────────────────────────────
PRIORITY_MAX_RUN_MIN: int = RUN_SHORT_MAX
PRIORITY_CONTINUOUS_THRESHOLD: int = RUN_CONTINUOUS_MIN

# ═══════════════════════════════════════════════════════════════════════════
# v6.1: Multi-Pass Refinement 파라미터 (2026-03-02)
# ═══════════════════════════════════════════════════════════════════════════
#
# 핵심 철학: "한 번에 모든 것을 판단하지 않고, 단계별로 확인하며 참값에 수렴"
#
# 반복적 정제(Iterative Refinement) 프로세스:
#   1. 각 Pass가 특정 유형의 오류만 담당
#   2. Pass 간 결과를 검증하며 점진적 개선
#   3. 변경이 수렴할 때까지 반복 (최대 3회)
#
# ┌─────────────────────────────────────────────────────────────────────────┐
# │  Pass 1: Ghost Signal 제거                                              │
# │    - active_signal_count=0인 연속 구간 탐지                              │
# │    - 인근 RACK 장소로 통일                                               │
# │    - 예: 새벽/밤 걸이대↔공사현장 번갈음 → 걸이대로 통일                    │
# ├─────────────────────────────────────────────────────────────────────────┤
# │  Pass 2: 번갈음 패턴 해소                                                │
# │    - 공간 우선순위 적용 (RACK > REST > WORK > TRANSIT)                   │
# │    - A↔B 번갈음에서 우선순위 높은 쪽으로 흡수                             │
# │    - 예: FAB↔휴게실 번갈음 → 휴게실로 흡수                                │
# ├─────────────────────────────────────────────────────────────────────────┤
# │  Pass 3: 맥락 검증                                                       │
# │    - 하루 스토리라인 일관성 확인                                          │
# │    - 비근무시간 WORK, 점심시간 무활성 WORK 등 이례적 패턴 탐지              │
# ├─────────────────────────────────────────────────────────────────────────┤
# │  Pass 4: 물리적 이상치 탐지                                              │
# │    - 1분 내 500+ 좌표 이동 (텔레포트)                                     │
# │    - 2분 내 다른 건물 이동 (불가능 점프)                                  │
# │    - A-B-A 노이즈 잔류 흡수                                              │
# └─────────────────────────────────────────────────────────────────────────┘

# ─── [Pass 1] Ghost Signal 파라미터 ──────────────────────────────────────────
GHOST_MIN_BLOCK_LEN: int = 5           # Ghost 블록 최소 길이 (분) — 너무 짧으면 일시정지
GHOST_WORK_MIN_BLOCK_LEN: int = 20     # 근무시간대 Ghost 최소 길이 (분) — 작업 중 잠시 멈춤과 구분

# ─── [Pass 3] 맥락 검증 파라미터 ──────────────────────────────────────────────
NARRATIVE_ANCHOR_MIN_DWELL: int = 10   # 앵커 공간(휴게실/RACK) 최소 체류 (분)
NARRATIVE_WORK_MIN_RATIO: float = 0.3  # 근무시간 중 작업 구역 비율 최소값

# ─── [Pass 4] 물리적 이상치 파라미터 ─────────────────────────────────────────
IMPOSSIBLE_MOVE_SPEED: float = 500.0   # 1분 내 최대 이동 가능 좌표 거리
IMPOSSIBLE_BUILDING_JUMP_MIN: int = 2  # 건물 간 이동 최소 소요 시간 (분)

# ─── [수렴 판단] ────────────────────────────────────────────────────────────
CONVERGENCE_CHANGE_THRESH: int = 5     # Pass에서 변경된 행 수 < 이 값이면 수렴 완료
MULTI_PASS_MAX_ITERATIONS: int = 3     # 최대 반복 횟수

# ─── 생산성/안전성 임계값 ───────────────────────────────────────────
# 피로 위험도: 연속 작업 임계값
FATIGUE_THRESHOLD_MIN = 120  # 2시간 연속 작업 시 피로 위험

# 헬멧 준수율 관련 상수 제거됨 (2026-02)
# 이유: BLE 신호만으로는 헬멧 착용 여부를 신뢰성 있게 추정 불가.
# 신호 수신 여부가 헬멧 착용 여부와 직접 상관관계가 없으며,
# 헬멧 미착용 구분은 별도의 물리적 센서 없이는 구현 불가.

# 단독 작업 위험 반경 (좌표 단위)
ALONE_RISK_RADIUS = 50

# ─── 데이터 파일 경로 패턴 ──────────────────────────────────────────
DATA_FOLDER_PREFIX = "Y1_Worker_TWard_"
CACHE_FILE_PREFIX = "processed_"
CACHE_FILE_SUFFIX = ".parquet"
CSV_ENCODING = "utf-8-sig"
DATETIME_FORMAT = "%Y.%m.%d %H:%M:%S"

# ─── 6개 활동 상태 카테고리 (Journey 시각화용) ─────────────────────────
ACTIVITY_COLORS = {
    "high_work":     "#1E5AA8",  # 진파랑: 고활성 작업
    "low_work":      "#6FA8DC",  # 하늘색: 저활성 작업
    "standby":       "#F4D03F",  # 노랑: 대기
    "transit":       "#E67E22",  # 주황: 이동
    "rest":          "#27AE60",  # 초록: 휴게
    "rest_facility": "#27AE60",  # 초록: 휴게 시설 (별칭)
    "off_duty":      "#BDC3C7",  # 연회색: 비근무
}

ACTIVITY_LABELS = {
    "high_work":     "고활성 작업",
    "low_work":      "저활성 작업",
    "standby":       "대기",
    "transit":       "이동",
    "rest":          "휴게",
    "rest_facility": "휴게",
    "off_duty":      "비근무",
}

# 레거시 호환용 별칭
TIME_CATEGORY_COLORS = ACTIVITY_COLORS
TIME_CATEGORY_LABELS = ACTIVITY_LABELS


# ═══════════════════════════════════════════════════════════════════════════
# Intelligent Journey Correction v5 — 시퀀스 기반 맥락 해석
# ═══════════════════════════════════════════════════════════════════════════

# 공간별 체류 기대값 (Space Dwell Profile)
# - min_normal_dwell: 최소 정상 체류 분 (이 이하면 의심)
# - is_anchor: True이면 짧은 체류도 유효 (이동 판정 억제)
# - transit_tolerance: 이 분 이하의 단독 체류는 무조건 transit으로 간주
SPACE_DWELL_PROFILE: dict = {
    SpaceFunction.WORK:             {"min_normal_dwell": 10, "is_anchor": False, "transit_tolerance": 3},
    SpaceFunction.WORK_HAZARD:      {"min_normal_dwell": 5,  "is_anchor": False, "transit_tolerance": 2},
    SpaceFunction.TRANSIT_GATE:     {"min_normal_dwell": 1,  "is_anchor": False, "transit_tolerance": 0},
    SpaceFunction.TRANSIT_CORRIDOR: {"min_normal_dwell": 1,  "is_anchor": False, "transit_tolerance": 0},
    SpaceFunction.TRANSIT_WORK:     {"min_normal_dwell": 3,  "is_anchor": False, "transit_tolerance": 1},
    SpaceFunction.REST:             {"min_normal_dwell": 1,  "is_anchor": True,  "transit_tolerance": 0},
    SpaceFunction.RACK:             {"min_normal_dwell": 30, "is_anchor": True,  "transit_tolerance": 0},
    SpaceFunction.OUTDOOR_MISC:     {"min_normal_dwell": 5,  "is_anchor": False, "transit_tolerance": 2},
    SpaceFunction.UNKNOWN:          {"min_normal_dwell": 5,  "is_anchor": False, "transit_tolerance": 2},
}

# 시퀀스 분석 파라미터
SEQUENCE_WINDOW_MIN = 10        # 슬라이딩 윈도우 크기 (분)
SEQUENCE_VARIETY_THRESH = 3     # 윈도우 내 장소 종류가 이 이상이면 이동 의심
SEQUENCE_TRANSIT_RATIO = 0.6    # 윈도우 내 transit_tolerance 이하 체류 비율 임계값

# 시각화 파라미터
MIN_DISPLAY_MINUTES = 3         # Gantt 차트 최소 표시 폭 (분)
SHORT_BLOCK_THRESHOLD_MIN = 3   # 짧은 블록 강조 임계값 (분)

