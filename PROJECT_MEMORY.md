# SKEP DataAnalysis_Productivity — 프로젝트 메모리

> **용도**: Claude 등 AI에게 현재 코드베이스의 구조·로직·상태·결정 배경을 전달하기 위한 참조 문서.  
> **업데이트 규칙**: 새 기능 추가, 로직 변경, 버그 수정, 설계 결정 시 해당 섹션을 즉시 갱신할 것.  
> **최종 수정**: 2026-03-04 (M15X 새벽 off-duty 판정, LLM Journey Shift, *_tuned 캐시)
> **⚠️ 현재 단계**: 샘플 데이터 기반 Feasibility 검증. 5명 작업자 데이터로 개념 검증 중.

---

## 1. 프로젝트 개요

**목적**: SK하이닉스 용인 반도체 클러스터 건설현장(Y-Project) 작업자의 UWB/BLE 위치 데이터를 분석하여  
**생산성 및 안전성 지표를 추출**하는 Streamlit 대시보드.

| 항목 | 값 |
|------|-----|
| 실행 명령 | `streamlit run main.py` |
| 가상환경 | `/Users/Tony_mac/Desktop/TJLABS/TJLABS_Research/.venv` |
| 파이프라인 흐름 | `Raw CSV → SSMP 장소 분류 → Journey 보정 → Parquet 캐시 → 대시보드 시각화` |
| 데이터 주기 | 1분 단위 위치 기록 (T-Ward BLE 태그 기반) |
| 현장 규모 | Y-Project 단일 섹터, 복수 건물 (FAB, WWT 등), 복수 층 |

### 프로젝트 단계

```
┌─────────────────────────────────────────────────────────────────────┐
│  현재 단계: ⚠️ Feasibility 검증 (PoC) — v6.3 메뉴 재구성               │
│  ─────────────────────────────────────────────────────────────────  │
│  · 샘플 데이터: 5명 작업자, 1일 (20260225)                            │
│  · 목적: Deep Con (건설현장용 공간 AI) 기반 기술 검증                  │
│  · 완료: 공간+시간+빈도 종합 추론, 원본 기반 보정, Journey 문장화      │
│  · 다음: 집단 패턴 학습, 멀티데이 분석, 이상 행동 탐지                 │
│  · 주의: 현재 지표 수치는 참고용. 대규모 데이터 적용 시 재검증 필요     │
└─────────────────────────────────────────────────────────────────────┘

개발 로드맵:
  Phase 1: 규칙 기반 보정         ✅ 완료 (v6.2) — 공간+시간+빈도 종합 추론
  Phase 2: Journey 문장화 보정    ✅ 완료 (v6.3) — Run 시퀀스 기반 전체 맥락
  Phase 2.5: EWI 출퇴근 기반 계산  ✅ 완료 (v6.4) — detect_work_shift
  Phase 2.6: 생산성 분석 + Claude API ✅ 완료 (v6.5) — 음영지역 고려 EWI, AI 내러티브
  Phase 2.7: Journey 기반 LLM 출퇴근 해석 ✅ 완료 (v6.6) — 전체 Journey 컨텍스트 기반 clock_in/clock_out 제안
  Phase 3: 집단 패턴 학습          ⏳ 다음 — 같은 회사 작업자 패턴
  Phase 4: 멀티데이 + 이상탐지     ⏳ 대기 — 여러 날 데이터, 비정상 행동
```

### Deep Con — 건설현장용 공간 AI 비전

```
┌────────────────────────────────────────────────────────────────────────────┐
│  Journey = 문장 (Sentence)                                                 │
│  장소 = 단어 (Word)                                                        │
│                                                                            │
│  "걸이대 → FAB 1F → 휴게실 → FAB 1F → 걸이대"                              │
│     ↓                                                                      │
│  "출근 → 작업 → 휴식 → 작업 → 퇴근"                                        │
│                                                                            │
│  이 "문장"에서 문법(패턴)을 학습하면:                                        │
│  - 어색한 단어 조합 탐지 → 노이즈 보정                                       │
│  - 비정상 문장 구조 탐지 → 이상 행동 알림                                    │
│  - 같은 직군의 문장 패턴 → 집단 행동 분석                                    │
└────────────────────────────────────────────────────────────────────────────┘

핵심 철학:
  1. 부분이 아닌 전체 Journey 맥락 — 하루 전체를 하나의 문장으로 분석
  2. 원본 데이터 기반 합리적 추론 — 없는 장소로 보정하지 않음
  3. 공간 + 시간 + 이동 특성 종합 — 다차원 증거 통합 판단
  4. 개인 → 집단 → 시계열 확장 — 점진적 패턴 학습

데이터 활용 전략:
  · 1명 1일: 개인 Journey 맥락 (현재 구현)
  · N명 1일: 같은 회사 집단 패턴 (Phase 3)
  · 1명 N일: 개인 시계열 패턴 (Phase 4)
  · N명 N일: 현장 전체 공간 활용 패턴 (미래)
```

---

## 2. 디렉토리 구조

```
DataAnalysis_Productivity/
├── main.py                              # 앱 진입점, 사이드바, 페이지 라우팅
├── requirements.txt                     # Python 의존성 (anthropic, python-dotenv 추가됨)
├── PROJECT_MEMORY.md                    # 이 파일
├── .env.example                         # API 키 템플릿 (.env는 .gitignore에 포함)
├── .gitignore                           # .env, cache/, *.parquet 등 제외
│
├── Datafile/
│   ├── Y1 _Worker_TWard_YYYYMMDD/      # 날짜별 Raw CSV 폴더 (폴더명 공백 주의)
│   │   └── *.csv                       # 작업자별 또는 통합 CSV 파일
│   └── ssmp_structure/                  # SSMP 공간 메타데이터 CSV (2026-02 추가)
│       ├── ssmp_buildings.csv           # building_id, sector_id, building_name, status
│       ├── ssmp_levels.csv              # level_id, building_id, level_name, level_index, coordinate_system, status
│       ├── ssmp_zones.csv               # zone_id, building_id, level_id, zone_name, zone_type, ref_type, ...
│       ├── ssmp_spots.csv               # spot_id, ref_type, ref_id, building_id, level_id, spot_name, x, y, z
│       ├── ssmp_service_sections.csv    # service_section_id, service_section_name, service_domain, risk_level
│       ├── ssmp_service_section_members.csv  # service_section_id, member_type, member_id, role
│       ├── ssmp_sectors.csv             # sector_id, sector_name, coordinate_system
│       ├── ssmp_polygons.csv            # polygon_id, ref_type, ref_id, geometry_json
│       ├── ssmp_swards.csv              # sward_id, sward_name, sward_model
│       ├── ssmp_sward_attachments.csv   # sward_id, spot_id, role
│       └── ssmp_sward_positions.csv     # sward_id, ref_type, ref_id, x, y, z
│
├── cache/                               # processed_YYYYMMDD.parquet 저장 위치
│
├── .streamlit/
│   └── config.toml                      # 라이트 테마 강제 설정
│
└── src/
    ├── data/
    │   ├── schema.py                    # RawColumns, ProcessedColumns 상수 클래스
    │   ├── loader.py                    # CSV 로드: scan_data_folders, load_date_folder, get_folder_for_date
    │   ├── preprocessor.py              # Journey 보정 파이프라인 (Phase 0 DBSCAN 추가)
    │   ├── spatial_loader.py            # SSMP 공간 구조 로더 (SpatialContext 클래스)
    │   └── cache_manager.py             # Parquet 저장/로드, load_multi_date_cache, get_date_cache_status
    ├── metrics/
    │   ├── aggregator.py                # 작업자/업체/날짜별 집계
    │   ├── productivity.py              # 생산성 지표 계산 (이동거리 좌표계별 분리)
    │   ├── safety.py                    # 안전성 지표 (피로, 이상이동, 단독작업) ※헬멧 제거
    │   ├── soif.py                      # ★ SOIF 5계층 지표 (EWI/OFI/CRE/BS/ZU/ZoneTime/FlowEdge)
    │   ├── drill_down.py                # 드릴다운 분석 엔진
    │   └── trend_analyzer.py            # 다중 날짜 트렌드 분석
    ├── pages/
    │   ├── journey_verify.py            # 🔍 Journey 검증 (Q1: 보정 검증)
    │   ├── site_analysis.py             # 📊 현장 분석 (Q2·Q3: 지표+맥락, 4개 서브탭)
    │   ├── future_preview.py            # 🔮 확장 가능성 (Q4: 미래 기능 Preview)
    │   ├── pipeline.py                  # ⚙️ Admin: Pipeline (SSMP 연동, 전처리 실행)
    │   ├── journey_review.py            # 🔧 Admin: Journey Debug (보정 상세 디버그)
    │   └── space_config.py              # 🗺️ Admin: 공간 속성 (space_function 정의/파라미터)
    ├── utils/
    │   ├── constants.py                 # 전역 상수 (WORK_INTENSITY 추가, 헬멧 상수 삭제)
    │   ├── place_classifier.py          # 장소명 → PlaceType 분류 (SpatialContext 지원)
    │   ├── time_utils.py                # 시간대 분류, 날짜 파싱 유틸
    │   ├── theme.py                     # Plotly 레이아웃, GLOBAL_CSS, Color.TIME_CAT 추가 (라이트 테마)
    │   └── llm_interpreter.py           # ★ Claude API LLM 해석 레이어 (2026-03-02 추가)
```

---

## 3. 데이터 스키마

### 3-A. Raw CSV 컬럼 (`RawColumns` in `schema.py`)

| 상수명 | 실제 컬럼명 | 타입 | 설명 |
|--------|------------|------|------|
| `TIME` | `시간(분)` | Timestamp | 1분 단위 타임스탬프 (pandas Timestamp, timezone-naive) |
| `WORKER` | `작업자` | str | 작업자 이름 |
| `ZONE` | `구역` | str | 구역명 |
| `BUILDING` | `건물` | str | 건물명 |
| `FLOOR` | `층` | str | 층 |
| `PLACE` | `장소` | str | 장소명 (보정 전 원본) |
| `X` | `X` | float | X 좌표 (좌표계는 LOCATION_KEY 기준) |
| `Y` | `Y` | float | Y 좌표 |
| `TAG` | `태그` | str | T-Ward 기기 ID |
| `COMPANY` | `업체` | str | 소속 업체 |
| `SIGNAL_COUNT` | `신호갯수` | int | 1분간 수신된 BLE 신호 수 |
| `ACTIVE_SIGNAL_COUNT` | `활성신호갯수` | int | 활성(10초 주기) 신호 수 |

### 3-B. 전처리 후 추가 컬럼 (`ProcessedColumns` in `schema.py`)

| 상수명 | 실제 컬럼명 | 타입 | 설명 |
|--------|------------|------|------|
| `DATE` | `날짜` | str | 날짜 문자열 (`YYYY-MM-DD`) |
| `HOUR` | `시` | int | 시간 (0~23) |
| `MINUTE` | `분` | int | 분 (0~59) |
| `ACTIVE_RATIO` | `활성비율` | float | 활성신호갯수 / 신호갯수 (0.0~1.0) |
| `IS_ACTIVE` | `활동여부` | bool | 활성비율 ≥ 0.3 여부 |
| `PLACE_TYPE` | `장소유형` | str | `HELMET_RACK` / `REST` / `GATE` / `WORK_AREA` / `CONFINED_SPACE` / `INDOOR` / `OUTDOOR` / `UNKNOWN` |
| `SPACE_TYPE` | `공간유형` | str | `INDOOR` / `OUTDOOR` |
| `LOCATION_KEY` | `위치키` | str | 좌표계 구분 키. `OUTDOOR` 또는 `{bld_short}_L{level_index}` |
| `IS_HELMET_RACK` | `헬멧거치여부` | bool | PLACE_TYPE == HELMET_RACK 여부 |
| `PERIOD_TYPE` | `시간대유형` | str | `work` / `rest` / `off` |
| `CORRECTED_PLACE` | `보정장소` | str | Journey 보정 후 장소명 |
| `CORRECTED_X` | `보정X` | float | 보정된 X 좌표 |
| `CORRECTED_Y` | `보정Y` | float | 보정된 Y 좌표 |
| `IS_CORRECTED` | `보정여부` | bool | 보정 알고리즘이 적용된 행이면 True |
| `WORKER_KEY` | `작업자키` | str | `작업자이름_태그ID` (작업자 고유 식별자) |
| `ssmp_matched` | `ssmp_matched` | bool | SSMP 데이터 정확 매칭 여부 (False이면 키워드 폴백) |
| `coverage_gap` | `coverage_gap` | bool | signal_count=0 행 (커버리지 밖/배터리/간섭). ★ v3 추가 |
| `signal_confidence` | `signal_confidence` | Categorical | 신호 수 기반 신뢰도: `NONE`(0) / `LOW`(1~3) / `MED`(4~9) / `HIGH`(10+). ★ v3 추가 |
| `SPATIAL_CLUSTER` | `SPATIAL_CLUSTER` | int | DBSCAN 클러스터 ID (-1=노이즈). v3부터 캐시에 저장됨 |
| `CLUSTER_PLACE` | `CLUSTER_PLACE` | str | 클러스터별 대표 장소명 (signal_count 가중 최빈값). v3부터 캐시에 저장됨 |

> **주의**: `company.py` 집계 DataFrame에서는 `"company"` 영문 컬럼을 사용함.  
> `RawColumns.COMPANY` (`"업체"`)는 원본 Raw df에서만 사용하고, 집계 df에는 `"company"` 문자열을 직접 사용한다.

### 3-C. 실제 SSMP CSV 컬럼 (2026-02 확인)

| 파일 | 실제 컬럼명 |
|------|------------|
| `ssmp_buildings.csv` | `building_id, sector_id, building_name, status` |
| `ssmp_levels.csv` | `level_id, sector_id, building_id, level_name, level_index, coordinate_system, status` |
| `ssmp_zones.csv` | `zone_id, sector_id, ref_type, ref_id, building_id, level_id, zone_name, zone_type, definition_type, polygon_id, risk_level, congestion_prone, status` |
| `ssmp_spots.csv` | `spot_id, sector_id, ref_type, ref_id, building_id, level_id, spot_name, spot_type, x, y, z, status` |
| `ssmp_service_sections.csv` | `service_section_id, sector_id, service_section_name, service_domain, risk_level, congestion_prone, status` |
| `ssmp_service_section_members.csv` | `service_section_id, member_type, member_id, role, weight, active` |
| `ssmp_sectors.csv` | `sector_id, sector_name, timezone, coordinate_system, status` |
| `ssmp_polygons.csv` | `polygon_id, ref_type, ref_id, geometry_type, geometry_json` |

> **문서 컬럼명 ≠ 실제 컬럼명 가능성**: CSV 파일을 `pd.read_csv(f, nrows=0)` 로 헤더 확인 후 코딩할 것. 절대 문서 컬럼명을 그대로 하드코딩하지 말 것.

---

## 4. T-Ward 센서 동작 원리

```
T-Ward (BLE 태그):
  - 활성 상태:   10초 주기 advertising → 1분에 최대 6개 신호
  - 비활성 상태: 60초 주기 advertising → 1분에 최대 1개 신호

활성비율 계산:
  active_ratio = 활성신호갯수 / 신호갯수
  신호갯수 = 0 이면 active_ratio = 0.0

6개 시간 카테고리 기준 (2026-02 확정):
  ≥ 0.6         → 고활성 작업 (high_work): 이동·운반·체결 등 활발한 신체 활동
  0.15 ~ 0.6    → 저활성 작업 (low_work): 감독·측량·정밀작업 등 움직임 적은 생산 작업
  0.05 ~ 0.15   → 현장 대기 (standby): 작업공간 내 거의 정지 상태 (자재·지시·장비 대기)
  ≤ 0.05        → 비활성: 헬멧 걸이대 거치 또는 완전 정지

⚠️ BLE 신호만으로 헬멧 착용 여부 판단 불가:
  - 현장에 있으면 항상 신호가 수신됨 → "신호갯수 > 0" ≈ 출근 여부와 동일
  - 헬멧 착용/미착용 구분은 물리적 센서(압력 센서 등) 없이는 구현 불가
  - 따라서 헬멧 준수율 지표는 2026-02에 완전 제거됨
```

---

## 5. 전처리 파이프라인 (`src/data/preprocessor.py`)

`preprocess(df, spatial_ctx=None)` 함수가 6단계 순서로 실행됨.

### Step 1: 활성비율 계산 (`_calc_active_ratio`)

```python
# 활성신호 > 신호 이상값 자동 clamp
active = active.clip(upper=sig)
active_ratio = where(sig > 0, active / sig, 0.0)
is_active    = active_ratio >= ACTIVE_RATIO_WORKING_THRESHOLD  # 0.3

# ★ v3 추가: 센서 물리 신호 품질 컬럼
coverage_gap      = (sig == 0)                           # True = 커버리지 밖/배터리/간섭
signal_confidence = pd.cut(sig, [-1, 0, 3, 9, inf],     # NONE / LOW / MED / HIGH
                           labels=["NONE","LOW","MED","HIGH"])
```

### Step 2: 장소 분류 (`add_place_columns`)

`SpatialContext`가 전달된 경우 SSMP 기반 분류, 없으면 키워드 매칭으로 폴백.

**SSMP 기반 분류 우선순위** (`SpatialContext.classify_place`):
```
1. 헬멧 걸이대 키워드 (최우선 하드코딩)       → HELMET_RACK
   keywords: ["보호구 걸이대", "보호구걸이대", "헬멧 걸이", "안전모 걸이"]
2. ssmp_service_sections 이름 정확 매칭        → zone_type/service_domain 기반 PlaceType
3. ssmp_zones 이름 정확 매칭                   → zone_type 기반 PlaceType
4. ssmp_service_sections 이름 부분 매칭         → zone_type/service_domain 기반 PlaceType
5. 게이트/타각기 키워드 매칭                   → GATE
   keywords: ["타각기", "GATE", "게이트", "정문", "입구", "출구"]
6. 휴게 시설 키워드 매칭                       → REST
   keywords: ["휴게", "식당", "탈의실", "흡연장", "흡연실"]
7. building+floor 정보로 결정                  → INDOOR (둘 다 있을 때)
8. 나머지                                      → OUTDOOR 또는 UNKNOWN
```

**zone_type → PlaceType 매핑** (`_ZONE_TYPE_MAP`):
```
amenity_rest          → REST
amenity_smoking       → REST
checkpoint_gate       → GATE
checkpoint_timeclock  → GATE
work_area             → WORK_AREA
confined_space        → CONFINED_SPACE
target_area           → WORK_AREA
other                 → INDOOR  (기본값)
parking               → OUTDOOR
```

**service_domain → PlaceType 매핑** (`_DOMAIN_MAP`, level 멤버용):
```
facility      → REST     (단, 휴게 키워드 없으면 INDOOR로 보정)
access_control → GATE
safety         → WORK_AREA
productivity   → WORK_AREA
other          → INDOOR
```

**LOCATION_KEY 생성 규칙** (`SpatialContext._make_loc_key`):
```
ref_type == "sector"  →  "OUTDOOR"
ref_type == "level"   →  f"{building_id.split('-')[-1]}_L{level_index}"
예) building_id="BLD-00022-002", level_index=2  →  "002_L2"
```

**추가되는 컬럼**:
- `장소유형` (PLACE_TYPE): PlaceType 상수 문자열
- `공간유형` (SPACE_TYPE): `INDOOR` 또는 `OUTDOOR`
- `위치키` (LOCATION_KEY): 좌표계 구분 키
- `헬멧거치여부` (IS_HELMET_RACK): bool
- `ssmp_matched`: SSMP 정확 매칭 여부 (SpatialContext 전달 시만 추가)

**절대 금지 원칙**: `LOCATION_KEY`가 다른 행들의 좌표를 직접 비교하거나 거리 계산하면 안 됨.  
예) FAB 1F 좌표(100,100)와 WWT B1F 좌표(100,100)는 완전히 다른 좌표계임.

### Step 3: 작업자 키 생성 (`_add_worker_key`)

```python
작업자키 = 작업자이름 + "_" + 태그ID
예) "황*석_T-41-00007212"
```

### Step 4: 시간 파생 컬럼 (`_add_time_columns`)

`날짜` (YYYY-MM-DD), `시` (0~23), `분` (0~59) 컬럼 추가.

### Step 5: 작업자별 Journey 보정 (`_correct_worker_journey`)

작업자별로 독립적으로 실행. Phase 순서 중요.

```
Phase 0: DBSCAN 좌표계별 공간 클러스터링
  → _cluster_locations_by_key(df_worker)   # signal_count 가중 최빈값 ★ v3
  → _correct_noise_by_cluster(df_worker)   # nearest-cluster 방식 ★ v3
  scikit-learn 미설치 → ImportError → 기존 _correct_location_noise로 자동 폴백
  Phase 0 예외 발생   → Exception  → 기존 _correct_location_noise로 자동 폴백

Phase 1: 헬멧 거치 패턴 보정
  → _correct_helmet_rack_pattern(df_worker)
  ★ v3: place_type != "REST" 조건 추가 (휴게실 비활성은 정상 휴식)

Phase 2: 좌표 이상치 보정 (LOCATION_KEY 그룹별)
  → _correct_coord_outliers(df_worker)

Phase 2-post: 좌표↔장소명 정합성 검증 ★ v3
  → _validate_place_coord_consistency(df_worker)
  좌표 보정으로 centroid에서 크게 벗어난 행 → 가장 가까운 클러스터 장소로 재배정

[Step 5-post]: CORRECTED_PLACE 재분류 ★ v3
  → _reclassify_corrected_places(result, spatial_ctx)
  보정으로 장소가 바뀐 행의 PLACE_TYPE / LOCATION_KEY / IS_HELMET_RACK 재분류
```

#### Phase 0-A: DBSCAN 좌표계별 클러스터링 (`_cluster_locations_by_key`)

```
핵심 원칙: LOCATION_KEY별로 완전히 독립 수행.
           다른 좌표계를 같이 클러스터링하면 의미없는 결과.

★ v5.3: 앵커 행(REST/RACK space_function) 사전 처리
  → DBSCAN 실행 전 앵커 행에 자기 장소명 기반 cluster_id 부여
  → min_samples 제약 없이 1분 체류도 유효 클러스터로 보존
  → 앵커 판별: _build_anchor_mask() 사용 (space_function, place_type, 키워드 체크)

처리 흐름:
0. ★ v5.3 앵커 사전 처리:
   - _build_anchor_mask()로 앵커 행 식별
   - 앵커 행: 자기 장소명으로 SPATIAL_CLUSTER, CLUSTER_PLACE 설정
   - 같은 장소명 → 같은 cluster_id 공유
1. 앵커가 아닌 행만 LOCATION_KEY 로 그룹화
2. 각 그룹: CORRECTED_X, CORRECTED_Y 유효한 행만 추출 (NaN 제거)
3. 최소 3행 미만인 그룹 → 스킵
4. DBSCAN 실행:
   eps = DBSCAN_EPS_OUTDOOR(30) 또는 DBSCAN_EPS_INDOOR(15) ← constants.py
   min_samples = DBSCAN_MIN_SAMPLES(3)  ← constants.py
   ★ 좌표 단위: SSMP 좌표계 (실내=도면 단위 약10~30cm/unit, 실외=GPS 미터 단위)
5. 클러스터 ID = raw_label + cluster_id_offset
   (cluster_id_offset: 앵커 처리에서 이미 증가한 값 이어서 사용)
6. 각 클러스터의 대표 장소명 = signal_count 가중 최빈값 ★ v3 (기존: 단순 mode)
7. 결과 컬럼: SPATIAL_CLUSTER (int), CLUSTER_PLACE (str)
```

| 파라미터 | 상수명 | 값 | 설명 |
|----------|--------|-----|------|
| `eps` (OUTDOOR) | `DBSCAN_EPS_OUTDOOR` | 30 | GPS 기반, 약 30m |
| `eps` (INDOOR) | `DBSCAN_EPS_INDOOR` | 15 | 도면 기반, 약 1.5~4.5m |
| `min_samples` | `DBSCAN_MIN_SAMPLES` | 3 | 최소 3분 체류 = 실제 체류 |
| `cluster_id_offset` | — | 자동 누적 | 그룹 간 클러스터 ID 충돌 방지 |

> **SSMP 좌표계 스케일 팩터** (★ v3 문서화):
> 실내: S-Ward 측위 서버 출력 좌표. 건물 도면 기반, 1unit ≈ 10~30cm (현장별 상이).
> 실외: GPS 기반, 1unit ≈ 1m.
> 정확한 스케일 팩터는 `ssmp_buildings.csv` + 현장 도면 교차 검증 필요.

#### Phase 0-B: 노이즈 행 보정 (`_correct_noise_by_cluster`) ★ v3 전면 개편

```
기존 (v2): forward-fill (직전 유효 CLUSTER_PLACE로 채움)
  문제: A→B 이동 시작점이 노이즈이면 A로 채워져 B 도착 시간이 늦게 기록됨

변경 (v3): nearest-cluster 방식
  1. 연속 노이즈 ≥ 5분 → "[이동] A → B" transit 태깅
     _CONTINUOUS_NOISE_TRANSIT_MIN = 5 (preprocessor.py 상수)
     실제 이동 구간일 가능성 높음. 앞뒤 유효 클러스터 장소를 찾아 라벨링
  2. 개별(단발성) 노이즈 → 앞뒤 유효 행까지의 행 거리(시간 거리) 비교
     더 가까운 쪽의 CLUSTER_PLACE로 채움
     → A→B 전환 첫 분이 노이즈여도 B 쪽이 가까우면 B로 정확히 채워짐

★ v5.3: 앵커 행 보호
  → is_noise & is_anchor → SPATIAL_CLUSTER = -2 (앵커 보호 마커)
  → correction_target = is_noise & ~anchor_mask 로 범위 제한
  → 앵커 행의 CORRECTED_PLACE/CLUSTER_PLACE 절대 변경 안 함
  → -2는 유효한 클러스터로 취급 (_find_nearest_valid_place, _distance_to_nearest_valid)

결과: 실제로 장소명이 바뀐 행에 IS_CORRECTED = True 기록
주의: IS_CORRECTED가 이미 True인 행은 스킵
```

#### Phase 1: 헬멧 거치 패턴 보정 (`_correct_helmet_rack_pattern`)

```
대상: 야간/새벽(is_night_or_dawn) OR 점심(is_lunch_time)
     AND 활성비율 ≤ 0.05 (ACTIVE_RATIO_ZERO_THRESHOLD)
     AND NOT 앵커 공간 ★ v5.3 변경 (_build_anchor_mask()로 REST·RACK 모두 제외)
     AND 연속 구간 길이 ≥ 30분 (HELMET_RACK_MIN_DURATION_MIN)

처리:
1. 해당 구간 내 헬멧 걸이대(HELMET_RACK) 장소 행 탐색
2. 없으면 앞뒤 ±10행 내에서 탐색
3. 가장 많이 등장한 헬멧 걸이대 장소로 구간 전체 통일
4. CORRECTED_PLACE, CORRECTED_X, CORRECTED_Y, IS_CORRECTED 업데이트
```

#### Phase 1 (폴백): 슬라이딩 윈도우 노이즈 제거 (`_correct_location_noise`)

```
Phase 0 (DBSCAN) 실패 시 대신 실행.
방법: 슬라이딩 윈도우 최빈값 필터 (window = LOCATION_SMOOTHING_WINDOW = 5분)
효과: A,A,A,B,A,B,B,B → 순간 이탈 B 제거 → A,A,A,A,A,B,B,B

★ v5.3: 앵커 행 보호
  → IS_CORRECTED=True 행 + 앵커 행 모두 슬라이딩 윈도우 대상 제외
  → _is_anchor_row(place_type, space_function)로 개별 행 체크
조건: IS_CORRECTED=True (Phase 1 이전 보정) 행은 스킵
```

#### Phase 2: 좌표 이상치 보정 (`_correct_coord_outliers`)

```
방법: 같은 LOCATION_KEY 그룹 내에서 연속된 좌표 간 급격한 변화 탐지 후 선형 보간
임계값: COORD_OUTLIER_THRESHOLD = 200 (SSMP 좌표 단위, 실내≈20~60m, 실외≈200m)
처리: 이상치 → NaN → pandas interpolate(method="linear")
대상 컬럼: CORRECTED_X, CORRECTED_Y
절대 다른 LOCATION_KEY 그룹과 혼합하지 않음
```

#### Phase 2-post: 좌표↔장소명 정합성 검증 (`_validate_place_coord_consistency`) ★ v3

```
문제: Phase 2 좌표 보정으로 좌표가 크게 이동했는데, 장소명은 Phase 0 기준 그대로인 불일치
방법:
  1. 같은 LOCATION_KEY 내 클러스터별 centroid 계산
  2. 각 행이 자기 클러스터 centroid에서 COORD_OUTLIER_THRESHOLD 이상 이탈 → 불일치
  3. 불일치 행 → 좌표 기준 가장 가까운 다른 클러스터의 대표 장소명으로 재배정
  4. SPATIAL_CLUSTER, CLUSTER_PLACE, IS_CORRECTED 업데이트
```

#### Step 5-post: CORRECTED_PLACE 기반 재분류 (`_reclassify_corrected_places`) ★ v3

```
문제: Step 2에서 원본 PLACE 기준 PLACE_TYPE 분류 → Step 5 DBSCAN이 CORRECTED_PLACE를 변경
      → PLACE_TYPE은 원본 기준 그대로 → 휴게실로 보정된 행이 여전히 INDOOR(work) 집계
방법:
  CORRECTED_PLACE != 원본 PLACE 인 행에 대해:
  1. classify_place(CORRECTED_PLACE) → PLACE_TYPE 재분류
  2. is_helmet_rack(CORRECTED_PLACE) → IS_HELMET_RACK 재갱신
  3. make_location_key 또는 spatial_ctx.get_location_key → LOCATION_KEY 재갱신
  SpatialContext 있으면 SSMP 기반 재분류, 없으면 키워드 폴백
```

### Step 6: 활동 유형 분류 (`_classify_activity_period`)

각 행에 `PERIOD_TYPE` 컬럼 추가.

```python
분류 우선순위 (_classify_row 함수):
1. place_type == "HELMET_RACK"                    → "off"
2. place_type == "REST"                           → "rest"
3. corrected_place 에 REST_AREA_KEYWORDS 포함     → "rest"
   REST_AREA_KEYWORDS = ["휴게", "식당", "탈의실", "탈의", "로비"]
4. classify_activity_period(hour, active_ratio) 호출 (작업 구역용):
   - hour < WORK_HOURS_START or ≥ WORK_HOURS_END  → "off"
   - LUNCH_START ≤ hour < LUNCH_END:
       active_ratio ≥ ACTIVE_RATIO_WORKING_THRESHOLD (0.3) → "work" ★ v3 잔업 인정
       나머지 → "rest"
     ★ v3 변경: 점심에 작업 구역에서 활발히 움직이면 잔업(잔여 작업)으로 판단
   - active_ratio ≥ ACTIVE_RATIO_ZERO_THRESHOLD (0.05) → "work"
   - 나머지 (active_ratio < 0.05)                 → "off"
```

#### ⚠️ PERIOD_TYPE vs 6카테고리 관계 (2026-03-01 정비)

| 활성비율 | PERIOD_TYPE | 6카테고리 | 수정 내용 |
|---------|------------|-----------|----------|
| ≥ 0.6 | `work` | `high_work` | 변화 없음 |
| 0.15 ~ 0.6 | `work` | `low_work` | 변화 없음 |
| 0.05 ~ 0.15 | `work` ← **수정** | `standby` | 기존 `rest` → `work`로 수정 |
| < 0.05 | `off` | `off_duty` | 변화 없음 |
| REST 장소 | `rest` | `rest_facility` | 변화 없음 |
| 점심+비활성 | `rest` | (6카테고리 미분리) | 변화 없음 |
| 점심+활성(≥0.3) | `work` ★ v3 | (잔업 인정) | 기존 `rest` → `work`로 수정 (잔업) |

**설계 원칙**:
- `PERIOD_TYPE = "work"`: 작업 구역에 있고 active_ratio ≥ 0.05인 모든 경우 (현장 대기 포함)
- `PERIOD_TYPE = "rest"`: 실제 휴게 시설 또는 점심시간
- `PERIOD_TYPE = "off"`: 헬멧 걸이대 거치 또는 근무시간 외
- 6카테고리 세분화 (high/low_work/standby)는 `worker_detail._calc_time_breakdown`에서 별도 처리

---

## 6. SSMP 공간 구조 로더 (`src/data/spatial_loader.py`)

### 6-A. SpatialContext 클래스 API

```python
SpatialContext(ssmp_dir: Path)

# 집계 속성 (로드 성공 시 설정, 기본값 0)
.service_section_count  # 서비스구역 수 (매칭 가능한 장소명 수)
.zone_count             # Zone 수 (매칭 가능한 zone 수)
.spot_count             # ssmp_zones 테이블 행 수

# 메서드
.classify_place(place_name, building=None, floor=None)   → str (PlaceType)
.get_location_key(place_name=None, building=None, floor=None) → str
.calc_distance(loc_key_a, x_a, y_a, loc_key_b, x_b, y_b)    → float | None
.get_building_outdoor_coord(building_name)                → tuple[float, float] | None
.is_ssmp_matched(place_name)                              → bool
.get_place_metadata(place_name)                           → dict
.summary()                                                → str
```

### 6-B. Graceful Degradation 원칙

```
ssmp_structure/ 폴더 없음    → logger.warning → SpatialContext 생성 시도만 함
개별 CSV 읽기 실패           → logger.warning → 해당 파일 스킵
SpatialContext 없음          → place_classifier.py 키워드 매칭으로 폴백
is_ssmp_matched() == False   → 키워드 폴백 적용됨 의미
```

### 6-B-1. spot_count 속성 수정 (2026-03-01)

```
이전 (버그): self.spot_count = len(zones_df)   # ssmp_zones 행 수로 잘못 집계됨
현재 (수정): self.spot_count = len(spots_df)   # ssmp_spots.csv 행 수 (센서/장비 위치 포인트)
```

수정 위치: `spatial_loader.py` → `_load()` 메서드 마지막 집계 블록.  
`ssmp_spots.csv`를 `_safe_read`로 읽은 후 행 수를 집계하여 `spot_count`에 저장.

### 6-C. 내부 Lookup 테이블

| 딕셔너리 | 키 | 값 |
|----------|----|----|
| `_ss_lookup` | service_section_name | PlaceType |
| `_ss_ref_type` | service_section_name | ref_type |
| `_ss_location_key` | service_section_name | location_key |
| `_zone_lookup` | zone_name | PlaceType |
| `_zone_ref_type` | zone_name | ref_type |
| `_zone_location_key` | zone_name | location_key |
| `_building_coords` | building_name | (x, y) ← 현재 비어있음 |

### 6-D. LOCATION_KEY 생성 원칙

| 조건 | location_key 형식 | 가능한 연산 |
|------|-------------------|------------|
| `ref_type == "sector"` | `"OUTDOOR"` | Sector 좌표계 내 유클리드 거리 |
| `ref_type == "level"` | `"{bld_short}_L{level_index}"` | 같은 층 내 유클리드 거리 |
| 다른 location_key | — | **절대 비교 불가** → None 반환 |

**실제 데이터 예시**:
- FAB 건물 2층: `BLD-00022-002` + `level_index=2` → `"002_L2"`
- WWT 건물 B1층: `BLD-00022-001` + `level_index=1` → `"001_L1"`
- 실외 Sector: → `"OUTDOOR"`

---

## 7. 시간 카테고리 분류 로직 (`worker_detail.py`)

### 7-A. 6개 카테고리 정의

| key | 한국어 | Gantt 색상 | 시간분류 색상 | 활성비율 기준 | 설명 |
|-----|--------|-----------|-------------|--------------|------|
| `high_work` | 고활성 작업 | `#1A5276` (진파랑) | `#1A5276` | ≥ 0.6 | 이동, 운반, 체결 등 몸을 많이 쓰는 작업 |
| `low_work` | 저활성 작업 | `#5DADE2` (하늘색) | `#5DADE2` | 0.15 ~ 0.6 | 감독, 측량, 정밀 작업 등 |
| `standby` | 현장 대기 | `#F5A623` (주황) | `#F5A623` | < 0.15 | 자재·장비·지시 대기 (작업공간에 있지만 정지) |
| `transit` | 이동 | `#F7DC6F` (노랑) | `#F7DC6F` | — | GATE 통과 또는 작업 블록 사이 ≤10분 갭 |
| `rest_facility` | 휴게실 이용 | `#27AE60` (초록) | `#27AE60` | — | 물리적으로 REST 장소에 있는 시간 |
| `off_duty` | 비근무 | `#B0B8C8` (밝은회색) | `#95A5A6` (회색) | — | 근무시간 외 또는 헬멧 걸이대 |

> **색상 차이 (off_duty)**: Gantt 차트에서는 `#B0B8C8` (밝은 회색)으로 작업 색상과 대비 강화.
> 시간 분류 파이차트/바차트에서는 `#95A5A6` (표준 회색) 사용. `_ACTIVITY_COLORS`(Gantt) vs `Color.TIME_CAT`(시간분류) 참조.

**임계값** (`constants.py`):
```python
WORK_INTENSITY_HIGH_THRESHOLD = 0.6   # 고활성 작업 기준
WORK_INTENSITY_LOW_THRESHOLD  = 0.15  # 저활성 하한 / 현장 대기 기준
```

### 7-B. 분류 우선순위 (`_calc_time_breakdown` 코드 순서)

```python
1. place_type == "HELMET_RACK"              → off_duty
2. ~work_hrs_mask (근무시간 외)             → off_duty
3. rest_mask (REST 장소 OR 키워드 포함)    → rest_facility  ← 최우선 포획
4. place_type == "GATE"                     → transit
5. 근무시간 내 활성비율 기반:
     ≥ WORK_INTENSITY_HIGH_THRESHOLD (0.6)  → high_work
     ≥ WORK_INTENSITY_LOW_THRESHOLD  (0.15) → low_work
     < WORK_INTENSITY_LOW_THRESHOLD  (0.15) → standby
6. 나머지                                   → off_duty
```

### 7-C. 이동(transit) 3단계 판단 ★ v3 3단계 추가

```python
1단계: place_type == "GATE" 인 행
1.5단계 ★ v3: LOCATION_KEY 변경 감지 (직전 행과 다른 LOCATION_KEY)
  → 층간/건물간 이동은 GATE 미통과도 감지 (BLE 신호 전환만으로 판단)
  → 아직 미분류("") 행에만 적용 (기존 분류 덮어쓰기 방지)
2단계: 연속 작업 블록(calc_working_blocks) 사이의 갭 ≤ _TRANSIT_GAP_MAX_MIN (10분)
       해당 갭 내 standby/low_work 행 → transit으로 재분류
```

### 7-D. 핵심 설계 원칙

- **휴게실 이용** = 물리적으로 REST 시설에 있는 시간. 점심시간이라도 INDOOR에 있으면 rest_facility가 아님.
- **저활성 작업** ≠ 비활동. 감독, 측량 등 신체 움직임이 적어도 생산적인 작업.
- **현장 대기**: 작업공간(INDOOR)에 있지만 활성비율이 매우 낮음 → 생산성 손실 구간.
- `_REST_PLACE_KEYWORDS = ["휴게", "식당", "탈의실", "탈의", "로비"]`  
  + `ProcessedColumns.PLACE_TYPE == "REST"` 조합으로 판단.

---

## 8. Journey Gantt 블록 스키마

`_flush_block` (worker_detail), `_flush_gantt_block` (journey_review), `_flush` (journey_review 내부) 함수가 생성하는 `gantt_df` 행 구조.

| 컬럼 | 타입 | 생성 방법 | 설명 |
|------|------|-----------|------|
| `장소` | str | 직전 블록의 장소명 | worker_detail: CORRECTED_PLACE 기준, journey_review 원본탭: PLACE 기준 |
| `시작` | Timestamp | 블록 첫 행 `시간(분)` | 블록 시작 시각 |
| `종료` | Timestamp | 마지막 행 `시간(분)` + 1분 | 블록 종료 시각 (exclusive) |
| `장소유형` | str | 블록 첫 행 PLACE_TYPE | PlaceType 상수 |
| `활동상태` | str | `_classify_block_activity` 함수 | ★ 2026-03-01 추가. 6카테고리 활동 상태 (아래 참조) |
| `평균활성비율` | float | 블록 내 ACTIVE_RATIO 평균 | round(avg, 3) |
| `체류(분)` | int | 블록 내 행 수 | 1행 = 1분 |
| `고활성(분)` | int | 블록 내 ACTIVE_RATIO ≥ 0.6 인 분 수 | ★2026-02 추가 |
| `저활성(분)` | int | 블록 내 0.15 ≤ ACTIVE_RATIO < 0.6 인 분 수 | ★2026-02 추가 |

### 8-A. Gantt 블록 활동상태 색상 체계 ★ 2026-03-01 전면 개편

**변경 이유**: 기존에는 `PLACE_TYPE` (INDOOR/OUTDOOR/HELMET_RACK 등)으로 막대 색상을 구분했으나,
같은 장소에서도 활동 강도가 다를 수 있어 시각적 정보 전달이 부족했음.
이제 **활동 상태(6카테고리)** 기반으로 색상이 결정되어, 한눈에 고활성/저활성/휴식/비근무 구간을 파악 가능.

**`_classify_block_activity(place_type, avg_ratio, hour)` 분류 로직**:

```python
# 분류 우선순위 (worker_detail.py 정의)
1. place_type == "HELMET_RACK"                    → "off_duty"    (회색)
2. place_type == "REST"                           → "rest_facility" (초록)
3. place_type == "GATE"                           → "transit"     (노랑)
4. hour < WORK_HOURS_START or >= WORK_HOURS_END   → "off_duty"    (회색)
5. avg_ratio >= WORK_INTENSITY_HIGH_THRESHOLD(0.6) → "high_work"  (진파랑)
6. avg_ratio >= WORK_INTENSITY_LOW_THRESHOLD(0.15) → "low_work"   (옅은파랑)
7. avg_ratio >= ACTIVE_RATIO_ZERO_THRESHOLD(0.05)  → "standby"    (주황)
8. 나머지                                         → "off_duty"    (회색)
```

**`_ACTIVITY_COLORS` 색상 맵 (worker_detail.py 모듈 수준)**:

| 활동상태 | 색상 | 시각적 의미 |
|---------|------|------------|
| `high_work` | `#1A5276` (진한 파랑) | 활발한 작업 — 이동·운반·체결 |
| `low_work` | `#5DADE2` (옅은 파랑) | 가벼운 작업 — 감독·측량·정밀작업 |
| `standby` | `#F5A623` (주황) | 현장 대기 — 작업공간 내 정지 |
| `transit` | `#F7DC6F` (노랑) | 이동 — 게이트·구간 전환 |
| `rest_facility` | `#27AE60` (초록) | 휴식 — 휴게시설 체류 |
| `off_duty` | `#B0B8C8` (회색) | 비근무 — 거치대·근무시간 외 |

**범례**: `_LEGEND_ORDER` = `[high_work, low_work, standby, transit, rest_facility, off_duty]`
고정 순서 더미 trace로 등록 후 실제 bar trace는 `showlegend=False`.
차트 위에 HTML 색상 범례 가이드 (`_legend_html`) 별도 표시.

**적용 범위**:
- `worker_detail.py`: 전체 Journey Gantt + Journey 상세 기록 Gantt (2개 차트)
- `journey_review.py`: `_flush_gantt_block`, `_flush`, `_make_gantt_figure` (3개 함수)
- **공유**: `_classify_block_activity`, `_ACTIVITY_COLORS`, `_ACTIVITY_LABELS`는 `worker_detail.py`에 정의, `journey_review.py`에서 import하여 사용

### 8-B. AI Journey 해석 내러티브 ★ 2026-03-01 추가

**위치**: 전체 Journey Gantt 차트 바로 아래, 활성비율 시계열 위의 `🧠 AI Journey 해석` expander.

**함수**: `_generate_journey_narrative(gantt_df, worker_name) → str (HTML)`

**생성 로직 (Rule-based, LLM 불필요)**:

```
1. 작업 구간(work_phases) 식별: high_work/low_work/standby 블록을 같은 장소 기준 병합
2. 이벤트 시퀀스(events) 생성: 전체 블록 시계열
3. 내러티브 조립:
   - 출근 시간: 첫 작업 블록 시작 시각 + 장소명
   - 주요 작업 구간별: "HH:MM~HH:MM 장소에서 N분간 작업 (고활성/저활성 위주, 활성비율 XX%)"
   - 구간 사이 갭 분석:
     · rest_facility → "N분간 휴식"
     · HELMET_RACK + 11~13시 → "점심시간 추정"
     · HELMET_RACK + 다른 시간대 → "헬멧 거치 (비활성 구간)"
     · ≤10분 짧은 갭 → "이동 N분 (경유 장소)"
     · >10분 긴 갭 → "N분간 비작업 구간"
   - 마지막 작업 종료 시각 + 장소명
   - 요약: 총 작업 N분 (고활성 N분 + 저활성 N분), N개 장소 방문

스타일: 배경 #F8FAFC, 좌측 파란 보더, **bold** → <b> 변환
```

**마우스 오버(hovertemplate) 데이터 전달 방식**:

```python
# 전달 방식
customdata=[[val0, val1, val2, val3, val4, val5]]   # 2D list, 한 포인트

# 올바른 접근 (Plotly hovertemplate)
%{customdata[0]}   → val0
%{customdata[1]}   → val1
...

# 잘못된 접근 (버그 원인 — Plotly 미지원)
%{customdata[0][0]}  ← 이중 인덱싱 → 수치 미표시
```

**현재 hover 표시 항목 (전체 Journey Gantt 기준)**:
```
장소명
체류: N분
활성비율: XX.X%
상태: 고활성 작업/저활성 작업/...
고활성: N분 · 저활성: N분
장소유형: INDOOR/OUTDOOR/...
```

---

## 9. 생산성 지표 (`src/metrics/productivity.py`)

### 함수 목록 및 반환값

```python
calc_active_ratio(df)        → float      # 총 활성신호 / 총 신호 (가중평균)
calc_working_time(df)        → timedelta  # period_type=="work" 행 수 (분)
                                          # ★ 2026-03-01: standby(0.05~0.15)도 "work"에 포함
calc_idle_time(df)           → timedelta  # 근무시간 내 "off" 행 수
calc_rest_time(df)           → timedelta  # period_type=="rest" 행 수
                                          # ★ 2026-03-01: REST 시설 + 점심시간만 해당
calc_onsite_duration(df)     → timedelta  # 최초~마지막 신호 시간 차
calc_working_blocks(df)      → DataFrame  # 연속 작업 블록 목록 (work 기준)
calc_fragmentation_index(df) → float      # 블록 수 / 작업시간(시간)
calc_total_distance(df)      → dict       # 이동거리 (좌표계별 분리)
calc_transition_efficiency(df) → float    # 총 이동거리 / 체류시간
calc_productivity_summary(df) → dict      # 위 모든 지표 통합
```

### `calc_total_distance` 반환 dict 구조

```python
{
  "total":                  float,  # 전체 이동 거리 합계 (같은 LOCATION_KEY 내만 합산)
  "indoor_distance_total":  float,  # INDOOR 좌표계들의 이동 거리 합계
  "outdoor_distance":       float,  # OUTDOOR 좌표계 이동 거리
  "inter_building_distance": 0.0,   # 건물 간 추정 거리 (현재 미구현, 향후 활성화)
  "distance_by_location":   dict,   # {location_key: distance} 맵
  "note":                   str,    # "층 간 이동 미포함. 같은 층 내 좌표만 집계."
}
```

> **거리 계산 제약**: 같은 `LOCATION_KEY` 내에서만 유클리드 거리 계산 가능.  
> 다른 건물/층 간 이동 거리는 `ssmp_buildings.csv`에 outdoor 좌표가 없어 현재 0으로 처리됨.

---

## 10. 안전성 지표 (`src/metrics/safety.py`)

### 헬멧 준수율 완전 제거 (2026-02)

**제거 이유**: BLE 신호만으로는 헬멧 착용 여부를 신뢰성 있게 추정 불가.  
현장에 있으면 항상 신호가 수신되므로 "신호갯수 > 0" ≈ 출근 여부에 불과.  
헬멧 미착용 구분은 별도의 물리적 센서(압력 센서 등) 없이는 구현 불가.

**제거된 코드/설정**:
- `calc_helmet_compliance()` 함수 삭제
- `calc_helmet_compliance_by_hour()` 함수 삭제
- `helmet_compliance`, `helmet_status` 반환값 삭제 (`calc_safety_summary`)
- Pipeline KPI 카드 "평균 헬멧 준수율" → "평균 활성비율"로 교체
- Safety Alert 헬멧 탭 삭제
- Overview 헬멧 KPI 카드 삭제
- `constants.py` `HELMET_COMPLIANCE_WARNING`, `HELMET_COMPLIANCE_DANGER` 삭제

### 현재 안전성 함수

```python
calc_fatigue_risk(df) → float
  # risk_score = Σ(블록 초과분 / 120분), 1.0 이상이면 HIGH

detect_anomaly_movement(df) → DataFrame
  # 이상 이동 패턴 감지: 급격한 위치 전환 / 장시간 비활성 정지
  # 컬럼: timestamp, anomaly_type, description, severity, place
  # ★ 호출 위치 (2026-03-01 명확화):
  #   - calc_safety_summary() 내부에서 호출 → anomaly_count 등 집계에 사용
  #   - safety_alert.py는 calc_safety_summary() 결과만 사용 (직접 호출 안 함)
  #   - journey_review.py에서 별도 호출 가능 (보정 전/후 이상 비교용)

calc_alone_risk(df_all, worker_key, radius=50) → float
  # 근무시간 내 반경 50 내 다른 작업자 없는 비율 (0.0~1.0)
  # ★ 2026-03-01 수정: LOCATION_KEY 필터 + NaN 가드 추가
  #   - 같은 LOCATION_KEY 내에서만 거리 비교 (다른 좌표계 혼합 방지)
  #   - 자신의 LOCATION_KEY 또는 좌표가 NaN이면 해당 분(分)은 스킵
  #   - 분모 = 유효한 분 수(valid_count), 기존엔 전체 work_df 행 수

calc_safety_summary(df, df_all=None) → dict:
  fatigue_risk          : float (0.0~∞)
  fatigue_status        : "HIGH" / "MEDIUM" / "LOW"
  anomaly_count         : int (detect_anomaly_movement 결과)
  abnormal_stop_count   : int (60분 이상 비활성 정지)
  rapid_transition_count: int (급격한 위치 전환)
  alone_risk            : float (0.0~1.0, df_all 있을 때만)
  alone_status          : "HIGH"(≥70%) / "MEDIUM"(≥40%) / "LOW"
```

---

## 11. 집계 레이어 (`src/metrics/aggregator.py`)

```python
aggregate_by_worker(df)          → DataFrame  # 작업자별 생산성+안전성 지표 집계
aggregate_by_company(df)         → DataFrame  # 업체별 평균 지표 (컬럼: "company")
aggregate_by_date(df)            → DataFrame  # 날짜별 전체 현황 요약 (1행)
get_zone_density_by_hour(df)     → DataFrame  # {hour, location_key, place, worker_count}
get_worker_journey_summary(df, worker_key) → dict  # 단일 작업자 요약
get_place_dwell_time(df)         → DataFrame  # {worker_key, place, dwell_min}
get_active_ratio_timeseries(df, worker_key) → DataFrame  # 시계열
```

> **중요**: `aggregate_by_company` 결과 df의 업체 컬럼명은 `"company"` (영문).  
> `RawColumns.COMPANY = "업체"` (한글) 와 다름. `company.py`에서 직접 `"company"` 사용.

---

## 12. 드릴다운 분석 엔진 (`src/metrics/drill_down.py`)

### `analyze_idle_episodes(df, worker_key) → DataFrame`

근무시간 내 idle 구간을 에피소드 단위로 분해.

| 반환 컬럼 | 설명 |
|-----------|------|
| `start_time`, `end_time` | 에피소드 시작/종료 Timestamp |
| `duration_min` | 지속 시간 (분) |
| `location` | 주요 발생 장소 (보정장소 기준) |
| `place_type` | 장소 유형 |
| `cause` | 원인 분류 (아래 참조) |
| `active_ratio_avg` | 평균 활성비율 |

**cause 분류 우선순위**:
```
helmet_off  ← active_ratio_avg ≤ 0.05 AND place_type != HELMET_RACK
transition  ← place_type==GATE OR 에피소드 내 장소 변화 있음
waiting     ← active_ratio 0.05~0.15 AND 같은 장소 10분 이상
slow_work   ← active_ratio 0.15~0.3 AND 이동 없음
unknown     ← 위 조건 모두 미해당
```

### `analyze_work_blocks(df, worker_key) → DataFrame`

연속 작업 블록 상세 분해.

| 반환 컬럼 | 설명 |
|-----------|------|
| `block_id` | 블록 번호 (1부터 시작) |
| `start_time`, `end_time` | 블록 시작/종료 |
| `duration_min` | 지속 시간 (분) |
| `location_sequence` | 이동 장소 순서 (최대 5개, `→` 구분) |
| `intensity` | `high`(≥0.6) / `medium`(0.3~0.6) / `low`(<0.3) |
| `avg_active_ratio` | 평균 활성비율 |
| `interrupted_by` | 블록 종료 원인 (다음 카테고리) |

### `analyze_fatigue_pattern(df, worker_key) → dict`

```python
{
  "risk_segments":        list[dict],  # 120분 초과 구간 목록
  "break_gaps":           list[dict],  # 휴식 구간 (proper≥15분 / short / micro)
  "longest_no_break_min": int,         # 최대 연속 작업 시간
  "recovery_score":       float,       # 적절한 휴식 비율 (0~1, 1에 가까울수록 좋음)
}
```

### `generate_worker_insight(df, worker_key, prod_summary, safety_summary) → list[dict]`

Rule-based 자동 인사이트 생성. LLM 불필요.

```python
# 반환 항목 구조
{"type": "warning"|"info"|"positive", "title": str, "message": str}

# 생성 규칙 (8가지)
idle 연속 ≥ 60분           → warning  "장시간 비활동 감지"
cause == "helmet_off"      → warning  "헬멧 미착용 의심"
작업 블록 ≥ 180분          → warning  "장시간 연속 작업"
fragmentation ≥ 5          → info     "작업이 자주 끊김"
active_ratio ≥ 0.7         → positive "높은 작업 집중도"
rest_time == 0분            → info     "휴게실 미방문"
점심시간 idle              → info     "점심시간 비활동"
근무외 활동 ≥ 10분         → info     "정규 근무시간 외 활동"
```

---

## 13. 다중 날짜 트렌드 분석 (`src/metrics/trend_analyzer.py`)

```python
calc_worker_trend(df_multi, worker_key) → DataFrame
  # 작업자의 날짜별 지표 트렌드 (1행 = 1날짜)

calc_company_trend(df_multi, company) → DataFrame
  # 업체의 날짜별 평균 지표 트렌드

calc_site_daily_summary(df_multi) → DataFrame
  # 전체 현장의 날짜별 요약 (작업자 수, 평균 활성비율 등)

detect_trend_anomaly(trend_df, metric_col, window=3) → DataFrame
  # 이동평균 ±1.5σ 기반 이상 날짜 감지
  # is_anomaly 컬럼 추가. 최소 3날짜 데이터 필요

compare_two_dates(df_multi, date_a, date_b) → dict
  # {metric: {date_a: va, date_b: vb, "delta": delta, "delta_pct": pct}}
```

---

## 13-B. SOIF 운영 인텔리전스 지표 (`src/metrics/soif.py`) ★ 2026-03-01 신규

> **⚠️ Feasibility 검증 단계**  
> 현재 2명 작업자 샘플 데이터로 개념 검증 중.  
> 대규모 데이터(수십~수백 명) 적용 시 파라미터 튜닝 및 지표 재검증 필요.  
> 특히 CRE의 Dynamic Pressure, Bottleneck Score의 임계값은 실 데이터 분포 확인 후 조정 필요.

### 아키텍처 개요

```
Layer 1: Journey Layer        ← preprocessor.py (기존 구현)
Layer 2: Behavioral Inference ← worker_detail._calc_time_breakdown (기존 구현)
Layer 3: Spatial State         ← soif.build_zone_time_table()      ★ 신규
Layer 4: Flow State            ← soif.build_flow_edge_table()      ★ 신규
                                 soif.calc_bottleneck_scores()      ★ 신규
Layer 5: Operational Intelligence
         ← soif.calc_ewi()                  (유효 작업 집중도)      ★ 신규
         ← soif.calc_ofi()                  (운영 마찰 지수)        ★ 신규
         ← soif.calc_cre()                  (복합 위험 노출도)      ★ 신규
         ← soif.calc_zone_utilization()     (구역 유효 활용도)      ★ 신규
```

### 13-B-1. EWI (Effective Work Intensity, 유효 작업 집중도)

```python
# ★ v6.6 변경: 하루 전체 Journey 기반 출퇴근 감지 + 분자/분모 일관 집계
EWI = (합계 High Work Time × 1.0 + 합계 Low Work Time × 0.5) / 합계 Work Duration

# 출퇴근 시점 감지 (Layer 5-0)
detect_work_shift(df) → dict:
  clock_in_idx: int          # 출근 시점 행 인덱스
  clock_out_idx: int         # 퇴근 시점 행 인덱스
  clock_in_time: Timestamp   # 출근 시각
  clock_out_time: Timestamp  # 퇴근 시각
  work_duration_min: int     # 출근~퇴근 사이 분 (행 기준)
  pre_work_min: int          # 출근 전 행 수 (off-duty 분)
  post_work_min: int         # 퇴근 후 행 수 (off-duty 분)

  # 로직(v6.6):
  #  1) ACTIVE_RATIO 기반 활동 Run 생성 (ACTIVE_RATIO ≥ LOW_THRESHOLD, PLACE_TYPE != HELMET_RACK)
  #  2) Run 사이에 4시간 이상 공백이 있으면 서로 다른 Shift로 분리
  #  3) 각 Shift별 고/저활성 시간을 합산하고, 가장 활동량이 큰 Shift를 당일 근무로 선택
  #  4) 선택된 Shift의 첫 Run 시작 시점을 clock_in, 마지막 Run 끝 시점을 clock_out 으로 사용
  #  → 하루 전체 Journey(문장)를 보고 전날 퇴근 꼬리 + 긴 off-duty + 당일 근무를 구분

calc_ewi(df, worker_key=None) → dict:
  ewi: float               # 0.0~1.0 (0.4 이상 양호)
  high_work_min: int       # 고활성 작업 분
  low_work_min: int        # 저활성 작업 분
  standby_min: int         # 현장 대기 분
  transit_min: int         # 이동 분
  rest_min: int            # 휴게 분
  off_duty_min: int        # 비근무 분 (출근 전 + 퇴근 후)
  onsite_min: int          # 전체 현장 체류 분
  work_duration_min: int   # ★ 출근~퇴근 사이 분 (EWI 분모)
  recorded_min: int        # 기록된 분 수 (coverage gap 제외)
  gap_min: int             # 음영지역 분 (work_duration_min - recorded_min)
  effective_work_min: int  # 근무 중 RACK 제외한 실효 분
  clock_in_time: Timestamp # 출근 시각
  clock_out_time: Timestamp# 퇴근 시각

calc_ewi_by_worker(df) → DataFrame    # 전체 작업자별 EWI (각자 shift 기반)
calc_ewi_by_company(df) → DataFrame   # 업체별 평균/최대/최소 EWI (작업자별 EWI의 가중 평균)

# 사이트 단위 EWI 집계 (v6.6):
calc_soif_summary(df) 내부:
  worker_ewi = calc_ewi_by_worker(df)
  numerator   = Σ( high_work_min_i + 0.5 × low_work_min_i )
  denominator = Σ( work_duration_min_i )
  site_ewi    = min( numerator / denominator, 1.0 )
```

**해석**: 현장에 머무는 시간 중 실제 생산 활동에 투입된 순수 몰입도.
고활성(이동·운반·체결)에 가중치 1.0, 저활성(감독·측량)에 0.5를 부여.

**v6.6 변경 요약**:
- 출퇴근 시점을 HELMET_RACK 기반이 아닌 **하루 전체 활동 Run + 긴 공백(4h+)** 을 보고 탐지.
- 전날 퇴근 꼬리(짧은 활동 + 긴 off-duty)는 자동으로 제외되고, 실제 근무 블록이 Shift로 선택됨.
- 사이트 EWI는 작업자별 EWI 계산 후, 분자/분모를 모두 합산해 다시 계산하여 **수학적으로 100%를 넘지 않도록 보장**.

### 13-B-2. Zone-Time Table (Layer 3: Spatial State)

```python
build_zone_time_table(df, time_slot_min=60) → DataFrame:
  zone: str                # 구역명 (CORRECTED_PLACE 기준)
  time_slot: int           # 시간 슬롯 (0~23시 또는 분 기준)
  worker_count: int        # 해당 슬롯 내 작업자 수 (unique)
  total_person_min: int    # 총 인·분 (1행=1분, 합산)
  high_work_count: int     # 고활성 상태 분
  low_work_count: int      # 저활성 상태 분
  standby_count: int       # 대기 상태 분
  transit_count: int       # 이동 상태 분
  rest_count: int          # 휴게 상태 분
  off_duty_count: int      # 비근무 상태 분
  avg_active_ratio: float  # 평균 활성비율
  zone_utilization: float  # (고활성+저활성) / 전체 = 생산적 활용 비율
```

**용도**: Zone-Time Heatmap, 구역별 시간대 밀집도 분석, Zone Utilization 산출.

### 13-B-3. Flow Edge Table (Layer 4: Flow State)

```python
build_flow_edge_table(df) → DataFrame:
  from_zone: str               # 출발 구역
  to_zone: str                 # 도착 구역
  transition_count: int        # 전이 횟수
  avg_transition_gap_min: float # 평균 전이 간격 (분)
  unique_workers: int          # 관련 작업자 수

# 생성 방법:
# 각 작업자 시계열에서 CORRECTED_PLACE 변경 순간을 추출
# (from, to) 쌍으로 엣지 생성 → 전체 집계
```

### 13-B-4. Bottleneck Score (병목 점수)

```python
calc_bottleneck_scores(zone_time_df, flow_edge_df) → DataFrame:
  zone: str
  total_person_min: int
  standby_total: int
  inflow: int            # 유입 전이 횟수
  outflow: int           # 유출 전이 횟수
  flow_imbalance: int    # inflow - outflow
  flow_imbalance_norm: float  # 정규화 (0~1)
  standby_pressure: float     # 정규화 (0~1)
  bottleneck_score: float     # BS = 흐름불균형(0.6) + 대기부하(0.4)

# BS 해석:
#   ≥ 0.6  → 위험 (인원 정체 심각)
#   0.3~0.6 → 주의 (흐름 불균형 또는 대기 과다)
#   < 0.3  → 양호
```

### 13-B-5. Zone Utilization (구역 유효 활용도)

```python
calc_zone_utilization(zone_time_df) → DataFrame:
  zone: str
  total_person_min: int
  productive_min: int        # 고활성 + 저활성 합
  utilization: float         # productive / total (0~1)
  waste_ratio: float         # standby / total (0~1)
  avg_active_ratio: float
  worker_count: int
```

### 13-B-6. CRE (Combined Risk Exposure, 복합 위험 노출도)

```python
CRE = Personal Risk × Static Space Risk × Dynamic Pressure

calc_cre(df, worker_key=None) → dict:
  cre: float              # 종합 위험 노출도 (1.0 이상 주의)
  personal_risk: float    # 0.5×피로 + 0.5×고립
  fatigue_score: float    # 연속 작업 최대 길이 / 120분 (cap 2.0)
  alone_score: float      # 같은 시간·장소에 다른 작업자 없는 비율
  static_risk: float      # 구역 고유 위험 가중치 평균
  dynamic_pressure: float # 밀집도 기반 (cap 2.0)

calc_cre_by_worker(df) → DataFrame  # 전체 작업자별 CRE + 구성요소

# Static Space Risk 가중치 (_STATIC_RISK):
#   CONFINED_SPACE: 2.0  |  WORK_AREA: 1.2
#   OUTDOOR: 1.1         |  INDOOR: 1.0
#   GATE: 0.8            |  REST/OFFICE/HELMET_RACK: 0.2~0.3
```

### 13-B-7. OFI (Operational Friction Index, 운영 마찰 지수)

```python
OFI = (Standby Time + Excess Transit Time) / Total On-site Duration

# Excess Transit = max(0, transit_time - onsite × 10%)
# 이동이 전체의 10% 이하면 정상, 초과분만 마찰로 계산

calc_ofi(df, company=None) → dict:
  ofi: float              # 0.0~1.0 (0.1 이하 양호)
  standby_min: int
  transit_min: int
  excess_transit_min: float
  onsite_min: int
  transit_pct: float

calc_ofi_by_company(df) → DataFrame  # 업체별 OFI
```

### 13-B-8. 통합 SOIF Summary

```python
calc_soif_summary(df) → dict:
  site_ewi: dict          # 현장 전체 EWI
  site_ofi: dict          # 현장 전체 OFI
  avg_cre: float          # 전체 작업자 평균 CRE
  max_cre: float          # 최대 CRE
  worker_count: int
  zone_count: int
  flow_edge_count: int
  top_bottlenecks: list    # 상위 5 병목 구역
  zone_time_df: DataFrame  # Zone-Time Table 원본
  flow_edge_df: DataFrame  # Flow Edge Table 원본
  bottleneck_df: DataFrame # Bottleneck Score 원본
  zone_util_df: DataFrame  # Zone Utilization 원본
  ewi_by_worker: DataFrame
  ewi_by_company: DataFrame
  cre_by_worker: DataFrame
  ofi_by_company: DataFrame
```

---

## 14. 멀티 날짜 캐시 (`src/data/cache_manager.py`)

```python
load_multi_date_cache(dates, cache_dir) → DataFrame
  # 여러 날짜 Parquet 병합. 누락 날짜는 경고 후 스킵

get_date_cache_status(data_dir, cache_dir) → DataFrame
  # Raw 폴더 vs 캐시 비교
  # status: "synced" | "needs_processing" | "cache_only"

class ParquetCacheManager:
  save(df, date_str)  → Parquet 저장 + 스키마 버전 메타데이터 기록
  load(date_str)      → DataFrame 로드 + 스키마 버전 검증 + 누락 컬럼 보완
```

### 캐시 스키마 버전 관리 (2026-03-01 추가)

```python
CACHE_SCHEMA_VERSION = "3"   # constants 아닌 cache_manager.py 모듈 상수

# Parquet 파일 저장 시 커스텀 메타데이터로 기록
metadata = {
    "cache_schema_version": "3",
    "processed_date": "20260225",
}
```

**버전 히스토리**:

| 버전 | 도입 시점 | 주요 변경 |
|------|---------|---------|
| `"1"` | 초기 | 기본 파이프라인 (헬멧거치 + 노이즈 + 좌표 보정) |
| `"2"` | 2026-02-28 | SSMP(`ssmp_matched`) 추가, PERIOD_TYPE 경계값 변경(standby → work) |
| `"3"` | 2026-03-01 | `coverage_gap`, `signal_confidence` 추가. `SPATIAL_CLUSTER`/`CLUSTER_PLACE` 캐시 저장. Step 5-post 재분류, nearest-cluster 노이즈 보정, 점심 잔업, 좌표↔장소 정합성 검증 |

**불일치 처리 원칙**:
- 경고 로그만 출력하고 앱 실행은 계속 (강제 종료 없음)
- `_REQUIRED_COLUMNS_V2` 딕셔너리에서 누락 컬럼을 기본값으로 추가
- Pipeline에서 재처리 권장 메시지 표시

**버전을 올려야 하는 경우**:
- 전처리 결과에 새 컬럼 추가/제거 시
- PERIOD_TYPE 등 핵심 분류 로직 변경 시

---

## 14-B. 전체 코드 리팩토링 (2026-03-02)

### 삭제된 파일

| 카테고리 | 삭제 파일 | 사유 |
|----------|-----------|------|
| 레거시 페이지 | `overview.py`, `company.py`, `trend.py`, `safety_alert.py`, `soif_dashboard.py` | `site_analysis.py`로 통합 |
| 레거시 페이지 | `worker_detail.py` | 공통 함수 추출 후 삭제 (`place_classifier.py`로 이동) |
| 미사용 시각화 | `visualization/timeline.py`, `visualization/heatmap.py`, `visualization/kpi_cards.py` | 미사용 |

### 공통 함수 이동

| 원본 위치 | 이동 위치 | 함수/상수 |
|-----------|-----------|-----------|
| `worker_detail.py` | `place_classifier.py` | `classify_block_activity()` |
| `worker_detail.py` | `constants.py` | `TIME_CATEGORY_COLORS`, `TIME_CATEGORY_LABELS` (기존 상수 활용) |

### 상수 정리

| 조치 | 상수 |
|------|------|
| 주석 처리 | `T_WARD_*` (도메인 참조용) |
| 삭제 | `CONGESTION_WARNING_COUNT`, `CONGESTION_DANGER_COUNT` (미사용) |
| 삭제 | `COLOR_*` Deprecated 상수, `PLACE_COLORS` |
| 추가 | `ProcessedColumns.SPATIAL_CLUSTER`, `ProcessedColumns.CLUSTER_PLACE` |

### 하드코딩 수정

| 파일 | 수정 내용 |
|------|----------|
| `preprocessor.py` | `"SPATIAL_CLUSTER"` → `ProcessedColumns.SPATIAL_CLUSTER` |
| `schema.py` | `CACHE_COLUMNS` 상수 참조로 변경 |

---

## 14-C. UI v2.0 재설계 (2026-03-02)

### 변경 포인트 3가지

**① 메뉴 8개 → 4개 압축**

```
기존 (8개)                      →    변경 후 (4개)
──────────────────────────────────────────────────────
⚙️ Pipeline          (내부용)        [Admin 탭으로 숨김]
🔧 Journey Review    (내부용)        [Admin 탭으로 숨김]
🏗️ Overview                    →    📊 현장 분석 (전체 탭)
👷 Worker Detail                →    📊 현장 분석 (작업자별 탭)
🏢 Company                      →    📊 현장 분석 (업체별 탭)
📈 Trend                        →    📊 현장 분석 (추이 탭)
🚨 Safety Alert                 →    📊 현장 분석 (작업자별 탭 내 통합)
🧠 SOIF Intelligence            →    🔮 확장 가능성
```

**최종 메뉴**:
| 순서 | 아이콘 | 이름 | 역할 | 고객 질문 |
|------|--------|------|------|-----------|
| 1 | 🔍 | Journey 검증 | 보정 전/후 비교, 로직 투명성 | Q1: 보정이 제대로 됐나? |
| 2 | 📊 | 현장 분석 | 지표 + 맥락 (4개 서브탭) | Q2·Q3: 지표 + 현장 의미 |
| 3 | 🔮 | 확장 가능성 | 미래 기능 Preview (Mock) | Q4: 앞으로 뭘 더 할 수 있나? |
| 4 | ⚙️ | Admin | 내부용 (접힘) | — |

**② 토글 원칙 통일**

모든 계산 로직은 `st.expander`로 기본 닫힘.
담당자가 원할 때만 열어서 "이 숫자가 어떻게 나왔는지" 직접 확인 가능.

**③ Mock Preview 명시**

Phase 2·3 기능은 Mock임을 명시하면서 시각화 → 비전은 전달하되 신뢰도 훼손 없음.

### 신규 파일

| 파일 | 역할 |
|------|------|
| `src/pages/journey_verify.py` | 🔍 Journey 검증 — 보정 전/후 Gantt 비교, space_function 기준표 |
| `src/pages/site_analysis.py` | 📊 현장 분석 — 4개 서브탭 (전체/작업자별/업체별/추이) |
| `src/pages/future_preview.py` | 🔮 확장 가능성 — 현재 구현 + Phase 2/3 Mock Preview |

### main.py 사이드바 구조

```python
NAV_MAIN = {
    "journey_verify": ("🔍", "Journey 검증",    "Q1: 보정 검증"),
    "site_analysis":  ("📊", "현장 분석",       "Q2·Q3: 지표 + 맥락"),
    "future_preview": ("🔮", "확장 가능성",     "Q4: 미래 기능"),
}

with st.expander("⚙️ Admin", expanded=False):
    admin_pages = {
        "pipeline":      ("⚙️", "Pipeline"),
        "journey_debug": ("🔧", "Journey Debug"),
        "space_config":  ("🗺️", "공간 속성"),    # ★ 2026-03-02 추가
    }
```

---

## 14-D. Journey 시각화 개선 (2026-03-02)

### 변경 사항

**① Gantt 차트 축 고정**

| 항목 | 변경 전 | 변경 후 |
|------|---------|---------|
| X축 | 데이터 존재 시간대만 표시 | **00:00~24:00 고정** |
| Y축 | 해당 작업자 방문 장소만 표시 | **전체 장소 목록 고정** (원본+보정 통합) |

→ 보정 전/후 두 차트를 직접 비교 가능

**② 활동 상태 색상 통일 (6개)**

| 상태 | 색상 | 코드 | 설명 |
|------|------|------|------|
| 고활성 작업 | 🔵 진파랑 | `#1E5AA8` | active_ratio ≥ 60% |
| 저활성 작업 | 🔵 하늘색 | `#6FA8DC` | active_ratio 15~60% |
| 대기 | 🟡 노랑 | `#F4D03F` | active_ratio < 15% |
| 이동 | 🟠 주황 | `#E67E22` | GATE/CORRIDOR |
| 휴게 | 🟢 초록 | `#27AE60` | REST 장소 |
| 비근무 | ⚪ 연회색 | `#BDC3C7` | RACK/야간 |

→ `constants.py`의 `ACTIVITY_COLORS`, `ACTIVITY_LABELS` 사용

**③ 보정 포인트 마커**

- **표시 방식**: Gantt 바 위에 작은 빨간색 점(●)
- **크기**: size=6, line width=0.5
- **Legend**: "● 보정 포인트"
- **Hover**: 원본 장소, 보정 장소, 활성비율 표시

**④ 데이터 공백 처리**

- `max_gap_min=5`: 5분 이상 데이터 공백 시 별도 블록으로 분리
- 점심시간 등 장시간 공백을 강제로 연결하지 않음
- `_build_gantt` 함수에서 시간순 정렬 필수 (`sorted_df`)

### 수정된 파일

| 파일 | 수정 내용 |
|------|-----------|
| `constants.py` | `ACTIVITY_COLORS`, `ACTIVITY_LABELS` 추가 (TIME_CATEGORY_* 레거시 별칭 유지) |
| `journey_verify.py` | `_build_gantt` 시간순 정렬, `_make_gantt_figure` legend 개선, `_add_correction_markers` 마커 스타일 |
| `journey_review.py` | `_build_gantt` 시간순 정렬, `ACTIVITY_COLORS` import |
| `site_analysis.py` | `_build_gantt` 이미 정렬됨 (확인 완료) |

---

## 14-E. 공간 속성 페이지 추가 (2026-03-02)

### 신규 파일

`src/pages/space_config.py` — Admin 전용 공간 속성 관리 페이지

### 기능

| 탭 | 내용 |
|----|------|
| 📋 Space Function 정의 | 9개 분류 체계 설명 + 키워드 매핑 테이블 |
| 🏗️ 현재 데이터 장소 목록 | 실제 장소별 space_function, hazard_weight, 기록수 집계 |
| ⚙️ 파라미터 테이블 | HAZARD_WEIGHT, DWELL_NORMAL_MAX, ABNORMAL_STOP_THRESHOLD, ALONE_RISK_MULTIPLIER |

### Space Function 분류 (9개)

| 코드 | 이름 | 위험도 | 체류한도 | 키워드 예시 |
|------|------|--------|----------|-------------|
| `WORK` | 실내 작업공간 | 0.3 | 무제한 | FAB, CUB, WWT |
| `WORK_HAZARD` | 고위험 작업공간 | 1.0 | 60분 | 밀폐, 맨홀, 탱크 |
| `TRANSIT_WORK` | 실외 공사/이동 | 0.5 | 30분 | 공사현장, 야적장 |
| `TRANSIT_GATE` | 출입구/타각기 | 0.2 | 5분 | 게이트, 정문 |
| `TRANSIT_CORRIDOR` | 이동통로 | 0.3 | 10분 | 통로, 복도, 계단 |
| `REST` | 휴게시설 | 0.0 | 60분 | 휴게, 식당, 탈의실 |
| `RACK` | 헬멧 거치대 | 0.0 | 무제한 | 걸이대, 보호구 |
| `OUTDOOR_MISC` | 실외 기타 | 0.5 | 30분 | 주차장 |
| `UNKNOWN` | 미분류 | 0.3 | 30분 | — |

---

## 14-F. 장소 전환 이동 감지 (Transition Travel Detection) ★v5 (2026-03-02)

### 문제 정의

BLE 신호는 1분 단위로 기록. km 단위 현장에서 장소가 바뀌면
물리적으로 반드시 이동 시간이 발생하지만, 현재는 이것이 무시됨.

```
현재 (문제):
  13:00  헬멧 거치대  space_function=RACK    → off_duty
  13:01  FAB 1F      space_function=WORK    → low_work  ← 이동 시간 0분 (불가능)

목표 (해결):
  13:00  헬멧 거치대  space_function=RACK    → off_duty
  13:01  FAB 1F      state_detail=transit_arrival → transit
  ...  (N분 동안)
  13:11  FAB 1F      space_function=WORK    → low_work  ← 실제 작업
```

### 핵심 설계 원칙 3가지

**원칙 1: 흔적 있는 이동 vs 흔적 없는 이동**
```
흔적 있는 이동 (스킵):
  - CORRECTED_PLACE에 "[이동]" 접두사
  - space_function == "TRANSIT_GATE"
  - state_detail이 이미 "transit"

흔적 없는 이동 (처리 대상):
  - 연속된 두 행에서 CORRECTED_PLACE가 바뀌었는데
  - 그 사이에 위의 흔적이 전혀 없는 경우
  → "abrupt transition" = 이번에 처리할 것
```

**원칙 2: 이동 규모를 LOCATION_KEY로 판단**
```
prev_LOCATION_KEY != curr_LOCATION_KEY
  → 건물/층 간 이동 = TRANSITION_INTER_LOCATION_MIN (10분)

prev_LOCATION_KEY == curr_LOCATION_KEY
  → 같은 층 내 이동 = TRANSITION_SAME_LOCATION_MIN (3분)
```

**원칙 3: 태깅 상한으로 과도한 태깅 방지**
```
N = min(travel_mins, 해당 장소 연속 체류 시간 × TRANSITION_MAX_RATIO)

예시: FAB에 3분만 있다가 떠나는 경우
  travel_mins=10 → 실제 태깅 = min(10, 3×0.5) = 1분
```

### 상수 정의 (`constants.py`)

| 상수 | 값 | 적용 조건 |
|------|---|----------|
| `TRANSITION_INTER_LOCATION_MIN` | 10분 | LOCATION_KEY 변경 (건물/층 간) |
| `TRANSITION_SAME_LOCATION_MIN` | 3분 | 같은 LOCATION_KEY 내 장소 변경 |
| `TRANSITION_FROM_ENTRY_MIN` | 10분 | 출발지가 RACK 또는 TRANSIT_GATE |
| `TRANSITION_FROM_REST_MIN` | 5분 | 출발지가 REST |
| `TRANSITION_MAX_RATIO` | 0.5 | 체류 시간의 최대 50%까지만 태깅 |

### 구현 함수 (`preprocessor.py`)

```python
def _calc_consecutive_dwell(places: np.ndarray) -> np.ndarray:
    """각 행에서 시작하는 연속 체류 길이. [A,A,A,B,B,A] → [3,2,1,2,1,1]"""

def _estimate_travel_mins(prev_sf, prev_loc_key, curr_loc_key) -> int:
    """출발 공간 유형과 LOCATION_KEY 변화로 이동 시간 추정."""

def _tag_transition_travel(df) -> pd.DataFrame:
    """장소 전환 시 이동 도착 시간을 transit_arrival로 태깅."""
```

### 파이프라인 위치

```
Step 6: _classify_activity_period(result)
  → 기본 PERIOD_TYPE, state_detail 분류
  → ★ Step 6-post: _tag_transition_travel(result)  ← 여기
```

### PERIOD_TYPE 매핑

`transit_arrival` → `work` (현장 투입 시간에 포함)
- 이동도 업무의 일부로 간주
- 6카테고리 시각화에서는 `transit`(주황)으로 표시

### 신규 지표 (`productivity.py`)

| 함수 | 설명 |
|------|------|
| `calc_transit_time(df)` | 전체 이동 시간 (GATE + transition_arrival) |
| `calc_transit_ratio(df)` | 현장 체류 시간 대비 이동 시간 비율 |
| `calc_transit_breakdown(df)` | 이동 시간 세부 분류 (gate/arrival/other) |

### 비즈니스 가치

```
transit_ratio = 12% →
  "작업자가 하루 현장 체류 시간의 12%를 이동에 소비합니다.
   작업 배치 최적화 또는 현장 내 셔틀 도입으로
   약 50분의 추가 생산 시간 확보가 가능합니다."
```

이동 시간 세부 분류 예시:
```
현재 황*석 transit 52분 (GATE 통과 기준만)

전환 태깅 적용 후:
  GATE 통과:    52분
  장소 전환 이동: +45분 (추정)
  총 이동:      97분  ← 현장 체류의 약 12%
```

---

## 15. 페이지별 기능 상세

### 15-A. ⚙️ Pipeline (`pipeline.py`)

**역할**: Raw CSV → 전처리(SSMP+보정) → Parquet 캐시 저장.

**주요 함수**:

| 함수 | 설명 |
|------|------|
| `render(...)` | 메인 렌더링 (날짜 선택, 실행 버튼, 결과 요약) |
| `_load_spatial_context(datafile_root)` | SpatialContext 생성. 실패 시 None 반환 + 경고 표시 |
| `_show_ssmp_match_summary(processed_df, spatial_ctx)` | SSMP 매칭 비율 + 미매칭 장소 목록 표시 |
| `_run_pipeline(date_str, ...)` | 단일 날짜 전처리 실행 (진행 표시 포함) |
| `_render_result_summary(...)` | 처리 결과 요약 표시 |
| `_render_stage1_result(df)` | Stage 1 보정 결과 (보정 통계 3-카테고리) |
| `_render_stage2_result(worker_df)` | Stage 2 작업자별 지표 요약 |
| `_render_stage3_preview(df, worker_df)` | Stage 3 미리보기 |
| `_render_batch_section(...)` | 일괄 처리 UI (미처리 날짜 자동 감지) |

**SSMP 연동 흐름** (2026-02 구현):

```python
# _run_pipeline 내부 순서
spatial_ctx = _load_spatial_context(datafile_root)  # 1. SSMP 로드
raw_df = load_date_folder(folder)                    # 2. Raw CSV 로드
processed_df = preprocess(raw_df, spatial_ctx=spatial_ctx)  # 3. 전처리 (SSMP 전달)
_show_ssmp_match_summary(processed_df, spatial_ctx)  # 4. 매칭 결과 표시
mgr.save(processed_df, date_str)                     # 5. 캐시 저장
```

**배치 처리**: SpatialContext는 날짜 루프 밖에서 1회만 생성 후 재사용.

**UI 진행 표시**:
```
3%  → SSMP 공간 구조 로드
8%  → Raw CSV 로드
20% → 전처리 (SSMP + 보정)
45% → Stage 1 완료 (보정 결과 표시)
55% → Stage 2 (지표 계산)
75% → Stage 2 완료
85% → Stage 3 (캐시 저장)
100%→ 완료
```

---

### 15-A-1. 🔍 Journey 검증 (`journey_verify.py`) ★ v2.0 신규

**역할**: 고객 Q1 "Journey 데이터가 제대로 보정되었나요?" 에 답변.

**핵심 컴포넌트**:
- 보정 전/후 Gantt 차트 나란히 비교
- 보정 요약 (전체 기록, 장소 변경 건수, 변경률)
- `st.expander` 내 보정 로직 설명 (DBSCAN, 헬멧 거치, 좌표 보정)
- `space_function` 분류 기준 테이블 (항상 노출)

**고객 신뢰 포인트**:
- 보정 전/후 Gantt 동일 스케일 → 변경 부분 시각적 확인
- 보정 로직이 합리적임을 설명 텍스트로 전달
- space_function 키워드 매핑 투명하게 공개

---

### 15-A-2. 📊 현장 분석 (`site_analysis.py`) ★ v2.0 신규

**역할**: 고객 Q2·Q3 "지표는 무엇이고, 현장에서 무슨 의미인가요?" 에 답변.

**서브탭 구성**:

| 탭 | 내용 |
|----|------|
| 🏗️ 현장 전체 | KPI 4개 (인원, EWI, 대기, 확인필요) + 구역별 히트맵 + 시간대별 상태 |
| 👷 작업자별 | 요약 카드 + AI 내러티브 + Journey Gantt + expander (시간배분/생산성/안전성) |
| 🏢 업체별 | EWI 비교 바차트 + 안전지표 + 해석 텍스트 |
| 📈 추이 | 날짜별 EWI/Standby/피로위험 트렌드 라인차트 |

**핵심 원칙**:
- 모든 지표 정의/계산식은 `st.expander`로 기본 닫힘
- AI Journey 내러티브 → 상단 요약 카드로 배치
- `state_detail`, `anomaly_flag` 컬럼을 hover tooltip에 활용

---

### 15-A-3. 🔮 확장 가능성 (`future_preview.py`) ★ v2.0 신규

**역할**: 고객 Q4 "앞으로 이 데이터로 뭘 더 할 수 있나요?" 에 답변.

**섹션 구성**:

| 섹션 | 내용 |
|------|------|
| 현재 구현 (√) | 1분 Journey 보정, space_function 맥락 해석, 6-state 분류, SOIF L3/L4 |
| Phase 2: 공간 인텔리전스 | 구역별 혼잡도 히트맵 (Mock), 게이트 병목 시뮬레이션 (Mock) |
| Phase 3: 예측 및 실시간 | 미래 공간 상태 예측 (개념도), 실시간 알림 시나리오 (카드) |

**핵심 원칙**:
- Phase 2·3는 "⚠️ Mock 데이터 기반 Preview" 경고 명시
- 비전 전달하되 신뢰도 훼손 없음
- `st.expander`로 SOIF 지표 정의 참고 제공

---

### 15-B. 🔧 Worker's Journey (`journey_review.py`)

**역할**: **Admin 전용 보정 디버깅** — 보정 전/후를 비교하고 보정 결과의 신뢰성을 확인하는 내부 메뉴.

> ⚠️ Admin 메뉴로 이동 (고객 화면에서 숨김). 고객 대면용은 Journey 검증 사용.

**주요 함수**:

| 함수 | 설명 |
|------|------|
| `render(df)` | 작업자 선택 + 전체 탭 렌더링 |
| `_build_export_df(wdf)` | 다운로드용 df (원본_장소 + 보정_장소 컬럼 추가) |
| `_to_csv_bytes(export_df)` | CSV 바이트 변환 |
| `_to_excel_bytes(export_df)` | Excel 바이트 변환 (openpyxl 필요) |
| `_render_download_section(wdf, name)` | CSV/Excel 다운로드 버튼 |
| `_compute_correction_stats(wdf)` | 보정 통계 3-카테고리 계산 |
| `_render_worker_stat_bar(...)` | 작업자별 보정 통계 바 |
| `_get_global_axes_jr(df, use_original)` | 공통 x/y 축 범위 계산 |
| `_render_full_journey_overview(wdf, name, stats, df)` | 탭 0: 전체 Journey (원본 기준) |
| `_flush_gantt_block(rows, place, start_ts, buf)` | Gantt 블록 생성 헬퍼 (고활성/저활성/활동상태 포함) ★ 활동상태 추가 |
| `_render_correction_logic(wdf)` | 보정 로직 4종 설명 토글 |
| `_render_journey_comparison(wdf, name, df)` | 탭 1: 원본 vs 보정 나란히 비교 |
| `_render_correction_change_summary(wdf)` | 보정 변경 요약 |
| `_render_correction_table(wdf, worker_key)` | 보정 상세 테이블 (시간/장소 필터) |
| `_render_active_ratio_comparison(wdf, name)` | 활성비율 비교 차트 |
| `_build_gantt(df, place_col)` | Gantt DataFrame 생성 |
| `_flush(rows, place, start_ts, buf)` | _build_gantt용 블록 생성 헬퍼 ★ 활동상태 추가 |
| `_add_correction_pins(fig, wdf, gantt_df, ...)` | 보정 핀 오버레이 추가 |
| `_make_gantt_figure(gantt_df, ...)` | Gantt Plotly Figure 생성 (보정 비교용) |

**탭 구성**:

| 탭 번호 | 내용 | 특징 |
|---------|------|------|
| 탭 0 🗺️ | 전체 Journey | **원본 PLACE 기준** Gantt + 주황 핀(보정 위치) + 보정된 사항 테이블 |
| 탭 1 🔄 | 원본 vs 보정 비교 | 나란히 비교 (x=0~24시, y=당일 전체 장소) + 활성비율 비교 |

**보정 통계 3-카테고리** (보정 검증 기준):
```
장소명 변경  = 보정 전후 장소명이 실제로 달라진 행 수
              헬멧 거치 통일(DBSCAN/윈도우) + 노이즈 제거로 발생
좌표만 보정  = 장소명은 동일, X/Y 좌표만 보정된 행 수
              _correct_coord_outliers로 발생
원본 유지    = IS_CORRECTED=False 인 행 수
장소 변경률  = 장소명 변경 건수 / 전체 기록
```

**마우스 오버 정보** (★ 2026-03-01 활동상태 추가):
```
장소명
체류: N분
활성비율: XX.X%
상태: 고활성 작업/저활성 작업/...
고활성: N분 · 저활성: N분
장소유형: INDOOR/OUTDOOR/...
```

**보정 핀** (`_add_correction_pins`):
- 원본 Journey Gantt 위에 주황색 1분 너비 핀 오버레이
- 마우스 오버 시 원본 장소 → 보정 장소, 시간, 활성비율 표시

---

### 15-C. 🏗️ Overview (`overview.py`)

- 전체 인원 현황, 활성비율 분포
- 구역별 인원 밀도 히트맵
- **오늘의 현장 요약**: `generate_worker_insight` 기반 전체 작업자 경고/정보/긍정 인사이트 집계

---

### 15-D. 👷 Worker Detail (`worker_detail.py`)

**역할**: **보정 완료 데이터 기반 분석 전용** — 보정 전/후 비교 없이 분석 결과만 표시.

> 역할 분리 원칙: Worker Detail = 분석, Journey Review = 보정 검증.  
> 보정 관련 UI 요소(핀, 보정 사항 테이블 등) 완전 제거.

**주요 함수**:

| 함수 | 설명 |
|------|------|
| `render(df, selected_worker_key)` | 메인 렌더링 (작업자 선택, KPI, 5탭) |
| `_is_rest_place(place_series, ...)` | 휴게시설 판단 (SSMP REST + 키워드) |
| `_calc_time_breakdown(wdf)` | 6개 카테고리 분 단위 집계 |
| `_render_kpi_cards(summary, wdf)` | KPI 카드 4개 (활성비율/작업블록/피로/고활성비율) |
| `_render_time_breakdown(wdf)` | 시간 분류 파이차트 + 스택 바 + 정의 expander |
| `_fmt_min(minutes)` | 분 → "Xh Ym" 형식 변환 |
| `_get_global_axes(df, use_original=False)` | 공통 x축(0~24시) + y축(전체 장소) 계산. `use_original=False` → CORRECTED_PLACE 기준 |
| `_render_journey_tab(wdf, name, df)` | 탭 1: Journey Timeline + AI 해석 |
| `_flush_block(gantt_rows, place, start_ts, block_rows)` | Gantt 블록 생성 (고활성/저활성/활동상태 포함) ★ 활동상태 추가 |
| `_classify_block_activity(place_type, avg_ratio, hour)` | ★ 블록의 6카테고리 활동 상태 분류 (worker_detail.py 정의, journey_review.py에서 import) |
| `_generate_journey_narrative(gantt_df, worker_name)` | ★ Rule-based AI Journey 해석 HTML 생성 |
| `_render_activity_tab(wdf, name, df)` | 탭 2: 활성비율 분석 |
| `_render_blocks_tab(wdf)` | 탭 3: 작업 블록 분석 |
| `_count_visits(timestamps)` | 방문 횟수 계산 |
| `_render_drilldown_tab(wdf, df, worker_key)` | 탭 4: 드릴다운 분석 |
| `_render_insight_tab(wdf, df, worker_key, summary)` | 탭 5: AI 인사이트 |

**탭 구성 (5탭, 2026-02 개편)**:

| 탭 | 이름 | 핵심 내용 |
|----|------|-----------|
| 탭 1 | 📍 Journey Timeline | CORRECTED_PLACE 기준 Gantt + 활성비율 시계열 + 장소별 체류시간 |
| 탭 2 | 📊 활성비율 분석 | 시간대별 추이, 6개 카테고리 파이차트, 장소별 활성비율 |
| 탭 3 | ⏱️ 작업 블록 | 연속 작업 블록 강도(고/중/저) 바 차트 + 피로 기준선 |
| 탭 4 | 🔍 드릴다운 분석 | Idle 에피소드 원인 분류 + 작업 블록 상세 + 피로 패턴 |
| 탭 5 | 💡 AI 인사이트 | Rule-based 자동 해석 카드 (경고/정보/긍정) |

**Journey Timeline 탭 세부 (탭 1)**:
- **Gantt 기준**: `CORRECTED_PLACE` (보정 후 장소). 원본 장소 기준 아님.
- **KPI 카드**: 기록 시간 / 방문 장소 수 / 장소 블록 수 (3개)
- **x축**: 0시~24시 고정 (`t_min.isoformat()` ~ `t_max.isoformat()`)
- **y축**: 당일 전체 작업자의 모든 보정 후 장소 (`_get_global_axes(df, use_original=False)`)
- **막대 색상**: ★ 활동상태(6카테고리) 기반 — 진파랑(고활성)→옅은파랑(저활성)→주황(대기)→노랑(이동)→초록(휴식)→회색(비근무)
- **색상 범례**: 차트 위에 HTML 인라인 범례 표시 + Plotly 범례 고정 순서 (더미 trace)
- **AI Journey 해석**: Gantt 차트 바로 아래 `🧠 AI Journey 해석` expander (Rule-based 자동 생성)
- **마우스 오버**: 장소명, 체류(분), 활성비율, **상태(활동상태)**, 고활성(분), 저활성(분), 장소유형
- **보정 관련 요소 완전 제거**: 주황 핀 없음, 보정 사항 테이블 없음, 보정 마커 없음

**Plotly 날짜 x축 타임스탬프 처리 (중요)**:
```python
# Gantt Bar의 base 파라미터
base=row["시작"].strftime("%Y-%m-%d %H:%M:%S")  # ← 문자열 형식 (로컬 시간 그대로)

# x축 range
range=[t_min.isoformat(), t_max.isoformat()]    # ← ISO 문자열

# ⚠️ 절대 금지: int(row["시작"].timestamp() * 1000)
#   → Plotly.js가 UTC ms로 해석 → KST +9시간 shift 버그
# ⚠️ 절대 금지: int(row["시작"].value // 1_000_000)
#   → 같은 이유로 9시간 shift
```

---

### 15-E. 🏢 Company (`company.py`)

- 업체별 평균 생산성/안전성 지표 비교
- `aggregate_by_company(df)` 결과 사용
- 집계 df의 업체 컬럼명은 `"company"` (영문) — `RawColumns.COMPANY = "업체"` 아님

---

### 15-E2. 🧠 SOIF Intelligence (`soif_dashboard.py`) ★ 2026-03-01 신규

**역할**: SOIF 5계층 아키텍처 기반 **현장 운영 인텔리전스** 대시보드.

> Layer 1(Journey)·2(Behavioral Inference)는 기존 preprocessor+worker_detail에서 처리.
> 이 페이지는 **Layer 3(Spatial State)·4(Flow State)·5(Operational Intelligence)** 를 시각화.

**탭 구성 (4탭)**:

| 탭 | 이름 | 핵심 내용 |
|----|------|-----------|
| 탭 1 | 📊 운영 Overview | EWI/OFI/CRE 핵심 KPI 카드, 시간 배분 파이, 작업자별 EWI 바, 업체별 EWI vs OFI 산점도 |
| 탭 2 | 🗺️ 공간 상태 | Zone-Time Heatmap (구역×시간대 인원밀도), Zone Utilization 바차트 |
| 탭 3 | 🔀 흐름 분석 | 주요 이동 경로 Top 15, Bottleneck Score 바차트 |
| 탭 4 | 🚨 위험 분석 | 작업자별 CRE 바차트, CRE 구성요소 분해(개인/공간/동적), 업체별 OFI |

**데이터 원본**: `src/metrics/soif.py` → `calc_soif_summary(df)` 1회 호출로 전 지표 일괄 계산.

---

### 15-F. 🚨 Safety (`safety_alert.py`)

- **피로 위험 작업자**: 연속 작업 120분 초과 작업자 목록 + 해당 구간 상세
- **이상 이동 패턴**: 급격한 위치 전환 / 장시간 비활성 정지 감지
- **단독 작업 위험**: 근무시간 내 반경 50 내 다른 작업자 없는 비율이 높은 작업자
- ※ 헬멧 준수율 탭 없음 (2026-02 제거)

---

### 15-G. 🗓️ Trend (`trend.py`)

| 탭 | 내용 |
|----|------|
| 탭 A | 현장 전체 트렌드: 날짜별 지표 라인 차트 + 이상 날짜 강조 (빨간 다이아몬드) |
| 탭 B | 작업자 트렌드: 최대 5명 선택, 지표별 멀티 라인 |
| 탭 C | 날짜 비교: 두 날짜 지표를 delta와 함께 `st.metric` 카드로 비교 |
| 탭 D | 데이터 현황: `get_date_cache_status` 기반 날짜별 처리 상태 테이블 |

---

## 16. 주요 상수 (`src/utils/constants.py`)

```python
# ── T-Ward 활성비율 임계값 ────────────────────────────────────────────
ACTIVE_RATIO_WORKING_THRESHOLD = 0.3    # work 판단 기준 (PERIOD_TYPE="work")
ACTIVE_RATIO_ZERO_THRESHOLD    = 0.05   # 비활성 판단 기준 (헬멧거치 보정 트리거)

# ── 근무시간 ──────────────────────────────────────────────────────────
WORK_HOURS_START = 7    # 07:00 이상 → 근무시간
WORK_HOURS_END   = 20   # 20:00 미만 → 근무시간
LUNCH_START      = 12   # 12:00 점심 시작
LUNCH_END        = 13   # 13:00 점심 종료

# ── Journey 보정 파라미터 ─────────────────────────────────────────────
LOCATION_SMOOTHING_WINDOW    = 5    # 슬라이딩 윈도우 노이즈 제거 (분, Phase 0 폴백용)
HELMET_RACK_MIN_DURATION_MIN = 30   # 헬멧 거치 보정 최소 지속시간 (분)
COORD_OUTLIER_THRESHOLD      = 200  # 좌표 이상치 임계값 (픽셀 단위, 동일 좌표계 내)

# ── 안전성 ────────────────────────────────────────────────────────────
FATIGUE_THRESHOLD_MIN = 120   # 피로 위험 연속작업 기준 (분)
ALONE_RISK_RADIUS     = 50    # 단독 작업 감지 반경 (좌표 단위)
# ※ HELMET_COMPLIANCE_WARNING / HELMET_COMPLIANCE_DANGER 삭제됨 (2026-02)
#   이유: BLE 신호로 헬멧 착용 여부 판단 불가

# ── 작업 강도 임계값 (6개 시간 카테고리 분류) ─────────────────────────
WORK_INTENSITY_HIGH_THRESHOLD = 0.6   # 고활성 작업 기준 (활성비율 ≥ 0.6)
WORK_INTENSITY_LOW_THRESHOLD  = 0.15  # 저활성/현장대기 경계 (활성비율 < 0.15 = standby)
# 0.6 이상       → 고활성 작업 (high_work)
# 0.15 ~ 0.6    → 저활성 작업 (low_work)
# 0.15 미만      → 현장 대기   (standby)

# ── worker_detail 이동 분류 ───────────────────────────────────────────
_TRANSIT_GAP_MAX_MIN = 10   # 작업 블록 사이 이동으로 인정하는 최대 갭 (분)

# ── DBSCAN 파라미터 (★ v3: constants.py 상수화) ──────────────────────
DBSCAN_EPS_INDOOR    = 15   # 실내 도면 좌표계 (약 1.5~4.5m)
DBSCAN_EPS_OUTDOOR   = 30   # 실외 GPS 좌표계 (약 30m)
DBSCAN_MIN_SAMPLES   = 3    # 최소 3분 = 실제 체류로 인정

# ── 앵커 공간 보호 (★ v5.3 신규) ────────────────────────────────────────
ANCHOR_SPACE_FUNCTIONS = frozenset({SpaceFunction.REST, SpaceFunction.RACK})
ANCHOR_PLACE_KEYWORDS  = ["휴게", "흡연", "식당", "탈의", "화장실", "휴식", "걸이대", "거치대", "보호구"]

# ── 공간 우선순위 (★ v5.4 신규) — 번갈음 패턴 해석 시 사용 ─────────────────
# 낮은 숫자 = 더 신뢰할 수 있는 실제 체류 공간
# "신호 수(다수결)보다 공간의 물리적 특성이 더 강한 증거"
SPACE_FUNCTION_PRIORITY = {
    RACK:             1,   # 헬멧 거치대 — 퇴근/출근 확실한 체류
    REST:             2,   # 휴게실·흡연장 — 들어가서 머무는 공간
    WORK:             3,   # 일반 작업공간
    WORK_HAZARD:      4,   # 고위험 작업공간
    TRANSIT_WORK:     5,   # 실외 공사구역 (이동+작업 혼재)
    TRANSIT_CORRIDOR: 6,   # 복도·계단 (이동 통로)
    TRANSIT_GATE:     7,   # 출입구·타각기 (통과 전용, 체류 불가)
    OUTDOOR_MISC:     8,
    UNKNOWN:          9,
}

TRANSIT_ONLY_FUNCTIONS = frozenset({TRANSIT_GATE, TRANSIT_CORRIDOR})
```

---

## 17. 파라미터 정의 표시 원칙

모든 파라미터는 사용자가 화면에서 직접 정의를 확인할 수 있어야 함.

| 방법 | 적용 대상 |
|------|----------|
| `st.metric(..., help="정의")` | 툴팁으로 간단한 정의 |
| `st.expander("📖 파라미터 정의", expanded=False)` | 상세 정의·계산 기준 토글 |

**보정 통계 파라미터 정의** (Journey Review, Pipeline 표시):

| 파라미터 | 정의 |
|----------|------|
| 전체 기록(분) | 작업자의 분 단위 위치 기록 총 수. 하루 최대 1,440분 |
| 장소명 변경 | 보정 전후 장소명이 실제로 달라진 행 수. DBSCAN/슬라이딩윈도우 또는 헬멧 거치 통일로 발생 |
| 좌표만 보정 | 장소명은 동일하나 X·Y 좌표가 보정된 행 수. 좌표 이상치 보간으로 발생 |
| 원본 유지 | 보정 알고리즘이 적용되지 않은 행 수 (`IS_CORRECTED=False`) |
| 장소 변경률 | 장소명 변경 건수 ÷ 전체 기록. 실제 의미 있는 보정 비율 |

> `IS_CORRECTED=True` = 장소명 변경 + 좌표만 보정 합계. 이를 "보정률"로 표시하면 과장될 수 있음.

---

## 18. 테마 및 스타일

### `.streamlit/config.toml`
```toml
[theme]
base = "light"  # 라이트 테마 강제 (다크 모드 차단)
```

### 색상 팔레트 (`src/utils/theme.py` — 유일한 색상 진실 소스)

> `constants.py`의 `COLOR_*` / `PLACE_COLORS`는 **Deprecated**. 신규 코드에서는 반드시 `theme.py`의 `Color` 클래스를 사용할 것.

| 이름 | 값 | 용도 |
|------|-----|------|
| `PRIMARY` | `#1B3A6B` | 헤더, 제목, 진한 네이비 |
| `SECONDARY` | `#2E6FD9` | 주요 강조색, 활성비율 라인 |
| `ACCENT` | `#F5A623` | 주황: 보정 핀, 경고 |
| `SAFE` | `#27AE60` | 초록: 정상, 휴게 |
| `WARNING` | `#F39C12` | 주황: 경고 |
| `DANGER` | `#E74C3C` | 빨강: 위험 |
| `BG_PAGE` | `#F4F6FA` | 페이지 배경 |
| `BG_CARD` | `#FFFFFF` | 컨테이너/카드 |
| `TEXT_DARK` | `#1B2A4A` | 본문 텍스트 |
| `TEXT_MUTED` | `#6B7A99` | 보조 텍스트 |

### 6개 시간 카테고리 색상 (`Color.TIME_CAT`)

| key | 색상 | 한국어 |
|-----|------|--------|
| `high_work` | `#1A5276` | 고활성 작업 |
| `low_work` | `#5DADE2` | 저활성 작업 |
| `standby` | `#F5A623` | 현장 대기 |
| `transit` | `#F7DC6F` | 이동 |
| `rest_facility` | `#27AE60` | 휴게실 이용 |
| `off_duty` | `#95A5A6` | 비근무 |

### 장소 유형별 색상 (`Color.PLACE`)

| 장소유형 | 색상 | 한국어 |
|---------|------|--------|
| `HELMET_RACK` | `#95A5A6` | 헬멧 걸이대 |
| `REST` | `#27AE60` | 휴게 시설 |
| `OFFICE` | `#2E6FD9` | 사무소 |
| `GATE` | `#F5A623` | 게이트 |
| `OUTDOOR` | `#8BC34A` | 실외 |
| `INDOOR` | `#3F51B5` | 실내 |
| `UNKNOWN` | `#BDC3C7` | 미분류 |

### 장소명 유사도 정렬 (`src/utils/place_utils.py`)

Journey Gantt 차트의 Y축(장소)을 유사한 장소끼리 인접하게 정렬하는 유틸리티.

**v6.3 신규: 이동 기반 스마트 정렬**

실제 작업자 이동 데이터를 분석하여 자주 오가는 장소들을 Y축에서 인접하게 배치.
전체 데이터(5명 작업자)의 이동 패턴을 활용하여 물리적으로 가까운 장소를 추정.

**핵심 함수:**
- `extract_place_prefix(place_name)` → `(접두사, 건물우선순위, 기능키, 층수)` 4-튜플 반환
- `sort_places_by_similarity(places)` → 이름 유사도 기반 정렬 (폴백)
- `build_transition_matrix(df, place_col)` → 장소 간 이동 빈도 행렬 구축 ★ v6.3
- `sort_places_by_transitions(places, transitions)` → 이동 빈도 기반 정렬 ★ v6.3
- `sort_places_smart(places, df, place_col)` → 이동 데이터 있으면 이동 기반, 없으면 이름 기반 ★ v6.3
- `get_place_group(place_name)` → 건물/구역 이름만 추출
- `are_places_similar(place1, place2)` → 두 장소가 같은 그룹인지 판단

**이동 기반 정렬 알고리즘 (Greedy Nearest Neighbor):**
1. 가장 이동이 많은 장소부터 시작
2. 현재 장소에서 가장 자주 이동하는 이웃을 다음에 배치
3. 모든 장소가 배치될 때까지 반복
4. 이동 데이터 없는 장소는 이름 유사도로 정렬 후 끝에 추가

**이름 유사도 정렬 기준 (폴백):**
1. **건물/구역 접두사**: FAB, 본진, WWT 등
2. **기능 키워드**: 휴게 < 식당 < 흡연 < 걸이대 < 타각기 < 출구 < 공사
3. **층수/번호**: 1층 < 2층 < 3층...
4. **원본 문자열**: 동일 그룹 내 안정 정렬

**기능 키워드 우선순위 맵:**
| 키워드 | 정렬키 |
|--------|--------|
| 휴게 | A_휴게 |
| 식당 | B_식당 |
| 흡연 | C_흡연 |
| 걸이대/거치대/보호구 | D_걸이대 |
| 타각기 | E_타각기 |
| 출구/입구/게이트 | F_출구 |
| 공사/작업 | G_공사 |
| (기타) | Z_기타 |

**예시 (이동 기반):**
```
A → B 이동 100회, A → C 이동 5회
결과: A와 B가 Y축에서 인접 배치
```

**예시 (이름 유사도):**
```
입력: ["FAB 1F", "본진 타각기", "FAB_휴게실", "본진 휴게실"]
출력: ["FAB_휴게실", "FAB 1F", "본진 휴게실", "본진 타각기"]
```

**적용 위치:**
- `journey_verify.py` → `_get_global_axes()` 내부에서 호출
- `journey_review.py` → `_get_common_axes()` 내부에서 호출  
- `site_analysis.py` → Gantt 차트 생성 시 Y축 정렬

---

## 19. 의존성 (`requirements.txt`)

```
pandas>=2.0.0
numpy>=1.24.0
streamlit>=1.32.0
plotly>=5.18.0
pyarrow>=14.0.0
scipy>=1.11.0
scikit-learn>=1.3.0    # DBSCAN 클러스터링 선택 의존성 (Phase 0)
                       # 미설치 시 Phase 0 스킵 → 슬라이딩 윈도우로 자동 폴백
openpyxl>=3.1.0        # ★ 2026-03-01 추가 (Excel 다운로드 필수 의존성)
                       # journey_review.py의 _to_excel_bytes() 에서 사용
```

> `scipy` 사용처: `scipy.spatial` (거리 행렬) 또는 통계 함수. 현재 직접 import 위치는 trend_analyzer.py 이상 감지 부분으로 추정. 미사용 시 추후 제거 검토.

### scipy 사용처 확인 필요
requirements.txt에 있으나 실제 코드에서 `import scipy`가 어디서 쓰이는지 명시 안 됨. 다음 점검 시 확인 요망.

---

## 20. 알려진 이슈 및 해결 기록

| 이슈 | 원인 | 해결 방법 |
|------|------|----------|
| `KeyError: '업체'` in `company.py` | 집계 df는 `"company"` 컬럼 사용 | `RawColumns.COMPANY` → `"company"` 직접 사용 |
| 보정 상세 테이블 미출력 | Streamlit 위젯 키 충돌 (작업자 전환 시 options 변경) | 위젯 키에 `wk_hash` 포함하여 고유화 |
| 사이드바 날짜 드롭다운 텍스트 미표시 | 글로벌 CSS가 사이드바 텍스트를 연한 색으로 덮어씀 | CSS 셀렉터로 selectbox 선택 텍스트 강제 다크 |
| 휴게실 체류가 `work`로 분류 | `장소유형`이 원본 `장소` 기준, `보정장소` 기준 재분류 없음 | Step 6에서 `보정장소` 기반 추가 체크 로직 삽입 |
| Journey 보정 시각화 과장 | 보정 블록 전체를 주황으로 하이라이트 | 1분 너비 핀으로 정확한 보정 위치만 표시 |
| `openpyxl` 미설치 | Excel 다운로드 기능 추가 후 의존성 누락 | `pip install openpyxl` |
| 보정률 59.6% vs 장소변경 344건 혼동 | `IS_CORRECTED=True`(좌표 포함)와 장소명 실제 변경 혼재 | 3-카테고리(장소명 변경/좌표만 보정/원본 유지) + 장소 변경률로 재정의 |
| 전체 Journey에 보정 장소만 표시 | 보정 결과만 보여서 "어디가 보정됐는지" 파악 어려움 | 원본 장소 기준 Gantt + 보정 핀 오버레이 + 보정된 사항 테이블 |
| Journey Gantt hover 수치 미표시 | `customdata=[[a,b,c]]` 전달 시 `%{customdata[0][i]}` 이중 인덱싱 → Plotly 미지원 | `%{customdata[0][i]}` → `%{customdata[i]}` 단일 인덱싱으로 전부 수정 |
| Journey 그래프 시간 +9시간 shift | `base=int(row["시작"].timestamp()*1000)` → UTC ms, Plotly.js가 KST로 변환 | `base=row["시작"].strftime("%Y-%m-%d %H:%M:%S")` 문자열로 변경 (로컬 시간 그대로 전달) |
| Worker Detail에 보정 비교 탭 노출 | 분석 메뉴와 보정 검증 메뉴가 혼재 | Worker Detail = 분석 전용(CORRECTED_PLACE 기준). "보정 비교" 탭 삭제 |
| SSMP 미연결로 키워드 폴백만 동작 | `pipeline.py`에서 `preprocess(raw_df)` 호출, spatial_ctx 미전달 | `_load_spatial_context()` 헬퍼 추가 후 `preprocess(raw_df, spatial_ctx=spatial_ctx)` 전달 |
| DBSCAN에서 좌표계 다른 그룹 혼합 | LOCATION_KEY 구분 없이 전체 좌표 클러스터링 | `_cluster_locations_by_key()` 구현: LOCATION_KEY별 독립 DBSCAN |
| **[수정됨 2026-03-01]** PERIOD_TYPE 0.05~0.15 구간 "rest" 오분류 | `classify_activity_period`에서 `active_ratio >= 0.05 → "rest"` (현장 대기를 휴식으로 오분류) | 임계값을 `ACTIVE_RATIO_ZERO_THRESHOLD(0.05)` 기준으로 단순화. 0.05 이상 → "work". REST 분류는 place_type에서 처리 |
| **[수정됨 2026-03-01]** `calc_rest_time` 현장대기 포함 오류 | PERIOD_TYPE "rest"에 현장 대기(standby)가 포함되어 휴식 시간 과다 집계 | PERIOD_TYPE 수정으로 REST 시설+점심만 "rest"로 분류됨 |
| **[수정됨 2026-03-01]** `spot_count` 속성이 ssmp_zones 행 수 | `_load()`에서 `spot_count = len(zones_df)` 로 잘못 집계 | `ssmp_spots.csv` 별도 로드 후 `spot_count = len(spots_df)` 로 수정 |
| **[수정됨 2026-03-01]** `openpyxl` requirements.txt 누락 | Excel 다운로드 기능 추가 후 의존성 미기재 | `openpyxl>=3.1.0` requirements.txt 에 추가 |
| **[수정됨 2026-03-01]** `detect_anomaly_movement` 호출 위치 불명확 | safety_alert.py와 calc_safety_summary 중 어디서 호출하는지 불명확 | `calc_safety_summary` docstring에 호출 구조 명시 |
| **[수정됨 2026-03-01]** Parquet 캐시 스키마 버전 없음 | DBSCAN/SSMP 추가 후 이전 캐시와 컬럼 불일치 가능 | `CACHE_SCHEMA_VERSION = "2"` 메타데이터 저장, 로드 시 버전 검증 + 누락 컬럼 자동 보완 |
| **[수정됨 2026-03-01]** kpi_cards.py ImportError | constants에서 제거된 `HELMET_COMPLIANCE_*` import 시도 | import 제거, `theme.py` Color 클래스 사용으로 전환 |
| **[수정됨 2026-03-01]** trend_analyzer.py 헬멧 잔존 | `calc_worker_trend` 등에서 `helmet_compliance` 컬럼 생성·참조 | 모든 함수에서 helmet 관련 코드 완전 제거 |
| **[수정됨 2026-03-01]** trend.py 기본 선택에 helmet 포함 | `_SITE_METRICS` default에 `helmet_compliance_avg` 포함 → KeyError 가능 | `fatigue_risk_avg`로 교체 |
| **[수정됨 2026-03-01]** company.py 헬멧 차트 노출 | 안전성 탭에 헬멧 착용률 차트 표시 | 단독작업 비율 차트로 교체, 스코어카드에서 헬멧→작업시간 |
| **[수정됨 2026-03-01]** drill_down.py "헬멧 미착용 의심" 오해 | BLE로 헬멧 착용 판단 불가인데 "미착용 의심" 레이블 사용 | "완전 비활성 (정지/부재)"로 정확한 표현으로 변경 |
| **[수정됨 2026-03-01]** constants.py/theme.py 색상 불일치 | `COLOR_SAFE=#4CAF50` vs `Color.SAFE=#27AE60` 등 이중 정의 | constants 색상을 theme과 동기화, Deprecated 표시 |
| **[P0 수정 2026-03-01]** CORRECTED_PLACE 변경 후 PLACE_TYPE 미재분류 | Step 2에서 원본 PLACE 기준 분류, Step 5에서 보정 후 미갱신 → 휴게실 보정 행이 INDOOR(work) 집계 | Step 5-post `_reclassify_corrected_places` 추가. CORRECTED_PLACE != PLACE 행 재분류 |
| **[P0 수정 2026-03-02]** REST·흡연장 방문이 보정 후 소실 | DBSCAN min_samples=3으로 1~2분 REST 클러스터 미형성 → 노이즈(-1) 처리 → Phase 0-B nearest-cluster가 주변 WORK로 덮어씀 → Phase 1 슬라이딩 윈도우 최빈값이 WORK → REST 제거 | v5.3: `ANCHOR_SPACE_FUNCTIONS`, `ANCHOR_PLACE_KEYWORDS` 정의. Phase 0-A 앵커 사전 처리(자기 장소명 기반 cluster_id), Phase 0-B `correction_target = is_noise & ~anchor_mask` + SPATIAL_CLUSTER=-2 앵커 마커, Phase 1 폴백 앵커 행 스킵, 헬멧 거치 보정 앵커 제외 |
| **[P0 수정 2026-03-02]** 번갈음 패턴에서 신호 다수결로 REST 소실 | FAB_휴게실↔FAB 1F 번갈음 시 신호 많은 FAB 1F 승리 → 휴게실 기록 소실 | v5.4: `SPACE_FUNCTION_PRIORITY` 공간 우선순위 도입. `_weighted_mode_place()` 우선순위 기반 대표 장소 선정. `_detect_alternating_pattern()` 번갈음 패턴에서 우선순위 낮은 장소를 높은 장소로 흡수 |
| **[P0 수정 2026-03-02]** 타각기 출구가 체류 공간으로 오인 | TRANSIT_GATE(통과 전용)가 RACK과 번갈아 나올 때 신호 다수결로 GATE 승리 | v5.4: `TRANSIT_ONLY_FUNCTIONS` 정의. TRANSIT 공간이 비-TRANSIT과 같은 클러스터면 비-TRANSIT 선택. 번갈음 패턴에서 TRANSIT은 항상 패자 |
| **[P1 수정 2026-03-02]** 체류시간 1분 집계 오류 | 번갈음 패턴이 각각 독립 블록으로 집계되어 총 체류시간 과소 표시 | v5.4: 번갈음 패턴 보정으로 연속 블록화. `calc_place_dwell_stats()`, `calc_all_place_dwell_summary()` 유틸리티 추가 |
| **[P0 수정 2026-03-01]** Phase 1 헬멧거치 보정이 REST 장소에 적용 | 점심 휴게실 비활성 → 헬멧 거치로 잘못 보정 | `place_type != "REST"` 조건 추가 |
| **[P0 수정 2026-03-01]** signal_count=0 처리 불완전 | 커버리지 밖/배터리/간섭 모두 active_ratio=0 → off_duty 동일 처리 | `coverage_gap` 플래그 + `signal_confidence` (NONE/LOW/MED/HIGH) 컬럼 추가 |
| **[P1 수정 2026-03-01]** DBSCAN 노이즈 ffill 구조적 한계 | A→B 전환 첫 분이 노이즈이면 이전 장소 A로 채워져 B 도착 늦게 기록 | nearest-cluster 방식 교체. 앞뒤 유효 행 거리 비교하여 가까운 쪽 채움 |
| **[P1 수정 2026-03-01]** 연속 노이즈를 이전 장소로 채움 | 5분+ 연속 노이즈가 실제 이동인데 이전 장소로 backfill | ≥5분 연속 노이즈 → `[이동] A → B` transit 태깅 |
| **[P1 수정 2026-03-01]** LOCATION_KEY 변경이 transit 미분류 | GATE 미통과 층간/건물간 이동 감지 불가 | `_calc_time_breakdown`에 LOCATION_KEY 변경 감지 로직 추가 |
| **[P1 수정 2026-03-01]** eps 파라미터 하드코딩 | 15/30 물리적 단위 불명, preprocessor에 매직넘버 | `constants.py`에 `DBSCAN_EPS_INDOOR/OUTDOOR/MIN_SAMPLES` 상수 등록 + 스케일 팩터 문서화 |
| **[P1 수정 2026-03-01]** signal_count 기반 신뢰도 없음 | 신호 1~2개 행의 active_ratio 통계적 불안정 | `signal_confidence` 컬럼 (NONE/LOW/MED/HIGH) 추가 (P0-3과 동시 구현) |
| **[P2 수정 2026-03-01]** 클러스터 대표 장소명 비결정적 | 단순 mode() 동수 시 비결정적 | signal_count 가중 최빈값 (`_weighted_mode_place`) 구현 |
| **[P2 수정 2026-03-01]** 점심시간 잔업 오분류 | 점심에 active_ratio ≥ 0.3이어도 전체 rest 처리 | `classify_activity_period`: 점심+활성비율 ≥ 0.3 → "work" (잔업 인정) |
| **[P2 수정 2026-03-01]** Phase 2 좌표 보정 후 장소명↔좌표 불일치 | 좌표 크게 보정됐는데 장소명 Phase 0 기준 유지 | `_validate_place_coord_consistency` 추가: centroid에서 벗어난 행 → 가장 가까운 클러스터 장소로 재배정 |
| **[UI 강화 2026-02-27]** SOIF 핵심 KPI 대시보드 미표시 | EWI만 표시, BS/CRE 지표 누락 → 생산성·안전성 추론 불가 | `site_analysis.py`: 현장 전체 탭에 EWI/BS/CRE 6개 KPI 카드 추가, 병목 구역 Top 3 리스트 표시, 지표 정의 expander 확장. 작업자별 탭에 CRE 레벨 색상 표시 |
| **[버그 수정 2026-02-27]** _compute_correction_stats NaN 비교 오류 | PLACE 컬럼 NaN 시 place_diff 계산 불일치 | `fillna("").astype(str)` 적용으로 안전한 문자열 비교 |
| **[메뉴 재구성 2026-02-27]** 5개 메인 메뉴로 확장 | 기존 3개 메뉴(검증/분석/확장)에서 생산성·안전성 분리 필요 | Journey 검증 → 현장 분석 → **생산성 분석** → **안전성 분석** → 확장 가능성. `productivity_analysis.py`, `safety_analysis.py` 신규 생성 |
| **[UI 개선 2026-02-27]** 로직 설명 토글 강화 | 각 페이지에 계산 로직 설명 필요 | Journey 검증에 Multi-Pass v6.3 상세 설명, 생산성 분석에 EWI 계산식, 안전성 분석에 CRE 계산식 및 공간별 위험 가중치 표 추가 |
| **[버그 수정 2026-02-27]** Space Function 테이블 미표시 | st.dataframe 렌더링 문제 | st.table로 변경 + expander로 래핑 |
| **[기능 추가 2026-02-27]** 생산성 분석 작업자별 상세 시간 분석 | 작업자별 출퇴근 시간, 활동 유형별 시간/비율 분석 필요 | `productivity_analysis.py`: 작업자 선택 → 출근/퇴근 시간, 근무 시간, 고활성/저활성/대기/이동/휴게 시간표 + 파이차트 + 시간대별 활성비율 바차트. `calc_ewi`의 `clock_in_time`, `clock_out_time`, `work_duration_min` 활용 |
| **[기능 추가 2026-02-27]** Claude API 상태 표시 | AI 해석 기능 동작 안함 → 원인 불명확 | `llm_interpreter.py`: `get_llm_status()` 함수 추가 (anthropic 설치 여부, API 키 설정 여부, 준비 상태 반환). `main.py` 사이드바에 LLM 상태 표시 + API 설정 안내 expander. 플레이스홀더 키 감지 로직 추가 |
| **[UI 개선 2026-02-27]** Journey Y축 전체 장소 일관성 | 작업자별로 Y축 장소가 다름 → 비교 불가 | `_get_global_axes`, `_get_global_axes_jr`, `_render_journey_gantt` 수정: **전체 데이터(full_df)**의 모든 장소를 Y축에 포함. 작업자가 1개 장소만 방문해도 전체 장소 목록 표시. 기존 스마트 정렬(이름 유사도 + 이동 빈도) 유지 |
| **[EWI 수정 2026-02-27]** 음영지역 고려 EWI 계산 | 음영지역(점심, 통신 음영)으로 신호 미수집 시 EWI 과대평가 | `calc_ewi` 수정: 분모를 `len(work_period_df)` (기록 수)에서 `(clock_out_time - clock_in_time)` (실제 시간 차이)로 변경. 예) 600분 근무, 400분 기록 → 분모 600분. `gap_min` (음영지역 시간) 반환 추가. UI에 음영지역 표시 및 경고 |
| **[인프라 2026-03-03]** Claude API 연동 완료 | anthropic 패키지 미설치로 AI 해석 비활성화 | 1) `.venv`에 `anthropic` 패키지 설치, 2) `llm_interpreter.py`에서 `.env` 경로 명시적 지정 (`Path(__file__).parent.parent.parent / ".env"`), 3) API 키 설정 완료. 현장 분석/작업자별 탭에서 AI 내러티브 생성 가능 |
| **[캐시 2026-03-03]** 캐시 스키마 v6.5 재생성 | EWI 계산 로직 변경으로 캐시 무효화 필요 | `rebuild_cache.py` 실행 → 20260225 데이터 3,119행 재처리 완료 (73.3KB). 보정된 행 502개 (16.1%) |
| **[배포 2026-03-03]** Analytics 캐시 | 대시보드에서 매 세션 calc_soif_summary 등 재계산 | `cache_manager`: `save_analytics()`/`load_analytics()`, `load_analytics_or_compute()`. Pipeline에서 전처리 후 지표 일괄 저장(analytics_YYYYMMDD_*.parquet, _meta.json). 현장/생산성/안전성 페이지는 `get_analytics(date, df)`로 캐시 우선 사용 |
| **[배포 2026-03-03]** Claude API 키 로드 | 클라우드에서 .env 없음 | `llm_interpreter._get_api_key()`: **1순위 st.secrets**, 2순위 .env(로컬). `get_llm_status()` 동일 순서 반영 |
| **[배포 2026-03-03]** CLOUD_MODE | Pipeline/Admin 클릭 시 CSV 없음 오류 | `main.py`: `CLOUD_MODE=os.getenv("CLOUD_MODE","false")`. True 시 Admin 메뉴·Pipeline/Journey Debug/공간속성 비노출. `pipeline.render()` 상단에서 CLOUD_MODE 시 안내 메시지 후 return |
| **[배포 2026-03-03]** requirements 분리 | 클라우드 빌드/메모리 절감 | `requirements.txt`: 대시보드용만 (pandas, streamlit, plotly, pyarrow, anthropic, python-dotenv). `requirements-local.txt`: scikit-learn, scipy, openpyxl (로컬 전처리용). scipy는 src 미사용으로 제거 |
| **[배포 2026-03-03]** calc_alone_risk 벡터화 | O(n²) 루프로 작업자 증가 시 지연 | `safety.calc_alone_risk`: LOCATION_KEY 기준 merge 후 거리 일괄 계산, iterrows 제거. 단독 카운트는 min_dist.reindex(work_df)로 일괄 판정 |
| **[배포 2026-03-03]** .gitignore | 원본 CSV 제외, 캐시 포함 가능 | `Datafile/Y1_*/`, `Datafile/Y1 */` 추가. cache/, *.parquet 주석 처리로 배포 시 캐시 커밋 가능 |

---

## 21. 현재 구현 상태

### ✅ 완료된 기능 (2026-03-03 기준)

**v6.5 업데이트 (2026-03-03)**

1. **생산성 분석 페이지 개선** (`productivity_analysis.py`)
   - 작업자 선택 → 개인별 상세 시간 분석
   - 출근/퇴근 시간, 근무 시간, 수집 시간, 음영지역 시간 표시
   - 활동 유형별 시간표 (고활성/저활성/대기/이동/휴게/음영지역)
   - 시간대별 활성비율 바차트
   - EWI 계산 로직 설명 (음영지역 고려)

2. **EWI 계산 로직 수정** (`soif.py`)
   - 분모: `len(work_period_df)` → `(clock_out_time - clock_in_time)` (실제 시간 차이)
   - 음영지역(점심, 통신 음영) 시간이 EWI에 반영됨
   - 새 반환값: `recorded_min`, `gap_min`

3. **Journey 그래프 Y축 전체 장소 표시**
   - `_get_global_axes`, `_get_global_axes_jr`, `_render_journey_gantt` 수정
   - 작업자가 1개 장소만 방문해도 전체 장소 목록 표시
   - 작업자 간 일관된 비교 가능

4. **Claude API 연동 완료**
   - `anthropic` 패키지 설치 (v0.84.0)
   - `.env` 파일 경로 명시적 지정 (프로젝트 루트 기준)
   - 사이드바에 LLM 상태 표시 + API 설정 안내
   - AI 내러티브 생성: 현장 분석 → 작업자별/현장 전체 탭

5. **캐시 스키마 v6.5 재생성**
   - `rebuild_cache.py` 실행
   - 20260225 데이터 3,119행, 73.3KB

**Space-Aware Journey Interpretation (v4) ★ 2026-03-02 구현**
- `constants.py`: `SpaceFunction` 클래스, `SPACE_KEYWORDS`, `DBSCAN_EPS_MULTIPLIER`, `DWELL_NORMAL_MAX`, `ABNORMAL_STOP_THRESHOLD`, `HAZARD_WEIGHT_*`, `ALONE_RISK_MULTIPLIER` 상수 추가
- `schema.py`: `ProcessedColumns`에 `SPACE_FUNCTION`, `HAZARD_WEIGHT`, `STATE_DETAIL`, `ANOMALY_FLAG`, `DWELL_EXCEEDED`, `JOURNEY_PATTERN` 추가
- `place_classifier.py`:
  - `classify_space_function()`: 장소명/zone_type → space_function 분류 (키워드 우선순위 적용)
  - `get_hazard_weight()`: space_function × risk_level → hazard_weight 반환
  - `classify_state_by_space()`: 공간 맥락 × active_ratio → state_detail 분류
  - `add_place_columns()`: space_function, hazard_weight 컬럼 추가
- `spatial_loader.py`: `get_zone_type()`, `get_risk_level()` 메서드 추가
- `preprocessor.py`:
  - `_get_adaptive_eps()`: space_function별 DBSCAN eps 배수 적용
  - `_classify_activity_period()`: space_function 기반 state_detail/anomaly_flag/dwell_exceeded 계산
  - `_calc_dwell_minutes()`: 연속 체류시간 계산
  - `_reclassify_corrected_places()`: 보정 후 space_function/hazard_weight 재분류
- `cache_manager.py`: `CACHE_SCHEMA_VERSION = "4"`, v4 컬럼 자동 보완
- `safety.py`:
  - `calc_contextual_risk()`: Personal Risk × hazard_weight × Dynamic Pressure
  - `detect_anomaly_events()`: 공간 맥락 기반 이벤트 감지 (abnormal_stop, gate_congestion 등)
  - `calc_safety_summary()`에 contextual_risk 통합

**기반 인프라**
- 전처리 파이프라인 (SSMP 장소 분류 + 헬멧거치·DBSCAN 노이즈·좌표 보정)
- Parquet 캐시 저장/로드, 멀티 날짜 캐시 (`load_multi_date_cache`)
- Pipeline 일괄 처리 (미처리 날짜 자동 감지 → 선택 실행)
- main.py 날짜 범위 모드 (단일/범위 전환)

**SSMP 공간 구조**
- `spatial_loader.py` SpatialContext 클래스 구현
- SSMP 기반 장소 분류 (service_section_name / zone_name 매칭)
- LOCATION_KEY 기반 좌표계 분리 원칙 전 코드에 적용
- `pipeline.py` SSMP 실제 연결 (`preprocess(df, spatial_ctx=ctx)`)
- SSMP 매칭 비율 + 미매칭 장소 목록 UI 표시

**Journey 보정**
- DBSCAN Phase 0: `_cluster_locations_by_key` (signal_count 가중 최빈값) + `_correct_noise_by_cluster` (nearest-cluster)
- 연속 노이즈 ≥5분 → transit 자동 태깅
- Phase 1 헬멧 거치 보정 (`_correct_helmet_rack_pattern`, REST 장소 제외)
- Phase 2 좌표 이상치 보정 (`_correct_coord_outliers`, LOCATION_KEY 그룹별)
- Phase 2-post 좌표↔장소명 정합성 검증 (`_validate_place_coord_consistency`)
- Step 5-post CORRECTED_PLACE → PLACE_TYPE 재분류 (`_reclassify_corrected_places`)
- scikit-learn 미설치 시 자동 폴백 (ImportError 처리)

**분석 기능**
- 6개 시간 카테고리 분류 (고활성/저활성/현장대기/이동/휴게실/비근무)
- 시간 카테고리 정의 expander (각 카테고리 정의·계산·주의사항)
- 장소별 체류시간, 이동거리 (좌표계별 분리)
- 드릴다운 분석 (Idle 에피소드 원인 분류, 작업 블록 강도, 피로 패턴)
- AI 인사이트 8가지 rule-based 자동 생성
- 다중 날짜 트렌드 분석 + 이상 날짜 감지

**시각화**
- Worker Detail 5탭 개편 (CORRECTED_PLACE 기준, 보정 요소 완전 제거)
- Journey Review 보정 검증 전용 (원본 Gantt + 핀 + 보정된 사항)
- ★ **Journey Gantt 색상 활동상태 기반 전환** (2026-03-01): PLACE_TYPE 색상 → 6카테고리 활동상태 색상
  - 진파랑(고활성) → 옅은파랑(저활성) → 주황(대기) → 노랑(이동) → 초록(휴식) → 회색(비근무)
  - worker_detail.py + journey_review.py 전체 Gantt 차트 동기화
  - `_classify_block_activity` 함수 + `_ACTIVITY_COLORS`/`_ACTIVITY_LABELS` 딕셔너리
  - 고정 순서 범례 (더미 trace) + HTML 인라인 색상 가이드
- ★ **AI Journey 해석 내러티브** (2026-03-01): `_generate_journey_narrative` Rule-based 자동 생성
  - 출근/작업구간/이동/휴식/점심추정/퇴근 시계열 내러티브
  - 구간 사이 갭 분석 (HELMET_RACK+점심시간→점심 추정, 짧은 갭→이동, REST→휴식)
  - 요약 통계 (총 작업시간, 고/저활성, 방문 장소 수)
- Journey Gantt hover: 체류·활성비율 + **상태(활동상태)** + 고활성(분)·저활성(분) 표시
- Journey x축 0~24시 고정, y축 당일 전체 장소 (비교 기준 통일)
- Plotly hover 수치 미표시 버그 수정 (`customdata[0][i]` → `customdata[i]`)
- Plotly 타임스탬프 +9시간 shift 버그 수정 (int ms → strftime 문자열)
- 보정 데이터 CSV/Excel 다운로드

**SOIF 5계층 아키텍처 (2026-03-01 구현) — ⚠️ Feasibility 검증 단계**
- ★ **`src/metrics/soif.py`** 신규 모듈: 7개 핵심 지표 계산 함수
  - `calc_ewi` (유효 작업 집중도): (고활성×1.0 + 저활성×0.5) / 현장 체류시간
  - `build_zone_time_table` (Zone-Time Table): 구역×시간대 공간 상태 벡터
  - `build_flow_edge_table` (Flow Edge Table): 구역 간 전이 엣지 테이블
  - `calc_bottleneck_scores` (병목 점수): 흐름불균형(60%) + 대기부하(40%)
  - `calc_zone_utilization` (구역 유효 활용도): 생산적 시간 / 총 점유 시간
  - `calc_cre` (복합 위험 노출도): 개인위험 × 공간위험 × 동적부하
  - `calc_ofi` (운영 마찰 지수): (대기 + 초과이동) / 현장 체류시간
- ★ **`src/pages/soif_dashboard.py`** 신규 페이지: 4탭 운영 인텔리전스 대시보드
  - 탭 1: 핵심 KPI (EWI/OFI/CRE), 시간 배분 파이, 업체별 EWI vs OFI 산점도
  - 탭 2: Zone-Time Heatmap, Zone Utilization 바차트
  - 탭 3: 주요 이동 경로 Top 15, Bottleneck Score 바차트
  - 탭 4: 작업자별 CRE 바차트, 구성요소 분해, 업체별 OFI
- ★ **`main.py`** 네비게이션에 `🧠 SOIF Intelligence` 추가
- **현재 제약**: 2명 샘플 데이터 기반 PoC. 실 현장 적용 시 파라미터 재튜닝 필요

**제거된 항목**
- 헬멧 준수율 관련 함수·KPI·탭·상수 완전 제거
- Worker Detail "보정 비교" 탭 삭제 (`_render_correction_tab` 함수 삭제)

**P0 버그 수정 (2026-03-01 1차)**
- `PERIOD_TYPE` 분류 오류 수정: `time_utils.classify_activity_period` 경계값 변경  
  (0.05 이상 → "work". 현장 대기가 기존 "rest"로 잘못 분류되던 문제 해소)
- `calc_working_time` / `calc_rest_time` 자동 수혜 (PERIOD_TYPE 기반이므로)
- `calc_alone_risk` NaN 가드 추가: LOCATION_KEY/좌표 NaN 행 스킵, 분모를 valid_count로 변경
- `spot_count` 속성 수정: `ssmp_zones` → `ssmp_spots.csv` 행 수로 올바르게 집계
- `openpyxl>=3.1.0` requirements.txt 추가 (Excel 다운로드 필수 의존성)
- `detect_anomaly_movement` 호출 구조 docstring으로 명확화
- Parquet 캐시 스키마 버전 관리 도입 (`CACHE_SCHEMA_VERSION="2"`, 구 버전 캐시 자동 컬럼 보완)

**P0~P2 보완 (2026-03-01 2차 — Journey Correction Audit 반영)**
- **[P0]** Step 5-post `_reclassify_corrected_places` 추가: CORRECTED_PLACE 변경 후 PLACE_TYPE/LOCATION_KEY 자동 재분류 (가장 심각한 집계 오류 해소)
- **[P0]** Phase 1 헬멧거치 보정에 `place_type != "REST"` 조건 추가: 점심 휴게실 비활성 → 헬멧 거치 오보정 방지
- **[P0]** `coverage_gap` 플래그 + `signal_confidence` (NONE/LOW/MED/HIGH) 컬럼 추가: signal_count=0 원인 구분 가능
- **[P1]** DBSCAN 노이즈 보정: ffill → nearest-cluster 방식 교체 (A→B 이동 시작점 보정 개선)
- **[P1]** 연속 노이즈 ≥5분 → `[이동] A → B` transit 자동 태깅
- **[P1]** `_calc_time_breakdown`에 LOCATION_KEY 변경 감지 transit 로직 추가 (층간/건물간 이동 감지)
- **[P1]** `DBSCAN_EPS_INDOOR/OUTDOOR/MIN_SAMPLES` 상수 `constants.py` 등록 + 좌표계 스케일 팩터 문서화
- **[P2]** 클러스터 대표 장소: signal_count 가중 최빈값 (`_weighted_mode_place`) 구현
- **[P2]** 점심시간 잔업 오분류 수정: 점심 + 활성비율 ≥ 0.3 → "work" (잔업 인정)
- **[P2]** Phase 2-post `_validate_place_coord_consistency`: 좌표↔장소명 정합성 검증 + 자동 재배정
- 캐시 스키마 `CACHE_SCHEMA_VERSION = "3"` (coverage_gap, signal_confidence, SPATIAL_CLUSTER, CLUSTER_PLACE 캐시 저장)

**전체 코드베이스 점검 및 정리 (2026-03-01)**
- **헬멧 잔존 코드 완전 제거**:
  - `kpi_cards.py`: `HELMET_COMPLIANCE_WARNING/DANGER` import 제거, 헬멧 착용률 KPI 제거
  - `trend.py`: `_SITE_METRICS`, `_WORKER_METRICS`, `_COMPARE_META`에서 helmet_compliance 항목 삭제
  - `trend_analyzer.py`: `calc_worker_trend`, `calc_company_trend`, `calc_site_daily_summary`, `compare_two_dates`에서 helmet_compliance 관련 코드 전부 제거
  - `company.py`: 스코어카드의 "헬멧준수" → "작업시간", 안전성 탭의 "헬멧 착용률 차트" → "단독작업 비율 차트", 상세 테이블 컬럼 변경
  - `main.py`: Landing 카드의 Safety 설명에서 "헬멧 준수율" → "피로 위험 · 이상 이동 · 단독 작업 알림"
  - `drill_down.py`: "헬멧 미착용 의심" 레이블 → "완전 비활성 (정지/부재)"로 변경 (BLE 신호 기반 착용 여부 판단 불가 원칙 반영)
- **미사용 import 정리**:
  - `worker_detail.py`: `get_place_dwell_time` 제거
  - `journey_review.py`: `Counter`, `make_subplots` 제거
  - `overview.py`: `get_zone_density_by_hour` 제거, 미사용 `_cached_worker_df` 함수 제거
  - `theme.py`: `plotly.io` 제거
- **색상 체계 통일**:
  - `constants.py`의 `COLOR_*`/`PLACE_COLORS` → Deprecated 표시, `theme.py`의 `Color` 클래스로 통일 안내
  - `constants.py` 색상값을 `theme.py` `Color` 값과 동기화 (하위 호환 유지)
  - `theme.py`에 `Color.TIME_CAT` 딕셔너리 추가 (6개 시간 카테고리 색상)
- **차트 디자인 개선**:
  - `overview.py`: "작업 강도 분포" 섹션 신규 추가 (활성비율 기반 고활성/저활성/대기/비활성 분류), 활성비율 히스토그램 threshold 라인 3단계(0.6/0.15/0.05)로 세분화, 히트맵 colorscale theme 기반 통일
  - `trend.py`: 날짜 비교 value 포맷 개선 (비율은 %, 실수는 소수점 1자리)
  - `company.py`: 피로 위험도 세로 막대 차트 정상 확인, 단독작업 비율 가로 막대 차트 추가
- **데드코드 정리**: `visualization/` 폴더 전체 삭제 (2026-03-02)

---

## 21-B. Space-Aware Journey Interpretation 설계 문서 ★ 2026-03-02 ✅ 구현 완료

> **✅ 구현 완료** (2026-03-02): 아래 설계가 `constants.py`, `place_classifier.py`, `preprocessor.py`, `safety.py` 등에 반영됨.
> 이 섹션은 **설계 참조 문서**로 유지.

### 목적

Journey 보정과 상태 분류를 `active_ratio` 단일 기준에서 벗어나,
**공간 속성 × 신호 증거 × 시간 문맥 × Journey 흐름**을 종합한
**맥락 기반 해석(Contextual Interpretation)**으로 전환한다.

### 핵심 원칙

```
같은 active_ratio라도 공간에 따라 의미가 다르다.
  - 휴게실 0.1 = 정상 휴식
  - FAB 1F 0.1 = 비정상 대기 (자재/지시 대기)

각 분(分)은 독립된 점이 아니라 하루 이야기의 한 문장이다.

공간이 기대하는 행동(expected behavior)을 먼저 정의하고,
신호가 그것과 얼마나 일치/이탈하는지로 상태를 해석한다.
```

### 4개 증거 계층 (Evidence Layers)

| Layer | 명칭 | 내용 | 질문 |
|-------|------|------|------|
| 1 | 신호 증거 (Signal Evidence) | active_ratio + signal_confidence + coverage_gap | "이 분에 얼마나 움직였고, 측정은 얼마나 신뢰되나?" |
| 2 | 공간 맥락 (Spatial Context Prior) | space_function + hazard_weight + congestion_prone | "이 장소에서 어떤 행동이 기대되는가?" |
| 3 | 시간 문맥 (Temporal Context) | 앞 N분 + 뒤 N분 + 체류 지속시간 + 시간대 | "이 분 전후에 무슨 일이 있었나?" |
| 4 | Journey 서사 (Journey Narrative) | 오늘 주 작업공간 + 이동 패턴 유형 + 전체 흐름 | "이 작업자의 오늘 전체 흐름에서 이 분은 무엇인가?" |

### 공간 기능 정의 (space_function)

| space_function | 설명 | 장소명 키워드 예시 | state_override | dwell_expectation |
|----------------|------|-------------------|----------------|-------------------|
| `WORK` | 실내 작업공간 | FAB, CUB, WWT, MDF, 1F/2F/B1 | ❌ active_ratio 기반 | LONG (30분~) |
| `WORK_HAZARD` | 고위험 작업공간 | 밀폐, 맨홀, 탱크, 고소, confined | ❌ + 위험 가중 | SHORT~MEDIUM |
| `TRANSIT_WORK` | 실외 공사/이동+작업 혼재 | 공사현장, 야적장, 옥외, 외부 | ❌ 이동 우선 해석 | SHORT~MEDIUM |
| `TRANSIT_GATE` | 출입구/타각기 | 게이트, GATE, 타각기, 정문 | ✅ transit | VERY SHORT (<5분) |
| `TRANSIT_CORRIDOR` | 건물 내 이동통로 | 통로, 복도, 계단, 엘리베이터 | ✅ transit | SHORT (<10분) |
| `REST` | 휴게 시설 | 휴게, 식당, 탈의실, 흡연 | ✅ rest_facility | MEDIUM (15~60분) |
| `RACK` | 헬멧 거치대 | 걸이대, 거치대, 보호구, 안전모 걸이 | ✅ off_duty | LONG (야간) |
| `OUTDOOR_MISC` | 실외 기타 | 주차장, 야외, 섹터, 외곽 | ❌ active_ratio 참고 | SHORT |
| `UNKNOWN` | 미분류 | 위 키워드 미해당 | ❌ 기존 로직 유지 | — |

### 키워드 우선순위 (매칭 순서)

```
1순위: RACK 키워드 → RACK
2순위: TRANSIT_GATE 키워드 → TRANSIT_GATE
3순위: REST 키워드 → REST
4순위: WORK_HAZARD 키워드 → WORK_HAZARD
5순위: TRANSIT_CORRIDOR 키워드 → TRANSIT_CORRIDOR
6순위: TRANSIT_WORK 키워드 → TRANSIT_WORK
7순위: WORK 키워드 (건물명, 층 포함) → WORK
8순위: SSMP zone_type 매핑
9순위: OUTDOOR_MISC (실외 fallback)
10순위: UNKNOWN
```

### 상태 해석 행렬 (space_function × active_ratio)

| space_function | ≥0.6 | 0.15~0.6 | 0.05~0.15 | <0.05 | coverage_gap |
|----------------|------|----------|-----------|-------|--------------|
| WORK | high_work | low_work | standby | off_duty ⚠️ | 앞뒤 문맥 보간 |
| WORK_HAZARD | high_work ⚠️ | low_work ⚠️ | standby ⚠️ | 🚨 abnormal_stop | 🚨 최우선 확인 |
| TRANSIT_WORK | high_work/transit | low_work/transit | transit_slow | transit_idle | 이동 중 음영 |
| TRANSIT_GATE | transit | transit | transit_queue | gate_congestion 🚧 | 병목 신호 유지 |
| TRANSIT_CORRIDOR | transit | transit | transit_slow | corridor_block 🚧 | 이동 중 음영 |
| REST | rest_facility | rest_facility | rest_facility | rest_facility | 정상 |
| RACK | off_duty | off_duty | off_duty | off_duty | off_duty |

### Journey 보정 전략 (space_function별)

| space_function | DBSCAN 전략 | eps 배수 | 노이즈 처리 | 연속 노이즈 ≥5분 |
|----------------|------------|---------|-------------|-----------------|
| WORK | 공격적 클러스터링 | ×1.0 기본 | nearest-cluster | → transit 태깅 |
| WORK_HAZARD | 매우 공격적 | ×0.7 작게 | 보수적 유지 | 🚨 이상 이동 플래그 |
| TRANSIT_WORK | 완화 | ×1.5 크게 | 이동으로 해석 | 정상 이동 구간 |
| TRANSIT_GATE | 불필요 | — | 장소가 상태 결정 | transit_queue |
| TRANSIT_CORRIDOR | 불필요 | — | 장소가 상태 결정 | corridor_block |
| REST | 불필요 | — | 보정 스킵 | 정상 휴식 |
| RACK | 불필요 | — | 보정 스킵 | 정상 거치 |

### 이상 신호 해석 (space_function별)

| 이상 신호 | WORK | WORK_HAZARD | TRANSIT_* | REST | RACK |
|----------|------|-------------|-----------|------|------|
| active_ratio < 0.05 (30분+) | standby 과다 ⚠️ | 🚨 abnormal_stop 즉시 | transit_idle | 정상 (깊은 휴식) | 정상 |
| coverage_gap 5분+ | 앞뒤 보간 | 🚨 최우선 확인 | 이동 중 음영 | 정상 | 무시 |
| dwell > normal_max | 생산성 경고 | 🚨 즉시 확인 | 병목 경보 🚧 | 정상 | 정상 |
| alone (반경 내 동료 없음) | alone_standby ⚠️ | 🚨 lone_hazard 최고위험 | 해당없음 | 정상 | 해당없음 |

### 위험 맥락 가중치 (hazard_weight)

| space_function | hazard_weight | alone_risk_multiplier | abnormal_stop_threshold |
|----------------|---------------|----------------------|------------------------|
| WORK (LOW) | 0.3 | ×1.0 | 60분 |
| WORK (MEDIUM) | 0.5 | ×1.5 | 30분 |
| WORK (HIGH) | 0.8 | ×2.0 | 15분 |
| WORK_HAZARD | 1.0 | ×3.0 | 🚨 5분 |
| TRANSIT_GATE | 0.2 | ×0.5 | — |
| REST | 0.0 | ×0.0 | — |
| RACK | 0.0 | ×0.0 | — |

### 신규 추가 컬럼 ✅ 구현 완료

| 컬럼명 | 타입 | 설명 | 구현 파일 |
|--------|------|------|----------|
| `space_function` | str | WORK/WORK_HAZARD/TRANSIT_GATE/REST 등 | `place_classifier.py` |
| `hazard_weight` | float | 0.0~1.0 공간 위험 가중치 | `place_classifier.py` |
| `state_detail` | str | transit_queue/transit_slow/standby 등 | `preprocessor.py` |
| `anomaly_flag` | str/None | abnormal_stop/gate_congestion/lone_hazard 등 | `preprocessor.py` |
| `dwell_exceeded` | bool | 체류가 normal_max_min 초과 여부 | `preprocessor.py` |
| `journey_pattern` | str | zone_fixed/zone_cycle/explorer | ⏳ 미구현 (#24) |

### 파이프라인 변경 ✅ v4 구현 완료

```
[v3 → v4 구현 완료]
raw → 신호품질평가 → 장소분류 + space_function    ✅ place_classifier.py
    → 공간적응형 DBSCAN 보정 (eps 배수 차등)      ✅ preprocessor.py
    → 공간맥락 상태 분류 (행렬 기반)               ✅ preprocessor.py
    → 이상 신호 감지 + anomaly_flag               ✅ preprocessor.py, safety.py
    
[미구현]
    → Journey 선독 (하루 패턴 파악)               ⏳ #24 journey_pattern
```

### 미결 사항 (현장 데이터로 확인 필요)

| 항목 | 내용 | 확인 방법 | 상태 |
|------|------|----------|------|
| 통로 구분 | 현재 zone_type에 통로 미지정 | 장소명 목록에서 통로 패턴 확인 | 키워드 매칭으로 대체 |
| SSMP risk_level | ssmp_zones.csv에 값 존재 여부 | `value_counts()` 확인 | 기본값 적용 중 |
| 좌표계 스케일 | 실내 1unit = 실제 몇 cm | 알려진 두 지점 좌표 vs 실측 거리 비교 | 미확인 |
| dwell 기준값 | GATE 5분, CORRIDOR 10분 | 실제 데이터 체류시간 분포 확인 | 기본값 적용 중 |

---

### 🔜 다음 단계 (개선 우선순위)

| 우선순위 | 항목 | 상세 |
|----------|------|------|
| ~~1~~ | ~~SSMP 기반 전처리 실제 적용~~ | ✅ 완료 |
| ~~2~~ | ~~DBSCAN 좌표계별 분리~~ | ✅ 완료 |
| ~~P0~~ | ~~PERIOD_TYPE vs 6카테고리 불일치~~ | ✅ 완료 (2026-03-01) |
| ~~P0~~ | ~~alone_risk LOCATION_KEY 검증 누락~~ | ✅ 완료 (2026-03-01) |
| ~~P0~~ | ~~spot_count 속성 오류~~ | ✅ 완료 (2026-03-01) |
| ~~P0~~ | ~~openpyxl requirements.txt 누락~~ | ✅ 완료 (2026-03-01) |
| ~~P0~~ | ~~캐시 스키마 버전 관리 없음~~ | ✅ 완료 (2026-03-01) |
| 3 | **DBSCAN 적용 후 실제 데이터 재처리** | Pipeline 재실행 → PERIOD_TYPE 변경 효과 수치 확인 |
| ~~4~~ | ~~**Overview 페이지 콘텐츠 보강**~~ | ✅ 완료: 작업 강도 분포 섹션 추가, 히스토그램 threshold 3단계 세분화 |
| ~~5~~ | ~~**Company 페이지 차트/지표 정의**~~ | ✅ 완료: 헬멧→단독작업 차트 교체, 스코어카드 작업시간 추가 |
| 6 | **ssmp_buildings.csv outdoor 좌표 추가** | 건물 간 이동 거리 추정 활성화. 현재 `inter_building_distance=0` |
| 7 | **DBSCAN eps 자동 튜닝** | 현재 실내 15 / 실외 30 고정 → 좌표 분포 기반 자동 계산 |
| 8 | **6카테고리 로직 metrics 모듈 분리** | `_calc_time_breakdown` → `metrics/time_category.py` 추출 |
| 9 | **지표 기반 리포트 자동 생성** | PDF/Excel 내보내기 |
| 10 | **실시간 데이터 연동** | 파일 감시(watchdog) → 자동 파이프라인 실행 |
| ~~11~~ | ~~**데드코드 정리**~~ | ✅ 완료 (2026-03-02): visualization/ 폴더 삭제, 레거시 페이지 삭제 |
| ~~12~~ | ~~**[P3] SOIF Layer 3 (Space State)**~~ | ✅ 완료: `build_zone_time_table`, `calc_zone_utilization` 구현 |
| ~~13~~ | ~~**[P3] SOIF Layer 4 (Flow State)**~~ | ✅ 완료: `build_flow_edge_table`, `calc_bottleneck_scores` 구현 |
| 14 | **SOIF Layer 5 확장: Future Space State Forecast** | 15~30분 후 혼잡 예측 → 동선 분산 제안. 현재는 현재 상태 분석까지만 구현 |
| 15 | **SOIF CCR (군집 충돌 위험)** | 좁은 게이트/통로에서 밀도+흐름 충돌 지점 포착. Flow Edge + Zone density 결합 필요 |
| 16 | **coverage_gap 활용** | coverage_gap 행을 off_duty 대신 별도 카테고리로 표시하거나 보간 로직 구현 |
| 17 | **signal_confidence 활용** | LOW confidence 행의 active_ratio 표시 시 경고 마커 추가. 집계 시 가중치 반영 검토 |
| 18 | **SOIF 멀티데이 트렌드** | EWI/OFI/CRE의 날짜별 추이 분석. 현재 Trend 페이지와 통합 가능 |
| ~~**19**~~ | ~~**★ Space-Aware Journey Interpretation**~~ | ✅ 완료 (2026-03-02) |
| ~~20~~ | ~~**space_function 컬럼 추가**~~ | ✅ 완료: WORK/WORK_HAZARD/TRANSIT_GATE/REST 등 9개 분류 |
| ~~21~~ | ~~**상태 해석 행렬 적용**~~ | ✅ 완료: space_function × active_ratio → state_detail |
| ~~22~~ | ~~**공간적응형 DBSCAN**~~ | ✅ 완료: space_function별 eps 배수 차등 적용 |
| ~~23~~ | ~~**anomaly_flag 컬럼**~~ | ✅ 완료: abnormal_stop, gate_congestion 등 |
| 24 | **Journey 선독(Pre-scan)** | ⏳ 미구현: 하루 전체 패턴 파악 후 보정 전략 결정 (zone_fixed/zone_cycle/explorer) |
| ~~25~~ | ~~**2단계: UI 전면 재설계**~~ | ✅ 완료 (2026-03-02): 메뉴 8→4, journey_verify/site_analysis/future_preview 신규 |
| 26 | **3단계: 작업자 분석 스토리라인** | ⏳ 다음: site_analysis 작업자별 탭 상세화, AI 내러티브 개선 |
| 27 | **4단계: 위험 알림 단순화** | ⏳ 대기: anomaly_flag 기반 즉시/주의 리스트 |
| 28 | **레거시 페이지 정리** | overview/worker_detail/company/trend/safety_alert/soif_dashboard 실제 삭제 검토 |

---

## 22. 실제 데이터 현황 (20260225 기준)

```
총 행 수: 3,119행 (1분 단위)
작업자 수: 2명 확인
  - 황*석 (태그: T-41-00007212)
  - 김*형 (기록시간: 06:51~17:03)

SSMP 장소 분류 예시 (20260225 기준):
  FAB_휴게실_F02_1층1번   → REST (ssmp_service_sections 매칭)
  FAB_휴게실_F03_1층2번   → REST
  본진 타각기 앞 보호구 걸이대 → HELMET_RACK (키워드 매칭)
  FAB 1F (전체)            → INDOOR (ssmp_service_sections 매칭)
  WWT B1F (전체)           → INDOOR (ssmp_service_sections 매칭)
  WWT 1F (전체)            → INDOOR

황*석 보정 통계 예시 (슬라이딩 윈도우 기준, DBSCAN 이전):
  전체 기록: 1,192분
  장소명 변경: 344건 (헬멧 거치 통일 274건 + 노이즈 제거 70건)
  좌표만 보정: 366건 (좌표 이상치 보간)
  원본 유지:   482건
  장소 변경률: 28.9% (344 / 1,192)

시간 분류 (6개 카테고리, 황*석 예시 — DBSCAN 적용 전 수치):
  high_work:     ~200분  (활성비율 ≥ 60%)
  low_work:      ~100분  (활성비율 15~60%)
  standby:        ~84분  (활성비율 < 15%)
  transit:        ~52분  (GATE 통과 + 블록 간 갭)
  rest_facility:   ~4분  (휴게 시설 체류)
  off_duty:      ~752분  (근무시간 외 + 헬멧 걸이대)
  ※ DBSCAN Phase 0 적용 후 수치는 재처리 필요
```

---

## 23. 알려진 이슈 및 수정 내역 (2026-03-02)

### 23-A. 수정된 버그

| 버그 | 원인 | 수정 | 상태 |
|------|------|------|------|
| 휴게실 데이터 잘못 보정 | `place_type != "REST"` 조건에서 `PLACE_TYPE`("INDOOR" 등)과 `SpaceFunction`("REST")을 혼동 | `space_function != SpaceFunction.REST`로 변경 + 장소명 키워드 추가 확인 | ✅ 수정 |
| ImportError: calc_soif_metrics | 함수명 변경 후 import 미수정 | `calc_soif_summary`로 import 수정 | ✅ 수정 |
| 보정 요약/시간 분류 비교 데이터 미표시 | 컬럼 접근 방식 문제 + 예외 처리 부재 | 컬럼 존재 확인 + try-except + st.metric으로 UI 변경 | ✅ 수정 |
| **FAB_휴게실이 3~4시간으로 과대 집계 (v5.4)** | v5.3 `_weighted_mode_place`에 space_function priority 적용 → 클러스터 내 REST 1행만 있어도 전체 클러스터 대표가 REST로 됨 | `_weighted_mode_place`에서 priority 제거 (signal_count 가중 최빈값으로 원복). `_detect_alternating_pattern`에 연속 구간 조건 추가 (`PRIORITY_MAX_RUN_MIN=5`, `PRIORITY_CONTINUOUS_THRESHOLD=10`) | ✅ v5.4 수정 |
| **새벽/야간 Ghost Signal 미보정 (v6.0)** | active_signal_count=0 + 위치 불안정을 Ghost로 판단하는 로직 없었음 → 걸이대에 걸린 태그가 인근 공사현장/타각기출구로 번갈아 표시 | `_collect_evidence()`로 e_ghost_candidate 플래그 생성, `_segment_day()`로 pre_work/post_work 구간 식별, `_correct_ghost_signals()`로 Ghost 구간 → 인근 RACK 보정 | ✅ v6.0 수정 |
| **번갈음 보정 과/미보정 반복 (v6.0)** | 각 Phase가 독립 판단 → 서로 충돌하고 덮어씀 | v6: 4개 증거(E1+E2+E3+E4) 통합 판단 + 강화조건(TRANSIT/점심/무활성). `_correct_alternating_by_context()` 구현 | ✅ v6.0 수정 |

### 23-B. 알려진 UI 이슈 (진행 중)

| 이슈 | 상세 | 상태 |
|------|------|------|
| Journey 그래프 짧은 시간 블록 가시성 | 1~2분짜리 블록이 그래프에서 너무 얇게 표시됨 | ⏳ 개선 필요: 최소 너비 설정 또는 진한 테두리 추가 |
| 이동(transit) 시간 표시 부족 | 현재 GATE 통과만 이동으로 표시, 장소 간 이동 시간 미반영 | ⏳ 개선 필요: Transition Travel Detection 기능 활성화 |
| 보정 요약 섹션 컨텐츠 미표시 | HTML 기반 마크다운이 Streamlit 버전에 따라 다르게 렌더링 | ✅ st.metric으로 변경하여 해결 |

### 23-C. Journey 보정 인텔리전스 개선 제안

현재 Journey 보정은 DBSCAN 클러스터링과 헬멧 거치 패턴 보정에 의존함. 
더 인텔리전트한 보정을 위해 다음 세 가지 특성을 종합 고려하는 개선이 필요:

```
┌─────────────────────────────────────────────────────────────────────┐
│  인텔리전트 Journey 보정 (v5 제안)                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1️⃣ 시간 특성 (Temporal Context)                                   │
│     · 점심시간 전후 (11:30~12:30) 휴게 장소는 보정 제외             │
│     · 야간/새벽 (20:00~07:00) 비활성은 off_duty 추정               │
│     · 작업 시작/종료 시간대 (07:00~08:00, 17:00~18:00) 게이트 통과   │
│                                                                     │
│  2️⃣ 공간 특성 (Spatial Context)                                    │
│     · space_function 기반 보정 예외 (REST, RACK → 보정 제외)        │
│     · 인접 장소 패턴 (같은 층 vs 다른 건물)                         │
│     · 이동 거리 기반 타당성 검증 (1분 내 km 이동은 불가능)          │
│                                                                     │
│  3️⃣ 이동 특성 (Movement Context)                                   │
│     · 장소 전환 시 이동 시간 추정 (Transition Travel Detection)     │
│     · LOCATION_KEY 변화 = 건물/층 간 이동 = 긴 이동시간             │
│     · GATE 통과 → 작업구역 도달까지 이동 시간 태깅                  │
│                                                                     │
│  ┌───────────┐     ┌───────────┐     ┌───────────┐                  │
│  │ 시간 특성 │ ──► │ 공간 특성 │ ──► │ 이동 특성 │                  │
│  │ (언제?)   │     │ (어디서?) │     │ (어떻게?) │                  │
│  └───────────┘     └───────────┘     └───────────┘                  │
│        │                 │                 │                        │
│        └─────────────────┼─────────────────┘                        │
│                          ▼                                          │
│              ┌─────────────────────┐                                │
│              │  종합 판단 로직     │                                │
│              │  (맥락 기반 보정)   │                                │
│              └─────────────────────┘                                │
│                                                                     │
│  예시:                                                              │
│  · 12:15 FAB_휴게실 (활성비율 0%) → 점심 직후 + REST 장소           │
│    → 정상 휴식으로 판단, 보정 제외 ✅                               │
│  · 06:30 FAB 1F (활성비율 0%) → 야간 + 작업구역                     │
│    → 헬멧 거치대 보정 대상 (인근 RACK 확인) ✅                      │
│  · 13:01 FAB 1F (12:59 헬멧 거치대에서 전환) → 건물 간 이동          │
│    → transit_arrival 태깅 (10분간) ✅                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 23-D. 다음 작업 우선순위

| 우선순위 | 항목 | 상태 |
|----------|------|------|
| ⭐ P0 | 캐시 삭제 후 데이터 재처리 | Intelligent Journey v5 적용 확인 필요 |
| ~~P1~~ | ~~Journey 그래프 짧은 블록 가시성 개선~~ | ✅ 완료 (MIN_DISPLAY_MINUTES + 진한 테두리) |
| P1 | Transition Travel Detection 기능 검증 | 황*석 transit 시간 증가 확인 |
| ~~P2~~ | ~~인텔리전트 Journey 보정 v5 설계~~ | ✅ 완료 (2026-03-02) |
| P2 | Space Function 분류 기준 표 가시성 | st.dataframe 확인 필요 |

### 23-E. 사이드바 저작권 표시 추가

```
Designed, Developed & Deployed by
TJLABS
All Rights Reserved © 2026

v1.0 · Post-Processing Analytics
```

---

## 24. Intelligent Journey Correction v5 (2026-03-02 구현 완료)

### 24-A. 핵심 개념: Context-Aware Sequence Interpreter

기존 문제점: 각 분(行)을 독립적으로 판단 → 공간/시간/이동 맥락 무시
해결 방향: **시퀀스(연속 구간) 단위로 해석** → 공간특성 × 시간맥락 × 이동패턴 통합

### 24-B. 4개 판정 규칙 (우선순위 순)

| 규칙 | 이름 | 설명 | 예시 |
|------|------|------|------|
| **1** | Solo Suspicious Dwell | 짧은 체류 + 비앵커 공간 → transit_passing | FAB 2분 체류 → 이동 중 순간 태깅 |
| **2** | Sequence Variety | 10분 윈도우 내 3곳+ → transit_sequence | FAB↔CUB↔WWT 번갈아 = 이동 구간 |
| **3** | Anchor Return | [앵커→X→앵커] 패턴 → X 흡수 | 휴게실→FAB 2분→휴게실 = 휴게 유지 |
| **4** | Temporal Context | 시간대별 맥락 강화 | 13:00~14:00 REST = rest_postlunch |

### 24-C. 공간별 체류 기대값 (SPACE_DWELL_PROFILE)

```python
# constants.py
SPACE_DWELL_PROFILE = {
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

SEQUENCE_WINDOW_MIN = 10        # 슬라이딩 윈도우 크기 (분)
SEQUENCE_VARIETY_THRESH = 3     # 이동 판정 장소 종류 임계값
MIN_DISPLAY_MINUTES = 3         # Gantt 차트 최소 표시 폭 (분)
SHORT_BLOCK_THRESHOLD_MIN = 3   # 짧은 블록 강조 임계값 (분)
```

### 24-D. 파이프라인 흐름 (v6.2 원본 기반 전체 맥락 추론)

```
★ v6.2 핵심 철학: "원본 데이터에서 전체 맥락을 보고 합리적으로 추론"

┌────────────────────────────────────────────────────────────────────────────┐
│  Journey = 문장                                                            │
│  장소 = 단어                                                               │
│                                                                            │
│  부분을 보정하는 것이 아니라, 전체 Journey를 하나의 문장으로 분석            │
│  → 공간 특성 + 시간 맥락 + 빈도를 종합해서 합리적 장소 추론                  │
│  → 원본에 없는 장소로는 절대 보정하지 않음                                  │
└────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│  v6.2 Multi-Pass Refinement 흐름                                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  [증거 수집]                                                                  │
│    Phase E1: _collect_evidence() — 활성신호, 위치 안정성                       │
│    Phase E2: _segment_day() — 하루 구간 분절 (pre_work/work/lunch/post_work)  │
│                     ↓                                                        │
│  [장소 보정 — 1차]                                                            │
│    DBSCAN 클러스터링 (앵커 보호 포함)                                          │
│                     ↓                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  ★ Multi-Pass Loop (최대 3회)                                           │ │
│  │                                                                          │ │
│  │    Pass 1: Ghost Signal 보정 (v6.2)                                     │ │
│  │      → _correct_ghost_signals()                                         │ │
│  │      → 무활성(active=0) 구간 탐지                                        │ │
│  │      → 블록 내 합리성 점수 계산 후 최고 점수 장소로 통일                   │ │
│  │                     ↓                                                    │ │
│  │    Pass 1.5: Journey 문장화 보정 ★ v6.3 신규                             │ │
│  │      → _correct_journey_as_sentence()                                   │ │
│  │      → Run 시퀀스 생성 (1440분 → N개 Run으로 압축)                        │ │
│  │      → 전체 맥락 분석 (주요 장소, 출퇴근 패턴)                             │ │
│  │      → 흡수 판단: A-X-A 노이즈, TRANSIT 단발, 맥락 이상                   │ │
│  │      → 앵커 보호: 휴게실/걸이대는 짧아도 흡수 안 함                        │ │
│  │                     ↓                                                    │ │
│  │    Pass 2: 번갈음 패턴 해소                                              │ │
│  │      → _correct_alternating_by_context()                                │ │
│  │      → 공간 우선순위 적용 (RACK > REST > WORK > TRANSIT)                  │ │
│  │                     ↓                                                    │ │
│  │    Pass 3: 전체 맥락 검증                                                │ │
│  │      → _pass3_verify_narrative()                                        │ │
│  │      → 하루 스토리라인 일관성 확인                                        │ │
│  │                     ↓                                                    │ │
│  │    Pass 4: 물리적 이상치 탐지                                            │ │
│  │      → _pass4_detect_impossible_movement()                              │ │
│  │      → 텔레포트, 건물 점프, A-B-A 노이즈 잔류 처리                        │ │
│  │                     ↓                                                    │ │
│  │    [수렴 체크]                                                           │ │
│  │      변경 < 5개 → 조기 종료 / 아니면 → 다음 반복                          │ │
│  │                                                                          │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                     ↓                                                        │
│  [최종 분류]                                                                  │
│    Step 6: _classify_activity_period()                                       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

4개 증거 (변경 없음):
  E1. 활성신호 (active_signal_count) — "사람이 움직였는가?"
  E2. 공간 속성 (space_function)     — "이 장소에서 머무는 게 정상인가?"
  E3. 시간 속성 (시간대)             — "지금 시각에 여기 있는 게 자연스러운가?"
  E4. 이동 패턴 (위치 연속성)        — "위치가 안정적인가, 점프하는가?"

★ v6.2 핵심 변경 — Pass 1 Ghost Signal 보정:
  [v6.1 문제점]
    · 블록 내 RACK 없으면 → 블록 외부 30분까지 탐색
    · 원본에 없는 장소 (예: "본진 B동앞 보호구 걸이대")로 잘못 보정
  
  [v6.2 해결]
    · 블록 내 장소만 사용 (원본 기반)
    · 공간 점수 × 시간 보정 × 빈도 = 합리성 점수
    · 최고 점수 장소로 통일 (원본 내에서만)

수렴 로직:
  - 각 Pass에서 변경된 행 수를 집계
  - 총 변경 < 5 → 수렴 완료, 조기 종료
  - 최대 3회 반복 후 강제 종료
```

**v6.2 원본 기반 추론 예시** (황*석 데이터):
```
예시 1: 20:00~24:00 무활성 구간
  원본 장소: 본진 타각기 앞 보호구 걸이대 (120회), 공사현장 (80회), 타각기 출구 (40회)
  
  [v6.1 — 잘못된 결과]
    블록 내 RACK 검색 → 없음 → 외부 30분 탐색 → "본진 B동앞 보호구 걸이대" 발견
    → 원본에 없는 장소로 보정 ❌
  
  [v6.2 — 올바른 결과]
    합리성 점수 계산 (post_work 시간대):
      · 본진 타각기 앞 보호구 걸이대: 120 × 10(RACK) × 2.0(야간) = 2400점 ✓
      · 공사현장:                    80 × 5(WORK) × 0.3(야간) = 120점
      · 타각기 출구:                 40 × 1(TRANSIT) × 1.0 = 40점
    → "본진 타각기 앞 보호구 걸이대"로 통일 (원본 내 최고 점수)

예시 2: Multi-Pass 수렴
[1회차]
  Pass 1: 새벽 0:00~4:41 Ghost → 걸이대로 통일 (200행)
  Pass 2: FAB↔휴게실 번갈음 → 휴게실로 흡수 (15행)
  Pass 4: A-B-A 노이즈 잔류 → 흡수 (8행)
  총 변경: 223행

[2회차]
  Pass 1: 잔여 Ghost 0행
  Pass 2: 잔여 번갈음 2행
  Pass 4: 노이즈 잔류 1행
  총 변경: 3행 < 5 → 수렴 완료!

결과: 2회 반복으로 참값에 수렴
```

**v6 Ghost Signal 탐지 예시** (황*석 새벽 0:00~4:41):
```
활성신호갯수 = 0          → E1: none (사람 움직임 없음)
위치: 걸이대↔공사현장↔타각기출구 → E4: 불안정 (BLE 반사로 위치 점프)
시간: 새벽 (비근무)       → E3: pre_work
공간: RACK + TRANSIT_GATE → E2: RACK이 체류 공간

4개 증거 합산:
"새벽 + 무활성 + 위치불안정 + RACK 인근"
= Ghost Signal (태그가 걸이대에 걸려있음)
→ 전체 구간 → RACK 보정
```

### 24-E. 시각화 개선

| 문제 | 해결책 | 구현 |
|------|--------|------|
| 1~2분 블록이 너무 좁아 안 보임 | MIN_DISPLAY_MINUTES=3 최소 표시 폭 보장 | journey_verify.py |
| 짧은 블록이 긴 블록에 묻힘 | 3분 이하 블록에 진한 테두리 추가 | theme.py: get_block_color() |

### 24-F. 구현 파일 목록

| 파일 | 변경 내용 |
|------|---------|
| `constants.py` | `SPACE_DWELL_PROFILE`, `SEQUENCE_*`, `MIN_DISPLAY_MINUTES`, `SPACE_FUNCTION_PRIORITY`, `TRANSIT_ONLY_FUNCTIONS` 상수 추가. **v6**: `ACTIVE_SIG_*`, `LOCATION_*`, `RUN_*`, `GHOST_SIGNAL_RACK_SEARCH_WINDOW` 등 4증거 파라미터 추가. **v6.1**: `GHOST_MIN_BLOCK_LEN`, `GHOST_WORK_MIN_BLOCK_LEN`, `NARRATIVE_*`, `IMPOSSIBLE_*`, `CONVERGENCE_CHANGE_THRESH`, `MULTI_PASS_MAX_ITERATIONS` Multi-Pass 파라미터 추가 |
| `preprocessor.py` | `_get_runs()`, `_get_run_length_at()`, `_interpret_sequence_context()`, `_detect_alternating_pattern()` 추가. **v6**: `_collect_evidence()`, `_segment_day()`, `_correct_ghost_signals()`, `_correct_alternating_by_context()` 추가. **v6.1**: `_pass3_verify_narrative()`, `_pass4_detect_impossible_movement()`, `_count_corrections()` 추가 + 메인 파이프라인 Multi-Pass Loop 재구성 |
| `schema.py` | **v6.1**: `ANOMALY_FLAG` 컬럼 확장 (impossible_teleport, impossible_building_jump 등 추가) |
| `theme.py` | `get_block_color()`, `_darken()`, `SHORT_BLOCK_THRESHOLD_MIN` 추가 |
| `journey_verify.py` | MIN_DISPLAY_MINUTES 적용, get_block_color() 사용 |
| `cache_manager.py` | `CACHE_SCHEMA_VERSION = "6.2"` (v6.1 → v6.2). v6.2: 원본 기반 전체 맥락 추론 (블록 외부 RACK 탐색 제거, 공간+시간+빈도 점수 계산) |
| `place_utils.py` | **v6.3**: Y축 유사도 정렬 유틸리티 전면 리팩토링. `extract_place_prefix()` 시그니처 변경 (3-튜플 → 4-튜플), `_BUILDING_PRIORITY` 건물 우선순위 맵 추가, `get_place_group()` / `are_places_similar()` 헬퍼 함수 추가 |
| `site_analysis.py` | **v6.3**: `sort_places_by_similarity` import 추가, Gantt Y축 유사도 정렬 적용 |

### 24-F2. v6.2 원본 기반 맥락 추론 상수 상세

```python
# constants.py v6.1 파라미터

# ═══════════════════════════════════════════════════════════════════════════
# [E1] 활성신호 기반 상태 판단
# ═══════════════════════════════════════════════════════════════════════════
ACTIVE_SIG_GHOST_MAX: int = 0      # 이 값 이하 = 무활성 (Ghost Signal 후보)
ACTIVE_SIG_TRANSIT_MAX: int = 2    # 이 값 이하 + 위치 변화 = 이동 중
ACTIVE_SIG_WORK_MIN: int = 3       # 이 값 이상 = 실제 작업 활동

# ═══════════════════════════════════════════════════════════════════════════
# [E4] 위치 안정성 판단
# ═══════════════════════════════════════════════════════════════════════════
LOCATION_ENTROPY_WINDOW: int = 5   # 위치 안정성 계산 윈도우 (분)
LOCATION_UNSTABLE_THRESH: int = 2  # 윈도우 내 고유 장소 수 ≥ 이 값 = 불안정
RUN_SHORT_MAX: int = 5             # 단발 체류 최대 길이 (분)
RUN_CONTINUOUS_MIN: int = 10       # 연속 체류 최소 길이 (분)

# ═══════════════════════════════════════════════════════════════════════════
# [E3] 시간대 구간 정의
# ═══════════════════════════════════════════════════════════════════════════
NIGHT_END_HOUR: int = 5            # 0~5시 = 야간 (비근무)
PREDAWN_WORK_START: int = 5        # 5시부터 출근 가능
POST_WORK_HOUR: int = 20           # 20시 이후 = 퇴근 후

# ═══════════════════════════════════════════════════════════════════════════
# [Pass 1] Ghost Signal 파라미터
# ═══════════════════════════════════════════════════════════════════════════
GHOST_MIN_BLOCK_LEN: int = 5           # Ghost 블록 최소 길이 (분)
GHOST_WORK_MIN_BLOCK_LEN: int = 20     # 근무시간대 Ghost 최소 길이 (분)
GHOST_SIGNAL_RACK_SEARCH_WINDOW: int = 30  # Ghost 구간에서 RACK 탐색 범위 (분)

# ═══════════════════════════════════════════════════════════════════════════
# [Pass 3] 맥락 검증 파라미터
# ═══════════════════════════════════════════════════════════════════════════
NARRATIVE_ANCHOR_MIN_DWELL: int = 10   # 앵커 공간 최소 체류 (분)
NARRATIVE_WORK_MIN_RATIO: float = 0.3  # 근무시간 중 작업 구역 최소 비율

# ═══════════════════════════════════════════════════════════════════════════
# [Pass 4] 물리적 이상치 파라미터
# ═══════════════════════════════════════════════════════════════════════════
IMPOSSIBLE_MOVE_SPEED: float = 500.0   # 1분 내 최대 이동 거리 (좌표 단위)
IMPOSSIBLE_BUILDING_JUMP_MIN: int = 2  # 건물 간 이동 최소 시간 (분)

# ═══════════════════════════════════════════════════════════════════════════
# [수렴 판단]
# ═══════════════════════════════════════════════════════════════════════════
CONVERGENCE_CHANGE_THRESH: int = 5     # Pass에서 변경된 행 수 < 이 값 = 수렴
MULTI_PASS_MAX_ITERATIONS: int = 3     # 최대 반복 횟수
```

### 24-G. v5.2 앵커 공간 보호 강화 (2026-03-02 추가)

**문제**: 휴게실, 흡연장 등 앵커 공간이 DBSCAN 클러스터링 및 노이즈 보정 과정에서 다른 장소로 덮어씌워짐.

**원인 분석**:
1. DBSCAN 클러스터링 시 휴게실 행도 포함되어 다른 장소와 같은 클러스터로 묶임
2. 클러스터 대표 장소 결정 시 신호 수 가중치로 선택 → 휴게실이 소수면 탈락
3. 노이즈 보정 시 nearest-cluster로 대체 → 휴게실이 노이즈로 판정되면 다른 장소로 변경

**수정 내용** (`preprocessor.py`):

| 함수 | 변경 |
|------|------|
| `_cluster_locations_by_key()` | 앵커 공간(휴게실, 흡연장)을 클러스터링 대상에서 **완전 제외** |
| `_cluster_locations_by_key()` (대표 장소 결정) | 앵커 공간은 `CLUSTER_PLACE`로 덮어쓰지 않음 |
| `_correct_noise_by_cluster()` | 앵커 공간은 노이즈여도 **원본 유지** |
| `_weighted_mode_place()` | 클러스터에 휴게실이 있으면 해당 장소를 **우선 선택** |

**앵커 공간 키워드**:
```python
rest_keywords = ["휴게", "흡연", "식당", "탈의", "화장실", "휴식"]
```

### 24-G2. v5.4 공간 우선순위 기반 번갈음 패턴 보정 (2026-03-02 추가 → 과보정 수정)

**핵심 통찰**:
```
"두 장소가 번갈아 나올 때, 어느 쪽이 진짜인지는 공간 특성이 결정한다"
"신호 수(다수결)보다 공간의 물리적 특성이 더 강한 증거"
```

**v5.4 초기 문제** (과보정 버그):
```
❌ v5.3에서 _weighted_mode_place()에 space priority를 적용했더니:
   → 클러스터 내 FAB_휴게실이 1행만 있어도 전체 클러스터 대표 장소가 됨
   → FAB 1F 30분짜리 DBSCAN 클러스터 전체가 FAB_휴게실로 덮어씌워짐
   → Gantt에서 FAB_휴게실이 3~4시간으로 과대 표시
```

**v5.4 수정 핵심 원칙**:
```
⚠️ Space Priority는 "번갈음 단발성 패턴"에만 적용해야 한다.

적용해야 할 경우:
  FAB_휴게실(2분) → FAB 1F(1분) → FAB_휴게실(3분)  → 양쪽 단발, 번갈음 → 휴게실 우세
  타각기출구(2분) → 보호구걸이대(1분) → 타각기출구(1분) → TRANSIT_GATE loser → RACK 우세

적용하면 안 되는 경우:
  FAB 1F 연속 30분 (클러스터) + FAB_휴게실 1행 포함
    → FAB 1F가 압도적 연속 → 클러스터 대표는 FAB 1F 유지
    → FAB_휴게실 1행은 앵커 보호로 독립 보존 (기존 v5.2 로직)
```

**공간 우선순위 (SPACE_FUNCTION_PRIORITY)**:
```
RACK             = 1   # 헬멧 거치대 — 퇴근/출근 확실한 체류
REST             = 2   # 휴게실·흡연장 — 들어가서 머무는 공간
WORK             = 3   # 일반 작업공간
WORK_HAZARD      = 4
TRANSIT_WORK     = 5   # 실외 공사구역 (이동+작업 혼재)
TRANSIT_CORRIDOR = 6   # 복도·계단 (이동 통로)
TRANSIT_GATE     = 7   # 출입구·타각기 (통과 전용, 체류 불가)
OUTDOOR_MISC     = 8
UNKNOWN          = 9
```

**v5.4 신규 상수** (`constants.py`):
```python
PRIORITY_MAX_RUN_MIN = 5            # Space Priority 동작 최대 단발 구간 (분)
PRIORITY_CONTINUOUS_THRESHOLD = 10  # 이 이상 연속이면 Space Priority 비적용 (분)
```

**수정 내용** (`preprocessor.py`):

| 함수 | v5.3 → v5.4 변경 |
|------|-----------------|
| `_weighted_mode_place()` | ❌ space_function priority 제거 → **signal_count 가중 최빈값으로 원복** (과보정 원인 제거) |
| `_detect_alternating_pattern()` | 연속 구간 조건 추가: run ≤ `PRIORITY_MAX_RUN_MIN` 이고 윈도우 내 최장 run < `PRIORITY_CONTINUOUS_THRESHOLD` 일 때만 적용 |
| `_calc_run_lengths()` (v5.4 신규) | 각 행의 연속 구간 길이 계산: [A,A,A,B,A,A] → [3,3,3,1,2,2] |

**번갈음 패턴 규칙 (Rule 5 - Space Priority Resolution) v5.4 조건**:
1. **조건 1**: 현재 행의 연속 구간 길이 ≤ `PRIORITY_MAX_RUN_MIN` (5분)
2. **조건 2**: 윈도우(10분) 내 고유 장소가 정확히 2개
3. **조건 3**: 윈도우 내 최장 연속 구간 < `PRIORITY_CONTINUOUS_THRESHOLD` (10분)
4. **조건 4**: A-B-A 또는 B-A-B 번갈음 패턴 확인
5. **조건 5**: TRANSIT 전용 공간이 loser이거나 우선순위 차이가 1 이상
6. → loser 장소 행들을 winner로 교체, `state_detail = "absorbed_by_priority"`

**체류시간 집계 유틸리티 (v5.4 추가)**:
```python
calc_place_dwell_stats(df_worker, place)
  → {"total_min": 12, "max_run": 8, "visit_count": 3, "runs": [8, 2, 2]}

calc_all_place_dwell_summary(df_worker)
  → DataFrame: 장소, 총체류(분), 최장연속(분), 방문횟수
```

### 24-H. 검증 체크리스트

| 케이스 | 기대 결과 | 상태 |
|--------|---------|------|
| FAB → CUB → FAB → WWT (각 2~3분) | 전체 이동(transit_sequence)으로 판정 | ⏳ 검증 필요 |
| 휴게실 1분 → FAB → 휴게실 5분 | 중간 FAB이 휴게실로 흡수(rest_absorbed) | ⏳ 검증 필요 |
| 흡연장 1분 (앞뒤 작업) | 유효 REST 체류로 유지 **(v5.2 핵심 수정)** | ✅ v5.2 앵커 사전 처리로 해결 |
| 점심 후 13:10 휴게실 3분 | rest_postlunch로 강화 | ⏳ 검증 필요 |
| 출근 직후 07:03 FAB 4분 | transit_arrival로 이동 처리 | ⏳ 검증 필요 |
| 짧은 블록(1~3분)이 Gantt에 표시됨 | MIN_DISPLAY_MINUTES + 진한 테두리로 가시성 확보 | ⏳ 검증 필요 |
| **FAB_휴게실 데이터 유지** | 보정 후에도 휴게실 데이터 보존 **(v5.2 핵심)** | ✅ v5.2 앵커 사전 처리로 해결 |
| **흡연장 데이터 유지** | 보정 후에도 흡연장 데이터 보존 **(v5.2 핵심)** | ✅ v5.2 앵커 사전 처리로 해결 |
| **FAB 1F 30분 연속 작업** | 그대로 유지 (run=30 > CONTINUOUS_THRESHOLD=10) **(v5.4 핵심)** | ⏳ 검증 필요 |
| **FAB_휴게실(5분) ↔ FAB 1F(2분) ↔ FAB_휴게실(3분)** | 중간 FAB 1F가 휴게실로 흡수 (run=2 ≤ MAX=5) **(v5.4 핵심)** | ⏳ 검증 필요 |
| **타각기 출구 ↔ 보호구 걸이대 (19시 이후)** | 타각기 출구가 걸이대로 흡수 (TRANSIT_GATE loser) **(v5.4)** | ⏳ 검증 필요 |
| **FAB_휴게실이 3~4시간으로 표시** | 사라짐 (FAB 1F 연속 구간 보호됨) **(v5.4 과보정 수정)** | ⏳ 검증 필요 |
| **체류시간 정확 집계** | 휴게실 5분이 "1분"이 아닌 연속 블록으로 표시 **(v5.4)** | ⏳ 검증 필요 |
| **황*석 0:00~4:41 Ghost Signal** | 걸이대 단일 구간으로 통일. 공사현장·타각기출구 소멸 **(v6 핵심)** | ⏳ 검증 필요 |
| **황*석 19시 이후 퇴근 Ghost** | 타각기출구 소멸. 보호구 걸이대만 표시 **(v6 핵심)** | ⏳ 검증 필요 |
| **segment_type 컬럼** | pre_work/work/lunch/post_work 올바르게 분절됨 **(v6 검증)** | ⏳ 검증 필요 |
| **e_ghost_candidate 플래그** | 0:00~4:41 구간 True, 작업 시간대 False **(v6 검증)** | ⏳ 검증 필요 |
| **Multi-Pass 수렴** | 2~3회 반복 후 변경 < 5개로 수렴 **(v6.1 핵심)** | ⏳ 검증 필요 |
| **Pass 3 맥락 이상** | 비근무시간 WORK 구간 `anomaly_work_in_off_hours` 태깅 **(v6.1)** | ⏳ 검증 필요 |
| **Pass 4 텔레포트** | 1분 내 500+ 좌표 이동 `impossible_teleport` 태깅 **(v6.1)** | ⏳ 검증 필요 |
| **Pass 4 노이즈 잔류** | A-B-A 패턴 중간 B가 흡수됨 `noise_residual_absorbed` **(v6.1)** | ⏳ 검증 필요 |

---

## 25. Claude API 연동 LLM 해석 레이어 (2026-03-02 구현)

### 25-A. 목적

보정 완료된 집계 데이터(숫자)를 받아 **자연어 내러티브**를 생성.
Journey 보정 자체는 rule-based 유지 — **LLM은 '해석'에만 사용**.

```
┌────────────────────────────────────────────────────────────────────┐
│  데이터 흐름                                                        │
├────────────────────────────────────────────────────────────────────┤
│  Raw CSV → 전처리(rule-based) → 집계 지표 → Claude API → 내러티브  │
│                                                                    │
│  ⚠️ LLM은 데이터를 보정하지 않음. 해석만 담당.                       │
│  ⚠️ API 실패 시 rule-based fallback 자동 전환.                      │
└────────────────────────────────────────────────────────────────────┘
```

### 25-B. 사용처

| 위치 | 함수 | 설명 |
|------|------|------|
| 현장 전체 탭 | `_render_site_ai_summary()` | 현장 일일 요약 카드 (페이지 최상단) |
| 작업자별 탭 | `_render_worker_ai_narrative()` | 작업자 하루 내러티브 카드 |
| Safety Alert (예정) | `generate_anomaly_explanation()` | 이상 패턴 설명 |

### 25-C. API 키 설정

```
⚠️ 절대 코드에 하드코딩 금지!

방법 A (로컬 개발): .env 파일
  ANTHROPIC_API_KEY=sk-ant-api03-...

방법 B (배포): .streamlit/secrets.toml
  ANTHROPIC_API_KEY = "sk-ant-api03-..."

※ 두 파일 모두 .gitignore에 포함되어 있음
```

### 25-D. 의존성

```
# requirements.txt
anthropic>=0.25.0      # Claude API (선택 의존성 — 없어도 fallback 동작)
python-dotenv>=1.0.0   # .env 파일 로드
```

### 25-E. 핵심 파일

| 파일 | 내용 |
|------|------|
| `src/utils/llm_interpreter.py` | Claude API 래퍼, 캐싱, fallback 로직 |
| `.env.example` | API 키 템플릿 (실제 키는 .env에 입력) |
| `.gitignore` | .env, .streamlit/secrets.toml 포함 |

### 25-F. 주요 함수

```python
# llm_interpreter.py

is_llm_available() -> bool
    # LLM 기능 사용 가능 여부 확인 (패키지 + API 키)

generate_worker_narrative(summary: dict, worker_name: str) -> str
    # 작업자 하루 집계 → 자연어 내러티브
    # summary 필수 키: date, onsite_hours, ewi, high_work_min, ...

generate_site_daily_summary(site_summary: dict, date_str: str) -> str
    # 현장 전체 일일 지표 → 자연어 요약
    # site_summary 필수 키: worker_count, avg_ewi, total_standby_min, ...

generate_anomaly_explanation(anomaly: dict) -> str
    # 이상 패턴 이벤트 → 자연어 설명

# 캐싱 래퍼 (같은 입력 1시간 내 재호출 방지)
cached_worker_narrative(summary_frozen: tuple, worker_name: str) -> str
cached_site_summary(summary_frozen: tuple, date_str: str) -> str
```

### 25-G. Fallback 동작

```
API 사용 가능:
  Claude claude-sonnet-4-5 모델 호출 → 자연어 내러티브 반환

API 사용 불가 (패키지 미설치, 키 없음, 호출 실패):
  _fallback_worker_narrative() → 템플릿 기반 rule-based 내러티브
  _fallback_site_summary() → 템플릿 기반 rule-based 요약
  ※ 대시보드 기능 중단 없음
```

### 25-H. UI 표시 원칙

```
· st.spinner("🧠 AI 분석 중...")으로 로딩 표시
· "🧠 AI 작업 분석" 레이블 명시 (담당자가 AI 해석임을 인지)
· st.cache_data(ttl=3600)으로 같은 데이터 재호출 방지
· 프로토타입 면책 문구:
  "⚠️ 이 AI 해석은 BLE 센서 데이터 기반 추정입니다.
   실제 현장 상황과 다를 수 있으며, 관리자 판단을 대체하지 않습니다."
```

### 25-I. 모델 설정

```python
# llm_interpreter.py
_MODEL       = "claude-sonnet-4-5"   # claude-haiku-4-5로 교체 시 더 빠름/저렴
_MAX_TOKENS  = 400
_TEMPERATURE = 0.3   # 낮을수록 일관성 ↑, 창의성 ↓
```

---

## 26. Deep Con — 건설현장용 공간 AI 미래 방향성

> **Deep Con**: 건설현장의 작업자 이동 데이터를 "문장"처럼 분석하여 노이즈 보정, 이상 행동 탐지, 공간 활용 최적화를 수행하는 공간 AI 시스템

### 26-A. 핵심 철학

```
┌────────────────────────────────────────────────────────────────────────────┐
│  Journey = 문장 (Sentence)                                                 │
│  장소 = 단어 (Word)                                                        │
│                                                                            │
│  "걸이대 → FAB 1F → 휴게실 → FAB 1F → 걸이대"                              │
│     ↓                                                                      │
│  "출근 → 작업 → 휴식 → 작업 → 퇴근"                                        │
│                                                                            │
│  이 "문장"에서 문법(패턴)을 학습하면:                                        │
│  1. 어색한 단어 조합 탐지 → 노이즈 보정                                      │
│  2. 비정상 문장 구조 탐지 → 이상 행동 알림                                   │
│  3. 같은 직군의 문장 패턴 → 집단 행동 분석                                   │
│  4. 시간대별 단어 출현 확률 → 공간 활용 예측                                 │
└────────────────────────────────────────────────────────────────────────────┘

원칙:
  1. 부분이 아닌 전체 Journey 맥락 — 하루 전체를 하나의 문장으로 분석
  2. 원본 데이터 기반 합리적 추론 — 없는 장소로 보정하지 않음
  3. 공간 + 시간 + 이동 특성 종합 — 다차원 증거 통합 판단
  4. 개인 → 집단 → 시계열 확장 — 점진적 패턴 학습
```

### 26-B. 구현 로드맵

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Phase 1: 규칙 기반 보정 ✅ 완료 (v6.2)                                      │
│  ─────────────────────────────────────────────────────────────────────────  │
│  · 공간 특성 + 시간 맥락 + 빈도 종합 추론                                    │
│  · 원본 데이터 내에서만 보정                                                 │
│  · 개인 1일 데이터 분석                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 2: Journey 문장화 보정 ✅ 완료 (v6.3)                                 │
│  ─────────────────────────────────────────────────────────────────────────  │
│  · Run 시퀀스 생성: 1440분 → N개 Run으로 압축 (= 문장의 단어들)              │
│  · 전체 맥락 분석: 주요 장소, 출퇴근 패턴, 짧은 Run 통계                     │
│  · 흡수 판단: A-X-A 노이즈, TRANSIT 단발, 맥락 이상 등                       │
│  · 앵커 보호: 휴게실/걸이대는 짧아도 흡수하지 않음                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 2.5: EWI 출퇴근 기반 계산 ✅ 완료 (v6.4)                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│  · detect_work_shift(): 출근/퇴근 시점 자동 감지                            │
│  · 출근: 헬멧 거치대(RACK)에서 처음 벗어나는 시점                            │
│  · 퇴근: 마지막으로 헬멧 거치대(RACK)로 돌아오는 시점                        │
│  · EWI 분모: 출근~퇴근 사이 시간만 사용 (거치대 시간 제외)                   │
│  · 효과: 황*석 EWI 25.2% → 76.8% (야간 거치대 시간 685분 제외)              │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 2.6: 생산성 분석 + Claude API ✅ 완료 (v6.5)                          │
│  ─────────────────────────────────────────────────────────────────────────  │
│  · 음영지역(shadow area) 고려 EWI 계산                                      │
│  · 분모 변경: 기록된 데이터 수 → 실제 시간 차이 (출퇴근 시간 차이)           │
│  · 작업자별 상세 시간 분석: 출퇴근/근무/수집/음영지역 시간 표시              │
│  · Claude API 연동 완료: 사이드바 상태 표시, AI 내러티브 생성                │
│  · .env 경로 명시적 지정으로 API 키 로드 문제 해결                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 3: 집단 패턴 학습 ⏳ 다음                                             │
│  ─────────────────────────────────────────────────────────────────────────  │
│  · 같은 회사 작업자들의 Journey 패턴 분석                                    │
│  · 시간대별 장소 출현 확률 모델                                              │
│  · "이 시간에 이 장소는 비정상" 판단 근거                                    │
│  · 집단 대비 개인 이상치 탐지                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  Phase 4: 멀티데이 + 이상탐지 ⏳ 대기                                        │
│  ─────────────────────────────────────────────────────────────────────────  │
│  · 여러 날 데이터로 개인 시계열 패턴 학습                                    │
│  · 정상 패턴에서 벗어난 행동 탐지                                            │
│  · 피로 누적, 동선 변화, 이상 징후 조기 경보                                 │
│  · 현장 전체 공간 활용 최적화 제안                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 26-C. 데이터 활용 전략

| 범위 | 설명 | 활용 |
|------|------|------|
| 1명 1일 | 개인 Journey 맥락 | v6.2 현재 구현 |
| N명 1일 | 같은 회사 집단 패턴 | Phase 3 계획 |
| 1명 N일 | 개인 시계열 패턴 | Phase 4 계획 |
| N명 N일 | 현장 전체 공간 활용 패턴 | 미래 확장 |

### 26-D. 패턴 학습 예시

```
[A사 작업자 20명 학습 결과]
  일반 패턴: "걸이대 → 타각기 → 작업장 → 휴게실 → 작업장 → 타각기 → 걸이대"
  
[황*석 원본 Journey] (새벽, 활성신호=0)
  "걸이대 → 공사현장 → 타각기출구 → 공사현장 → 걸이대..."
  
[Deep Con 추론]
  - 새벽 + 무활성 + 타각기/공사현장은 이 시간대에 다른 A사 작업자 0% 출현
  - "걸이대"만 이 시간대에 90%+ 출현
  → "걸이대"로 통일 (집단 패턴 + 개인 맥락 종합)
```

### 26-E. 이상 행동 탐지 예시 (미래)

```
[정상 패턴 학습 후]

이상 패턴 1: 단독 위험 구역 체류
  · 학습된 패턴: 위험 구역 체류 시 평균 2명 이상 동반
  · 탐지: 황*석, 11:30 위험 구역 30분 단독 체류
  → 알림: "⚠️ 단독 위험작업 의심"

이상 패턴 2: 비정상 동선
  · 학습된 패턴: A사 작업자는 오전에 FAB 1F 집중
  · 탐지: 황*석, 오전 내내 WWT 1F 체류 (집단 대비 이탈)
  → 알림: "📊 동선 이상 — 확인 필요"

이상 패턴 3: 피로 누적 징후
  · 학습된 패턴: 황*석 평균 휴게실 사용 20분/일
  · 탐지: 3일 연속 휴게실 사용 5분 미만
  → 알림: "🔋 휴식 부족 징후"
```

### 26-F. 기술 스택 (예정)

| 구성 요소 | 현재 | 미래 |
|-----------|------|------|
| 보정 엔진 | 규칙 기반 (v6.2) | 패턴 학습 기반 |
| 패턴 학습 | - | Sequence Model / Transformer |
| 이상 탐지 | anomaly_flag (규칙) | Anomaly Detection ML |
| 해석 | LLM (Claude) | LLM + 패턴 근거 |
| 데이터 | 1일 단위 | 시계열 DB (다중 날짜) |

### 26-G. 관련 파일

| 파일 | 현재 역할 | Deep Con 확장 |
|------|----------|---------------|
| `preprocessor.py` | Journey 보정 | Pattern-aware 보정 추가 |
| `soif.py` | 지표 계산 | 집단 패턴 지표 추가 |
| `llm_interpreter.py` | 내러티브 생성 | 패턴 근거 해석 |
| (신규) `pattern_learner.py` | - | 집단/개인 패턴 학습 |
| (신규) `anomaly_detector.py` | - | 이상 행동 탐지 |
