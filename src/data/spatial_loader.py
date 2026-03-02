"""
SSMP 공간 구조 통합 로더.

Datafile/ssmp_structure/ CSV 파일들을 읽어 장소 메타데이터,
좌표계 정보, 건물 간 거리 계산을 제공한다.

좌표계 원칙:
  (A) ref_type == "sector" → Outdoor: Sector 좌표계, 건물 간 거리 계산 가능
  (B) ref_type == "level"  → Indoor: 해당 Building+Level 독립 직각좌표계
      절대 다른 Building이나 Level의 좌표와 직접 비교하지 말 것
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ─── 장소 유형 상수 ──────────────────────────────────────────────────────────
class PlaceType:
    """SSMP 기반 장소 유형 상수."""
    HELMET_RACK    = "HELMET_RACK"    # 헬멧/보호구 걸이대
    GATE           = "GATE"           # 출입 통제 지점 (타각기, 게이트)
    REST           = "REST"           # 휴게 시설 (휴게실, 식당, 흡연장 등)
    WORK_AREA      = "WORK_AREA"      # 작업 구역
    CONFINED_SPACE = "CONFINED_SPACE" # 밀폐공간 (위험 구역)
    INDOOR         = "INDOOR"         # 실내 (분류 불가 시 기본)
    OUTDOOR        = "OUTDOOR"        # 실외 (분류 불가 시 기본)
    UNKNOWN        = "UNKNOWN"        # 분류 실패


# zone_type → PlaceType 매핑
_ZONE_TYPE_MAP: dict[str, str] = {
    "amenity_rest":          PlaceType.REST,
    "amenity_smoking":       PlaceType.REST,
    "checkpoint_gate":       PlaceType.GATE,
    "checkpoint_timeclock":  PlaceType.GATE,
    "work_area":             PlaceType.WORK_AREA,
    "confined_space":        PlaceType.CONFINED_SPACE,
    "target_area":           PlaceType.WORK_AREA,
    "other":                 PlaceType.INDOOR,  # 기본값: 실내 작업 구역
    "parking":               PlaceType.OUTDOOR,
}

# service_domain → PlaceType 매핑 (zone 정보 없는 level 멤버 처리)
_DOMAIN_MAP: dict[str, str] = {
    "facility":        PlaceType.REST,     # 편의시설 = 휴게
    "access_control":  PlaceType.GATE,     # 출입 통제
    "safety":          PlaceType.WORK_AREA,# 안전 관리 구역 = 작업 구역
    "productivity":    PlaceType.WORK_AREA,# 생산성 구역 = 작업 구역
    "other":           PlaceType.INDOOR,   # 기타 = 실내
}

# 헬멧 걸이대 이름 키워드 (폴백 매칭용)
_HELMET_RACK_KW = ["보호구 걸이대", "보호구걸이대", "헬멧 걸이", "안전모 걸이"]
# 타각기/게이트 키워드 (name 보조 매칭)
_GATE_KW = ["타각기", "GATE", "게이트", "정문", "입구", "출구"]
# 휴게 시설 키워드 (폴백)
_REST_KW = ["휴게", "식당", "탈의실", "흡연장", "흡연실"]


class SpatialContext:
    """
    SSMP 공간 구조 메타데이터 로더.

    ssmp_structure/ 폴더의 CSV 파일들을 읽어서
    장소 분류, 좌표계 정보, 건물 간 거리 계산을 제공한다.

    없는 파일은 경고 후 스킵 (graceful degradation).
    매칭 실패 시 키워드 기반 폴백 분류를 수행한다.
    """

    def __init__(self, ssmp_dir: Path) -> None:
        """
        ssmp_structure/ 폴더를 읽어 공간 메타데이터 초기화.

        Args:
            ssmp_dir: ssmp_structure/ 폴더 경로
        """
        self._ssmp_dir = Path(ssmp_dir)
        self._loaded   = False

        # UI 표시용 집계 속성 (로드 성공 시 덮어씀)
        self.service_section_count = 0
        self.zone_count            = 0
        self.spot_count            = 0

        # 내부 lookup 테이블
        self._ss_lookup: dict[str, str] = {}        # service_section_name → PlaceType
        self._ss_ref_type: dict[str, str] = {}       # service_section_name → ref_type
        self._ss_location_key: dict[str, str] = {}   # service_section_name → location_key
        self._zone_lookup: dict[str, str] = {}        # zone_name → PlaceType
        self._zone_ref_type: dict[str, str] = {}      # zone_name → ref_type
        self._zone_location_key: dict[str, str] = {}  # zone_name → location_key
        self._building_coords: dict[str, tuple[float, float]] = {}  # building_name → (x, y)
        # v4 신규: space_function 분류용
        self._zone_type_lookup: dict[str, str] = {}   # place_name → zone_type (raw)
        self._risk_level_lookup: dict[str, str] = {}  # place_name → risk_level

        self._load(ssmp_dir)

    # ─── 로딩 ────────────────────────────────────────────────────────────────

    def _load(self, ssmp_dir: Path) -> None:
        """CSV 파일들을 순차적으로 로드하여 lookup 테이블 구성."""
        ssmp_dir = Path(ssmp_dir)
        if not ssmp_dir.exists():
            logger.warning(f"ssmp_structure 폴더 없음: {ssmp_dir}. 키워드 폴백으로 동작.")
            return

        # ── 1. levels: level_id → (building_id, level_index) ──────────────
        levels_df = self._safe_read(ssmp_dir / "ssmp_levels.csv")
        level_to_bld: dict[str, str] = {}
        level_to_idx: dict[str, int] = {}
        if levels_df is not None:
            for _, r in levels_df.iterrows():
                lid = str(r.get("level_id", ""))
                bid = str(r.get("building_id", ""))
                idx = int(r.get("level_index", 0)) if pd.notna(r.get("level_index")) else 0
                if lid:
                    level_to_bld[lid] = bid
                    level_to_idx[lid] = idx

        # ── 2. zones: zone_id → (zone_name, zone_type, ref_type, location_key) ─
        zones_df = self._safe_read(ssmp_dir / "ssmp_zones.csv")
        zone_id_to_type: dict[str, str] = {}
        if zones_df is not None:
            for _, r in zones_df.iterrows():
                zid  = str(r.get("zone_id", ""))
                name = str(r.get("zone_name", "")).strip()
                ztype = str(r.get("zone_type", "other"))
                ref_type = str(r.get("ref_type", "sector"))
                level_id = str(r.get("level_id", ""))
                building_id = str(r.get("building_id", ""))

                if not zid:
                    continue

                place_type = _ZONE_TYPE_MAP.get(ztype, PlaceType.INDOOR)
                # 이름에 "밀폐" 포함 시 강제 CONFINED_SPACE
                if "밀폐" in name:
                    place_type = PlaceType.CONFINED_SPACE

                # 헬멧 걸이대 키워드 우선
                if any(kw in name for kw in _HELMET_RACK_KW):
                    place_type = PlaceType.HELMET_RACK

                loc_key = self._make_loc_key(ref_type, building_id, level_id, level_to_idx)

                zone_id_to_type[zid] = place_type
                if name:
                    self._zone_lookup[name] = place_type
                    self._zone_ref_type[name] = ref_type
                    self._zone_location_key[name] = loc_key
                    self._zone_type_lookup[name] = ztype  # v4: raw zone_type 저장

        # ── 3. service sections + members ──────────────────────────────────
        ss_df = self._safe_read(ssmp_dir / "ssmp_service_sections.csv")
        ssm_df = self._safe_read(ssmp_dir / "ssmp_service_section_members.csv")

        if ss_df is not None and ssm_df is not None:
            # ss_id → service_section_name, service_domain, risk_level
            ss_info: dict[str, dict] = {}
            for _, r in ss_df.iterrows():
                sid = str(r.get("service_section_id", ""))
                name = str(r.get("service_section_name", "")).strip()
                domain = str(r.get("service_domain", "other"))
                risk = str(r.get("risk_level", "medium"))
                if sid and name:
                    ss_info[sid] = {"name": name, "domain": domain, "risk": risk}

            for _, r in ssm_df.iterrows():
                sid = str(r.get("service_section_id", ""))
                member_type = str(r.get("member_type", "level"))
                member_id = str(r.get("member_id", ""))

                if sid not in ss_info:
                    continue

                ss_name = ss_info[sid]["name"]
                domain  = ss_info[sid]["domain"]
                risk    = ss_info[sid]["risk"]

                if member_type == "zone" and member_id in zone_id_to_type:
                    place_type = zone_id_to_type[member_id]
                    # zones_df에서 ref_type, location_key 조회
                    ref_type = "sector"
                    loc_key  = "OUTDOOR"
                    if zones_df is not None:
                        zm = zones_df[zones_df["zone_id"] == member_id]
                        if not zm.empty:
                            zr = zm.iloc[0]
                            ref_type = str(zr.get("ref_type", "sector"))
                            bid = str(zr.get("building_id", ""))
                            lid = str(zr.get("level_id", ""))
                            loc_key = self._make_loc_key(ref_type, bid, lid, level_to_idx)
                else:
                    # level member: 전체 층 단위
                    place_type = _domain_to_place(ss_name, domain, risk)
                    ref_type = "level"
                    bid = level_to_bld.get(member_id, "")
                    loc_key = self._make_loc_key(ref_type, bid, member_id, level_to_idx)

                self._ss_lookup[ss_name] = place_type
                self._ss_ref_type[ss_name] = ref_type
                self._ss_location_key[ss_name] = loc_key
                self._risk_level_lookup[ss_name] = risk  # v4: risk_level 저장

        # ── 4. spots: spot 수 집계 (UI 표시용) ─────────────────────────────
        spots_df = self._safe_read(ssmp_dir / "ssmp_spots.csv")

        self._loaded = True
        # UI 표시용 집계 속성
        # service_section_count: 매칭 가능한 서비스구역 이름 수
        # zone_count: 매칭 가능한 Zone 이름 수
        # spot_count: ssmp_spots.csv 행 수 (센서/장비 위치 포인트)
        #   ⚠️ 이전 버전에서 spot_count = len(zones_df)로 잘못 집계됨 (2026-02 수정)
        self.service_section_count = len(self._ss_lookup)
        self.zone_count            = len(self._zone_lookup)
        self.spot_count            = len(spots_df) if spots_df is not None else 0
        logger.info(
            f"SpatialContext 로드 완료: "
            f"서비스구역 {self.service_section_count}개, "
            f"Zone {self.zone_count}개, "
            f"Spot {self.spot_count}개"
        )

    def _safe_read(self, path: Path) -> Optional[pd.DataFrame]:
        """CSV 파일 안전 읽기. 없거나 오류면 None 반환."""
        if not path.exists():
            logger.warning(f"SSMP 파일 없음 (스킵): {path.name}")
            return None
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except Exception as e:
            logger.warning(f"SSMP 파일 읽기 실패 {path.name}: {e}")
            return None

    @staticmethod
    def _make_loc_key(
        ref_type: str,
        building_id: str,
        level_id: str,
        level_to_idx: dict[str, int],
    ) -> str:
        """
        좌표계 구분 위치 키 생성.
          ref_type == "sector" → "OUTDOOR"
          ref_type == "level"  → f"{building_id}_L{level_index}"
        """
        if ref_type == "sector" or not building_id:
            return "OUTDOOR"
        idx = level_to_idx.get(level_id, 0)
        bld_short = building_id.split("-")[-1] if "-" in building_id else building_id
        return f"{bld_short}_L{idx}"

    # ─── Public API ───────────────────────────────────────────────────────────

    def classify_place(
        self,
        place_name: str,
        building: Optional[str] = None,
        floor: Optional[str] = None,
    ) -> str:
        """
        장소명으로 PlaceType 반환.

        우선순위:
        1. 헬멧 걸이대 키워드 (하드코딩, 최우선)
        2. SSMP 서비스구역(ssmp_service_sections) 이름 매칭
        3. SSMP Zone(ssmp_zones) 이름 매칭
        4. 게이트/타각기 키워드 매칭
        5. 휴게 시설 키워드 매칭
        6. building+floor 정보로 INDOOR/OUTDOOR 결정 (폴백)

        Args:
            place_name: 장소명 (보정 장소 기준)
            building: 건물명 (선택)
            floor: 층 정보 (선택)

        Returns:
            PlaceType 상수 문자열
        """
        if not place_name:
            return PlaceType.UNKNOWN

        name = str(place_name).strip()

        # 1. 헬멧 걸이대 우선
        if any(kw in name for kw in _HELMET_RACK_KW):
            return PlaceType.HELMET_RACK

        # 2. 서비스구역 정확 매칭
        if name in self._ss_lookup:
            return self._ss_lookup[name]

        # 3. Zone 이름 정확 매칭
        if name in self._zone_lookup:
            return self._zone_lookup[name]

        # 4. 서비스구역 부분 매칭 (zone_name 이 ss_name의 서브스트링인 경우)
        for ss_name, ptype in self._ss_lookup.items():
            if ss_name in name or name in ss_name:
                return ptype

        # 5. 게이트/타각기 키워드
        if any(kw in name for kw in _GATE_KW):
            return PlaceType.GATE

        # 6. 휴게 시설 키워드
        if any(kw in name for kw in _REST_KW):
            return PlaceType.REST

        # 7. building+floor 폴백
        has_building = bool(building and str(building).strip())
        has_floor = bool(floor and str(floor).strip())
        if has_building and has_floor:
            return PlaceType.INDOOR
        if not has_building and not has_floor:
            return PlaceType.OUTDOOR

        return PlaceType.UNKNOWN

    def get_location_key(
        self,
        place_name: Optional[str] = None,
        building: Optional[str] = None,
        floor: Optional[str] = None,
    ) -> str:
        """
        좌표계 구분을 위한 위치 키 반환.

        서비스구역/Zone 이름 매칭으로 위치 키 결정.
        매칭 실패 시 building+floor 정보로 폴백.

        규칙:
          Sector 기반 (OUTDOOR) → "OUTDOOR"
          Level 기반 (INDOOR)   → f"{building_id_short}_L{level_index}"

        중요: 반환값이 같은 행들만 좌표 비교/거리 계산 가능.

        Returns:
            위치 키 문자열
        """
        if place_name:
            name = str(place_name).strip()
            if name in self._ss_location_key:
                return self._ss_location_key[name]
            if name in self._zone_location_key:
                return self._zone_location_key[name]

        # building+floor 폴백
        has_building = bool(building and str(building).strip())
        has_floor = bool(floor and str(floor).strip())
        if has_building and has_floor:
            b = str(building).strip().replace(" ", "_")
            f = str(floor).strip().replace(" ", "_")
            return f"{b}_{f}"
        return "OUTDOOR"

    def calc_distance(
        self,
        loc_key_a: str,
        x_a: float,
        y_a: float,
        loc_key_b: str,
        x_b: float,
        y_b: float,
    ) -> Optional[float]:
        """
        두 위치 간 거리 계산.

        규칙:
          loc_key_a == loc_key_b → 유클리드 거리 반환
          loc_key_a != loc_key_b (다른 층/건물):
            - 두 건물 모두 outdoor 기준 좌표 있으면 → 추정 거리 반환
            - 없으면 → None 반환 (비교 불가)

        Args:
            loc_key_a: 위치 A의 위치 키
            x_a, y_a: 위치 A의 좌표
            loc_key_b: 위치 B의 위치 키
            x_b, y_b: 위치 B의 좌표

        Returns:
            거리 (float) 또는 None (비교 불가)
        """
        if pd.isna(x_a) or pd.isna(y_a) or pd.isna(x_b) or pd.isna(y_b):
            return None

        if loc_key_a == loc_key_b:
            return math.sqrt((x_b - x_a) ** 2 + (y_b - y_a) ** 2)

        # 다른 좌표계: 건물 outdoor 좌표로 추정 (현재 ssmp_buildings.csv에 outdoor 좌표 없음)
        # 향후 ssmp_buildings.csv에 outdoor_x, outdoor_y 컬럼 추가 시 활성화
        return None

    def get_building_outdoor_coord(
        self,
        building_name: str,
    ) -> Optional[tuple[float, float]]:
        """
        건물의 Outdoor 좌표계 기준 위치 반환.

        현재 ssmp_buildings.csv에 outdoor_x, outdoor_y 컬럼이 없어
        None을 반환한다.
        향후 SSMP 데이터 업데이트 시 활성화 예정.

        Args:
            building_name: 건물명

        Returns:
            (x, y) 튜플 또는 None
        """
        return self._building_coords.get(str(building_name).strip())

    def is_ssmp_matched(self, place_name: str) -> bool:
        """
        해당 장소명이 SSMP 데이터에서 매칭되었는지 여부.

        Args:
            place_name: 장소명

        Returns:
            True이면 SSMP 매칭, False이면 키워드 폴백
        """
        if not place_name:
            return False
        name = str(place_name).strip()
        return name in self._ss_lookup or name in self._zone_lookup

    def get_zone_type(self, place_name: str) -> Optional[str]:
        """
        장소명에 해당하는 SSMP zone_type 반환.

        Args:
            place_name: 장소명

        Returns:
            zone_type 문자열 또는 None
        """
        if not place_name:
            return None
        name = str(place_name).strip()
        return self._zone_type_lookup.get(name)

    def get_risk_level(self, place_name: str) -> Optional[str]:
        """
        장소명에 해당하는 SSMP risk_level 반환.

        Args:
            place_name: 장소명

        Returns:
            risk_level 문자열 (LOW/MEDIUM/HIGH/CRITICAL) 또는 None
        """
        if not place_name:
            return None
        name = str(place_name).strip()
        return self._risk_level_lookup.get(name)

    def get_place_metadata(self, place_name: str) -> dict:
        """
        장소명에 대한 전체 메타데이터 반환.

        Args:
            place_name: 장소명

        Returns:
            {place_type, location_key, ref_type, ssmp_matched} 딕셔너리
        """
        name = str(place_name).strip() if place_name else ""
        place_type = self.classify_place(name)
        loc_key = self.get_location_key(place_name=name)
        ref_type = (
            self._ss_ref_type.get(name)
            or self._zone_ref_type.get(name)
            or ("level" if loc_key != "OUTDOOR" else "sector")
        )
        return {
            "place_type":   place_type,
            "location_key": loc_key,
            "ref_type":     ref_type,
            "ssmp_matched": self.is_ssmp_matched(name),
        }

    def summary(self) -> str:
        """로드된 데이터 요약 문자열 반환."""
        return (
            f"SpatialContext(loaded={self._loaded}, "
            f"service_sections={len(self._ss_lookup)}, "
            f"zones={len(self._zone_lookup)})"
        )


# ─── 헬퍼 함수 ───────────────────────────────────────────────────────────────

def _domain_to_place(name: str, domain: str, risk: str) -> str:
    """
    서비스 섹션 이름/도메인/위험도로 PlaceType 결정.
    Zone 정보 없는 level-member 서비스 섹션에 적용.
    """
    # 헬멧 걸이대 키워드 최우선
    if any(kw in name for kw in _HELMET_RACK_KW):
        return PlaceType.HELMET_RACK

    # 밀폐공간
    if "밀폐" in name or risk == "critical":
        return PlaceType.CONFINED_SPACE

    # 타각기/게이트
    if any(kw in name for kw in _GATE_KW):
        return PlaceType.GATE

    # 도메인 기반
    if domain in _DOMAIN_MAP:
        mapped = _DOMAIN_MAP[domain]
        # facility 중 휴게 키워드 없으면 INDOOR로 보정
        if mapped == PlaceType.REST and not any(kw in name for kw in _REST_KW + ["FAB_휴게실", "WWT 2F 휴게실"]):
            return PlaceType.INDOOR
        return mapped

    return PlaceType.INDOOR


def load_spatial_context(ssmp_dir: Path) -> SpatialContext:
    """
    SpatialContext 인스턴스를 생성하여 반환하는 팩토리 함수.

    Args:
        ssmp_dir: ssmp_structure/ 폴더 경로

    Returns:
        SpatialContext 인스턴스
    """
    return SpatialContext(ssmp_dir)
