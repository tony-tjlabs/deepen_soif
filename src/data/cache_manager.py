"""
캐시 데이터 파일 관리 모듈.

전처리된 DataFrame을 Parquet 형식으로 저장/로드하며,
유효성 검사 및 날짜 목록 조회 기능을 제공한다.
추후 DB 연동으로 교체 가능한 인터페이스 구조로 설계되었다.

═══════════════════════════════════════════════════════════════════════════
스키마 버전 관리
═══════════════════════════════════════════════════════════════════════════
CACHE_SCHEMA_VERSION: Parquet 파일의 커스텀 메타데이터로 저장됨.
로드 시 현재 버전과 불일치하면 경고 로그 출력 + 누락 컬럼을 기본값으로 채움.
버전을 올려야 하는 경우: 전처리 결과에 새 컬럼이 추가/제거될 때.

버전 히스토리:
┌─────────┬──────────────────────────────────────────────────────────────────┐
│ 버전    │ 변경 내용                                                        │
├─────────┼──────────────────────────────────────────────────────────────────┤
│ "1"     │ 기본 파이프라인 (헬멧거치 + 노이즈 + 좌표 보정)                   │
│ "2"     │ SSMP 장소 분류, DBSCAN 보정, PERIOD_TYPE 경계값 변경             │
│ "3"     │ coverage_gap, signal_confidence 추가, nearest-cluster 노이즈 보정│
│ "4"     │ Space-Aware Journey Interpretation v4                            │
│         │ (space_function, hazard_weight, state_detail 등)                 │
│ "5.x"   │ Intelligent Journey Correction v5                                │
│         │ (앵커 보호, Space Priority Resolution)                           │
│ "6.0"   │ 4증거 통합 Journey 보정                                          │
│         │ (e1_active_level, segment_type 등 증거 컬럼 추가)                │
│ "6.1"   │ Multi-Pass Refinement ★ 현재                                    │
│         │ (Pass 1~4 반복 수렴, anomaly_flag 확장)                          │
└─────────┴──────────────────────────────────────────────────────────────────┘
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.constants import CACHE_FILE_PREFIX, CACHE_FILE_SUFFIX

# 분석 결과 캐시 파일 접두사 (배포 시 사전 생성된 지표 로드용)
ANALYTICS_PREFIX = "analytics_"
ANALYTICS_META_SUFFIX = "_meta.json"

# 현재 캐시 스키마 버전 (전처리 로직·컬럼 구성 변경 시 올릴 것)
# v5: Intelligent Journey v5 (시퀀스 기반 맥락 해석)
# v5.3: 앵커 공간 보호 강화 (DBSCAN 사전처리, 노이즈 보정 제외, 슬라이딩 윈도우 스킵)
# v5.4: 공간 우선순위 기반 번갈음 패턴 보정 (Space Priority Resolution)
# v6.0: 4증거 통합 Journey 보정 (활성신호 기반 Ghost Signal 탐지, 하루 구간 분절)
CACHE_SCHEMA_VERSION = "6.5"  # v6.5: EWI 음영지역 고려 (실제 시간 차이 기준)

# 버전별 필수 컬럼과 기본값 (로드 시 누락 컬럼 자동 보완)
_REQUIRED_COLUMNS_V2: dict[str, object] = {
    "ssmp_matched": False,
    "보정여부":       False,
    "보정장소":       "",
    "보정X":          None,
    "보정Y":          None,
    "위치키":         "OUTDOOR",
    "장소유형":       "UNKNOWN",
    "활성비율":       0.0,
    "시간대유형":     "off",
    # v3 신규
    "coverage_gap":       False,
    "signal_confidence":  "MED",
    "SPATIAL_CLUSTER":    -1,
    "CLUSTER_PLACE":      "",
    # v4 신규 (Space-Aware Journey Interpretation)
    "space_function":     "UNKNOWN",
    "hazard_weight":      0.3,
    "state_detail":       None,
    "anomaly_flag":       None,
    "dwell_exceeded":     False,
    "journey_pattern":    None,
    # v6 신규 (4증거 통합 Journey 보정)
    "e1_active_level":    "none",
    "e4_location_stable": True,
    "e4_run_length":      1,
    "e_ghost_candidate":  False,
    "e_transition":       False,
    "segment_type":       "work",
    # v6.1 Multi-Pass Refinement
    "anomaly_flag":       "",
}

logger = logging.getLogger(__name__)


def _json_safe(obj):  # noqa: C901
    """dict/list/scalar를 JSON 직렬화 가능한 형태로 변환 (datetime, numpy 등)."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if hasattr(obj, "isoformat"):  # datetime, date, time
        return obj.isoformat()
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return str(obj)


class BaseCacheManager(ABC):
    """캐시 관리자 추상 기반 클래스. 실시간 전환 시 DB 구현체로 교체 가능."""

    @abstractmethod
    def save(self, df: pd.DataFrame, date_str: str) -> bool:
        """캐시 저장."""

    @abstractmethod
    def load(self, date_str: str) -> Optional[pd.DataFrame]:
        """캐시 로드."""

    @abstractmethod
    def is_valid(self, date_str: str) -> bool:
        """캐시 유효성 확인."""

    @abstractmethod
    def get_available_dates(self) -> list[str]:
        """사용 가능한 날짜 목록."""

    @abstractmethod
    def delete(self, date_str: str) -> bool:
        """캐시 삭제."""


class ParquetCacheManager(BaseCacheManager):
    """
    Parquet 파일 기반 캐시 관리자.
    파일명: processed_YYYYMMDD.parquet (sector 없음) 또는 processed_{SECTOR}_YYYYMMDD.parquet
    Sector별 장소가 다르므로 Y1 / M15X 등 구분 저장.
    """

    def __init__(self, cache_dir: Path) -> None:
        """
        Args:
            cache_dir: 캐시 파일을 저장할 디렉토리 경로
        """
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, date_str: str, sector: Optional[str] = None) -> Path:
        """날짜(및 Sector)로 캐시 파일 경로 생성. sector 없으면 기존 형식(processed_YYYYMMDD)."""
        if sector:
            return self.cache_dir / f"{CACHE_FILE_PREFIX}{sector}_{date_str}{CACHE_FILE_SUFFIX}"
        return self.cache_dir / f"{CACHE_FILE_PREFIX}{date_str}{CACHE_FILE_SUFFIX}"

    def save(self, df: pd.DataFrame, date_str: str, sector: Optional[str] = None) -> bool:
        """
        DataFrame을 Parquet 파일로 저장.
        sector 있으면 processed_{sector}_{date}.parquet, 없으면 processed_{date}.parquet(기존 호환).

        Args:
            df: 저장할 DataFrame
            date_str: 날짜 문자열 (YYYYMMDD)
            sector: Sector 코드 (Y1, M15X 등, None이면 기존 형식)
        """
        if df is None or df.empty:
            logger.warning(f"저장할 데이터 없음: {date_str}")
            return False

        cache_path = self._get_path(date_str, sector)
        try:
            # 스키마 버전을 Parquet 커스텀 메타데이터로 저장
            table = pa.Table.from_pandas(df, preserve_index=False)
            existing_meta = table.schema.metadata or {}
            new_meta = {
                **existing_meta,
                b"cache_schema_version": CACHE_SCHEMA_VERSION.encode(),
                b"processed_date": date_str.encode(),
            }
            table = table.replace_schema_metadata(new_meta)
            pq.write_table(table, cache_path)
            logger.info(f"캐시 저장 완료: {cache_path.name} ({len(df)}행, schema_v{CACHE_SCHEMA_VERSION})")
            return True
        except Exception as e:
            logger.error(f"캐시 저장 실패: {cache_path} - {e}")
            return False

    def load(self, date_str: str, sector: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Parquet 캐시 파일 로드.
        sector 있으면 processed_{sector}_{date}.parquet, 없으면 processed_{date}.parquet 먼저 시도.

        Args:
            date_str: 날짜 문자열 (YYYYMMDD)
            sector: Sector 코드 (None이면 기존 형식 파일만 로드)
        """
        cache_path = self._get_path(date_str, sector)

        # 1순위: 튜닝된 캐시(_tuned.parquet)가 있으면 그것을 우선 사용
        tuned_path = cache_path.with_name(cache_path.stem + "_tuned" + cache_path.suffix)
        if tuned_path.exists():
            logger.info(f"튜닝된 캐시 사용: {tuned_path.name}")
            cache_path = tuned_path
        else:
            # 2순위: 일반 캐시 (sector 있는 형식 → 없으면 기본 형식 재시도)
            if not cache_path.exists() and sector:
                cache_path = self._get_path(date_str, None)
            if not cache_path.exists():
                logger.warning(f"캐시 파일 없음: {cache_path.name}")
                return None

        try:
            parquet_file = pq.read_table(cache_path)
            meta = parquet_file.schema.metadata or {}
            cached_version = meta.get(b"cache_schema_version", b"1").decode()

            df = parquet_file.to_pandas()

            # 시간 컬럼 타입 복원
            time_col = "시간(분)"
            if time_col in df.columns:
                df[time_col] = pd.to_datetime(df[time_col], errors="coerce")

            # 스키마 버전 검증
            if cached_version != CACHE_SCHEMA_VERSION:
                logger.warning(
                    f"캐시 스키마 버전 불일치: {cache_path.name} "
                    f"(저장: v{cached_version}, 현재: v{CACHE_SCHEMA_VERSION}). "
                    f"누락 컬럼을 기본값으로 보완합니다. "
                    f"Pipeline에서 재처리를 권장합니다."
                )
                # 누락 컬럼 기본값으로 보완 (앱 실행 유지)
                for col, default in _REQUIRED_COLUMNS_V2.items():
                    if col not in df.columns:
                        df[col] = default
                        logger.info(f"  누락 컬럼 보완: '{col}' = {default!r}")

            logger.info(
                f"캐시 로드 완료: {cache_path.name} "
                f"({len(df)}행, schema_v{cached_version})"
            )
            return df
        except Exception as e:
            logger.error(f"캐시 로드 실패: {cache_path} - {e}")
            return None

    def is_valid(self, date_str: str, sector: Optional[str] = None) -> bool:
        """캐시 파일 존재 및 비어있지 않은지 확인. sector 있으면 해당 파일만."""
        cache_path = self._get_path(date_str, sector)
        if not cache_path.exists():
            return False
        return cache_path.stat().st_size > 0

    def get_available_dates(self) -> list[str]:
        """사용 가능한 날짜 목록 (중복 제거, sector 무관). 기존 호환용."""
        seen: set[str] = set()
        for _s, d in self.get_available_entries():
            seen.add(d)
        return sorted(seen)

    def get_available_entries(self) -> list[tuple[str, str]]:
        """
        캐시 디렉토리에서 (sector, date_str) 목록 반환.
        파일명: processed_YYYYMMDD.parquet → ("", date), processed_Y1_20260225.parquet → ("Y1", "20260225")
        """
        entries: list[tuple[str, str]] = []
        for f in self.cache_dir.glob(f"{CACHE_FILE_PREFIX}*{CACHE_FILE_SUFFIX}"):
            stem = f.stem.replace(CACHE_FILE_PREFIX, "")
            if len(stem) == 8 and stem.isdigit():
                entries.append(("", stem))  # legacy: sector 없음
            else:
                parts = stem.split("_")
                if len(parts) >= 2 and len(parts[-1]) == 8 and parts[-1].isdigit():
                    sector = "_".join(parts[:-1])
                    entries.append((sector, parts[-1]))
        return sorted(set(entries), key=lambda x: (x[0], x[1]))

    def delete(self, date_str: str, sector: Optional[str] = None) -> bool:
        """캐시 파일 삭제. sector 있으면 해당 파일만."""
        cache_path = self._get_path(date_str, sector)
        try:
            if cache_path.exists():
                cache_path.unlink()
                logger.info(f"캐시 삭제: {cache_path.name}")
            return True
        except Exception as e:
            logger.error(f"캐시 삭제 실패: {cache_path} - {e}")
            return False

    def get_cache_info(self) -> list[dict]:
        """캐시 파일 정보 목록. get_available_entries() 기준."""
        info_list = []
        for sector, date_str in self.get_available_entries():
            cache_path = self._get_path(date_str, sector or None)
            if not cache_path.exists():
                continue
            size_kb = cache_path.stat().st_size / 1024
            try:
                df = pd.read_parquet(cache_path, columns=["시간(분)"])
                row_count = len(df)
            except Exception:
                row_count = -1
            info_list.append({
                "sector": sector or "Y1",
                "date": date_str,
                "path": str(cache_path),
                "size_kb": round(size_kb, 1),
                "row_count": row_count,
            })
        return info_list

    # ─── 분석 결과 캐시 (배포 시 사전 생성, 대시보드는 읽기 전용) ─────────────────

    def _analytics_meta_path(self, date_str: str, sector: Optional[str] = None) -> Path:
        if sector:
            return self.cache_dir / f"{ANALYTICS_PREFIX}{sector}_{date_str}{ANALYTICS_META_SUFFIX}"
        return self.cache_dir / f"{ANALYTICS_PREFIX}{date_str}{ANALYTICS_META_SUFFIX}"

    def _analytics_parquet_path(self, date_str: str, key: str, sector: Optional[str] = None) -> Path:
        if sector:
            return self.cache_dir / f"{ANALYTICS_PREFIX}{sector}_{date_str}_{key}.parquet"
        return self.cache_dir / f"{ANALYTICS_PREFIX}{date_str}_{key}.parquet"

    def save_analytics(self, analytics_dict: dict, date_str: str, sector: Optional[str] = None) -> bool:
        """
        지표 결과를 Parquet/JSON으로 저장.
        DataFrame은 개별 parquet, 그 외는 meta.json에 저장.

        Args:
            analytics_dict: calc_soif_summary + worker_summary, company_summary 구조
            date_str: YYYYMMDD

        Returns:
            저장 성공 여부
        """
        if not analytics_dict:
            logger.warning("저장할 분석 데이터 없음")
            return False

        meta = {}
        try:
            for key, value in analytics_dict.items():
                if isinstance(value, pd.DataFrame):
                    path = self._analytics_parquet_path(date_str, key, sector)
                    value.to_parquet(path, index=False)
                    logger.debug(f"분석 캐시 저장: {path.name}")
                else:
                    meta[key] = _json_safe(value)
            if meta:
                meta_path = self._analytics_meta_path(date_str, sector)
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=0)
                logger.info(f"분석 캐시 저장 완료: {ANALYTICS_PREFIX}{sector or ''}{date_str} (meta + {len(analytics_dict) - len(meta)}개 DataFrame)")
            return True
        except Exception as e:
            logger.error(f"분석 캐시 저장 실패: {date_str} - {e}")
            return False

    def load_analytics(self, date_str: str, sector: Optional[str] = None) -> Optional[dict]:
        """
        저장된 지표 로드. sector 있으면 analytics_{sector}_{date}_*, 없으면 기존 형식.
        """
        meta_path = self._analytics_meta_path(date_str, sector)
        if not meta_path.exists() and sector:
            meta_path = self._analytics_meta_path(date_str, None)
        if not meta_path.exists():
            return None
        result = {}
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            result.update(meta)
            # 메타와 쌍인 parquet 접두사 (예: analytics_20260225_ 또는 analytics_Y1_20260225_)
            meta_stem = meta_path.stem.replace("_meta", "")
            parquet_prefix = meta_stem + "_"
            for path in self.cache_dir.glob(f"{parquet_prefix}*.parquet"):
                key = path.stem.replace(parquet_prefix, "")
                if key:
                    result[key] = pd.read_parquet(path)
            if result:
                logger.info(f"분석 캐시 로드: {ANALYTICS_PREFIX}{sector or ''}{date_str}")
            return result if result else None
        except Exception as e:
            logger.error(f"분석 캐시 로드 실패: {date_str} - {e}")
            return None

    def get_available_analytics_entries(self) -> list[tuple[str, str]]:
        """분석 캐시 (sector, date) 목록. analytics_*_meta.json 파싱."""
        entries: list[tuple[str, str]] = []
        for f in self.cache_dir.glob(f"{ANALYTICS_PREFIX}*{ANALYTICS_META_SUFFIX}"):
            stem = f.stem.replace(ANALYTICS_PREFIX, "").replace("_meta", "")
            if len(stem) == 8 and stem.isdigit():
                entries.append(("", stem))
            elif "_" in stem:
                parts = stem.split("_")
                if len(parts) >= 2 and len(parts[-1]) == 8 and parts[-1].isdigit():
                    entries.append(("_".join(parts[:-1]), parts[-1]))
        return sorted(set(entries), key=lambda x: (x[0], x[1]))

    def get_available_analytics_dates(self) -> list[str]:
        """분석 캐시가 있는 날짜 목록 (sector 무관, 기존 호환)."""
        return sorted(set(d for _, d in self.get_available_analytics_entries()))


def load_analytics_or_compute(
    cache_mgr: ParquetCacheManager,
    date_str: str,
    df: pd.DataFrame,
    sector: Optional[str] = None,
) -> dict:
    """
    분석 결과 로드. 없으면 전처리 DataFrame으로 계산하여 반환.
    sector 있으면 해당 Sector 캐시 우선.
    """
    analytics = cache_mgr.load_analytics(date_str, sector)
    if analytics is not None:
        return analytics
    from src.metrics.aggregator import aggregate_by_worker, aggregate_by_company
    from src.metrics.soif import calc_soif_summary
    analytics = {
        "worker_summary": aggregate_by_worker(df, include_safety=True),
        "company_summary": aggregate_by_company(df),
    }
    analytics.update(calc_soif_summary(df))
    return analytics


def load_multi_date_cache(
    dates: list[str],
    cache_dir: Path,
    sector: Optional[str] = None,
) -> pd.DataFrame:
    """
    여러 날짜의 캐시를 합쳐서 반환. Sector별 캐시 사용.

    Args:
        dates: 로드할 날짜 목록 (YYYYMMDD)
        cache_dir: 캐시 디렉토리 경로
        sector: Sector (Y1, M15X 등). None이면 legacy 형식.
    """
    mgr = ParquetCacheManager(cache_dir)
    parts: list[pd.DataFrame] = []
    for d in dates:
        df = mgr.load(d, sector)
        if df is None or df.empty:
            logger.warning(f"멀티 날짜 로드 스킵 (캐시 없음): {d}")
            continue
        if "날짜" not in df.columns:
            df["날짜"] = d
        parts.append(df)

    if not parts:
        logger.warning("멀티 날짜 로드: 유효한 데이터 없음")
        return pd.DataFrame()

    result = pd.concat(parts, ignore_index=True)
    logger.info(f"멀티 날짜 로드 완료: {len(dates)}개 날짜 → {len(result):,}행")
    return result


def get_date_cache_status(
    data_dir: Path,
    cache_dir: Path,
) -> pd.DataFrame:
    """
    Raw 폴더와 캐시 파일을 비교하여 처리 상태 반환.

    Args:
        data_dir: Raw 데이터 루트 폴더 (Datafile/)
        cache_dir: 캐시 디렉토리 경로

    Returns:
        DataFrame:
          date, raw_exists, cache_exists, raw_rows, cache_rows, status, last_updated
    """
    import datetime
    from src.utils.time_utils import extract_date_from_folder

    mgr = ParquetCacheManager(cache_dir)

    raw_dates: dict[str, Path] = {}
    if data_dir.exists():
        for folder in data_dir.iterdir():
            if folder.is_dir():
                d = extract_date_from_folder(folder.name)
                if d:
                    raw_dates[d] = folder

    cache_dates = set(mgr.get_available_dates())
    all_dates   = sorted(set(raw_dates.keys()) | cache_dates, reverse=True)

    records: list[dict] = []
    for d in all_dates:
        raw_exists   = d in raw_dates
        cache_exists = d in cache_dates
        cache_path   = mgr._get_path(d)

        raw_rows: int = 0
        if raw_exists:
            try:
                csvs = list(raw_dates[d].glob("*.csv"))
                import pandas as _pd
                raw_rows = sum(
                    len(_pd.read_csv(f, encoding="utf-8-sig", nrows=None))
                    for f in csvs
                )
            except Exception:
                raw_rows = -1

        cache_rows: int = 0
        last_updated = None
        if cache_exists:
            try:
                tmp = pd.read_parquet(cache_path, columns=["시간(분)"])
                cache_rows = len(tmp)
                last_updated = datetime.datetime.fromtimestamp(cache_path.stat().st_mtime)
            except Exception:
                cache_rows = -1

        if raw_exists and cache_exists:
            status = "synced"
        elif raw_exists and not cache_exists:
            status = "needs_processing"
        else:
            status = "cache_only"

        records.append({
            "date":         d,
            "raw_exists":   raw_exists,
            "cache_exists": cache_exists,
            "raw_rows":     raw_rows,
            "cache_rows":   cache_rows,
            "status":       status,
            "last_updated": last_updated,
        })

    return pd.DataFrame(records)


def build_pipeline(
    data_folder: Path,
    cache_dir: Path,
    date_str: str,
    force_rebuild: bool = False,
) -> Optional[pd.DataFrame]:
    """
    전체 데이터 파이프라인 실행.
    Raw CSV → 전처리 → 캐시 저장( Sector별 파일명) → 반환.
    폴더명에서 Sector 추출 (Y1, M15X 등) — Sector별 장소·항목이 다름.

    Sector별 SSMP 디렉터리가 있을 때만 SSMP를 사용하고,
    없으면 SSMP 없이 키워드 기반 분류만 사용한다.
    (예: Datafile/ssmp_structure_Y1, Datafile/ssmp_structure_M15X)
    """
    from src.data.loader import load_date_folder
    from src.data.preprocessor import preprocess
    from src.utils.time_utils import extract_sector_from_folder
    from src.data.spatial_loader import SpatialContext

    cache_mgr = ParquetCacheManager(cache_dir)
    sector = extract_sector_from_folder(data_folder.name) if data_folder else None

    if not force_rebuild and cache_mgr.is_valid(date_str, sector):
        logger.info(f"기존 캐시 사용: {sector or 'legacy'} {date_str}")
        return cache_mgr.load(date_str, sector)

    logger.info(f"Raw CSV 처리 시작: {sector or ''} {date_str}")
    raw_df = load_date_folder(data_folder)
    if raw_df is None or raw_df.empty:
        logger.error(f"Raw 데이터 로드 실패: {date_str}")
        return None

    # ── Sector별 SSMP 디렉터리 결정 ─────────────────────────────────────
    spatial_ctx = None
    try:
        data_root = data_folder.parent if data_folder else cache_dir.parent
        ssmp_dir = None

        # 1순위: Sector 전용 SSMP (예: ssmp_structure_M15X)
        if sector:
            cand = data_root / f"ssmp_structure_{sector}"
            if cand.exists():
                ssmp_dir = cand
            else:
                logger.warning(
                    f"Sector={sector} 전용 SSMP 폴더 없음: {cand} → "
                    "SSMP 없이 키워드 매칭만 사용"
                )

        # 2순위: 기본 SSMP (Sector 정보 없거나 Y1/legacy일 때만 사용)
        if ssmp_dir is None and (not sector or sector == "Y1"):
            cand_default = data_root / "ssmp_structure"
            if cand_default.exists():
                ssmp_dir = cand_default

        if ssmp_dir is not None:
            spatial_ctx = SpatialContext(ssmp_dir)
            logger.info(
                f"SSMP 로드 완료 (sector={sector or 'Y1'}): {ssmp_dir}"
            )
        else:
            logger.info(
                f"SSMP 폴더 없음 또는 Sector 불일치 (sector={sector or 'UNKNOWN'}) → "
                "SSMP 없이 키워드 기반 분류만 사용"
            )
    except Exception as e:
        logger.warning(f"SSMP 로드 중 오류 (sector={sector or 'UNKNOWN'}): {e}")
        spatial_ctx = None

    # 전처리 (Sector별 SSMP가 있으면 SpatialContext 사용)
    processed_df = preprocess(raw_df, spatial_ctx=spatial_ctx)
    if processed_df is None or processed_df.empty:
        logger.error(f"전처리 실패: {date_str}")
        return None

    cache_mgr.save(processed_df, date_str, sector)

    try:
        from src.metrics.aggregator import aggregate_by_worker, aggregate_by_company
        from src.metrics.soif import calc_soif_summary
        analytics = {
            "worker_summary": aggregate_by_worker(processed_df, include_safety=True),
            "company_summary": aggregate_by_company(processed_df),
        }
        analytics.update(calc_soif_summary(processed_df))
        cache_mgr.save_analytics(analytics, date_str, sector)
    except Exception as e:
        logger.warning(f"분석 캐시 저장 스킵: {e}")

    return processed_df
