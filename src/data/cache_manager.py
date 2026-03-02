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
    파일명: processed_YYYYMMDD.parquet
    """

    def __init__(self, cache_dir: Path) -> None:
        """
        Args:
            cache_dir: 캐시 파일을 저장할 디렉토리 경로
        """
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, date_str: str) -> Path:
        """날짜 문자열로 캐시 파일 경로 생성."""
        return self.cache_dir / f"{CACHE_FILE_PREFIX}{date_str}{CACHE_FILE_SUFFIX}"

    def save(self, df: pd.DataFrame, date_str: str) -> bool:
        """
        DataFrame을 Parquet 파일로 저장.
        스키마 버전(CACHE_SCHEMA_VERSION)을 Parquet 파일 메타데이터에 기록.

        Args:
            df: 저장할 DataFrame
            date_str: 날짜 문자열 (YYYYMMDD)

        Returns:
            저장 성공 여부
        """
        if df is None or df.empty:
            logger.warning(f"저장할 데이터 없음: {date_str}")
            return False

        cache_path = self._get_path(date_str)
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

    def load(self, date_str: str) -> Optional[pd.DataFrame]:
        """
        Parquet 캐시 파일 로드.
        스키마 버전을 검증하고, 구 버전 캐시는 누락 컬럼을 기본값으로 보완한다.

        버전 불일치 시 처리:
          - 경고 로그 출력 (앱 실행은 계속)
          - _REQUIRED_COLUMNS_V2에 정의된 누락 컬럼을 기본값으로 추가
          - 재처리를 권장하는 메시지 출력

        Args:
            date_str: 날짜 문자열 (YYYYMMDD)

        Returns:
            로드된 DataFrame, 실패 시 None
        """
        cache_path = self._get_path(date_str)
        if not cache_path.exists():
            logger.warning(f"캐시 파일 없음: {cache_path.name}")
            return None

        try:
            # 메타데이터에서 스키마 버전 확인
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

    def is_valid(self, date_str: str) -> bool:
        """
        캐시 파일 존재 및 비어있지 않은지 확인.

        Args:
            date_str: 날짜 문자열 (YYYYMMDD)

        Returns:
            유효하면 True
        """
        cache_path = self._get_path(date_str)
        if not cache_path.exists():
            return False
        return cache_path.stat().st_size > 0

    def get_available_dates(self) -> list[str]:
        """
        캐시 디렉토리에서 사용 가능한 날짜 목록 반환.

        Returns:
            날짜 문자열 목록 (오름차순, YYYYMMDD)
        """
        dates = []
        for f in self.cache_dir.glob(f"{CACHE_FILE_PREFIX}*{CACHE_FILE_SUFFIX}"):
            date_str = f.stem.replace(CACHE_FILE_PREFIX, "")
            if len(date_str) == 8 and date_str.isdigit():
                dates.append(date_str)
        return sorted(dates)

    def delete(self, date_str: str) -> bool:
        """
        캐시 파일 삭제.

        Args:
            date_str: 날짜 문자열 (YYYYMMDD)

        Returns:
            삭제 성공 여부
        """
        cache_path = self._get_path(date_str)
        try:
            if cache_path.exists():
                cache_path.unlink()
                logger.info(f"캐시 삭제: {cache_path.name}")
            return True
        except Exception as e:
            logger.error(f"캐시 삭제 실패: {cache_path} - {e}")
            return False

    def get_cache_info(self) -> list[dict]:
        """
        모든 캐시 파일의 정보 목록 반환.

        Returns:
            {date, path, size_kb, row_count} 딕셔너리 목록
        """
        info_list = []
        for date_str in self.get_available_dates():
            cache_path = self._get_path(date_str)
            size_kb = cache_path.stat().st_size / 1024
            try:
                df = pd.read_parquet(cache_path, columns=["시간(분)"])
                row_count = len(df)
            except Exception:
                row_count = -1
            info_list.append({
                "date": date_str,
                "path": str(cache_path),
                "size_kb": round(size_kb, 1),
                "row_count": row_count,
            })
        return info_list

    # ─── 분석 결과 캐시 (배포 시 사전 생성, 대시보드는 읽기 전용) ─────────────────

    def _analytics_meta_path(self, date_str: str) -> Path:
        return self.cache_dir / f"{ANALYTICS_PREFIX}{date_str}{ANALYTICS_META_SUFFIX}"

    def _analytics_parquet_path(self, date_str: str, key: str) -> Path:
        return self.cache_dir / f"{ANALYTICS_PREFIX}{date_str}_{key}.parquet"

    def save_analytics(self, analytics_dict: dict, date_str: str) -> bool:
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
                    path = self._analytics_parquet_path(date_str, key)
                    value.to_parquet(path, index=False)
                    logger.debug(f"분석 캐시 저장: {path.name}")
                else:
                    meta[key] = _json_safe(value)
            if meta:
                meta_path = self._analytics_meta_path(date_str)
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=0)
                logger.info(f"분석 캐시 저장 완료: {ANALYTICS_PREFIX}{date_str} (meta + {len(analytics_dict) - len(meta)}개 DataFrame)")
            return True
        except Exception as e:
            logger.error(f"분석 캐시 저장 실패: {date_str} - {e}")
            return False

    def load_analytics(self, date_str: str) -> Optional[dict]:
        """
        저장된 지표 로드. 없으면 None.

        Returns:
            calc_soif_summary와 동일 구조 + worker_summary, company_summary
        """
        meta_path = self._analytics_meta_path(date_str)
        if not meta_path.exists():
            return None
        result = {}
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            result.update(meta)
            for path in self.cache_dir.glob(f"{ANALYTICS_PREFIX}{date_str}_*.parquet"):
                key = path.stem.replace(f"{ANALYTICS_PREFIX}{date_str}_", "")
                result[key] = pd.read_parquet(path)
            if result:
                logger.info(f"분석 캐시 로드: {ANALYTICS_PREFIX}{date_str} ({len([k for k in result if isinstance(result[k], pd.DataFrame)])}개 DataFrame)")
            return result if result else None
        except Exception as e:
            logger.error(f"분석 캐시 로드 실패: {date_str} - {e}")
            return None

    def get_available_analytics_dates(self) -> list[str]:
        """분석 캐시가 있는 날짜 목록."""
        dates = []
        for f in self.cache_dir.glob(f"{ANALYTICS_PREFIX}*{ANALYTICS_META_SUFFIX}"):
            # stem 예: analytics_20260225_meta → 20260225
            date_str = f.stem.replace(ANALYTICS_PREFIX, "").replace("_meta", "")
            if len(date_str) == 8 and date_str.isdigit():
                dates.append(date_str)
        return sorted(dates)


def load_analytics_or_compute(
    cache_mgr: ParquetCacheManager,
    date_str: str,
    df: pd.DataFrame,
) -> dict:
    """
    분석 결과 로드. 없으면 전처리 DataFrame으로 계산하여 반환.
    배포 환경에서는 캐시만 사용하므로 로컬에서 Pipeline 실행 후 캐시를 포함해 배포할 것.
    """
    analytics = cache_mgr.load_analytics(date_str)
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
) -> pd.DataFrame:
    """
    여러 날짜의 캐시를 합쳐서 반환.
    날짜 컬럼(ProcessedColumns.DATE)이 반드시 포함된다.
    누락 날짜는 경고 로그 후 스킵.

    Args:
        dates: 로드할 날짜 목록 (YYYYMMDD)
        cache_dir: 캐시 디렉토리 경로

    Returns:
        병합된 DataFrame (빈 날짜 스킵)
    """
    mgr = ParquetCacheManager(cache_dir)
    parts: list[pd.DataFrame] = []
    for d in dates:
        df = mgr.load(d)
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
    Raw CSV → 전처리 → 캐시 저장 → 반환

    Args:
        data_folder: 날짜별 raw data 폴더 경로
        cache_dir: 캐시 저장 디렉토리
        date_str: 처리할 날짜 (YYYYMMDD)
        force_rebuild: True이면 기존 캐시 무시하고 재처리

    Returns:
        처리된 DataFrame, 실패 시 None
    """
    from src.data.loader import load_date_folder
    from src.data.preprocessor import preprocess

    cache_mgr = ParquetCacheManager(cache_dir)

    # 캐시 유효성 확인
    if not force_rebuild and cache_mgr.is_valid(date_str):
        logger.info(f"기존 캐시 사용: {date_str}")
        return cache_mgr.load(date_str)

    # Raw CSV 로드
    logger.info(f"Raw CSV 처리 시작: {date_str}")
    raw_df = load_date_folder(data_folder)
    if raw_df is None or raw_df.empty:
        logger.error(f"Raw 데이터 로드 실패: {date_str}")
        return None

    # 전처리
    processed_df = preprocess(raw_df)
    if processed_df is None or processed_df.empty:
        logger.error(f"전처리 실패: {date_str}")
        return None

    # 캐시 저장
    cache_mgr.save(processed_df, date_str)

    # 분석 결과 캐시 생성 (대시보드 배포 시 읽기 전용으로 사용)
    try:
        from src.metrics.aggregator import aggregate_by_worker, aggregate_by_company
        from src.metrics.soif import calc_soif_summary
        analytics = {
            "worker_summary": aggregate_by_worker(processed_df, include_safety=True),
            "company_summary": aggregate_by_company(processed_df),
        }
        soif = calc_soif_summary(processed_df)
        analytics.update(soif)
        cache_mgr.save_analytics(analytics, date_str)
    except Exception as e:
        logger.warning(f"분석 캐시 저장 스킵 (대시보드에서 계산 사용): {e}")

    return processed_df
