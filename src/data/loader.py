"""
Raw CSV 데이터 로딩 모듈.
UTF-8 BOM 인코딩 처리, 시간 컬럼 파싱, 스키마 검증, 날짜 폴더 자동 스캔을 담당한다.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns, RAW_CSV_DTYPES, validate_raw_df
from src.utils.constants import CSV_ENCODING, DATETIME_FORMAT, DATA_FOLDER_PREFIX
from src.utils.time_utils import extract_date_from_folder, extract_sector_from_folder

logger = logging.getLogger(__name__)


def load_raw_csv(filepath: Path) -> Optional[pd.DataFrame]:
    """
    단일 Raw CSV 파일을 로드하고 기본 전처리를 수행.

    - UTF-8 BOM 인코딩 처리
    - 시간 컬럼을 datetime으로 파싱
    - 스키마 유효성 검증

    Args:
        filepath: CSV 파일 경로

    Returns:
        로드된 DataFrame, 실패 시 None
    """
    try:
        df = pd.read_csv(
            filepath,
            encoding=CSV_ENCODING,
            dtype={
                col: str
                for col in [
                    RawColumns.WORKER, RawColumns.ZONE, RawColumns.BUILDING,
                    RawColumns.FLOOR, RawColumns.PLACE, RawColumns.TAG,
                    RawColumns.COMPANY, RawColumns.EQUIPMENT,
                ]
            },
        )
        logger.info(f"CSV 로드 성공: {filepath.name} ({len(df)}행)")
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(filepath, encoding="utf-8")
            logger.warning(f"utf-8-sig 실패, utf-8으로 재시도: {filepath.name}")
        except Exception as e:
            logger.error(f"CSV 로드 실패: {filepath} - {e}")
            return None
    except Exception as e:
        logger.error(f"CSV 로드 실패: {filepath} - {e}")
        return None

    # 시간 컬럼 파싱
    if RawColumns.TIME in df.columns:
        df[RawColumns.TIME] = pd.to_datetime(
            df[RawColumns.TIME].str.strip(), format=DATETIME_FORMAT, errors="coerce"
        )
        nat_count = df[RawColumns.TIME].isna().sum()
        if nat_count > 0:
            logger.warning(f"시간 파싱 실패 행: {nat_count}건 (NaT)")

    # 수치 컬럼 변환
    for col in [RawColumns.SIGNAL_COUNT, RawColumns.ACTIVE_SIGNAL_COUNT]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in [RawColumns.X, RawColumns.Y]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 스키마 검증
    is_valid, errors = validate_raw_df(df)
    if not is_valid:
        for err in errors:
            logger.warning(f"스키마 경고 [{filepath.name}]: {err}")

    # 파일명에서 태그/작업자 정보 보완 (NA 태그 대응)
    df = _fill_tag_from_filename(df, filepath)

    # 정렬 (시간 오름차순)
    if RawColumns.TIME in df.columns:
        df = df.sort_values(RawColumns.TIME).reset_index(drop=True)

    return df


def _fill_tag_from_filename(df: pd.DataFrame, filepath: Path) -> pd.DataFrame:
    """
    파일명에서 태그 ID를 추출하여 누락된 태그 컬럼을 보완.
    파일명 패턴: 이름_형_(소속)_업체명_구역_날짜.csv

    Args:
        df: DataFrame
        filepath: CSV 파일 경로

    Returns:
        태그 보완된 DataFrame
    """
    # (NA) 가 포함된 파일의 경우 태그 정보가 없을 수 있음
    if RawColumns.TAG not in df.columns:
        df[RawColumns.TAG] = "UNKNOWN"
    return df


def load_date_folder(folder_path: Path) -> Optional[pd.DataFrame]:
    """
    날짜 폴더 내의 모든 CSV 파일을 로드하여 하나의 DataFrame으로 병합.

    Args:
        folder_path: 날짜 폴더 경로 (예: Y1_Worker_TWard_20260225/)

    Returns:
        병합된 DataFrame, 실패 시 None
    """
    if not folder_path.exists() or not folder_path.is_dir():
        logger.error(f"폴더가 존재하지 않음: {folder_path}")
        return None

    csv_files = sorted(folder_path.glob("*.csv"))
    if not csv_files:
        logger.warning(f"CSV 파일 없음: {folder_path}")
        return None

    dfs = []
    for csv_file in csv_files:
        df = load_raw_csv(csv_file)
        if df is not None and not df.empty:
            # 파일명에서 작업자 키 정보 추가
            df["_source_file"] = csv_file.name
            dfs.append(df)

    if not dfs:
        logger.error(f"로드 가능한 CSV 없음: {folder_path}")
        return None

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(
        f"날짜 폴더 로드 완료: {folder_path.name} | "
        f"{len(csv_files)}개 파일 | {len(combined)}행"
    )
    return combined


def scan_data_folders(base_path: Path) -> list[Path]:
    """
    base_path 하위의 날짜별 데이터 폴더를 스캔.
    Sector 구분: Y1_Worker_TWard_YYYYMMDD, M15X_Worker_TWard_YYYYMMDD 등
    (Sector별 장소·SSMP가 다름 — 반환은 기존 호환용 Path 목록, sector 정보는 scan_data_folders_with_sector 사용)
    """
    entries = scan_data_folders_with_sector(base_path)
    return [p for _, __, p in entries]


def scan_data_folders_with_sector(base_path: Path) -> list[tuple[str, str, Path]]:
    """
    base_path 하위의 데이터 폴더를 Sector·날짜와 함께 스캔.
    폴더명 패턴: {SECTOR}_Worker_TWard_YYYYMMDD (예: Y1, M15X)

    Returns:
        (sector, date_str, folder_path) 목록, sector·날짜 오름차순
    """
    if not base_path.exists():
        logger.error(f"데이터 루트 폴더 없음: {base_path}")
        return []

    import re

    entries: list[tuple[str, str, Path]] = []
    for item in base_path.iterdir():
        if not item.is_dir():
            continue
        normalized = re.sub(r"[\s_]+", "_", item.name)
        # Worker + TWard + 8자리 날짜 포함 폴더만 (Y1, M15X 등 Sector 지원)
        if "Worker" not in normalized or "TWard" not in normalized:
            if not normalized.startswith(DATA_FOLDER_PREFIX):
                continue
        date_str = extract_date_from_folder(item.name)
        sector = extract_sector_from_folder(item.name)
        if date_str and sector:
            entries.append((sector, date_str, item))

    entries.sort(key=lambda x: (x[0], x[1]))
    logger.info(f"데이터 폴더 스캔: {len(entries)}개 (Sector·날짜) 발견 ({base_path})")
    return entries


def get_available_dates(base_path: Path) -> list[str]:
    """
    사용 가능한 날짜 목록 반환 (YYYYMMDD 문자열 리스트).

    Args:
        base_path: Datafile 루트 폴더 경로

    Returns:
        날짜 문자열 목록 (오름차순)
    """
    folders = scan_data_folders(base_path)
    dates = []
    for folder in folders:
        date_str = extract_date_from_folder(folder.name)
        if date_str:
            dates.append(date_str)
    return dates


def get_folder_for_date(base_path: Path, date_str: str, sector: Optional[str] = None) -> Optional[Path]:
    """
    날짜(및 선택적 Sector)에 해당하는 데이터 폴더 경로 반환.

    Args:
        base_path: Datafile 루트 경로
        date_str: 날짜 문자열 (YYYYMMDD)
        sector: Sector 코드 (None이면 첫 번째 매칭 폴더)

    Returns:
        폴더 경로, 없으면 None
    """
    entries = scan_data_folders_with_sector(base_path)
    for s, d, folder in entries:
        if d != date_str:
            continue
        if sector is None or s == sector:
            return folder
    return None
