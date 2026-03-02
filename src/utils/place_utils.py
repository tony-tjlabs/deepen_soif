"""
장소명 유사도 분석 및 정렬 유틸리티.

Journey Gantt 차트의 Y축을 유사한 장소끼리 인접하게 정렬하여
작업자 이동 패턴을 더 쉽게 이해할 수 있도록 지원.

v6.3 정렬 전략:
  1. 이름 유사도 우선: 같은 건물/구역(FAB, 본진 등)끼리 그룹화
  2. 그룹 내 정렬: 메인 장소(FAB) → 세부 장소(FAB_휴게실) 순서
  3. 그룹 간 정렬: 실제 이동 패턴 기반으로 자주 오가는 그룹끼리 인접 배치
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Optional

import pandas as pd


# 건물/구역 접두사 기본 우선순위 (이동 데이터 없을 때 사용)
_BUILDING_PRIORITY: dict[str, int] = {
    "FAB": 10,
    "본진": 20,
    "WWT": 30,
    "CMP": 35,
    "공사현장": 40,
    "outdoor": 45,
}

# 기능 키워드 → 정렬키 (그룹 내 세부 정렬)
# Z_기타가 가장 앞에 와서 메인 장소(FAB)가 FAB_휴게실보다 먼저
_FUNC_KEYWORDS: dict[str, str] = {
    "휴게": "M_휴게",      # M = Middle
    "식당": "M_식당",
    "흡연": "M_흡연",
    "걸이대": "N_걸이대",  # N = Near end
    "거치대": "N_거치대",
    "보호구": "N_보호구",
    "RACK": "N_rack",
    "타각기": "O_타각기",
    "출구": "P_출구",
    "입구": "P_입구",
    "게이트": "P_게이트",
    "gate": "P_gate",
    "공사": "Q_공사",
    "작업": "Q_작업",
    "현장": "R_현장",
    "사무": "S_사무",
    "office": "S_office",
}


def get_place_group(place_name: str) -> str:
    """
    장소명에서 그룹(건물/구역) 이름 추출.
    
    예: "FAB_휴게실_1층" → "FAB"
        "본진 타각기" → "본진"
    """
    place = str(place_name).strip()
    if not place or place == "nan":
        return "ZZZ_unknown"
    
    parts = re.split(r'[_\s]+', place)
    return parts[0] if parts else place


def get_place_sort_key_in_group(place_name: str) -> tuple[str, int, str]:
    """
    그룹 내 정렬 키 반환.
    
    메인 장소(FAB)가 세부 장소(FAB_휴게실)보다 앞에 오도록.
    
    Returns:
        (기능키, 층수, 원본문자열)
    """
    place = str(place_name).strip()
    if not place or place == "nan":
        return ("ZZZ", 0, place)
    
    # 기능 키워드 추출
    place_lower = place.lower()
    func_key = "A_main"  # 기능 키워드 없으면 메인 장소 → 가장 앞
    
    for keyword, sort_key in _FUNC_KEYWORDS.items():
        if keyword.lower() in place_lower or keyword in place:
            func_key = sort_key
            break
    
    # 층수 추출
    floor_num = 0
    floor_patterns = [
        r'(\d+)층',
        r'(\d+)\s*F\b',
        r'\bF(\d+)',
        r'\bL(\d+)',
        r'\bB(\d+)',
    ]
    for pattern in floor_patterns:
        match = re.search(pattern, place, re.IGNORECASE)
        if match:
            floor_num = int(match.group(1))
            if pattern.startswith(r'\bB'):
                floor_num = -floor_num
            break
    
    return (func_key, floor_num, place)


def sort_places_within_group(places: list[str]) -> list[str]:
    """
    같은 그룹 내 장소들을 정렬.
    메인 장소 → 휴게 → 걸이대 → 타각기 → 출입구 순서.
    """
    if not places:
        return []
    return sorted(places, key=get_place_sort_key_in_group)


def build_transition_matrix(df: pd.DataFrame, place_col: str = "장소") -> dict[tuple[str, str], int]:
    """
    전체 데이터에서 장소 간 이동 빈도 행렬 구축.
    
    Returns:
        {(장소A, 장소B): 이동횟수} (양방향 합산, 정렬된 키)
    """
    transitions: dict[tuple[str, str], int] = defaultdict(int)
    
    if place_col not in df.columns:
        return dict(transitions)
    
    # 작업자 컬럼 찾기
    worker_col = None
    for col in ["작업자키", "worker_key", "작업자"]:
        if col in df.columns:
            worker_col = col
            break
    
    # 시간 컬럼 찾기
    time_col = None
    for col in ["시간(분)", "time", "시간"]:
        if col in df.columns:
            time_col = col
            break
    
    if worker_col is None:
        # 작업자 구분 없이 전체 데이터로 처리
        if time_col:
            df_sorted = df.sort_values(time_col)
        else:
            df_sorted = df
        places = df_sorted[place_col].fillna("").astype(str).tolist()
        for i in range(len(places) - 1):
            p1, p2 = places[i], places[i + 1]
            if p1 and p2 and p1 != p2:
                key = tuple(sorted([p1, p2]))
                transitions[key] += 1
    else:
        for _, wdf in df.groupby(worker_col):
            if time_col:
                wdf_sorted = wdf.sort_values(time_col)
            else:
                wdf_sorted = wdf
            places = wdf_sorted[place_col].fillna("").astype(str).tolist()
            for i in range(len(places) - 1):
                p1, p2 = places[i], places[i + 1]
                if p1 and p2 and p1 != p2:
                    key = tuple(sorted([p1, p2]))
                    transitions[key] += 1
    
    return dict(transitions)


def build_group_transition_matrix(
    transitions: dict[tuple[str, str], int]
) -> dict[tuple[str, str], int]:
    """
    장소 이동 행렬을 그룹 이동 행렬로 변환.
    
    FAB → FAB_휴게실 이동은 같은 그룹이므로 무시.
    FAB → 본진 이동은 그룹 간 이동으로 카운트.
    """
    group_transitions: dict[tuple[str, str], int] = defaultdict(int)
    
    for (p1, p2), count in transitions.items():
        g1 = get_place_group(p1)
        g2 = get_place_group(p2)
        
        if g1 != g2:  # 그룹 간 이동만 카운트
            key = tuple(sorted([g1, g2]))
            group_transitions[key] += count
    
    return dict(group_transitions)


def sort_groups_by_transitions(
    groups: list[str],
    group_transitions: dict[tuple[str, str], int],
) -> list[str]:
    """
    그룹들을 이동 빈도 기반으로 정렬 (Greedy Nearest Neighbor).
    
    자주 오가는 그룹들이 Y축에서 인접하게 배치.
    """
    if not groups or len(groups) <= 1:
        return groups
    
    if not group_transitions:
        # 이동 데이터 없으면 기본 우선순위로 정렬
        return sorted(groups, key=lambda g: (_BUILDING_PRIORITY.get(g, 50), g))
    
    # 그룹별 총 이동 횟수 계산
    group_total: dict[str, int] = defaultdict(int)
    for (g1, g2), count in group_transitions.items():
        group_total[g1] += count
        group_total[g2] += count
    
    # Greedy Nearest Neighbor
    result: list[str] = []
    remaining = set(groups)
    
    # 가장 이동이 많은 그룹부터 시작
    current = max(groups, key=lambda g: group_total.get(g, 0))
    result.append(current)
    remaining.remove(current)
    
    while remaining:
        # 현재 그룹에서 가장 자주 이동하는 다음 그룹 찾기
        best_next = None
        best_count = -1
        
        for candidate in remaining:
            key = tuple(sorted([current, candidate]))
            count = group_transitions.get(key, 0)
            if count > best_count:
                best_count = count
                best_next = candidate
        
        if best_next is None or best_count == 0:
            # 연결이 없으면 기본 우선순위로 선택
            best_next = min(remaining, key=lambda g: (_BUILDING_PRIORITY.get(g, 50), g))
        
        result.append(best_next)
        remaining.remove(best_next)
        current = best_next
    
    return result


def sort_places_smart(
    places: list[str],
    df: Optional[pd.DataFrame] = None,
    place_col: str = "장소",
) -> list[str]:
    """
    스마트 정렬: 이름 유사도 우선 + 이동 패턴 기반 그룹 순서.
    
    정렬 전략:
      1. 장소들을 그룹(건물/구역)별로 분류
      2. 각 그룹 내에서 메인 장소 → 세부 장소 순서로 정렬
      3. 그룹 간 순서는 실제 이동 패턴 기반 (자주 오가는 그룹끼리 인접)
    
    예시:
      입력: ["본진 휴게실", "FAB", "FAB_휴게실", "본진 타각기"]
      출력: ["FAB", "FAB_휴게실", "본진", "본진 휴게실", "본진 타각기"]
             (FAB↔본진 이동이 많으면 두 그룹이 인접)
    """
    if not places:
        return []
    
    # 1. 그룹별로 장소 분류
    groups: dict[str, list[str]] = defaultdict(list)
    for place in places:
        group = get_place_group(place)
        groups[group].append(place)
    
    # 2. 각 그룹 내에서 정렬 (메인 → 세부)
    for group in groups:
        groups[group] = sort_places_within_group(groups[group])
    
    # 3. 그룹 간 순서 결정
    group_list = list(groups.keys())
    
    if df is not None and not df.empty:
        # 이동 데이터로 그룹 순서 결정
        transitions = build_transition_matrix(df, place_col)
        group_transitions = build_group_transition_matrix(transitions)
        sorted_groups = sort_groups_by_transitions(group_list, group_transitions)
    else:
        # 기본 우선순위로 정렬
        sorted_groups = sorted(group_list, key=lambda g: (_BUILDING_PRIORITY.get(g, 50), g))
    
    # 4. 최종 결과 조합
    result: list[str] = []
    for group in sorted_groups:
        result.extend(groups[group])
    
    return result


# ─── 레거시 호환 함수들 ─────────────────────────────────────────────────

def extract_place_prefix(place_name: str) -> tuple[str, int, str, int]:
    """
    레거시 호환: 장소명에서 그룹화 키를 추출.
    """
    place = str(place_name).strip()
    if not place or place == "nan":
        return ("ZZZ", 99, "ZZZ_empty", 0)
    
    group = get_place_group(place)
    building_priority = _BUILDING_PRIORITY.get(group, 50)
    func_key, floor_num, _ = get_place_sort_key_in_group(place)
    
    return (group, building_priority, func_key, floor_num)


def sort_places_by_similarity(places: list[str]) -> list[str]:
    """
    레거시 호환: 이름 유사도 기반 정렬.
    이동 데이터 없이 이름만으로 정렬.
    """
    return sort_places_smart(places, df=None)


def are_places_similar(place1: str, place2: str) -> bool:
    """두 장소가 같은 그룹인지 판단."""
    return get_place_group(place1) == get_place_group(place2)
