SSMP Migration (Minimal v3: Zones vs Service Sections clarified)
Generated: 2026-01-28T02:38:57

Key clarification applied:
- Level 하위는 (Zone: 영역) + (Spot: PoI) 로 구성
- 기존 spot.csv의 '관리 단위'는 Service Section으로 재분류
  - '(전체)' 포함 (예: 'WWT 2F (전체)') => Service Section member_type=level
  - 그 외:
    - polygon이 있는 경우 => Zone 생성 + Service Section member_type=zone
    - polygon이 없는 경우 => Service Section member_type=level (fallback)

zone_type / risk_level / congestion_prone rules:
- work_area: WWT/FAB/CUB/공사현장 포함 또는 div=workFloor|constructionSite -> risk=high
- confined_space: div=confinedSpace 또는 '밀폐' -> risk=critical
- checkpoint_timeclock: '타각기' + '입구/출구' -> risk=high, congestion_prone=True
- checkpoint_gate: 'GATE' -> risk=high
- hoist: '호이스트/Hoist' -> risk=high, congestion_prone=True
- amenity_rest: '휴게/휴게실' 또는 div=restSpace -> risk=low, congestion_prone=True
- amenity_smoking: '흡연/smoking' -> risk=low, congestion_prone=True
- parking: div=parkingLot 또는 '주차' -> risk=medium
- target_area: '목적지' 또는 div=innerTarget -> risk=medium
- other: default -> risk=medium

Files:
- ssmp_zones.csv : true sub-areas (polygon-defined only)
- ssmp_service_sections.csv / ssmp_service_section_members.csv : manager overlays
