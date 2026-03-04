from __future__ import annotations

"""
LLM용 Journey 컨텍스트 빌더.

작업자 1명의 하루 DataFrame(분 단위)을 Run 단위로 압축해서
LLM이 읽기 좋은 형식으로 변환한다.

주요 출력:
    - journey_token: "[07:01~07:03|GATE|OUT|↑|2m] 타각기출구 → ..." 형태 한 줄 요약
    - runs: 각 Run의 상세 딕셔너리 리스트
    - stats: 비활성/앵커/작업 구간 요약 통계
"""

import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import pandas as pd

from src.data.schema import RawColumns, ProcessedColumns

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────────────────────────────

def _active_symbol(ratio: float, coverage_gap: bool = False) -> str:
    """활성비율을 직관적 기호로 변환."""
    if coverage_gap:
        return "?"       # 신호 없음 (커버리지 밖 or 배터리)
    if ratio >= 0.6:
        return "↑↑"     # 고활성
    if ratio >= 0.15:
        return "↑"       # 저활성
    if ratio >= 0.05:
        return "~"       # 대기
    return "○"           # 비활성


_PLACE_TYPE_SHORT = {
    "HELMET_RACK":    "RACK",
    "REST":           "REST",
    "GATE":           "GATE",
    "WORK_AREA":      "WORK",
    "CONFINED_SPACE": "CONF",
    "INDOOR":         "IN",
    "OUTDOOR":        "OUT",
    "OFFICE":         "OFF",
    "UNKNOWN":        "?",
}


@dataclass
class JourneyRun:
    """하루 Journey를 구성하는 하나의 연속 구간."""

    idx: int
    place: str
    place_type: str
    space_type: str
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp
    duration_min: int
    avg_active_ratio: float
    min_active_ratio: float
    max_active_ratio: float
    hour_start: int
    hour_end: int
    coverage_gap_pct: float      # coverage_gap=True 인 분 비율 (0~1)
    signal_conf: str             # NONE/LOW/MED/HIGH 최빈값

    @property
    def start_hhmm(self) -> str:
        return self.start_ts.strftime("%H:%M")

    @property
    def end_hhmm(self) -> str:
        return self.end_ts.strftime("%H:%M")

    @property
    def active_symbol(self) -> str:
        return _active_symbol(self.avg_active_ratio, self.coverage_gap_pct > 0.5)

    @property
    def place_type_short(self) -> str:
        return _PLACE_TYPE_SHORT.get(self.place_type, "?")

    def to_token(self) -> str:
        """
        LLM 프롬프트용 한 줄 토큰.
        예) [07:01~07:03|GATE|OUT|↑|2m] 타각기출구
        """
        gap = "|no-sig" if self.coverage_gap_pct > 0.5 else ""
        return (
            f"[{self.start_hhmm}~{self.end_hhmm}"
            f"|{self.place_type_short}"
            f"|{self.space_type[:3] if self.space_type else '?'}"
            f"|{self.active_symbol}"
            f"|{self.duration_min}m"
            f"{gap}] {self.place}"
        )

    def to_detail_dict(self) -> Dict[str, Any]:
        """LLM system prompt용 상세 딕셔너리."""
        return {
            "idx":           self.idx,
            "place":         self.place,
            "place_type":    self.place_type,
            "space":         self.space_type,
            "start":         self.start_hhmm,
            "end":           self.end_hhmm,
            "duration_min":  int(self.duration_min),
            "active_avg":    round(float(self.avg_active_ratio), 3),
            "active_min":    round(float(self.min_active_ratio), 3),
            "active_max":    round(float(self.max_active_ratio), 3),
            "coverage_gap%": round(float(self.coverage_gap_pct) * 100.0, 1),
            "signal_conf":   self.signal_conf,
        }


def build_journey_runs(df_worker: pd.DataFrame) -> List[JourneyRun]:
    """
    작업자 DataFrame → 연속된 같은 장소 Run 리스트 생성.

    장소명은 보정장소 우선(`CORRECTED_PLACE`), 없으면 원본 `PLACE` 사용.
    """
    if df_worker.empty:
        return []

    df = df_worker.sort_values(RawColumns.TIME).copy()

    place_col = (
        ProcessedColumns.CORRECTED_PLACE
        if ProcessedColumns.CORRECTED_PLACE in df.columns
        else RawColumns.PLACE
    )
    ptype_col = ProcessedColumns.PLACE_TYPE
    stype_col = ProcessedColumns.SPACE_TYPE
    aratio_col = ProcessedColumns.ACTIVE_RATIO
    gap_col = ProcessedColumns.COVERAGE_GAP
    sig_col = ProcessedColumns.SIGNAL_CONFIDENCE

    times = pd.to_datetime(df[RawColumns.TIME].values)
    places = df[place_col].fillna("").astype(str).values

    # PLACE_TYPE: Categorical 안전 처리
    if ptype_col in df.columns:
        pt_series = df[ptype_col]
        if hasattr(pt_series, "cat"):
            try:
                pt_series = pt_series.cat.add_categories(["UNKNOWN"]).fillna("UNKNOWN")
            except Exception:
                pt_series = pt_series.astype(str).fillna("UNKNOWN")
        else:
            pt_series = pt_series.fillna("UNKNOWN")
        ptypes = pt_series.astype(str).values
    else:
        ptypes = ["UNKNOWN"] * len(df)

    # SPACE_TYPE: Categorical 안전 처리
    if stype_col in df.columns:
        st_series = df[stype_col]
        if hasattr(st_series, "cat"):
            try:
                st_series = st_series.cat.add_categories(["UNKNOWN"]).fillna("UNKNOWN")
            except Exception:
                st_series = st_series.astype(str).fillna("UNKNOWN")
        else:
            st_series = st_series.fillna("UNKNOWN")
        stypes = st_series.astype(str).values
    else:
        stypes = ["UNKNOWN"] * len(df)
    ratios = df[aratio_col].fillna(0.0).astype(float).values if aratio_col in df.columns else [0.0] * len(df)
    gaps = df[gap_col].fillna(False).astype(bool).values if gap_col in df.columns else [False] * len(df)

    # SIGNAL_CONFIDENCE: Categorical 안전 처리
    if sig_col in df.columns:
        sig_series = df[sig_col]
        if hasattr(sig_series, "cat"):
            try:
                sig_series = sig_series.cat.add_categories(["UNKNOWN"]).fillna("UNKNOWN")
            except Exception:
                sig_series = sig_series.astype(str).fillna("UNKNOWN")
        else:
            sig_series = sig_series.fillna("UNKNOWN")
        sigs = sig_series.astype(str).values
    else:
        sigs = ["UNKNOWN"] * len(df)

    runs: List[JourneyRun] = []
    cur_start: Optional[int] = None
    cur_place: Optional[str] = None
    cur_ptype: Optional[str] = None
    run_idx = 0

    def flush(end_idx: int) -> None:
        nonlocal run_idx, cur_start, cur_place, cur_ptype
        if cur_start is None:
            return
        s = cur_start
        e = end_idx
        duration = max(1, int((times[e] - times[s]).total_seconds() / 60) + 1)
        r_slice = ratios[s : e + 1]
        g_slice = gaps[s : e + 1]
        sig_slice = sigs[s : e + 1]

        # signal_confidence 최빈값
        sig_counts: Dict[str, int] = {}
        for sconf in sig_slice:
            sig_counts[sconf] = sig_counts.get(sconf, 0) + 1
        sig_mode = max(sig_counts, key=sig_counts.get) if sig_counts else "UNKNOWN"

        run = JourneyRun(
            idx=run_idx,
            place=places[s] or "알 수 없음",
            place_type=ptypes[s],
            space_type=stypes[s],
            start_ts=times[s],
            end_ts=times[e],
            duration_min=duration,
            avg_active_ratio=float(pd.Series(r_slice).mean()) if len(r_slice) else 0.0,
            min_active_ratio=float(pd.Series(r_slice).min()) if len(r_slice) else 0.0,
            max_active_ratio=float(pd.Series(r_slice).max()) if len(r_slice) else 0.0,
            hour_start=int(times[s].hour),
            hour_end=int(times[e].hour),
            coverage_gap_pct=float(sum(g_slice)) / float(len(g_slice)) if len(g_slice) else 0.0,
            signal_conf=str(sig_mode),
        )
        runs.append(run)
        run_idx += 1
        cur_start = None
        cur_place = None
        cur_ptype = None

    n = len(df)
    for i in range(n):
        p = places[i]
        pt = ptypes[i]
        if cur_start is None:
            cur_start = i
            cur_place = p
            cur_ptype = pt
        else:
            if p != cur_place or pt != cur_ptype:
                flush(i - 1)
                cur_start = i
                cur_place = p
                cur_ptype = pt

    if cur_start is not None:
        flush(n - 1)

    return runs


def build_journey_context(
    df_worker: pd.DataFrame,
    worker_name: str,
    date_str: str,
    company: str = "",
) -> Dict[str, Any]:
    """
    하루 Journey 전체를 LLM에 넘기기 위한 컨텍스트 구성.

    Returns:
        {
          "worker_name": str,
          "date": str,
          "company": str,
          "journey_token": str,
          "runs": list[dict],
          "stats": dict,
          "space_legend": str,
        }
    """
    runs = build_journey_runs(df_worker)
    if not runs:
        return {}

    token_parts = [r.to_token() for r in runs]
    journey_token = " → ".join(token_parts)

    total_min = sum(r.duration_min for r in runs)
    rack_runs = [r for r in runs if r.place_type == "HELMET_RACK"]
    gate_runs = [r for r in runs if r.place_type == "GATE"]
    rest_runs = [r for r in runs if r.place_type == "REST"]
    inactive_runs = [
        r for r in runs
        if r.avg_active_ratio < 0.05 and r.place_type not in ("HELMET_RACK", "REST")
    ]

    stats = {
        "total_recorded_min": int(total_min),
        "run_count": len(runs),
        "rack_runs": len(rack_runs),
        "gate_runs": len(gate_runs),
        "rest_runs": len(rest_runs),
        "inactive_non_rack_runs": len(inactive_runs),
        "longest_inactive_min": int(max((r.duration_min for r in inactive_runs), default=0)),
        "place_types_seen": list({r.place_type for r in runs}),
        "unique_places": list({r.place for r in runs}),
    }

    space_legend = (
        "장소유형 설명:\n"
        "  RACK = 헬멧/보호구 걸이대 (출퇴근 거치 or 점심 거치)\n"
        "  GATE = 타각기/출입 게이트 (출퇴근 기록 장치)\n"
        "  REST = 휴게실·식당·탈의실 (공식 휴게 시설)\n"
        "  WORK/CONF/IN/OUT = 작업 구역 또는 실내/실외 일반 공간\n"
        "\n활성비율 기호:\n"
        "  ↑↑ = 고활성(≥0.6): 이동·운반 등 활발한 신체 활동\n"
        "  ↑  = 저활성(0.15~0.6): 감독·측량 등 움직임 적은 작업\n"
        "  ~  = 대기(0.05~0.15): 작업공간 대기\n"
        "  ○  = 비활성(<0.05): 완전 정지\n"
        "  ?  = 신호 없음: 커버리지 밖 또는 배터리 문제"
    )

    return {
        "worker_name": worker_name,
        "date": date_str,
        "company": company,
        "journey_token": journey_token,
        "runs": [r.to_detail_dict() for r in runs],
        "stats": stats,
        "space_legend": space_legend,
    }

