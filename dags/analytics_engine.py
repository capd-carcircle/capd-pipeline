"""
analytics_engine.py — 분석 엔진 (capd-pipeline 벤더링 사본)

원본: backend/app/services/analytics_engine.py (= ai/tools/data_engineering.py +
ai/tools/analytics.py 포팅본). 이 파일은 그 사본을 그대로 복사해 온 것 — Airflow DAG가
FastAPI 앱을 거치지 않고 독립적으로(별도 레포·별도 컨테이너) 같은 계산을 돌릴 수 있도록
하기 위함. 순수 Python(외부 라이브러리 의존 없음).

⚠️ 주의: backend/app/services/analytics_engine.py(그리고 그 원본인 ai/tools/*.py)가
바뀌면 이 파일도 반드시 동일하게 맞출 것. 세 레포(ai/backend/capd-pipeline)가 같은
입력에 다른 결과를 내면 안 됨. (capd-analytics/Kotlin 포팅본도 별도로 존재 — 그쪽은
정합성 테스트(ParityTest.kt)가 이미 있음. 이 Python 사본은 아직 자동 정합성 테스트가
없음 — 필요하면 backend/tests/test_ai_parity.py 방식을 확장해 이 파일도 포함시킬 것.)

원본: backend/app/services/analytics_engine.py
"""

import math
import statistics
from typing import Optional


# ================================================================
# data_engineering.py 포팅분
# Exchange Event Table -> Exchange Aggregate Table -> Daily Table -> Daily Model Row
# ================================================================

def _time_to_minutes(time_str: Optional[str]) -> Optional[int]:
    """HH:MM -> 자정 이후 분 변환. None이면 None 반환."""
    if not time_str:
        return None
    try:
        h, m = time_str.strip().split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return None


def _parse_bp(bp_str: Optional[str]) -> dict:
    """
    혈압 문자열 -> 파생 지표
    Returns: {systolic_bp, diastolic_bp, pulse_pressure, mean_arterial_pressure}
    """
    if not bp_str:
        return {}
    try:
        parts = bp_str.strip().split("/")
        sys_bp = int(parts[0])
        dia_bp = int(parts[1])
        return {
            "systolic_bp":            sys_bp,
            "diastolic_bp":           dia_bp,
            "pulse_pressure":         sys_bp - dia_bp,
            "mean_arterial_pressure": round(dia_bp + (sys_bp - dia_bp) / 3.0, 1),
        }
    except Exception:
        return {}


def _build_exchange_events(exchange_records: list[dict]) -> list[dict]:
    """
    raw exchange_records -> Exchange Event Table (슬롯별 파생 속성 계산)

    파생 속성:
    - exchange_time_minutes: 자정 이후 분
    - observed_flag:         실제 교환 데이터 있으면 1
    - dwell_minutes:         이전 교환 이후 경과 시간 (분)
    - calculated_uf_g:       drainage_volume - infusion_weight
    - uf_error_g:            ultrafiltration(보고) - calculated_uf_g
    """
    sorted_recs = sorted(exchange_records, key=lambda x: x.get("session_number", 0))

    events = []
    prev_minutes: Optional[int] = None

    for ex in sorted_recs:
        drainage    = ex.get("drainage_volume")
        infused     = ex.get("infusion_weight")
        reported_uf = ex.get("ultrafiltration")
        time_str    = ex.get("exchange_time")
        time_min    = _time_to_minutes(time_str)
        conc        = ex.get("infusion_concentration")

        observed = 1 if (drainage is not None and infused is not None) else 0

        dwell = None
        if time_min is not None and prev_minutes is not None:
            diff = time_min - prev_minutes
            if diff < 0:
                diff += 24 * 60
            dwell = diff

        calc_uf = None
        if drainage is not None and infused is not None:
            calc_uf = float(drainage) - float(infused)

        uf_error = None
        if reported_uf is not None and calc_uf is not None:
            uf_error = float(reported_uf) - calc_uf

        events.append({
            "session_number":         ex.get("session_number"),
            "exchange_time":          time_str,
            "exchange_time_minutes":  time_min,
            "drainage_volume":        float(drainage) if drainage is not None else None,
            "infusion_concentration": float(conc) if conc is not None else None,
            "infusion_weight":        float(infused) if infused is not None else None,
            "ultrafiltration":        float(reported_uf) if reported_uf is not None else None,
            "observed_flag":          observed,
            "dwell_minutes":          dwell,
            "calculated_uf_g":        round(calc_uf, 1) if calc_uf is not None else None,
            "uf_error_g":             round(uf_error, 1) if uf_error is not None else None,
        })

        if time_min is not None:
            prev_minutes = time_min

    return events


def _aggregate_exchanges(events: list[dict]) -> dict:
    """
    Exchange Event Table -> Exchange Aggregate (일 단위 집계)

    집계 속성:
    - exchange_count, missing_exchange_slots
    - drain_sum_g, infused_sum_g
    - recorded_uf_sum_g: 회차별 보고 UF 합계 (환자 입력값 sum)
    - calculated_uf_sum_g, uf_min_g, uf_std_g
    - dwell_mean_minutes, dwell_std_minutes
    - concentration_max
    """
    observed = [e for e in events if e["observed_flag"] == 1]
    exchange_count = len(observed)
    missing_slots  = 5 - exchange_count

    drain_sum   = sum(e["drainage_volume"] for e in observed if e["drainage_volume"] is not None)
    infused_sum = sum(e["infusion_weight"]  for e in observed if e["infusion_weight"]  is not None)

    # 회차별 보고 UF 합계
    recorded_ufs    = [e["ultrafiltration"] for e in observed if e["ultrafiltration"] is not None]
    recorded_uf_sum = round(sum(recorded_ufs), 1) if recorded_ufs else None

    # 계산 UF (drainage - infusion)
    calc_ufs    = [e["calculated_uf_g"] for e in observed if e["calculated_uf_g"] is not None]
    calc_uf_sum = round(sum(calc_ufs), 1) if calc_ufs else None
    uf_min      = round(min(calc_ufs), 1) if calc_ufs else None
    uf_std      = round(statistics.stdev(calc_ufs), 1) if len(calc_ufs) >= 2 else None

    dwells     = [e["dwell_minutes"] for e in events if e["dwell_minutes"] is not None]
    dwell_mean = round(sum(dwells) / len(dwells), 1) if dwells else None
    dwell_std  = round(statistics.stdev(dwells), 1) if len(dwells) >= 2 else None

    concs    = [e["infusion_concentration"] for e in observed if e["infusion_concentration"] is not None]
    conc_max = max(concs) if concs else None

    return {
        "exchange_count":         exchange_count,
        "missing_exchange_slots": missing_slots,
        "drain_sum_g":            round(drain_sum, 1) if drain_sum else None,
        "infused_sum_g":          round(infused_sum, 1) if infused_sum else None,
        "recorded_uf_sum_g":      recorded_uf_sum,
        "calculated_uf_sum_g":    calc_uf_sum,
        "uf_min_g":               uf_min,
        "uf_std_g":               uf_std,
        "dwell_mean_minutes":     dwell_mean,
        "dwell_std_minutes":      dwell_std,
        "concentration_max":      conc_max,
    }


def build_daily_model_row(daily_data: dict, exchange_records: list[dict]) -> dict:
    """
    하루치 기록 -> Daily Model Row (25개 컬럼)

    Args:
        daily_data:       {date/record_date, weight, blood_pressure,
                           total_ultrafiltration, turbid_peritoneal,
                           fasting_blood_glucose, urine_count, note/memo}
        exchange_records: [{session_number, exchange_time, drainage_volume,
                            infusion_concentration, infusion_weight, ultrafiltration}]

    Returns:
        Daily Model Row dict -- run_all_tasks()에 바로 입력 가능
    """
    events = _build_exchange_events(exchange_records or [])
    agg    = _aggregate_exchanges(events)
    bp     = _parse_bp(daily_data.get("blood_pressure"))

    reported_uf    = daily_data.get("total_ultrafiltration")
    calc_uf_sum    = agg.get("calculated_uf_sum_g")
    uf_discrepancy = (
        round(float(reported_uf) - float(calc_uf_sum), 1)
        if reported_uf is not None and calc_uf_sum is not None
        else None
    )

    row = {
        "date": daily_data.get("date") or daily_data.get("record_date"),
        # Exchange 집계
        **agg,
        "reported_total_uf_g": float(reported_uf) if reported_uf is not None else None,
        "uf_discrepancy_g":    uf_discrepancy,
        # Daily 기본
        "body_weight_kg":      float(daily_data["weight"]) if daily_data.get("weight") is not None else None,
        "fasting_blood_sugar": float(daily_data["fasting_blood_glucose"]) if daily_data.get("fasting_blood_glucose") is not None else None,
        "urination_count":     daily_data.get("urine_count"),
        "cloudy_dialysate":    1 if daily_data.get("turbid_peritoneal") else 0,
        # 혈압 파생
        **bp,
        # 자유 기술 메모 (텍스트, 수치 분석 제외 -- LLM 컨텍스트용)
        "note": daily_data.get("note") or daily_data.get("memo"),
    }

    return row


# ================================================================
# analytics.py 포팅분
# Task 1: Trend Analysis / Task 2: Anomaly Detection /
# Task 3: Attribute Correlation / Task 4: EDA
# ================================================================

def _valid(series: list) -> list[float]:
    return [float(v) for v in series if v is not None]


def _mean(vals: list[float]) -> Optional[float]:
    return round(sum(vals) / len(vals), 2) if vals else None


def _std(vals: list[float]) -> Optional[float]:
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    return round(math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1)), 2)


def _median(vals: list[float]) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    return (s[mid - 1] + s[mid]) / 2.0 if n % 2 == 0 else float(s[mid])


def _mad(vals: list[float]) -> Optional[float]:
    """Median Absolute Deviation"""
    if len(vals) < 2:
        return None
    med = _median(vals)
    return _median([abs(v - med) for v in vals])


def _rank(vals: list[float]) -> list[float]:
    """평균 순위 (동점 처리 포함)"""
    n = len(vals)
    indexed = sorted(enumerate(vals), key=lambda x: x[1])
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j < n - 1 and indexed[j + 1][1] == indexed[j][1]:
            j += 1
        avg = (i + j) / 2.0 + 1
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg
        i = j + 1
    return ranks


def _spearman(x: list[float], y: list[float]) -> Optional[float]:
    """scipy 없이 순수 Python으로 Spearman 상관계수 계산"""
    if len(x) != len(y) or len(x) < 3:
        return None
    rx, ry = _rank(x), _rank(y)
    n = len(rx)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    den_x = math.sqrt(sum((rx[i] - mx) ** 2 for i in range(n)))
    den_y = math.sqrt(sum((ry[i] - my) ** 2 for i in range(n)))
    if den_x == 0 or den_y == 0:
        return None
    return round(num / (den_x * den_y), 3)


_TREND_THRESH = {
    "body_weight_kg":         {"much": 1.5,  "normal": 0.5},
    "systolic_bp":            {"much": 15,   "normal": 5},
    "diastolic_bp":           {"much": 10,   "normal": 4},
    "mean_arterial_pressure": {"much": 10,   "normal": 4},
    "fasting_blood_sugar":    {"much": 30,   "normal": 10},
    "urination_count":        {"much": 5,    "normal": 2},
    "exchange_count":         {"much": 2,    "normal": 1},
    "dwell_mean_minutes":     {"much": 60,   "normal": 30},
    "concentration_max":      {"much": 1.0,  "normal": 0.5},
    "calculated_uf_sum_g":    {"pct_much": 20, "pct_normal": 10},
    "infused_sum_g":          {"pct_much": 20, "pct_normal": 10},
}

_UNITS = {
    "body_weight_kg":         "kg",
    "systolic_bp":            "mmHg",
    "diastolic_bp":           "mmHg",
    "mean_arterial_pressure": "mmHg",
    "fasting_blood_sugar":    "mg/dL",
    "urination_count":        "회",
    "exchange_count":         "회",
    "dwell_mean_minutes":     "분",
    "concentration_max":      "%",
    "calculated_uf_sum_g":    "g",
    "infused_sum_g":          "g",
}

TREND_ATTRS = list(_TREND_THRESH.keys())


def _classify_trend(diff: float, thresh: dict, baseline: float = None) -> str:
    if "pct_much" in thresh:
        if baseline and baseline != 0:
            pct = abs(diff) / abs(baseline) * 100
            if pct >= thresh["pct_much"]:
                return "much_higher_than_baseline" if diff > 0 else "much_lower_than_baseline"
            if pct >= thresh["pct_normal"]:
                return "higher_than_baseline" if diff > 0 else "lower_than_baseline"
        return "stable"
    much, normal = thresh["much"], thresh["normal"]
    if abs(diff) >= much:
        return "much_higher_than_baseline" if diff > 0 else "much_lower_than_baseline"
    if abs(diff) >= normal:
        return "higher_than_baseline" if diff > 0 else "lower_than_baseline"
    return "stable"


def task1_trend_analysis(today_row: dict, historical_rows: list[dict]) -> dict:
    """
    오늘 값 vs 7일/30일 baseline 비교

    Args:
        today_row:       오늘 Daily Model Row
        historical_rows: 최신->과거 순 (오늘 제외)
    """
    results = {}
    for attr in TREND_ATTRS:
        raw = today_row.get(attr)
        if raw is None:
            continue
        today_val = float(raw)

        hist = _valid([r.get(attr) for r in historical_rows])
        last_7d  = hist[:7]
        last_30d = hist[:30]

        thresh = _TREND_THRESH[attr]
        unit   = _UNITS.get(attr, "")
        entry  = {"today_value": today_val, "unit": unit}

        if last_30d:
            m30 = sum(last_30d) / len(last_30d)
            d30 = round(today_val - m30, 2)
            entry["previous_30d_mean"]        = round(m30, 2)
            entry["difference_from_30d_mean"] = d30
            entry["trend_30d"] = _classify_trend(d30, thresh, m30)

        if last_7d:
            m7  = sum(last_7d) / len(last_7d)
            d7  = round(today_val - m7, 2)
            pct = round((today_val - m7) / m7 * 100, 1) if m7 != 0 else None
            entry["previous_7d_mean"]               = round(m7, 2)
            entry["difference_from_7d_mean"]        = d7
            entry["percentage_change_from_7d_mean"] = pct
            entry["trend_7d"] = _classify_trend(d7, thresh, m7)

        entry["trend_summary"] = entry.get("trend_30d") or entry.get("trend_7d") or "insufficient_data"

        parts = [f"오늘 값 {today_val} {unit}."]
        if "trend_30d" in entry:
            parts.append(
                f"30일 평균 {entry['previous_30d_mean']} {unit} 대비 "
                f"{entry['difference_from_30d_mean']:+.2f} {unit} ({entry['trend_30d']})."
            )
        if "trend_7d" in entry:
            parts.append(
                f"7일 평균 {entry['previous_7d_mean']} {unit} 대비 "
                f"{entry['difference_from_7d_mean']:+.2f} {unit} ({entry['trend_7d']})."
            )
        if len(parts) == 1:
            parts.append("(과거 데이터 없음)")
        entry["statement"] = " ".join(parts)

        results[attr] = entry

    return {"task": "trend_analysis", "results": results}


# reported_total_uf_g·recorded_uf_sum_g는 여기 포함하지 않음 — 셋 다 "배액량-주입량"이라는
# 같은 원본에서 나온 같은 값이라(프론트가 회차별/일별 제수량을 전부 자동 계산해서 저장,
# 환자가 독립적으로 입력하는 경로가 없음) calculated_uf_sum_g 하나만 대표로 씀.
ANOMALY_ATTRS = [
    "body_weight_kg",
    "calculated_uf_sum_g",
    "systolic_bp",
    "diastolic_bp",
    "mean_arterial_pressure",
    "fasting_blood_sugar",
    "infused_sum_g",
]

_Z_LEVELS = [(3.0, "strong_anomaly"), (2.0, "mild_anomaly")]


def _z_label(z: float) -> str:
    for thresh, label in _Z_LEVELS:
        if abs(z) >= thresh:
            return label
    return "normal"


def task2_anomaly_detection(today_row: dict, historical_rows: list[dict]) -> dict:
    """
    오늘 값 vs 30일 window:
    - rolling z-score = (today - 30d_mean) / 30d_std
    - robust z-score  = 0.6745 * (today - 30d_median) / MAD
    """
    results = {}
    for attr in ANOMALY_ATTRS:
        raw = today_row.get(attr)
        if raw is None:
            continue
        today_val = float(raw)

        hist = _valid([r.get(attr) for r in historical_rows[:30]])
        unit = _UNITS.get(attr, "")

        if len(hist) < 3:
            results[attr] = {
                "today_value":    today_val,
                "sufficient_data": False,
                "statement": f"오늘 값 {today_val} {unit} -- 과거 데이터 부족 ({len(hist)}개, 최소 3개 필요)",
            }
            continue

        mean_30 = sum(hist) / len(hist)
        std_30  = _std(hist) or 0.001
        med_30  = _median(hist) or mean_30
        mad_30  = _mad(hist) or 0.001

        z_score  = round((today_val - mean_30) / std_30, 3)
        robust_z = round(0.6745 * (today_val - med_30) / mad_30, 3)

        z_label      = _z_label(z_score)
        robust_label = _z_label(robust_z)

        statement = (
            f"오늘 값 {today_val} {unit}, 30일 평균 {round(mean_30, 2)} {unit}, "
            f"표준편차 {round(std_30, 2)} {unit}. "
            f"Rolling z-score: {z_score} -> {z_label}. "
            f"Robust z-score: {robust_z} -> {robust_label}."
        )

        results[attr] = {
            "today_value":           today_val,
            "baseline_mean":         round(mean_30, 2),
            "baseline_std":          round(std_30, 2),
            "z_score_30d":           z_score,
            "z_interpretation":      z_label,
            "robust_z_score":        robust_z,
            "robust_interpretation": robust_label,
            "is_anomaly":            z_label != "normal" or robust_label != "normal",
            "sufficient_data":       True,
            "statement":             statement,
        }

    return {"task": "anomaly_detection", "results": results}


CORR_ATTRS = [
    "body_weight_kg",
    "calculated_uf_sum_g",
    "systolic_bp",
    "diastolic_bp",
    "mean_arterial_pressure",
    "fasting_blood_sugar",
    "urination_count",
    "exchange_count",
    "infused_sum_g",
    "dwell_mean_minutes",
    "concentration_max",
]

_CORR_LEVELS = [(0.9, "very strong"), (0.7, "strong"), (0.5, "moderate")]


def _corr_label(r: float) -> str:
    for thresh, label in _CORR_LEVELS:
        if abs(r) >= thresh:
            return label
    return "weak"


def task3_attribute_correlation(historical_rows: list[dict], window: int = 30) -> dict:
    """
    최근 window일치 Spearman 상관관계 -- 계산 가능한 쌍은 전부 반환(상관계수 무관).
    |r| >= 0.5 이상만 기본 노출할지는 호출 쪽(API 응답을 쓰는 화면)에서 결정.
    interpretation 필드가 "weak"면 |r| < 0.5인 쌍.
    """
    rows = historical_rows[:window]
    if len(rows) < 7:
        return {
            "task":        "attribute_correlation",
            "method":      "spearman_correlation",
            "window_days": len(rows),
            "results":     [],
            "note":        f"데이터 부족 ({len(rows)}일) -- 최소 7일 필요",
        }

    series: dict[str, list[float]] = {}
    for attr in CORR_ATTRS:
        vals = _valid([r.get(attr) for r in rows])
        if len(vals) >= 7:
            series[attr] = vals

    attrs = list(series.keys())
    pairs = []

    for i in range(len(attrs)):
        for j in range(i + 1, len(attrs)):
            a1, a2 = attrs[i], attrs[j]
            x_list, y_list = [], []
            for r in rows:
                v1, v2 = r.get(a1), r.get(a2)
                if v1 is not None and v2 is not None:
                    x_list.append(float(v1))
                    y_list.append(float(v2))
            if len(x_list) < 7:
                continue

            corr = _spearman(x_list, y_list)
            if corr is None:
                continue

            direction = "positive" if corr > 0 else "negative"
            label     = _corr_label(corr)
            pairs.append({
                "attr1":          a1,
                "attr2":          a2,
                "correlation":    corr,
                "direction":      direction,
                "interpretation": label,
                "statement": (
                    f"{a1} and {a2} has a correlation of {corr} "
                    f"showing a {label} {direction} correlation."
                ),
            })

    pairs.sort(key=lambda p: abs(p["correlation"]), reverse=True)

    return {
        "task":        "attribute_correlation",
        "method":      "spearman_correlation",
        "window_days": len(rows),
        "results":     pairs,
    }


EDA_ATTRS = CORR_ATTRS


def task4_eda(today_row: dict, historical_rows: list[dict]) -> dict:
    """기초 통계 (30d) + today vs 7d/30d 비교"""
    results = {}
    for attr in EDA_ATTRS:
        today_raw = today_row.get(attr)
        hist_30   = _valid([r.get(attr) for r in historical_rows[:30]])
        hist_7    = _valid([r.get(attr) for r in historical_rows[:7]])

        entry: dict = {}
        if today_raw is not None:
            entry["today_value"] = float(today_raw)
        if hist_30:
            entry["recent_30d_mean"] = _mean(hist_30)
            entry["recent_30d_std"]  = _std(hist_30)
            entry["recent_30d_min"]  = round(min(hist_30), 2)
            entry["recent_30d_max"]  = round(max(hist_30), 2)
        if hist_7:
            entry["recent_7d_mean"] = _mean(hist_7)
            entry["recent_7d_min"]  = round(min(hist_7), 2)
            entry["recent_7d_max"]  = round(max(hist_7), 2)

        if entry:
            results[attr] = entry

    return {"task": "exploratory_data_analysis", "results": results}


def run_all_tasks(today_row: dict, historical_rows: list[dict], window: int = 30) -> dict:
    """
    4가지 분석 Task 모두 실행

    Args:
        today_row:       build_daily_model_row()로 생성한 오늘 Daily Model Row
        historical_rows: 최신->과거 순 Daily Model Row 리스트 (오늘 제외)
        window:          task3(상관관계)에 쓸 최근 며칠치 기준(기본 30일).
                         task1/2/4는 "오늘 vs 7일/30일 평균"이라는 고정된 통계 정의라
                         window와 무관하게 항상 7일·30일 기준 그대로 계산함(의도된 동작).

    Returns:
        {
            "trend_analysis":        {...},
            "anomaly_detection":     {...},
            "attribute_correlation": {...},
            "eda":                   {...},
            "has_anomaly":           bool,
            "anomaly_attrs":         [str],
        }
    """
    trend   = task1_trend_analysis(today_row, historical_rows)
    anomaly = task2_anomaly_detection(today_row, historical_rows)
    corr    = task3_attribute_correlation(historical_rows, window=window)
    eda     = task4_eda(today_row, historical_rows)

    anomaly_attrs = [
        attr
        for attr, res in anomaly["results"].items()
        if isinstance(res, dict) and res.get("is_anomaly")
    ]

    return {
        "trend_analysis":        trend,
        "anomaly_detection":     anomaly,
        "attribute_correlation": corr,
        "eda":                   eda,
        "has_anomaly":           bool(anomaly_attrs),
        "anomaly_attrs":         anomaly_attrs,
    }
