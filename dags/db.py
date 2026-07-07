"""
db.py — capd-pipeline이 CAPD 공유 Supabase Postgres에 직접 접속하기 위한 헬퍼.

backend(FastAPI)의 SQLAlchemy ORM을 쓰지 않고 psycopg2로 raw SQL만 사용한다
(별도 레포라 backend 코드를 import할 수 없음, 그리고 배치 잡은 원래 DB에 직접
붙는 게 자연스러움 — 유저 인증/권한 체크는 온디맨드 API의 역할이고, 이 배치는
"온디맨드가 못 하는 일"인 전체 환자 선제 스캔이 목적이라 애초에 담당의 권한
스코프와 무관하게 전체 활성 환자를 대상으로 함).

연결 정보(CAPD_DB_URL)는 docker-compose 환경변수로 주입 — 배포된 backend와
동일한 Supabase(Session pooler 권장, Cloud Shell에서도 direct는 IPv6라 실패했던
전례가 있음 — CLAUDE.md 인프라 메모 참고) 인스턴스를 그대로 가리키면 됨.
"""
import os
from typing import Any, Optional

import psycopg2
import psycopg2.extras


def get_conn():
    dsn = os.environ["CAPD_DB_URL"]
    return psycopg2.connect(dsn)


def fetch_active_patients(conn) -> list[dict]:
    """role=patient, is_active=true인 전체 환자 (담당의 배정 여부와 무관 — 스캔 대상 자체는 스코프 없음).

    Slack 알림을 담당의별로 묶어 보여주기 위해 현재 담당의(doctor_id/doctor_name)도
    같이 조회함(1건 쿼리로 배치, N+1 방지). users.doctor_id는 현재 담당의를 가리키는
    비정규화 캐시 필드 — backend patients.py의 접근권한 체크가 patient_doctor_assignments를
    우선하고 이 필드는 레거시 폴백으로 쓰는 것과 달리, 여기서는 "누구 담당인지 표시"만
    목적이라 이 필드 하나로 충분(접근권한 판단에 쓰는 게 아님).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT p.id, p.name, p.doctor_id, d.name AS doctor_name
            FROM users p
            LEFT JOIN users d ON d.id = p.doctor_id
            WHERE p.role = 'patient' AND p.is_active = TRUE
            ORDER BY p.id
        """)
        return list(cur.fetchall())


def fetch_recent_records(conn, patient_id: int, limit: int) -> list[dict]:
    """
    최신순 최대 limit건의 submitted/reviewed 기록. limit = window + 1
    (첫 건 = 오늘 취급, 나머지 = historical).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, record_date, weight, blood_pressure, total_ultrafiltration,
                   turbid_peritoneal, fasting_blood_glucose, urine_count, memo
            FROM daily_records
            WHERE patient_id = %s AND status IN ('submitted', 'reviewed')
            ORDER BY record_date DESC
            LIMIT %s
            """,
            (patient_id, limit),
        )
        return list(cur.fetchall())


def fetch_exchanges(conn, daily_record_id: int) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT session_number, exchange_time, drainage_volume,
                   infusion_concentration, infusion_weight, ultrafiltration
            FROM exchange_records
            WHERE daily_record_id = %s
            """,
            (daily_record_id,),
        )
        return list(cur.fetchall())


def record_to_daily_data(r: dict) -> dict:
    return {
        "record_date":           r["record_date"].isoformat(),
        "weight":                float(r["weight"]) if r["weight"] is not None else None,
        "blood_pressure":        r["blood_pressure"],
        "total_ultrafiltration": float(r["total_ultrafiltration"]) if r["total_ultrafiltration"] is not None else None,
        "turbid_peritoneal":     r["turbid_peritoneal"],
        "fasting_blood_glucose": float(r["fasting_blood_glucose"]) if r["fasting_blood_glucose"] is not None else None,
        "urine_count":           r["urine_count"],
        "note":                  r["memo"],
    }


def exchanges_to_dicts(exchanges: list[dict]) -> list[dict]:
    return [
        {
            "session_number":         ex["session_number"],
            "exchange_time":          ex["exchange_time"],
            "drainage_volume":        float(ex["drainage_volume"]) if ex["drainage_volume"] is not None else None,
            "infusion_concentration": float(ex["infusion_concentration"]) if ex["infusion_concentration"] is not None else None,
            "infusion_weight":        float(ex["infusion_weight"]) if ex["infusion_weight"] is not None else None,
            "ultrafiltration":        float(ex["ultrafiltration"]) if ex["ultrafiltration"] is not None else None,
        }
        for ex in exchanges
    ]


# ── Silver/Gold upsert (best-effort, backend/app/api/v1/routes/analytics.py의
#    _upsert_cache와 동일 컬럼·동일 ON CONFLICT 구성. 파라미터 스타일만 psycopg2 %s로) ──

def upsert_metrics(conn, patient_id: int, record_date, today_row: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO patient_daily_metrics (
                patient_id, record_date,
                exchange_count, missing_exchange_slots, drain_sum_g, infused_sum_g,
                recorded_uf_sum_g, calculated_uf_sum_g, uf_min_g, uf_std_g,
                dwell_mean_minutes, dwell_std_minutes, concentration_max,
                reported_total_uf_g, uf_discrepancy_g,
                body_weight_kg, fasting_blood_sugar, urination_count, cloudy_dialysate,
                systolic_bp, diastolic_bp, pulse_pressure, mean_arterial_pressure,
                note, updated_at
            ) VALUES (
                %(patient_id)s, %(record_date)s,
                %(exchange_count)s, %(missing_exchange_slots)s, %(drain_sum_g)s, %(infused_sum_g)s,
                %(recorded_uf_sum_g)s, %(calculated_uf_sum_g)s, %(uf_min_g)s, %(uf_std_g)s,
                %(dwell_mean_minutes)s, %(dwell_std_minutes)s, %(concentration_max)s,
                %(reported_total_uf_g)s, %(uf_discrepancy_g)s,
                %(body_weight_kg)s, %(fasting_blood_sugar)s, %(urination_count)s, %(cloudy_dialysate)s,
                %(systolic_bp)s, %(diastolic_bp)s, %(pulse_pressure)s, %(mean_arterial_pressure)s,
                %(note)s, NOW()
            )
            ON CONFLICT (patient_id, record_date) DO UPDATE SET
                exchange_count          = EXCLUDED.exchange_count,
                missing_exchange_slots  = EXCLUDED.missing_exchange_slots,
                drain_sum_g             = EXCLUDED.drain_sum_g,
                infused_sum_g           = EXCLUDED.infused_sum_g,
                recorded_uf_sum_g       = EXCLUDED.recorded_uf_sum_g,
                calculated_uf_sum_g     = EXCLUDED.calculated_uf_sum_g,
                uf_min_g                = EXCLUDED.uf_min_g,
                uf_std_g                = EXCLUDED.uf_std_g,
                dwell_mean_minutes      = EXCLUDED.dwell_mean_minutes,
                dwell_std_minutes       = EXCLUDED.dwell_std_minutes,
                concentration_max       = EXCLUDED.concentration_max,
                reported_total_uf_g     = EXCLUDED.reported_total_uf_g,
                uf_discrepancy_g        = EXCLUDED.uf_discrepancy_g,
                body_weight_kg          = EXCLUDED.body_weight_kg,
                fasting_blood_sugar     = EXCLUDED.fasting_blood_sugar,
                urination_count         = EXCLUDED.urination_count,
                cloudy_dialysate        = EXCLUDED.cloudy_dialysate,
                systolic_bp             = EXCLUDED.systolic_bp,
                diastolic_bp            = EXCLUDED.diastolic_bp,
                pulse_pressure          = EXCLUDED.pulse_pressure,
                mean_arterial_pressure  = EXCLUDED.mean_arterial_pressure,
                note                    = EXCLUDED.note,
                updated_at              = NOW()
            """,
            {
                "patient_id": patient_id,
                "record_date": record_date,
                "exchange_count": today_row.get("exchange_count"),
                "missing_exchange_slots": today_row.get("missing_exchange_slots"),
                "drain_sum_g": today_row.get("drain_sum_g"),
                "infused_sum_g": today_row.get("infused_sum_g"),
                "recorded_uf_sum_g": today_row.get("recorded_uf_sum_g"),
                "calculated_uf_sum_g": today_row.get("calculated_uf_sum_g"),
                "uf_min_g": today_row.get("uf_min_g"),
                "uf_std_g": today_row.get("uf_std_g"),
                "dwell_mean_minutes": today_row.get("dwell_mean_minutes"),
                "dwell_std_minutes": today_row.get("dwell_std_minutes"),
                "concentration_max": today_row.get("concentration_max"),
                "reported_total_uf_g": today_row.get("reported_total_uf_g"),
                "uf_discrepancy_g": today_row.get("uf_discrepancy_g"),
                "body_weight_kg": today_row.get("body_weight_kg"),
                "fasting_blood_sugar": today_row.get("fasting_blood_sugar"),
                "urination_count": today_row.get("urination_count"),
                "cloudy_dialysate": today_row.get("cloudy_dialysate"),
                "systolic_bp": today_row.get("systolic_bp"),
                "diastolic_bp": today_row.get("diastolic_bp"),
                "pulse_pressure": today_row.get("pulse_pressure"),
                "mean_arterial_pressure": today_row.get("mean_arterial_pressure"),
                "note": today_row.get("note"),
            },
        )


def upsert_analytics(conn, patient_id: int, record_date, window_days: int, result: dict) -> None:
    import json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO patient_daily_analytics (
                patient_id, record_date, window_days,
                trend_json, anomaly_json, correlation_json, eda_json,
                has_anomaly, anomaly_attrs, computed_at
            ) VALUES (
                %(patient_id)s, %(record_date)s, %(window_days)s,
                %(trend_json)s::jsonb, %(anomaly_json)s::jsonb,
                %(correlation_json)s::jsonb, %(eda_json)s::jsonb,
                %(has_anomaly)s, %(anomaly_attrs)s, NOW()
            )
            ON CONFLICT (patient_id, record_date, window_days) DO UPDATE SET
                trend_json       = EXCLUDED.trend_json,
                anomaly_json     = EXCLUDED.anomaly_json,
                correlation_json = EXCLUDED.correlation_json,
                eda_json         = EXCLUDED.eda_json,
                has_anomaly      = EXCLUDED.has_anomaly,
                anomaly_attrs    = EXCLUDED.anomaly_attrs,
                computed_at      = NOW()
            """,
            {
                "patient_id": patient_id,
                "record_date": record_date,
                "window_days": window_days,
                "trend_json": json.dumps(result.get("trend_analysis"), ensure_ascii=False),
                "anomaly_json": json.dumps(result.get("anomaly_detection"), ensure_ascii=False),
                "correlation_json": json.dumps(result.get("attribute_correlation"), ensure_ascii=False),
                "eda_json": json.dumps(result.get("eda"), ensure_ascii=False),
                "has_anomaly": result.get("has_anomaly", False),
                "anomaly_attrs": result.get("anomaly_attrs", []),
            },
        )
