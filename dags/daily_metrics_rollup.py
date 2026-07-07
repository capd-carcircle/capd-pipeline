"""
daily_metrics_rollup DAG

"온디맨드가 못 하는 일" 담당 — 의사가 특정 환자의 분석 리포트를 열어야만 채워지던
patient_daily_metrics(Silver)/patient_daily_analytics(Gold) 캐시를, 매일 전체 활성
환자에 대해 선제적으로 채워 넣는다. 이상치가 있으면 의사가 리포트를 열어보지 않아도
Slack으로 알림.

흐름 (task 2개):
  1. rollup_all_patients — 활성 환자 전체 순회 → 최근 max(WINDOWS)+1건 기록 조회 →
     Daily Model Row 생성 → WINDOWS(7/30/90) 각각에 대해 run_all_tasks 실행 →
     window별로 Silver/Gold upsert(리포트 3개 탭 전부 캐시 선제 적중되게 함) →
     그중 하나의 window에서라도 이상치면 이상치 감지 목록에 포함(bool_or, 목록/
     대시보드 배지 로직과 동일한 기준 — 2026-07-08).
  2. send_slack_alert — 위 목록으로 Slack 메시지 구성·전송(웹훅 미설정 시 로그만).

이 DAG는 이 레포(capd-pipeline)의 로컬 docker-compose Airflow에서만 실행된다.
배포된 사이트(capd-backend)는 이 DAG와 무관하게 온디맨드로 계속 정상 작동한다
(Airflow가 꺼져 있어도 서비스 영향 없음 — speed layer와 batch layer 분리).

⚠️ 환자별 계산 실패는 개별 try/except로 격리 — 한 환자의 예외가 전체 배치를
중단시키지 않음. 실패한 환자는 로그로만 남기고 다음 환자로 계속 진행.

⚠️ window 앙상블 도입 배경(2026-07-08): 좁은 window(7일)는 표본이 적어 평균/
표준편차가 불안정해서 더 민감하게(과다검출 쪽으로) 이상치를 잡는 특성이 있음 —
실데이터에서 같은 환자·같은 날짜에 window=7은 이상치, window=30은 정상으로
갈리는 사례 확인됨. 이전엔 window=30 하나만 계산해서 이 DAG가 놓치던 걸,
이제 7/30/90 다 계산해서 "하나라도 잡히면 알림"으로 바꿈(안전지향 — 응급의료
조기경보점수 방식과 동일한 논리).
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import db
from analytics_engine import build_daily_model_row, run_all_tasks
from slack_alert import build_message, send_slack

logger = logging.getLogger(__name__)

WINDOWS = [7, 30, 90]  # 분석 리포트 페이지의 7/30/90일 탭과 동일한 기준


def _rollup_all_patients(**context) -> list[dict]:
    conn = db.get_conn()
    anomalies: list[dict] = []

    try:
        patients = db.fetch_active_patients(conn)
        logger.info("[rollup] 활성 환자 %d명 스캔 시작 (window %s 전부 계산)", len(patients), WINDOWS)

        for patient in patients:
            patient_id = patient["id"]
            patient_name = patient["name"]
            doctor_id = patient.get("doctor_id")
            doctor_name = patient.get("doctor_name")
            try:
                max_window = max(WINDOWS)
                records = db.fetch_recent_records(conn, patient_id, max_window + 1)
                if not records:
                    continue

                today_record, *historical_records_full = records

                today_row = build_daily_model_row(
                    db.record_to_daily_data(today_record),
                    db.exchanges_to_dicts(db.fetch_exchanges(conn, today_record["id"])),
                )
                # 전체 historical row는 한 번만 만들어서(DB 재조회 없이) window별로 슬라이스만 다르게 재사용
                historical_rows_full = [
                    build_daily_model_row(
                        db.record_to_daily_data(r),
                        db.exchanges_to_dicts(db.fetch_exchanges(conn, r["id"])),
                    )
                    for r in historical_records_full
                ]

                db.upsert_metrics(conn, patient_id, today_record["record_date"], today_row)

                patient_has_anomaly = False
                patient_anomaly_attrs: set[str] = set()
                for w in WINDOWS:
                    historical_rows = historical_rows_full[:w]
                    result = run_all_tasks(today_row, historical_rows, window=w)
                    db.upsert_analytics(
                        conn, patient_id, today_record["record_date"], len(historical_rows), result
                    )
                    if result.get("has_anomaly"):
                        patient_has_anomaly = True
                        patient_anomaly_attrs.update(result.get("anomaly_attrs", []))

                conn.commit()

                if patient_has_anomaly:
                    anomalies.append({
                        "patient_id": patient_id,
                        "patient_name": patient_name,
                        "doctor_id": doctor_id,
                        "doctor_name": doctor_name,
                        "record_date": today_record["record_date"].isoformat(),
                        "anomaly_attrs": sorted(patient_anomaly_attrs),
                    })

            except Exception:
                conn.rollback()
                logger.exception("[rollup] 환자 #%s(%s) 처리 실패 — 건너뜀", patient_id, patient_name)
                continue

        logger.info("[rollup] 완료 — 이상치 감지 %d명", len(anomalies))
        return anomalies

    finally:
        conn.close()


def _send_slack_alert(**context) -> None:
    ti = context["ti"]
    anomalies = ti.xcom_pull(task_ids="rollup_all_patients") or []
    text = build_message(anomalies)
    send_slack(text)


default_args = {
    "owner": "capd",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_metrics_rollup",
    description="전체 활성 환자 일일 분석 롤업(Silver/Gold 캐시 선제 upsert) + 이상치 Slack 알림",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["capd", "analytics", "rollup"],
) as dag:

    rollup_task = PythonOperator(
        task_id="rollup_all_patients",
        python_callable=_rollup_all_patients,
    )

    alert_task = PythonOperator(
        task_id="send_slack_alert",
        python_callable=_send_slack_alert,
    )

    rollup_task >> alert_task
