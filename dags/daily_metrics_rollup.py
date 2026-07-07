"""
daily_metrics_rollup DAG

"온디맨드가 못 하는 일" 담당 — 의사가 특정 환자의 분석 리포트를 열어야만 채워지던
patient_daily_metrics(Silver)/patient_daily_analytics(Gold) 캐시를, 매일 전체 활성
환자에 대해 선제적으로 채워 넣는다. 이상치가 있으면 의사가 리포트를 열어보지 않아도
Slack으로 알림.

흐름 (task 2개):
  1. rollup_all_patients — 활성 환자 전체 순회 → 최근 window(=30)+1건 기록 조회 →
     Daily Model Row 생성 → run_all_tasks 실행 → Silver/Gold upsert →
     이상치(has_anomaly) 있는 환자 목록을 XCom으로 다음 task에 전달.
  2. send_slack_alert — 위 목록으로 Slack 메시지 구성·전송(웹훅 미설정 시 로그만).

이 DAG는 이 레포(capd-pipeline)의 로컬 docker-compose Airflow에서만 실행된다.
배포된 사이트(capd-backend)는 이 DAG와 무관하게 온디맨드로 계속 정상 작동한다
(Airflow가 꺼져 있어도 서비스 영향 없음 — speed layer와 batch layer 분리).

⚠️ 환자별 계산 실패는 개별 try/except로 격리 — 한 환자의 예외가 전체 배치를
중단시키지 않음. 실패한 환자는 로그로만 남기고 다음 환자로 계속 진행.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import db
from analytics_engine import build_daily_model_row, run_all_tasks
from slack_alert import build_message, send_slack

logger = logging.getLogger(__name__)

WINDOW = 30  # 온디맨드 엔드포인트 기본값과 동일


def _rollup_all_patients(**context) -> list[dict]:
    conn = db.get_conn()
    anomalies: list[dict] = []

    try:
        patients = db.fetch_active_patients(conn)
        logger.info("[rollup] 활성 환자 %d명 스캔 시작", len(patients))

        for patient in patients:
            patient_id = patient["id"]
            patient_name = patient["name"]
            try:
                records = db.fetch_recent_records(conn, patient_id, WINDOW + 1)
                if not records:
                    continue

                today_record, *historical_records = records

                today_row = build_daily_model_row(
                    db.record_to_daily_data(today_record),
                    db.exchanges_to_dicts(db.fetch_exchanges(conn, today_record["id"])),
                )
                historical_rows = [
                    build_daily_model_row(
                        db.record_to_daily_data(r),
                        db.exchanges_to_dicts(db.fetch_exchanges(conn, r["id"])),
                    )
                    for r in historical_records
                ]

                result = run_all_tasks(today_row, historical_rows, window=WINDOW)

                db.upsert_metrics(conn, patient_id, today_record["record_date"], today_row)
                db.upsert_analytics(conn, patient_id, today_record["record_date"], len(historical_rows), result)
                conn.commit()

                if result.get("has_anomaly"):
                    anomalies.append({
                        "patient_id": patient_id,
                        "patient_name": patient_name,
                        "record_date": today_record["record_date"].isoformat(),
                        "anomaly_attrs": result.get("anomaly_attrs", []),
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
