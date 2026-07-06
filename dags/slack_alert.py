"""
slack_alert.py — 이상치 선제 알림용 Slack Incoming Webhook 전송.

표준 라이브러리(urllib)만 사용 — requests 등 추가 의존성 없음.
SLACK_WEBHOOK_URL이 비어 있으면(.env 미설정) 전송을 건너뛰고 로그만 남김
(개발 중이거나 아직 webhook을 안 만든 상태에서도 DAG 자체는 실패하지 않게).
"""
import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)


def build_message(anomalies: list[dict]) -> str:
    """이상치 있는 환자 목록 -> Slack 메시지 텍스트 (mrkdwn)."""
    if not anomalies:
        return "✅ CAPD 일일 롤업 완료 — 오늘 이상치 있는 환자 없음."

    lines = [f"🔴 CAPD 일일 롤업 — 이상치 감지 환자 {len(anomalies)}명"]
    for a in anomalies:
        attrs = ", ".join(a["anomaly_attrs"])
        lines.append(
            f"• *{a['patient_name']}* (환자 #{a['patient_id']}) — {a['record_date']} — {attrs}"
        )
    return "\n".join(lines)


def send_slack(text: str) -> None:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        logger.info("[slack_alert] SLACK_WEBHOOK_URL 미설정 — 전송 생략. 메시지:\n%s", text)
        return

    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            if resp.status != 200 or body != "ok":
                logger.warning("[slack_alert] 예상 밖 응답: status=%s body=%s", resp.status, body)
    except Exception as e:
        # Slack 실패가 DAG 전체를 실패시키면 안 됨 (부가 기능) — 로그만 남김.
        logger.error("[slack_alert] 전송 실패: %s", e)
