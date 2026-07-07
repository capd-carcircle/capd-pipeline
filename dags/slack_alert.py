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

# ANALYTICS_ENGINE.ANOMALY_ATTRS(analytics_engine.py 400번째 줄 부근) 7개와 1:1 동일 —
# frontend PatientAnalyticsPage.tsx의 ATTR_LABEL과 라벨 텍스트를 맞춰서 화면·알림 표기가 어긋나지 않게 함.
ATTR_LABEL_KO: dict[str, str] = {
    "body_weight_kg":         "체중",
    "calculated_uf_sum_g":    "제수량 합",
    "systolic_bp":            "수축기 혈압",
    "diastolic_bp":           "이완기 혈압",
    "mean_arterial_pressure": "평균 동맥압",
    "fasting_blood_sugar":    "공복 혈당",
    "infused_sum_g":          "주입량 합",
}


def _attr_label(attr: str) -> str:
    """컬럼명만으로는 뭘 뜻하는지 안 와닿아서, 매핑에 있으면 한국어 의미를 병기."""
    ko = ATTR_LABEL_KO.get(attr)
    return f"{ko}({attr})" if ko else attr


_UNASSIGNED_LABEL = "담당의 미배정"


def build_message(anomalies: list[dict]) -> str:
    """이상치 있는 환자 목록 -> Slack 메시지 텍스트 (mrkdwn).

    담당의별로 그룹핑해서 표시 — 의사 입장에서 "내 담당 환자 중 누가 이상치인지"를
    한눈에 보게 하려는 목적(차원 요청, 2026-07-07). 각 환자 항목의 doctor_name은
    db.fetch_active_patients()가 users.doctor_id로 조인해서 채워준 값 — 담당의가
    없으면 None이라 _UNASSIGNED_LABEL 그룹으로 묶임.
    """
    if not anomalies:
        return "✅ CAPD 일일 롤업 완료 — 오늘 이상치 있는 환자 없음."

    groups: dict[str, list[dict]] = {}
    for a in anomalies:
        key = a.get("doctor_name") or _UNASSIGNED_LABEL
        groups.setdefault(key, []).append(a)

    # 담당의 미배정 그룹은 맨 뒤로, 나머지는 이름 가나다순 — 매번 순서가 안 흔들리게.
    doctor_names = sorted(k for k in groups if k != _UNASSIGNED_LABEL)
    if _UNASSIGNED_LABEL in groups:
        doctor_names.append(_UNASSIGNED_LABEL)

    lines = [f"🔴 CAPD 일일 롤업 — 이상치 감지 환자 {len(anomalies)}명"]
    for doctor_name in doctor_names:
        label = doctor_name if doctor_name == _UNASSIGNED_LABEL else f"{doctor_name} 선생님"
        lines.append(f"\n*{label}*")
        for a in groups[doctor_name]:
            attrs = ", ".join(_attr_label(attr) for attr in a["anomaly_attrs"])
            lines.append(
                f"• {a['patient_name']} (환자 #{a['patient_id']}) — {a['record_date']} — {attrs}"
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
