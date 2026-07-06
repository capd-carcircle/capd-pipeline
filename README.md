# capd-pipeline

CAPD의 배치(batch) 레이어 — 로컬 Airflow(docker-compose)로 매일 전체 환자를
선제적으로 스캔해 분석 캐시를 채우고, 이상치가 있으면 Slack으로 알린다.

## 왜 필요한가

`capd-backend`의 `GET /analytics/patients/{id}` 엔드포인트는 의사가 특정 환자의
분석 리포트를 **직접 열었을 때만** 계산되는 온디맨드(speed layer) 경로다. 즉 의사가
한 번도 안 열어본 환자는 이상치가 있어도 아무도 모른다. 이 레포는 그 반대 축 —
매일 **전체 환자**를 자동으로 스캔해서:

1. `patient_daily_metrics`(Silver)·`patient_daily_analytics`(Gold) 캐시를 선제적으로
   채워 넣고(의사가 리포트를 열면 바로 캐시 적중),
2. 이상치(`has_anomaly=true`)가 있으면 Slack으로 알린다(의사가 안 열어봐도 인지 가능).

Airflow가 꺼져 있어도 배포된 사이트(`capd-backend`)는 100% 정상 작동한다 — 이
배치는 순전히 부가 기능이다.

## 스코프 (2026-07-06 확정)

- DAG **1개**만 존재: `daily_metrics_rollup`.
- 기존에 검토됐던 `medlineplus_ingest`·`test_data_backfill` DAG는 **포함하지 않음**
  — 이미 GCP Cloud Scheduler + Cloud Run Job으로 정상 운영 중인 것을 중복
  구현하는 것이라 실익이 없다고 판단(CLAUDE.md 참고).
- 새 DB 마이그레이션 없음 — `patient_daily_metrics`/`patient_daily_analytics`
  테이블과 `idx_pda_has_anomaly`(이상치 스캔용 부분 인덱스)는 이미 2단계에서
  생성돼 있음(`backend/scripts/migrate_add_analytics_tables.py`).

## 구성

```
capd-pipeline/
├── docker-compose.yml       # 단일 컨테이너 Airflow standalone
├── .env.example             # CAPD_DB_URL / SLACK_WEBHOOK_URL
├── requirements.txt         # dags/ 코드를 컨테이너 밖에서 테스트할 때만 필요
└── dags/
    ├── analytics_engine.py  # backend/app/services/analytics_engine.py 벤더링 사본
    ├── db.py                # psycopg2 raw SQL — 환자 조회/기록 조회/Silver·Gold upsert
    ├── slack_alert.py       # Incoming Webhook 전송 (표준 라이브러리만 사용)
    └── daily_metrics_rollup.py  # DAG 본체 (rollup_all_patients → send_slack_alert)
```

## 로컬 실행

1. `.env.example`을 `.env`로 복사하고 값 채우기:
   - `CAPD_DB_URL` — Supabase **Session pooler** 연결 문자열(direct host는 IPv6
     전용이라 실패한 전례 있음). 배포된 backend와 같은 DB를 가리켜야 함.
   - `SLACK_WEBHOOK_URL` — 없으면 비워둬도 됨(로그로만 알림 대체).
2. `docker compose up -d`
3. 몇 분 뒤 http://localhost:8080 접속(최초엔 admin 비밀번호가 로그 또는
   컨테이너 내부 `/opt/airflow/simple_auth_manager_passwords.json.generated`
   — Airflow 버전에 따라 `standalone_admin_password.txt`)에 생성됨.
4. DAG 목록에서 `daily_metrics_rollup`을 찾아 Unpause 후 수동 Trigger로 테스트,
   또는 스케줄(`@daily`, docker-compose가 켜져 있는 동안만 실행됨) 대기.

## 벤더링 관련 주의

`dags/analytics_engine.py`는 `backend/app/services/analytics_engine.py`(그 원본은
`ai/tools/data_engineering.py`+`ai/tools/analytics.py`)를 그대로 복사한 것 —
이 프로젝트는 공유 패키지 레포를 만들지 않기로 결정했기 때문에(레포 6개 대비
~450줄 중복 감수가 더 낫다는 판단, CLAUDE.md 참고) 파일 자체를 복사해 옴.
원본이 바뀌면 이 파일도 반드시 같이 맞출 것. 아직 이 사본을 포함하는 자동
정합성 테스트는 없음(`backend/tests/test_ai_parity.py`는 ai↔backend만 비교) —
필요하면 그 테스트를 확장해 이 파일도 포함시킬 수 있음.

## 알려진 한계

- 이 Cowork 세션 샌드박스는 docker/sudo가 없어 실제 `docker compose up`으로
  Airflow가 정상 기동하는지, DAG가 실제 DB에 붙어 끝까지 도는지는 이 세션에서
  검증하지 못함 — 차원 로컬 PC에서 최초 1회 실행 확인 필요.
- 재실행(수동 Trigger 반복 등)하면 같은 날 같은 이상치에 대해 Slack 알림이
  중복될 수 있음. 정상 스케줄(하루 1회)에서는 문제 없음 — 중복 방지가 필요해지면
  `patient_daily_analytics`에 `alerted_at` 컬럼을 추가하는 마이그레이션으로 해결 가능.
