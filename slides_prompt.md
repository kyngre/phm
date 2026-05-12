# Claude PPT 생성 프롬프트 — 항공기 엔진 PHM 데이터 레이크하우스 (10~15분 발표)

> 아래 프롬프트를 Claude(Code/Desktop/Web)에게 그대로 주면 됨. `README.md`, `project_guide.md`, `CLAUDE.md` 가 같은 워크스페이스에 있다고 가정.

---

## (Claude에게 줄 프롬프트 — 여기서부터 복사)

당신은 **시니어 데이터 엔지니어 + PHM 도메인 전문가** 입니다. 아래 레포(`/Users/kkr/final_project`)를 분석해서 **10~15분 발표용 PPT** 를 만들어 주세요.

### 0. 우선 읽을 파일 (순서대로, 전부 정독)
1. `project_guide.md` — **평가 기준 4가지** (① 운영 가시성 ② 100x 스케일 사고력 ③ Iceberg 필요성 ④ 협업·지속가능성) + **필수 구현 요건 4개** (Iceberg / 메달리온 / 매니지먼트 자동화 / 대시보드). **이 8개 항목이 슬라이드에 모두 명시적으로 매핑되어야 함.**
2. `README.md` — 도메인·아키텍처·테이블 설계·Iceberg 가치 주장(§4-1~4-3)·헬스/DQ·100x 시나리오·장애 대응·멱등성 — **사실관계는 100% 이 문서를 출처로**. 숫자/표/명령은 README 인용.
3. `CLAUDE.md` — 함정 / 컨벤션 / 의사결정 기록. 발표 톤(짧고 구체) + 모델 성능 표는 여기서 확인.
4. `code/pipelines/`, `orchestration/dags/`, `code/health-queries/`, `code/ddl/`, `code/maintenance/` — 슬라이드 인용 시 파일명 명시.

> **금지**: 위 문서에 없는 수치/주장 만들어내지 말 것. 모호하면 "README §X 참조" 라고 출처 표기.

### 1. 발표 컨텍스트
- **시간**: 10~15분 → 본문 슬라이드 **14~18장 권장** (제목·Q&A 제외). 슬라이드 1장 ≈ 40~60초로 가정.
- **청중**: 데이터 엔지니어링 스터디 동료 + 멘토. 기술 배경은 있지만 *이 레포는 처음 보는 사람*. PHM 도메인 지식 없음 가정.
- **목표**: ① 평가 기준 4가지를 채점자가 한눈에 체크 가능하게 만들 것 ② "왜 Iceberg 인가" 가 가장 강한 단일 메시지 ③ 시연 영상/스크린샷 자리는 placeholder 로 비워둘 것 (직접 채움).
- **언어**: 한국어. 코드/명령은 영어 그대로.

### 2. 슬라이드 구성안 (이 흐름을 따르되, 필요시 ±2장 자유롭게 조정)

| # | 슬라이드 | 핵심 메시지 | 출처 |
|---|---|---|---|
| 1 | **Title** | 프로젝트명 + 한 줄 요약 + 발표자/일자 | README §0 |
| 2 | **문제 정의 & 도메인** | 항공 터보팬 엔진 fleet RUL 예측 — 왜 lakehouse 가 필요한가 (운영 + 학술 동시 목표) | README §1 |
| 3 | **데이터 — NASA C-MAPSS 3종 파일** | train / test / RUL_FDxxx 역할 + 4 dataset 난이도 표 + 시간 합성 공식 (`base + jitter×unit + interval×(cycle-1)`) | README §0-1, §0-2 |
| 4 | **전체 아키텍처 다이어그램** | Kafka → Spark Streaming → Iceberg (Bronze/Silver/Gold) → Airflow / Trino / Superset. **AWS 매핑 1줄** 포함 (MinIO↔S3, REST↔Glue, Trino↔Athena, Spark↔EMR). | README §2 |
| 5 | **메달리온 — Bronze/Silver/Gold 의사결정** | 한 슬라이드 3열 표. 각 층의 **파티션 / 시간 컬럼 / MERGE 키 / 변환 내용** 핵심만. Bronze=`days(ingest_ts)` vs Silver=`days(event_ts)` 의 *왜* 강조 | README §3 |
| 6 | **★ 왜 Iceberg 인가 — 한 줄 요약 + 3축** | 한 줄 요약: "Parquet+Glue 였다면 4~5개 별도 시스템으로 분기됐을 능력이 한 카탈로그 안에 흡수". 3축(snapshot lineage / ACID MERGE / open table + 빌트인 유지보수) 카드 3장 | README §4 |
| 7 | **Iceberg 가치 — 매핑 표** | "능력 ↔ Parquet+Glue 부담 ↔ Iceberg 흡수" 8행 표 (그대로 인용) | README §4 끝 표 |
| 8 | **운영 가시성 — 헬스 쿼리 8개 + DQ 9 rule** | 두 축 분리 (5-1 인프라 시그널 / 5-2 데이터 시그널). 같은 대시보드에 섞으면 알람 노이즈 — *왜 분리했나* 1줄. | README §5-1, §5-2 |
| 9 | **Iceberg 유지보수 자동화** | `rewrite_data_files` / `expire_snapshots` / `remove_orphan_files` 3종 Airflow DAG. **expire 정책 100일 = 학습 윈도우 90d + 마진 10d** 라는 *의사결정* 강조. 회귀 테스트 (`tests/dags/test_backfill_safety.py`) 도 1줄. | README §8, `code/maintenance/` |
| 10 | **ML — 학습/추론 분리 + NASA 표준 평가** | 이전 설계(매 run train+predict = 데이터 누수) → `gold_train_dag` (주1) + `gold_rul_predict_dag` (시간) 분리. **`eval_split = train/holdout/nasa_test` 3종**. `model_metrics` 한 테이블에서 GBT(v0) vs LSTM(v1) SQL 한 줄 비교 가능. | README §3-3, CLAUDE.md 모델 표 |
| 11 | **모델 결과 표** | FD001~FD004 × {GBT v0 MAE, LSTM v1 MAE} 4×2 표. NASA 표준 평가 가능 (외부 비교 OK) 한 줄. | CLAUDE.md "모델 / 평가 현황" |
| 12 | **대시보드** | Superset 비즈니스 탭 / 운영 탭 분리. 스크린샷 placeholder 2장. `dashboard_export_*.zip` 보존 정책. | README §6 |
| 13 | **장애 시나리오 — 3가지** | ① Streaming OOM 복구(checkpoint+Silver dedup 흡수) ② 90일 백필(태그→MERGE→롤백) ③ 컴팩션 vs MERGE OCC 충돌(시간대 분리, partial-progress). 각 1줄 + 코드 위치. | README §8 |
| 14 | **★ 100x 스케일 사고력** | 깨지는 지점 5개 × 대응 — 표로. 일 100만→1억 이벤트 가정 명시. | README §7 |
| 15 | **협업·지속가능성** | ① 멱등성 원칙(Bronze append + Silver dedup, MERGE 키 명시) ② 테스트 (`tests/integration/test_silver_incremental.py` 등) ③ DDL/DAG/health-query 파일 단위 분리 ④ CLAUDE.md 함정 기록으로 신규 팀원 온보딩. | README §9, CLAUDE.md |
| 16 | **평가 기준 4가지 자가 체크** | project_guide §3 ①~④ 각각에 **이 발표의 몇번 슬라이드** 가 답하는지 매핑한 표. 채점자가 한눈에 보게. | project_guide §3 |
| 17 | **Closing — 한 줄 요약 & 향후** | "운영 가시성과 PHM 학술 기여를 한 카탈로그 위에서 동시 시연한 레퍼런스 아키텍처". PHM Korea 학회 outline (`paper/outline.md`) 언급. | README §10 |
| 18 | **Q&A** (선택) | — | — |

### 3. 슬라이드 디자인 규칙
- **1 slide = 1 message**. 슬라이드 제목이 곧 결론 문장이 되도록 (예: "Bronze 는 append-only, Silver 가 dedup 을 흡수한다" — NOT "Bronze 계층").
- **불릿은 최대 5개, 한 줄 ≤ 60자**. 긴 설명은 발표 노트(speaker notes)에.
- **표는 슬라이드당 1개**. README 의 표를 그대로 가져오되 폰트 ≥ 18pt.
- **시각 요소**: 아키텍처 다이어그램(슬라이드 4)은 README §2 의 ASCII 를 깔끔한 박스 다이어그램으로 재구성. 메달리온(슬라이드 5)은 좌→우 화살표 3박스. Iceberg 3축(슬라이드 6)은 3 카드 그리드.
- **색상**: 다크 톤 X. 흰 배경 + 강조색 1개(엔진/항공 도메인이라 진청색/티타늄 그레이 권장). 데이터 엔지니어링 발표 톤.
- **코드 인용 시**: 파일명만 작게 footer 처럼 (예: `code/pipelines/silver_transform.py:dedup_bronze`). 함수 본문은 붙이지 말 것 — placeholder.
- **이모지/장식 금지**.

### 4. 강조해야 할 단일 메시지 (전체 발표에서 반복적으로 회귀)
> **"이 프로젝트의 Iceberg 활용 가치는 신기능 사용이 아니라, Parquet+Glue 였다면 분기됐을 4~5개 책임을 한 카탈로그가 흡수해 운영 표면적이 줄었다는 것."**

이 문장이 슬라이드 6, 7, 15, 17 에서 다른 각도로 변주되어야 함.

### 5. 발표 노트 (speaker notes)
각 슬라이드마다 **발표자가 읽을 30~60초 분량** 의 한국어 노트를 함께 작성. 슬라이드 본문은 키워드, 노트는 완성된 문장. 노트에는 다음을 포함:
- 이 슬라이드에서 *왜* 이 결정을 했는지 (의사결정 기록)
- 청중이 던질 만한 예상 질문 1개 (있으면)
- 다음 슬라이드로의 자연스러운 연결 문장

### 6. 출력 형식 (택 1, 우선순위 순)
1. **PPTX 파일 직접 생성** — `python-pptx` 로 `slides.pptx` 빌드 스크립트(`build_slides.py`) 작성 후 실행. 발표 노트는 각 슬라이드 notes 슬롯에 채움.
2. (1번이 환경상 불가하면) **Marp 마크다운** (`slides.md`) — `---` 로 페이지 구분, `<!-- _notes -->` 로 노트.
3. (둘 다 안되면) **plain Markdown outline** — 슬라이드별 헤더 + 본문 + `> NOTE:` 노트.

> 어느 형식이든 최종 산출물은 레포 루트의 `slides/` 디렉토리에 저장.

### 7. 작업 절차
1. 위 0번 4개 파일 정독 후, 슬라이드 구성안의 각 항목에 들어갈 **사실/표/숫자/파일경로** 를 README/CLAUDE.md 에서 *직접 인용 가능한 형태로* 모두 추출. (이 단계 결과를 짧게 사용자에게 보여주고 진행 확인 받을 것.)
2. 추출이 끝나면 출력 형식 선택 후 슬라이드 생성.
3. 마지막에 **자가 검증 체크리스트** 출력:
   - [ ] project_guide §3 평가 4기준이 모두 1개 이상 슬라이드에서 다뤄짐
   - [ ] 필수 4요건(Iceberg/메달리온/자동화/대시보드)이 명시적으로 보임
   - [ ] 슬라이드 수 14~18, 1장당 ~50초 페이싱
   - [ ] 모든 수치가 README/CLAUDE.md 에서 출처 추적 가능
   - [ ] 발표 노트가 모든 슬라이드에 있음
   - [ ] "Iceberg 가치 한 줄 요약" 이 최소 3장에서 변주됨

### 8. 주의
- `CLAUDE.md` 의 "반복 함정" 8개 중 발표에 직접 안 들어감 (운영 디테일). 단, 슬라이드 13(장애 시나리오)에서 1번 함정(checkpoint wipe) 만 인용 가능.
- 모델 성능 표는 **CLAUDE.md "모델 / 평가 현황"** 의 4×2 표 그대로 (README 에는 없음).
- 스크린샷은 만들어내지 말 것 — placeholder `[스크린샷 자리: Superset 비즈니스 탭]` 형태로.
- 시간이 부족하면 슬라이드 11(모델 결과 표) 를 10(ML 분리) 에 흡수, 또는 슬라이드 15(협업) 를 16(평가 기준 체크) 에 흡수 — 우선순위 낮은 것부터 합치기.

준비되면 시작하세요.

---

## (프롬프트 끝)
