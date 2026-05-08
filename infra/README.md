# Infra — 로컬 Docker 환경

Spark + MinIO(S3) + **Iceberg REST Catalog** + Trino + Kafka + Airflow + Superset 스택으로
Iceberg 레이크하우스를 로컬에서 재현합니다.

## 구성 요소
| 서비스 | 역할 | 이미지 |
|---|---|---|
| **MinIO** | S3 호환 오브젝트 스토리지 (Iceberg warehouse) | `minio/minio` |
| **Iceberg REST Catalog** | 메타 카탈로그 (arm64 네이티브) | `tabulario/iceberg-rest:1.6.0` |
| **Spark** | 적재·정제·MERGE·컴팩션 실행 (커스텀 이미지, JAR 사전 포함) | `phm/spark:3.5.3-iceberg` |
| **Trino** | Iceberg SQL 쿼리 (BI 백엔드, Athena 대체) | `trinodb/trino:455` |
| **Kafka** | 센서 스트림 시뮬레이션 (KRaft mode) | `apache/kafka:3.8.0` |
| **Airflow** | DAG 오케스트레이션 (standalone, 커스텀 이미지에 docker CLI 포함) | `phm/airflow:2.10.3-docker-cli` |
| **Superset** | BI 대시보드 | `apache/superset:4.1.1` |

## 사전 빌드 (커스텀 이미지)
- **Spark** — Iceberg / hadoop-aws / aws-sdk(v1+v2) / kafka 의존 JAR 9개를 `/opt/spark/jars`에 사전 배치 → `spark-submit --packages` 매번 다운로드 비용 제거 + Ivy 권한 이슈 회피.
- **Airflow** — `docker-ce-cli` 사전 설치 → BashOperator 가 `docker exec phm-spark/phm-trino` 호출 가능. 매 기동마다 apt-get 재실행 회피.

```bash
cd infra
docker compose build              # spark / airflow 동시 빌드 (1회만)
```

## 자격증명 — `.env` 분리

자격증명·시크릿은 [infra/.env.example](.env.example) 을 복사해 `infra/.env` 로 두고 compose 가 자동 로드한다 ([.gitignore](../.gitignore) 대상).

```bash
cp infra/.env.example infra/.env
# 필요 시 값 수정 — 운영 자격증명은 절대 commit 하지 않는다.
```

| 변수 | 사용처 |
|---|---|
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_REGION` | MinIO root + Iceberg/Spark/Trino 의 S3 클라이언트 자격증명 |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Airflow standalone 초기 admin |
| `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` / `SUPERSET_SECRET_KEY` | Superset admin + 세션·CSRF 서명 키 |

**전파 방식**:
- compose 의 `${VAR:-default}` 가 `.env` 또는 OS env 에서 읽어 컨테이너 환경변수로 주입.
- [conf/spark/spark-defaults.conf](conf/spark/spark-defaults.conf) 는 자격증명을 명시하지 않음 → AWS SDK v1/v2 의 `EnvironmentVariableCredentialsProvider` 가 컨테이너 env 에서 자동 resolve.
- [conf/trino/catalog/iceberg.properties](conf/trino/catalog/iceberg.properties) 는 Trino 의 `${ENV:VAR}` 치환으로 컨테이너 env 를 참조.

`.env` 미생성 시에도 모든 변수에 `default` 가 있어 `docker compose up -d` 는 그대로 동작 (현 admin/admin12345 동작 보존).

## 기동
```bash
docker compose up -d
docker compose ps                 # 모든 컨테이너 Up 확인
docker compose logs iceberg-rest | tail   # "Started @..." 확인
```

## 접속
| 서비스 | URL | 계정 |
|---|---|---|
| MinIO Console | http://localhost:9001 | admin / admin12345 |
| Spark Driver UI | http://localhost:4040 | job 실행 중일 때만 활성화 |
| Trino UI | http://localhost:8080 | admin / - |
| Iceberg REST | http://localhost:8181 | — |
| Superset | http://localhost:8088 | admin / admin |
| Kafka (외부) | localhost:9094 | — |

> Spark 는 `local[*]` 모드로만 동작 (worker 없음). Standalone Master UI(8080) 는 의미 없는 idle JVM 이라 호스트 노출 생략. 실행 중 job 의 lineage/stage 는 4040 Driver UI 에서 확인.

## 동작 확인
```bash
# Spark에서 Iceberg 네임스페이스/테이블
docker exec -i phm-spark /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=phm \
  -e "SHOW NAMESPACES; SHOW TABLES IN phm.bronze;"

# Trino에서 동일하게 보이는지
docker exec -it phm-trino trino --execute "SHOW SCHEMAS FROM iceberg"
docker exec -it phm-trino trino --execute "SHOW TABLES FROM iceberg.bronze"
```

## 종료
```bash
docker compose down           # 컨테이너만
docker compose down -v        # 볼륨까지 (전체 초기화)
```

## 카탈로그 설계 — REST를 선택한 이유
- **arm64 네이티브** — Apple Silicon에서 Hive 4.0.0의 emulation 불안정 회피
- **단일 컨테이너** — Postgres + Hive Metastore 의존 제거
- **Spark·Trino 동일 인터페이스** — 둘 다 `catalog.type=rest`로 동일 코드 경로
- **AWS 환경 매핑 용이** — AWS Glue Iceberg REST endpoint, Tabular, Polaris 등으로 그대로 치환

### 메타 영속화
`tabulario/iceberg-rest:1.6.0` 의 default `CATALOG_URI` 는 in-memory SQLite (`jdbc:sqlite:file:/tmp/...mode=memory`) — 컨테이너 재기동 시 카탈로그 내용 전체 소실 + S3(MinIO) 의 데이터 파일과 inconsistent 한 상태가 됨.

이를 막기 위해 `CATALOG_URI` 를 file 기반으로 override 하고 named volume 에 저장:

```yaml
CATALOG_URI: jdbc:sqlite:file:/var/lib/iceberg-rest/catalog.db
volumes:
  - iceberg-rest-data:/var/lib/iceberg-rest
```

`docker compose down` 만으로는 카탈로그가 보존되며, 의도적 초기화 시 `down -v` 로 minio-data 와 함께 삭제.

AWS 환경에서는 Glue/Tabular/Polaris 가 영속을 보장하므로 이 처리는 로컬 한정.

## AWS 매핑
| 로컬 | AWS |
|---|---|
| MinIO | S3 |
| Iceberg REST | Glue Iceberg REST / Tabular / Polaris |
| Trino | Athena |
| Spark (compose) | EMR / EKS Spark |
| Kafka (compose) | MSK |
| Airflow (compose) | MWAA |
| Superset | QuickSight / Superset on ECS |

## 트러블슈팅
- **`UnknownHostException: <service>`** — 컨테이너가 같은 네트워크(`phm`)에 있는지 확인. `docker compose ps`로 모두 Up인지 점검.
- **Spark 첫 기동 시 Ivy 에러** — 커스텀 이미지를 빌드하지 않은 경우. `docker compose build spark` 후 재기동.
- **Kafka 이미지 못 찾음** — `bitnami/kafka`는 무료 태그가 사라졌음. `apache/kafka:3.8.0` 사용 (현재 적용됨).
- **`SdkClientException: Unable to load region`** — Iceberg S3FileIO(AWS SDK v2)가 region을 못 찾음. `spark` 서비스에 `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` 환경변수가 세팅되어 있는지 확인 (`docker compose up -d spark` 으로 재기동).
