#!/usr/bin/env bash
# Superset 초기 설정 — Trino DB 등록을 API 로 자동화.
#
# 전제: docker compose up -d superset 으로 phm-superset 가 떠있고, admin/admin 로그인 가능.
# 결과: "Trino-Iceberg" 라는 이름의 DB connection 이 생성되어 SQL Lab/Dataset 에서 사용 가능.
#
# 차트/대시보드 자체는 UI 에서 만든 뒤 `dashboard_export_biz.zip` / `dashboard_export_ops.zip` 으로 export 해 이 디렉토리에 보관.
set -euo pipefail

SUPERSET_URL="${SUPERSET_URL:-http://localhost:8088}"
USER="${SUPERSET_USER:-admin}"
PASS="${SUPERSET_PASS:-admin}"

# Trino 접속 URI — Superset 컨테이너에서 보는 호스트명 (docker network 내부에서는 'trino').
SQLALCHEMY_URI="${TRINO_URI:-trino://admin@trino:8080/iceberg}"

# Superset 의 CSRF 검증은 동일 cookie session 안에서 발급된 토큰만 인정.
# → curl 의 cookie jar 로 login → csrf_token → POST 를 한 흐름으로 묶는다.
COOKIE_JAR="$(mktemp)"
trap "rm -f $COOKIE_JAR" EXIT

echo "▶ 1) login — access token + 세션 cookie 획득"
TOKEN=$(curl -s -c "$COOKIE_JAR" -X POST "$SUPERSET_URL/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$USER\",\"password\":\"$PASS\",\"provider\":\"db\",\"refresh\":true}" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

echo "▶ 2) CSRF 토큰 획득 (같은 cookie 세션)"
CSRF=$(curl -s -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
  "$SUPERSET_URL/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['result'])")

echo "▶ 3) DB connection 'Trino-Iceberg' 생성 (멱등 — 이미 있으면 skip)"
EXISTS=$(curl -s -b "$COOKIE_JAR" \
  "$SUPERSET_URL/api/v1/database/?q=(filters:!((col:database_name,opr:eq,value:Trino-Iceberg)))" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('count', 0))")

if [ "$EXISTS" = "0" ]; then
  curl -s -b "$COOKIE_JAR" -X POST "$SUPERSET_URL/api/v1/database/" \
    -H "Authorization: Bearer $TOKEN" \
    -H "X-CSRFToken: $CSRF" \
    -H "Referer: $SUPERSET_URL/" \
    -H "Content-Type: application/json" \
    -d "{
      \"database_name\": \"Trino-Iceberg\",
      \"sqlalchemy_uri\": \"$SQLALCHEMY_URI\",
      \"expose_in_sqllab\": true,
      \"allow_ctas\": false,
      \"allow_cvas\": false,
      \"allow_dml\": false
    }" | python3 -m json.tool
else
  echo "  → 이미 등록됨 (count=$EXISTS), skip"
fi

echo "✓ 완료. http://localhost:8088 → SQL Lab → Trino-Iceberg 선택"
