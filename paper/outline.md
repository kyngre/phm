# PHM Korea 논문 초안 — Outline

## 제목 (가안)
**Apache Iceberg 기반 항공기 엔진 PHM 데이터 레이크하우스: 재현 가능한 RUL 예측 MLOps 아키텍처**

## Abstract (200~250 단어)
- 배경: PHM 연구는 모델 정확도에 집중, 운영·재현성 측면 공백
- 제안: Iceberg + 메달리온 아키텍처로 데이터 레이크하우스 ↔ 모델 서빙 ↔ 운영 가시성 통합
- 검증: NASA C-MAPSS FD001~FD004로 reference 구현
- 기여: ① 재현 가능 MLOps 레퍼런스 ② 백필 프로토콜 ③ 데이터/모델 드리프트 분리 모니터링 ④ 운영조건 기반 파티션 전략

## 1. 서론
- PHM·CBM 산업 동향, 운영 데이터 거버넌스 문제
- 본 연구의 목표 및 기여

## 2. 관련 연구
- C-MAPSS 기반 RUL 예측 모델 (LSTM, CNN, Transformer)
- 데이터 레이크하우스(Iceberg, Delta, Hudi) 비교
- PHM MLOps 사례

## 3. 시스템 아키텍처
- 3.1 메달리온 3계층 설계
- 3.2 Iceberg 활용 포인트 (time-travel, MERGE, schema evolution, OCC)
- 3.3 운영조건 클러스터링 기반 파티션 전략

## 4. RUL 예측 모델
- 4.1 피처 엔지니어링 (rolling, HI)
- 4.2 baseline (LSTM/1D-CNN)
- 4.3 학습/평가 프로토콜 — Iceberg time-travel로 학습 데이터 고정

## 5. 운영 가시성
- 5.1 헬스 쿼리 8선
- 5.2 대시보드 (비즈니스 + 운영 탭)
- 5.3 데이터/모델 드리프트 분리 모니터링

## 6. 실험
- 6.1 데이터셋: C-MAPSS FD001~FD004
- 6.2 모델 성능: MAE / PHM08 Score
- 6.3 운영 메트릭: 컴팩션 전후 파일 수·쿼리 시간, 백필 일관성

## 7. 논의
- 한계 (시뮬레이션 데이터, 단일 노드 검증)
- 100x 스케일 아웃 설계 시사점

## 8. 결론

## 참고문헌
- Saxena et al., PHM08
- Iceberg paper (Ryan Blue)
- 관련 RUL 논문 5~10편

---

## 제출 일정 (가안)
- Outline 확정: 2026-05-12
- 1차 draft: 2026-06-09
- 실험 결과 통합: 2026-06-23
- 최종 제출: 학회 일정에 맞춰 조정
