# Codex 작업: metrics_taker 과거/실시간 시간 의미 검증 — 1단계

Windows 프로젝트 루트: `F:\binance_trader`.

**이 작업은 READ_ONLY DIAGNOSTIC입니다.** `cmd/metricsalignmentaudit` 진단 도구 실행과 그 진단 도구에 필요한 독립적인 오류 수정 외에는 프로덕션 코드를 수정하지 마세요. Frozen Feature V2/모델/Entry/Risk 정책/레지스트리/분할 경계는 변경 금지. TEST/FINAL HOLDOUT에는 접근 금지. Binance 개인 API 및 주문 API 호출 금지. 실시간 장시간 테스트 금지.

1. 패키지의 `cmd/metricsalignmentaudit/main.go`, `main_test.go`를 현재 저장소에 넣고, 현재 저장소의 `go.mod`, `internal/market/secondbar.go`, `cmd/featurev2build/main.go`, `cmd/metricscanonical/main.go`와 스키마·입력 경로를 대조한다. 표준 Go 테스트 `go test ./cmd/metricsalignmentaudit -count=1`를 실행한다.
2. 우선 각 월 15일 7개(2024-01,04,07,09,10,11,12)의 canonical combined historical metrics와 **Futures** 1초 봉을 비교한다. `go run ./cmd/metricsalignmentaudit` 실행 후 `summary.json`과 `window_comparisons.csv`를 검토한다. 충분한 샘플이 없다면 2024 TRAIN/VALIDATION 범위에서 며칠만 추가한다. 2025 데이터를 읽지 않는다.
3. 특히 역사 통합 `TakerRatio`가 `[T-5m,T)`에 일치하는지 `[T,T+5m)`에 일치하는지 평가하되 `BASE`와 `QUOTE`를 별도 비교하고 `PREV_END`를 대조군으로 사용한다. 손실·누락 1초 바를 자동 보간하거나 매치 오차를 숨기지 않는다. 월별/매매량 상·하위 구간에서 표본 수, median/p90 오차, corr, 근접 일치율을 함께 비교한다.
4. 원본 ZIP 경로 `F:\binance_trader\history_external\raw\binance\futures\um\daily\metrics\BTCUSDT` 중 샘플 날짜의 원본 CSV `sum_taker_long_short_vol_ratio`, 타임스탬프를 읽기 전용으로 검사해 canonical parquet과 일치하는지 별도 검증한다. 원본 2024 데이터가 없으면 누락으로 보고하며 추정하지 않는다.
5. 실시간 코드의 `internal/live/binance/capture.go`는 5개 Metrics raw `timestamp`를 보존하고 `internal/live/runtimefeature/external.go`는 동일 raw timestamp끼리 조인하는 구조를 확인한다. Binance 공식 문서상 Taker `timestamp`는 period START, OI/LongShort `timestamp`는 period END지만, **과거 통합 CSV의 데이터 구간은 아직 미확정**이므로 `taker +300000`을 실시간에 바로 적용하지 않는다.
6. 검증 결과로 시간 정렬 후보와 신뢰수준을 근거 데이터와 함께 보고한다. 기존 frozen 모델과 의미가 완전히 일치하는 경우에만 **별도 다음 작업**으로 live 수집/조인 정규화를 제안한다. 의미가 일치하지 않으면 변경은 보류하고 영향 범위와 대안을 보고한다. `METRICS_STALE` 제한 600000ms와 안전 지연 5000ms 완화 금지.
7. 최종 출력: 진단 JSON·CSV의 절대 경로, 데이터/샘플 수, 각 후보 평가, raw ZIP 교차 검증, 불명확한 점, 실제 코드 수정 여부, 테스트 실행 결과. 주문/holdout/동결 변경 0을 확인한다.

추가 포인트: Taker가 동일 기간을 표현하더라도 API 게시 시각이 다를 수 있다. 데이터의 **period start/end**, **HTTP receive timestamp**, **historical combined timestamp**를 혼동하지 말고 구분한다. 5시간 Demo의 298개 1분 간격 스냅샷은 정확한 개별 5초 결정의 원인별 건수를 대체하지 못한다.
