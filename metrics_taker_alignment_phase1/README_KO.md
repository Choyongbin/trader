# metrics_taker 시간 정렬 감사 — 1단계 (읽기 전용)

이 패키지는 Binance Futures 5분 Metrics에서 `taker`의 시간 정의가 실시간과 과거 학습 데이터에서 일치하는지 조사하는 **오프라인 진단 도구**입니다. 데이터·모델·동결 정책·자동매매 실행 파일은 수정하지 않습니다. 주문이나 개인 API는 사용하지 않습니다.

## 사전 확인

- `F:\binance_trader`가 현재 저장소 루트이고, `go.mod`에 `github.com/parquet-go/parquet-go`가 설치되어 있어야 합니다.
- 다음 기존 데이터 파일이 존재해야 합니다.
  - `data\derived\1s\BTCUSDT\2024\BTCUSDT-1s-2024-01.parquet` 등
  - `data\external\metrics\v1\BTCUSDT\BTCUSDT-metrics-2024-01.parquet` 등
- 원본 2024 Daily ZIP과 Frozen 모델을 수정하지 않습니다. **2025-07 이후 FINAL HOLDOUT을 탐색하지 않습니다.** 이 도구는 날짜 인자로 오직 2024년만 받습니다.

## 설치와 실행

ZIP 안의 `cmd\metricsalignmentaudit` 폴더를 저장소 루트의 `cmd\` 아래에 복사합니다. 같은 이름의 기존 폴더가 있다면 덮어쓰지 마세요.

```powershell
cd F:\binance_trader

go test ./cmd/metricsalignmentaudit -count=1

go run ./cmd/metricsalignmentaudit
```

다른 날짜(2024 TRAIN/VALIDATION만)를 추가하려면:

```powershell
go run ./cmd/metricsalignmentaudit `
    -days "2024-01-15,2024-04-15,2024-07-15,2024-09-15,2024-10-15,2024-11-15,2024-12-15"
```

**기본 설정**은 2024년 1·4·7·9월 TRAIN, 10·11·12월 VALIDATION의 각 15일을 샘플링합니다. 각 월 Parquet을 읽지만 실제 비교에는 각 날짜의 하루만 사용합니다. 계정 연결이나 실시간 데이터 수집은 하지 않습니다.

## 무엇을 검증하는가

과거 통합 Metrics 행의 timestamp `T`와 `taker_long_short_volume_ratio`를 아래 세 후보의 Futures 1초 봉으로 독립적으로 재계산해 비교합니다.

- `END`: `[T-5분, T)` — 과거 결합 CSV의 T가 **해당 구간 종료 시각**이라는 가설.
- `START`: `[T, T+5분)` — 과거 결합 CSV의 T가 **해당 구간 시작 시각**이라는 가설.
- `PREV_END`: `[T-10분, T-5분)` — 5분 더 이전 구간과 잘못 결합된 경우를 가리는 대조군.

각 후보마다 BTC **Base Volume** 비율과 USDT **Quote Volume** 비율(`Buy/Sell`)을 따로 비교합니다. 5분 구간에 1초 봉이 정확히 300개 없으면 해당 비교는 제외하며 값을 임의 보간하지 않습니다. 모든 결과는 원시 시간축 그대로 남깁니다.

## 출력 및 해석

기본 출력: `data\reports\diagnostics\metrics-taker-alignment\<UTC실행시각>\`

- `summary.json`: 날짜별/전체 데이터 집계, 세 후보 × 두 단위별 중앙값·P90 절대오차·상관계수·샘플 수.
- `window_comparisons.csv`: 각 과거 행의 원본 T, 계산 구간, 두 비율 및 오차.

보고 상태는 의도적으로 `DIAGNOSTIC_ONLY`입니다. 단순 상관이나 오차 우위만으로 과거 라벨 의미를 확정하지 않습니다. 과거 **원본 daily ZIP의 해당 CSV 행**과 정규화 Parquet 값이 같은지 별도로 교차 검증하고, 매매량이 높은 구간 및 각 월에서 결과가 반복되는지 확인해야 합니다.

가져온 데이터를 로컬에서 읽기 때문에 실행 시간은 저장 장치 속도와 Parquet 크기에 따라 달라집니다. 실행 전 Demo 자동매매 프로세스를 중단할 필요는 없지만, 저장 장치 부하가 걱정된다면 현재 포지션 상태부터 확인하세요.

## 검증 범위

- 사전 수행: 소스에 `gofmt` 적용, **실제 parquet-go 대신 인터페이스 스텁을 사용한 컴파일 및 순수 함수 테스트 통과**.
- 미검증: 실제 저장소에서 parquet-go v0.32.0을 사용한 컴파일 및 실제 2024 데이터 스캔. 해당 단계는 위 두 명령을 사용자 Windows 저장소에서 실행해 확인해야 합니다.
- 거래/주문: 0건. 동결 Feature·모델·정책 변경: 없음.

작업 뒤 `summary.json`과 `window_comparisons.csv`를 압축해 전달하면 다음 단계에서 과거/실시간 시간 의미를 대조할 수 있습니다.
