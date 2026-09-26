# BTCUSDT Trader

Windows에서 Binance BTCUSDT 공개 시장 데이터, Feature V2 상태와 거래 안전 상태를 확인하는 Trading Console이다.

## 현재 안전 상태

- 기본 실행 모드: `PUBLIC_ONLY`
- Mainnet 주문: 비활성화
- Demo 주문: 기본 비활성화
- Frozen Feature V2: 128개
- Frozen 모델 자동 진입: `FROZEN_MODEL_TIME_ALIGNMENT_UNVERIFIED`로 차단
- TEST 및 Final Holdout: 운영 검증에서 접근하지 않음

Frozen 모델은 2024년 Metrics timestamp 정합성이 완전히 검증되지 않았다. 따라서 Console과 실시간 Feature Shadow는 실행할 수 있지만 신규 자동 진입은 허용하지 않는다.

## 안전한 실행

PowerShell에서 프로젝트 루트로 이동한 후 실행한다.

```powershell
Set-Location F:\binance_trader
$env:BINANCE_ENV = 'PUBLIC_ONLY'
$env:BINANCE_TESTNET_ENABLE_ORDERS = 'false'
$env:BINANCE_TESTNET_ENABLE_AUTO_ORDERS = 'false'
go run ./cmd/traderui
```

브라우저에서 `http://127.0.0.1:8080`을 연다. 기본 bind는 localhost로 제한된다.

상태 확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/api/status
Invoke-RestMethod http://127.0.0.1:8080/api/system
Invoke-RestMethod 'http://127.0.0.1:8080/api/entry-blockers?environment=TESTNET'
```

종료는 Console에서 `STOP`을 먼저 누른 뒤 실행 중인 PowerShell에서 `Ctrl+C`를 사용한다. `STOP`은 신규 진입만 차단한다. 이미 존재하는 포지션이나 결과가 불확실한 주문을 임의 청산하지 않으므로 UI의 `UNKNOWN_EXECUTION_STATE`, `POSITION_PROTECTED_STOPPED` 및 거래소 상태를 확인해야 한다.

## 전체 검증

다음 명령은 주문 플래그를 강제로 끄고, 격리된 PUBLIC_ONLY 실시간 검증과 Snapshot 복원을 수행한다.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify_all.ps1 -RuntimeMinutes 10 -ApiSeconds 90
```

결과는 `data/reports/project-recovery/<UTC timestamp>/`에 저장된다.

빠른 코드 검증만 필요한 경우:

```powershell
go test ./... -count=1
go vet ./...
go build ./...
git diff --check
```

## Demo 주문

현재 상태는 `DEMO_READY`가 아니다. Metrics 정합성 및 신규 모델 검증, 실제 Demo 주문 응답 의미에 대한 승인된 검증이 끝나기 전에는 주문 플래그를 활성화하지 않는다.
