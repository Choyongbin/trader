# Binance Futures DEMO 자동매매 5시간 감독 실행 패키지

## 버전과 범위

- 이 패키지는 Git 커밋 `ddf935c7decb39a437918fc7a48bc8a9d6328f44`의 API 및 실행 옵션을 기준으로 검토했습니다.
- **실제 Demo 주문을 허용합니다.** `PUBLIC_ONLY` 실험이 아닙니다. Mainnet 주문 활성화는 하지 않습니다.
- 기존에 확인된 STOP/START 동시성 및 수동 CLOSE/자동 청산 경합은 아직 해결되지 않았습니다. 감독 스크립트는 동시 START/STOP/수동 주문을 시도하지 않지만, 외부 UI 조작까지 막을 수는 없습니다.
- **Windows PowerShell 실제 실행과 Binance API 통합 테스트는 이 환경에서 실행하지 못했습니다.** GitHub 코드와 인터페이스 대조 및 스크립트 정적 점검을 마쳤으나 무인 실행을 보증하는 소프트웨어는 아닙니다.

## 설치 및 실행

1. ZIP을 `F:\binance_trader`에 해제합니다. 폴더를 하나 더 만들지 말고 `.ps1` 파일이 루트 바로 아래에 있어야 합니다.
2. 기존 Trader UI와 다른 자동 주문 프로그램을 모두 종료합니다. Demo 계정의 기존 포지션 및 미체결/조건부 주문이 없는지 Binance Demo 화면에서 직접 확인합니다. 본 패키지는 기존 주문을 임의로 취소하지 않습니다.
3. `config\binance_credentials.enc`에 **Demo 전용** Testnet API 키·시크릿이 설정되어 있어야 합니다. 키를 스크립트나 보고서에 적지 마세요.
4. 기기의 절전·최대 절전 설정을 해제하고, 노트북이라면 뚜껑을 닫지 마세요. 스크립트 실행 중에는 Windows의 유휴 절전을 억제하지만 강제 절전, 뚜껑 닫기, 정전, 업데이트 재부팅을 막지는 못합니다.
5. **Windows PowerShell**을 열고 다음 명령을 실행합니다.

```powershell
cd F:\binance_trader
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\demo_auto_5h_supervisor.ps1
```

스크립트는 다음을 자동 수행합니다.

- 다른 UI 프로세스·사용 중인 포트, Git 기준 커밋 및 로컬 변경, 이전 자동 실행 상태를 확인합니다. 안전 조건이 맞지 않으면 **주문하지 않고 중단**합니다.
- 주문 플래그가 꺼진 상태에서 `go test ./... -count=1`, `go vet ./...`, `go build`를 실행합니다. 하나라도 실패하면 시작하지 않습니다.
- 이 스크립트가 시작한 새 서버에만 TESTNET 주문 플래그를 적용합니다. 포트는 기본 `127.0.0.1:18080`입니다.
- 실제 시장 데이터와 Feature, 계정 및 주문 안전조건이 모두 준비되면 **자동 START 요청을 단 한 번** 보냅니다. 조건이 만족되지 않으면 임의 진입하거나 제한을 완화하지 않습니다.
- 서버 시작 시각부터 5시간 동안(준비 대기 시간 포함) 약 20초마다 확인하고 약 1분마다 관측 샘플을 기록합니다. **준비가 늦으면 실제 자동매매 시간은 5시간 미만이고, 조건이 계속 차단되면 주문이 0건일 수 있습니다.**
- 5시간 후 신규 진입을 STOP하고 재조회로 확인합니다. 일반 STOP이 실패하면 긴급 STOP을 시도합니다. 실패 시 로그에 경고를 남깁니다. 어떤 방법도 무인 중 100% STOP을 보장하지는 않습니다.
- **서버를 자동 종료하거나 포지션을 강제 청산하지 않습니다.** 서버를 종료하면 기존 포지션의 자동 관리가 중단될 수 있습니다. 완료 후 Demo 계정의 포지션, TP/SL, 주문을 직접 점검하세요.

## 안전상 한계

**이 스크립트를 실행해 자리를 비우기 전에 실제로 감수해야 하는 사항입니다.** 남아 있는 동시성 결함, 거래소/API 장애, PC 종료, 네트워크 단절, OS 절전 때문에 5시간 동안 자동 주문 실행 및 지정 시각의 안전한 종료를 보장할 수 없습니다. 별도의 원격 알림·모니터링도 제공하지 않습니다. Demo 이외의 키를 사용하지 마세요.

기존 자동매매 실행 상태 파일이 `FLAT`이 아니면, 이를 강제로 초기화하거나 삭제하지 말고 상태를 확인하세요.

## 별도 도구

읽기 전용으로 현재 상태를 확인하려면:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\demo_auto_status.ps1
```

감독 스크립트가 종료됐거나 수동으로 신규 진입을 즉시 금지하려면:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\demo_auto_emergency_stop.ps1
```

**긴급 STOP은 신규 진입 차단용입니다. 기존 포지션을 청산하지 않으며 TP/SL을 취소하거나 확인 완료해 주지 않습니다.** API 응답이 없으면 Demo 웹 화면에서 직접 확인하세요.

## 실행 결과

`F:\binance_trader\data\reports\demo_auto_5h\<실행날짜_시각>\`에 저장됩니다.

- `summary.json`: 실행·STOP 확인·최종 포지션/주문 개수·관리 검토 필요 여부
- `supervisor.log`: 진행 및 오류 메시지
- `telemetry.jsonl`: 약 1분 단위 Feature/모델·신선도·폴링 진단 요약
- `go-test.log`, `go-vet.log`, `go-build.log`: 사전 오프라인 검증 결과
- `traderui.stdout.log`, `traderui.stderr.log`: 서버 로그
- `final_positions.json`, `final_orders.json`: 종료 시 조회 가능한 경우에만 생성

결과 파일에는 Demo 계정/포지션 정보가 들어갈 수 있습니다. 타인에게 공유하기 전 검토하고, **API 비밀키 또는 설정 파일은 절대로 함께 보내지 마세요.**

## 수정 및 검토 내역

초안의 `-run-duration <10시간>` 강제 종료 옵션을 제거했습니다. 종료 시점에 포지션이 남아 있다면 서버와 위험 관리가 살아 있어야 하므로, **5시간 종료는 자동 신규 진입 중단만 수행**합니다. 기존 REST 엔드포인트, CSRF 헤더, 모델 프로필 ID, 시스템 진단 필드, Git 기준 커밋을 최신 저장소 코드와 대조했습니다. 파일 문법의 괄호 균형 및 사용 API 문자열은 정적으로 확인했으며, PowerShell 실제 파서·Windows 동작·Binance 실거래소 연동은 미검증입니다.
