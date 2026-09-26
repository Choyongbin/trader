# Binance Futures DEMO automatic trading supervisor, Windows PowerShell 5.1+.
# This is an actual DEMO order run (not PUBLIC_ONLY shadow). MAINNET is disabled.
# No source files, frozen artifacts, or trading rules are modified.
param(
    [string]$Root = 'F:\binance_trader',
    [int]$Hours = 5,
    [int]$Port = 18080,
    [bool]$KeepAwake = $true,
    [string]$ExpectedCommit = 'ddf935c7decb39a437918fc7a48bc8a9d6328f44'
)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
if ($Hours -lt 1 -or $Hours -gt 8) { throw 'Hours must be 1..8' }
$Root = (Resolve-Path -LiteralPath $Root).Path
Set-Location -LiteralPath $Root
$runDir = Join-Path $Root ("data\reports\demo_auto_5h\" + (Get-Date -Format 'yyyyMMdd_HHmmss'))
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
$log = Join-Path $runDir 'supervisor.log'
$base = 'http://127.0.0.1:' + $Port
$env:BINANCE_MAINNET_ENABLE_ORDERS = 'false'
$env:BINANCE_ENV = 'PUBLIC_ONLY'
$env:BINANCE_TESTNET_ENABLE_ORDERS = 'false'
$env:BINANCE_TESTNET_ENABLE_AUTO_ORDERS = 'false'
$env:RUN_DEMO_AUTO_SMOKE = '0'
if ([string]::IsNullOrWhiteSpace($env:BINANCE_CREDENTIAL_FILE)) {
    $env:BINANCE_CREDENTIAL_FILE = Join-Path $Root 'config\binance_credentials.enc'
}
$mutex = New-Object System.Threading.Mutex -ArgumentList @($false, 'Local\BinanceTraderDemoAuto5hSupervisor')
$mutexHeld = $false
$powerHold = $false
$proc = $null
$csrf = $null
$started = $false
$stopConfirmed = $false
$needManualReview = $false
$stopReason = 'WINDOW_COMPLETED'
$script:lastTelemetryAt = [datetime]::MinValue
$telemetryPath = Join-Path $runDir 'telemetry.jsonl'
$lastPositions = $null
$lastOrders = $null
$startTime = $null
$deadline = $null
$lastAuto = $null
$lastSystem = $null
$script:pollErrors = 0
function Log([string]$message) {
    $line = (Get-Date -Format 'yyyy-MM-ddTHH:mm:ssK') + ' ' + $message
    Write-Host $line
    Add-Content -LiteralPath $log -Value $line -Encoding UTF8
}
function ApiGet([string]$path) {
    return Invoke-RestMethod -Uri ($base + $path) -Method Get -TimeoutSec 20 -ErrorAction Stop
}
function ApiPost([string]$path, [hashtable]$body = @{}) {
    return Invoke-RestMethod -Uri ($base + $path) -Method Post -TimeoutSec 90 -ContentType 'application/json' -Headers @{'X-CSRF-Token'=$csrf} -Body (ConvertTo-Json $body -Compress) -ErrorAction Stop
}
function PortOpen([int]$p) {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $task = $client.ConnectAsync('127.0.0.1', $p)
        try {
            if (-not $task.Wait(350)) { return $false }
            $task.GetAwaiter().GetResult()
            return $true
        } catch { return $false }
    } finally { $client.Close() }
}
function SaveSummary([string]$phase) {
    $report = [ordered]@{
        phase = $phase
        root = $Root
        git_head = $script:gitHead
        testnet_only = $true
        mainnet_enabled = $false
        launched_at = $(if ($startTime) { $startTime.ToString('o') } else { $null })
        deadline = $(if ($deadline) { $deadline.ToString('o') } else { $null })
        auto_started = $started
        stop_confirmed = $stopConfirmed
        stop_reason = $stopReason
        requires_manual_review = $needManualReview
        pid = $(if ($proc) { $proc.Id } else { $null })
        server_exited = $(if ($proc) { $proc.HasExited } else { $null })
        last_auto = $lastAuto
        last_system = $(if ($lastSystem) { [ordered]@{ counters = $lastSystem.counters; feature_diagnostics = $lastSystem.feature_diagnostics; external_polls = $lastSystem.external_polls; metrics_sources = $lastSystem.metrics_sources } } else { $null })
        last_position_count = $(if ($null -ne $lastPositions) { @($lastPositions.positions | Where-Object { $null -ne $_ }).Count } else { $null })
        last_order_count = $(if ($null -ne $lastOrders) { @($lastOrders.orders | Where-Object { $null -ne $_ }).Count } else { $null })
        log_directory = $runDir
    }
    $report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $runDir 'summary.json') -Encoding UTF8
}
try {
    $mutexHeld = $mutex.WaitOne(0)
    if (-not $mutexHeld) { throw 'Another 5h supervisor is running. Do not start two.' }
    Log 'Preflight: this will place REAL ORDERS ON BINANCE FUTURES DEMO, never Mainnet.'
    if ($KeepAwake) {
        # Keep this Windows process active while supervising; closing a laptop lid,
        # power loss and user-forced sleep can still stop the trader.
        Add-Type -Namespace DemoNative -Name SleepControl -MemberDefinition @'
[System.Runtime.InteropServices.DllImport("kernel32.dll")]
public static extern uint SetThreadExecutionState(uint esFlags);
'@
        $powerResult = [DemoNative.SleepControl]::SetThreadExecutionState([uint32]2147483649) # ES_CONTINUOUS | ES_SYSTEM_REQUIRED
        if ($powerResult -eq 0) { throw 'Could not inhibit Windows idle sleep. Set sleep to Never before unattended testing.' }
        $powerHold = $true
        Log 'Windows idle sleep inhibition is ACTIVE while this supervisor runs. Do not close the laptop lid.'
    }
    if (-not (Test-Path -LiteralPath $env:BINANCE_CREDENTIAL_FILE)) {
        throw ('Demo credential file missing: ' + $env:BINANCE_CREDENTIAL_FILE)
    }
    if (PortOpen 8080) { throw 'Port 8080 is occupied. Stop any existing Trader UI (or choose a separate machine).'}
    if (PortOpen $Port) { throw ('Port ' + $Port + ' is already in use; refusing to share a server.') }
    # Do not accidentally leave a second Demo trader running on another port.
    $existingTrader = @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match '^traderui(?:-demo)?\.exe$' -or
        $_.CommandLine -match '(?i)go(?:\.exe)?\s+run\s+.*cmd[\\/]traderui'
    })
    if ($existingTrader.Count -ne 0) { throw 'Another Trader UI process appears to be running. Stop and verify it first.' }
    $script:gitHead = (& git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Could not read git HEAD.' }
    Log ('Git HEAD=' + $script:gitHead)
    if ($script:gitHead -ne $ExpectedCommit) { throw ('Code differs from independently reviewed commit ' + $ExpectedCommit + '. Review changes before this unattended run.') }
    $trackedChanges = @(& git status --porcelain --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $trackedChanges.Count -ne 0) { throw 'Tracked source has uncommitted changes; run was not started.' }
    $statePath = Join-Path $Root 'data\live_state\BTCUSDT\v1\auto-execution-state.json'
    if (Test-Path -LiteralPath $statePath) {
        try {
            $stateFile = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json -ErrorAction Stop
            if ($stateFile.execution_state -ne 'FLAT') { throw ('existing automatic execution state=' + $stateFile.execution_state) }
        } catch { throw ('Existing automatic execution state requires review before unattended Demo run: ' + $_.Exception.Message) }
    }
    Log 'Running fresh offline tests with all order flags OFF.'
    $testLog = Join-Path $runDir 'go-test.log'
    & go test ./... -count=1 *> $testLog
    if ($LASTEXITCODE -ne 0) { throw ('go test ./... FAILED; see ' + $testLog) }
    Log 'Full offline go test PASS.'
    & go vet ./... *> (Join-Path $runDir 'go-vet.log')
    if ($LASTEXITCODE -ne 0) { throw 'go vet ./... FAILED; no Demo run was started.' }
    Log 'go vet PASS.'
    $exe = Join-Path $runDir 'traderui-demo.exe'
    & go build -o $exe ./cmd/traderui *> (Join-Path $runDir 'go-build.log')
    if ($LASTEXITCODE -ne 0) { throw 'go build FAILED: no Demo run was started.' }
    Log 'Go build PASS. Switching ONLY this child process to TESTNET.'
    $env:BINANCE_ENV = 'TESTNET'
    $env:BINANCE_TESTNET_ENABLE_ORDERS = 'true'
    $env:BINANCE_TESTNET_ENABLE_AUTO_ORDERS = 'true'
    $env:BINANCE_MAINNET_ENABLE_ORDERS = 'false'
    # Deliberately omit -run-duration. A timed process shutdown could kill risk
    # reconciliation while a position is still open. STOP stops only NEW entries.
    $proc = Start-Process -FilePath $exe -ArgumentList @('-listen', ('127.0.0.1:' + $Port)) -WorkingDirectory $Root -RedirectStandardOutput (Join-Path $runDir 'traderui.stdout.log') -RedirectStandardError (Join-Path $runDir 'traderui.stderr.log') -PassThru -WindowStyle Hidden
    # Only the child Trader UI should have TESTNET order flags enabled.
    $env:BINANCE_ENV = 'PUBLIC_ONLY'
    $env:BINANCE_TESTNET_ENABLE_ORDERS = 'false'
    $env:BINANCE_TESTNET_ENABLE_AUTO_ORDERS = 'false'
    $startTime = Get-Date
    $deadline = $startTime.AddHours($Hours)
    Log ('TraderUI PID=' + $proc.Id + ' auto-deadline=' + $deadline.ToString('o') + ' server-stays-alive-after-stop=true')
    SaveSummary 'STARTING'
    # Wait for local server without ever sending an order prematurely.
    $readyServer = $false
    for ($i=0; $i -lt 60; $i++) {
        if ($proc.HasExited) { throw ('TraderUI exited at startup code=' + $proc.ExitCode) }
        try { $status = ApiGet '/api/status'; $readyServer = $true; break } catch { Start-Sleep -Seconds 2 }
    }
    if (-not $readyServer) { throw 'TraderUI did not start within 120 seconds.' }
    if (-not $status.credentials.testnet_api_key_present -or -not $status.credentials.testnet_secret_present) {
        throw 'Demo credentials not loaded; no auto start attempted.'
    }
    if ($status.execution_mode -ne 'TESTNET LIVE AUTO ORDERS') {
        throw ('Unexpected execution_mode=' + $status.execution_mode + '; auto trading not armed.')
    }
    $csrf = [string]$status.csrf_token
    if ([string]::IsNullOrWhiteSpace($csrf)) { throw 'CSRF token unavailable.' }
    Log 'Demo API and credentials verified. Waiting for real readiness gates, no forced entry.'
    $lastProgress = Get-Date
    while ((Get-Date) -lt $deadline) {
        if ($proc.HasExited) { throw ('TraderUI exited unexpectedly code=' + $proc.ExitCode) }
        try {
            $lastAuto = ApiGet '/api/auto-trading?environment=TESTNET'
            $lastSystem = ApiGet '/api/system'
            $script:pollErrors = 0
            $blockers = @($lastAuto.blockers | Where-Object { $null -ne $_ } | ForEach-Object { $_.code })
            if (-not $started) {
                # Do NOT bypass missing metrics, LOOKBACK, position reconciliation or stale public data.
                if ($lastAuto.running) {
                    $stopReason = 'UNEXPECTED_RUNNING_AUTO'
                    $needManualReview = $true
                    Log 'Unexpected running auto session on supervised server. Refusing another START and proceeding to STOP.'
                    break
                }
                if ($lastAuto.readiness.overall -eq $true -and $blockers.Count -eq 0) {
                    # Fail closed on dirty Demo accounts, even if the readiness endpoint changes.
                    $account = ApiGet '/api/account?environment=TESTNET'
                    $positions = ApiGet '/api/positions?environment=TESTNET'
                    $orders = ApiGet '/api/orders?environment=TESTNET'
                    if (-not $account.account.connected -or @($positions.positions | Where-Object { $null -ne $_ }).Count -ne 0 -or @($orders.orders | Where-Object { $null -ne $_ }).Count -ne 0) {
                        throw 'Demo account is disconnected or not FLAT/CLEAN; refusing to START.'
                    }
                    # Only ONE automated START is ever issued. No concurrent STOP/START by this script.
                    $response = ApiPost '/api/auto-trading/start?environment=TESTNET' @{model_profile_id='btc-feature-v2-production-v1'}
                    if ($response.running -ne $true) { throw 'START was not acknowledged as running.' }
                    $started = $true
                    Log 'AUTO START confirmed (DEMO orders only). Monitoring until five-hour deadline.'
                    SaveSummary 'AUTO_RUNNING'
                } elseif (((Get-Date) - $lastProgress).TotalMinutes -ge 5) {
                    Log ('Waiting for readiness; blockers=' + ($blockers -join ',') + ' latest_reason=' + $lastSystem.feature_diagnostics.last_eligibility_reason)
                    $lastProgress = Get-Date
                }
            } else {
                if ($lastAuto.state -eq 'UNKNOWN_EXECUTION_STATE' -or $lastAuto.state -eq 'ERROR') {
                    $stopReason = 'AUTO_ERROR'
                    Log ('Auto error=' + $lastAuto.state + '; stopping new entries, leaving broker online for risk management.')
                    break
                }
                if (-not $lastAuto.running) {
                    $stopReason = 'AUTO_STOPPED_BEFORE_DEADLINE'
                    Log ('Auto stopped unexpectedly: state=' + $lastAuto.state + '. Will NOT restart it.')
                    break
                }
                if (((Get-Date) - $lastProgress).TotalMinutes -ge 5) {
                    $ct = $lastSystem.counters
                    Log ('RUNNING decisions=' + $ct.feature_decisions + ' eligible=' + $ct.eligible_decisions + ' intents=' + $ct.auto_execution_intents + ' submit_attempts=' + $ct.actual_order_submits + ' lookback=' + ($lastSystem.feature_diagnostics.lookback_missing -join ','))
                    $lastProgress = Get-Date
                    SaveSummary 'AUTO_RUNNING'
                }
            }
            # Record hourly-window progress for later review without storing API keys.
            if (((Get-Date) - $script:lastTelemetryAt).TotalSeconds -ge 60) {
                $sample = [ordered]@{
                    at = (Get-Date).ToString('o')
                    running = [bool]$lastAuto.running
                    state = [string]$lastAuto.state
                    blockers = @($blockers)
                    feature_diagnostics = $lastSystem.feature_diagnostics
                    external_polls = $lastSystem.external_polls
                    metrics_sources = $lastSystem.metrics_sources
                    counters = $lastSystem.counters
                }
                Add-Content -LiteralPath $telemetryPath -Value ($sample | ConvertTo-Json -Depth 12 -Compress) -Encoding UTF8
                $script:lastTelemetryAt = Get-Date
            }
        } catch {
            $script:pollErrors++
            Log ('Polling/control error ' + $script:pollErrors + ': ' + $_.Exception.Message)
            if ($script:pollErrors -ge 4) { $stopReason = 'SUPERVISOR_POLL_FAILURE'; break }
        }
        Start-Sleep -Seconds 20
    }
    if (-not $started -and $stopReason -eq 'WINDOW_COMPLETED') {
        $stopReason = 'READINESS_NOT_REACHED_IN_WINDOW'
        Log 'Readiness never allowed AUTO START. Zero orders sent by supervisor.'
    }
} catch {
    $stopReason = 'SUPERVISOR_EXCEPTION'
    $needManualReview = ($null -ne $proc)
    Log ('FATAL: ' + $_.Exception.Message)
} finally {
    $env:BINANCE_ENV = 'PUBLIC_ONLY'
    $env:BINANCE_TESTNET_ENABLE_ORDERS = 'false'
    $env:BINANCE_TESTNET_ENABLE_AUTO_ORDERS = 'false'
    # STOP only suppresses NEW entries; the running child manages existing owned positions.
    if ($null -ne $proc -and -not $proc.HasExited -and -not [string]::IsNullOrWhiteSpace($csrf)) {
        for ($attempt=1; $attempt -le 4 -and -not $stopConfirmed; $attempt++) {
            try {
                $null = ApiPost '/api/auto-trading/stop?environment=TESTNET'
                $lastAuto = ApiGet '/api/auto-trading?environment=TESTNET'
                if ($lastAuto.running -eq $false) { $stopConfirmed = $true; Log 'STOP verified: no NEW auto entries; existing risk management stays alive.' }
            } catch { Log ('STOP attempt ' + $attempt + ' failed: ' + $_.Exception.Message) }
            if (-not $stopConfirmed) { Start-Sleep -Seconds 8 }
        }
        if (-not $stopConfirmed) {
            try {
                $null = ApiPost '/api/auto-trading/emergency-stop?environment=TESTNET'
                $lastAuto = ApiGet '/api/auto-trading?environment=TESTNET'
                $stopConfirmed = ($lastAuto.running -eq $false)
                if ($stopConfirmed) { Log 'EMERGENCY STOP verified: no new auto entries.' }
            } catch { Log ('Emergency stop FAILED: ' + $_.Exception.Message) }
        }
    }
    if (-not $stopConfirmed -and $null -ne $proc -and -not $proc.HasExited) {
        $needManualReview = $true
        Log 'WARNING: STOP not confirmed. Trader may still be placing new Demo orders! Inspect promptly; do NOT assume stopped.'
    }
    if ($null -ne $proc -and -not $proc.HasExited) {
        # Never kill the broker automatically while the owned Demo position may still be open.
        try {
            $lastAuto = ApiGet '/api/auto-trading?environment=TESTNET'
            $positions = ApiGet '/api/positions?environment=TESTNET'
            $orders = ApiGet '/api/orders?environment=TESTNET'
            $lastPositions = $positions
            $lastOrders = $orders
            $account = ApiGet '/api/account?environment=TESTNET'
            # Final local evidence. Protect the report folder: it includes account data.
            $positions | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $runDir 'final_positions.json') -Encoding UTF8
            $orders | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $runDir 'final_orders.json') -Encoding UTF8
            $positionCount = @($positions.positions | Where-Object { $null -ne $_ }).Count
            $orderCount = @($orders.orders | Where-Object { $null -ne $_ }).Count
            if ($account.account.connected -and $positionCount -eq 0 -and $orderCount -eq 0 -and $stopConfirmed) {
                Log 'Demo account FLAT/CLEAN at stop. TraderUI remains running for operator inspection.'
            } else {
                $needManualReview = $true
                Log ('Demo account needs REVIEW: account_connected=' + $account.account.connected + ' positions=' + $positionCount + ' open_orders=' + $orderCount)
            }
        } catch {
            $needManualReview = $true
            Log ('Final exchange verification failed: ' + $_.Exception.Message)
        }
    }
    try { SaveSummary 'FINISHED_AUTO_WINDOW' } catch { Log ('Failed to write summary: ' + $_.Exception.Message) }
    if ($null -ne $proc) { Log ('Server PID=' + $proc.Id + ' is NOT intentionally terminated. If still running, inspect Demo positions/TP/SL before stopping it.') }
    Log ('Reports: ' + $runDir)
    if ($powerHold) {
        [void][DemoNative.SleepControl]::SetThreadExecutionState([uint32]2147483648) # ES_CONTINUOUS: restore normal sleep policy
    }
    if ($mutexHeld) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
}
