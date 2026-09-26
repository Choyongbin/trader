param(
    [int]$RuntimeMinutes = 10,
    [int]$ApiSeconds = 90,
    [switch]$SkipLive
)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$started = [DateTime]::UtcNow
$stamp = $started.ToString('yyyyMMddTHHmmssZ')
$reportRoot = Join-Path $projectRoot 'data\reports\verification'
$runRoot = Join-Path $reportRoot $stamp
New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
if (-not (Test-Path -LiteralPath $runRoot -PathType Container)) { throw "verification result directory was not created: $runRoot" }
$runRoot = (Resolve-Path -LiteralPath $runRoot).Path

$env:BINANCE_ENV = 'PUBLIC_ONLY'
$env:BINANCE_TESTNET_ENABLE_ORDERS = 'false'
$env:BINANCE_TESTNET_ENABLE_AUTO_ORDERS = 'false'
$env:RUN_DEMO_AUTO_SMOKE = '0'
$env:BINANCE_CREDENTIAL_FILE = Join-Path $runRoot 'credentials-unavailable-by-design.enc'
$env:GOCACHE = Join-Path $projectRoot '.tmp_go_cache'

$results = New-Object System.Collections.ArrayList
$commands = New-Object System.Collections.ArrayList
$liveEvidence = $null
$restoreEvidence = $null
$apiEvidence = $null
$changedBefore = @(git -C $projectRoot status --short)
$existingProcesses = @(Get-Process -Name traderui,go -ErrorAction SilentlyContinue | Select-Object Id,ProcessName,Path,StartTime)

function Add-Result([string]$Name, [string]$Status, [string]$Detail, [string]$Log) {
    [void]$results.Add([ordered]@{ name=$Name; status=$Status; detail=$Detail; log=$Log })
}

function Invoke-Logged([string]$Name, [string]$File, [string[]]$Arguments, [switch]$AllowFailure) {
    $log = Join-Path $runRoot ($Name + '.log')
    [void]$commands.Add(($File + ' ' + ($Arguments -join ' ')))
    Push-Location $projectRoot
    try {
		$previousPreference = $ErrorActionPreference
		$ErrorActionPreference = 'Continue'
        & $File @Arguments 2>&1 | Out-File -LiteralPath $log -Encoding utf8
        $exitCode = $LASTEXITCODE
    } catch {
        $_ | Out-String | Out-File -LiteralPath $log -Encoding utf8
        $exitCode = 1
    } finally {
		$ErrorActionPreference = $previousPreference
        Pop-Location
    }
    if ($exitCode -eq 0) {
        Add-Result $Name 'PASS' 'exit_code=0' $log
    } else {
        Add-Result $Name 'FAIL' ("exit_code=" + $exitCode) $log
        if (-not $AllowFailure) { return $false }
    }
    return ($exitCode -eq 0)
}

function Write-Summaries {
    $ended = [DateTime]::UtcNow
    $failed = @($results | Where-Object { $_.status -eq 'FAIL' }).Count
    $blocked = @($results | Where-Object { $_.status -eq 'BLOCKED' }).Count
    $overall = if ($failed -eq 0 -and $blocked -eq 0) { 'PASS' } elseif ($failed -gt 0) { 'FAIL' } else { 'BLOCKED' }
    $changedAfter = @(git -C $projectRoot status --short)
    $summary = [ordered]@{
        version = 1
        status = $overall
        started_at_utc = $started.ToString('o')
        ended_at_utc = $ended.ToString('o')
        duration_seconds = [math]::Round(($ended - $started).TotalSeconds, 3)
        result_directory = $runRoot
        git_branch = (git -C $projectRoot branch --show-current).Trim()
        git_commit = (git -C $projectRoot rev-parse HEAD).Trim()
        changed_files_before = $changedBefore
        changed_files_after = $changedAfter
        go_version = (& go version)
        powershell_version = $PSVersionTable.PSVersion.ToString()
        environment = [ordered]@{ BINANCE_ENV=$env:BINANCE_ENV; BINANCE_TESTNET_ENABLE_ORDERS=$env:BINANCE_TESTNET_ENABLE_ORDERS; BINANCE_TESTNET_ENABLE_AUTO_ORDERS=$env:BINANCE_TESTNET_ENABLE_AUTO_ORDERS; RUN_DEMO_AUTO_SMOKE=$env:RUN_DEMO_AUTO_SMOKE }
        existing_trader_processes = $existingProcesses
        commands = @($commands)
        results = @($results)
        actual_demo_orders = 0
        actual_mainnet_orders = 0
        final_holdout_accessed = $false
        frozen_hashes = [ordered]@{
            feature_registry = 'a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef'
            entry_policy = '4fae120d9d54a5732dbf2009950beba69773bb0a28a7c3915b616e295cd3dd31'
            risk_policy = '21e3332b5cf286dd39827973a259fbd91dde325ca067f6a067237bb6f24596f7'
        }
        automated_scenarios = [ordered]@{
            normal = @('LONG entry/protection','SHORT entry/protection','Horizon reduce-only exit','sibling protective cleanup','duplicate entry block')
            failure_recovery = @('entry unknown','partial fill','TP failure/unknown','SL failure/unknown','quantity mismatch','side mismatch','owned-only cleanup','already-flat cleanup')
            timeout = @('independent bounded recovery context','recovery timeout','response loss lookup before retry','unknown exit no resend')
            crash_recovery = @('PENDING_ENTRY','ENTRY_CONFIRMED','PROTECTIVE_PARTIAL','POSITION_PROTECTED','EXIT_PENDING','FLAT atomic publication failure')
            repeat_count = 10
        }
        public_only_runtime = $liveEvidence
        snapshot_restore = $restoreEvidence
        http_api = $apiEvidence
        unresolved = @(
            'NET_MODE_MANUAL_INTERVENTION_OWNERSHIP_UNPROVEN',
            'DEMO_ORDER_RESPONSE_SEMANTICS_UNVERIFIED_WITHOUT_LIVE_ORDER',
            'LINUX_RACE_DETECTOR_UNAVAILABLE'
        )
        demo_auto_preverification = 'NOT_SATISFIED'
    }
    $jsonPath = Join-Path $runRoot 'verification-summary.json'
    $txtPath = Join-Path $runRoot 'verification-summary.txt'
    $summary | ConvertTo-Json -Depth 12 | Out-File -LiteralPath $jsonPath -Encoding utf8
    @(
        "STATUS=$overall",
        "STARTED_UTC=$($summary.started_at_utc)",
        "ENDED_UTC=$($summary.ended_at_utc)",
        "GIT_BRANCH=$($summary.git_branch)",
        "GIT_COMMIT=$($summary.git_commit)",
        "RESULT_DIRECTORY=$runRoot",
        "ACTUAL_DEMO_ORDERS=0",
        "ACTUAL_MAINNET_ORDERS=0",
        "FINAL_HOLDOUT_ACCESSED=false",
        "DEMO_AUTO_PREVERIFICATION=NOT_SATISFIED",
        "UNRESOLVED=NET_MODE_MANUAL_INTERVENTION_OWNERSHIP_UNPROVEN,DEMO_ORDER_RESPONSE_SEMANTICS_UNVERIFIED_WITHOUT_LIVE_ORDER,LINUX_RACE_DETECTOR_UNAVAILABLE",
        '',
        ($results | ForEach-Object { "[$($_.status)] $($_.name): $($_.detail)" })
    ) | Out-File -LiteralPath $txtPath -Encoding utf8
}

try {
    Set-Content -LiteralPath (Join-Path $runRoot 'result-directory.txt') -Value $runRoot -Encoding utf8
    Add-Result 'result-directory' 'PASS' $runRoot (Join-Path $runRoot 'result-directory.txt')

    $preflight = [ordered]@{
        project_exists = (Test-Path -LiteralPath $projectRoot -PathType Container)
        branch = (git -C $projectRoot branch --show-current).Trim()
        commit = (git -C $projectRoot rev-parse HEAD).Trim()
        changed_files = $changedBefore
        go_version = (& go version)
        powershell_version = $PSVersionTable.PSVersion.ToString()
        existing_processes = $existingProcesses
        model_policy_files = @(
            'data\reports\model\main\v2\phase9c-cde\stage-d-policy-freeze.json',
            'data\reports\production\v1\BTCUSDT\risk-policy-v1.json'
        ) | ForEach-Object { [ordered]@{ path=$_; exists=(Test-Path -LiteralPath (Join-Path $projectRoot $_) -PathType Leaf) } }
    }
    $preflight | ConvertTo-Json -Depth 8 | Out-File -LiteralPath (Join-Path $runRoot 'preflight.json') -Encoding utf8
    if (@($preflight.model_policy_files | Where-Object { -not $_.exists }).Count -gt 0) { Add-Result 'preflight' 'FAIL' 'required frozen artifact missing' (Join-Path $runRoot 'preflight.json') } else { Add-Result 'preflight' 'PASS' 'project and frozen inputs present' (Join-Path $runRoot 'preflight.json') }

    [void](Invoke-Logged 'go-fmt' 'go' @('fmt','./...') -AllowFailure)
    [void](Invoke-Logged 'frozen-artifact-load' 'go' @('test','./internal/live/autopipeline','-run','TestFrozenModelBindingAndDryRunRiskIntent','-count=1','-v') -AllowFailure)
    [void](Invoke-Logged 'go-test-all' 'go' @('test','./...','-count=1') -AllowFailure)
    [void](Invoke-Logged 'auto-order-repeat-10' 'go' @('test','./internal/uiapi','./internal/live/binance','-count=10') -AllowFailure)
    [void](Invoke-Logged 'go-vet' 'go' @('vet','./...') -AllowFailure)
    [void](Invoke-Logged 'go-build' 'go' @('build','./...') -AllowFailure)
    [void](Invoke-Logged 'git-diff-check' 'git' @('-C',$projectRoot,'diff','--check') -AllowFailure)

    $raceOK = Invoke-Logged 'race-detector' 'go' @('test','-race','./internal/live/binance','./internal/uiapi','-count=1') -AllowFailure
    if (-not $raceOK) {
        $race = $results[$results.Count-1]
		$raceLog = if (Test-Path -LiteralPath $race.log) { Get-Content -LiteralPath $race.log -Raw } else { '' }
		$unsupported = $raceLog -match '(?i)-race is not supported|race detector is not supported|race requires cgo|64-bit mode not compiled in'
		if ($unsupported) {
			$race.status = 'SKIP'
			$race.detail = 'race detector unsupported by the active Go target/toolchain; see log'
		} else {
			$race.status = 'FAIL'
			$race.detail = 'race-enabled build or test failed; failure is not treated as unsupported'
		}
    }

    if ($SkipLive) {
        Add-Result 'public-only-runtime' 'SKIP' 'explicit -SkipLive' ''
        Add-Result 'snapshot-restore' 'SKIP' 'public runtime skipped' ''
        Add-Result 'http-api-integration' 'SKIP' 'public runtime skipped' ''
    } elseif ($existingProcesses.Count -gt 0) {
        Add-Result 'public-only-runtime' 'BLOCKED' 'existing traderui/go process detected; user process was not terminated' (Join-Path $runRoot 'preflight.json')
        Add-Result 'snapshot-restore' 'BLOCKED' 'runtime isolation precondition failed' ''
        Add-Result 'http-api-integration' 'BLOCKED' 'runtime isolation precondition failed' ''
    } else {
        $liveRoot = Join-Path $runRoot 'live'
        New-Item -ItemType Directory -Path $liveRoot -Force | Out-Null
        $snapshot = Join-Path $liveRoot 'runtime-snapshot.json'
        $duration = ($RuntimeMinutes.ToString() + 'm')
        [void](Invoke-Logged 'public-only-runtime' 'go' @('run','./cmd/livevalidation','-mode','stage-g','-root',$liveRoot,'-duration',$duration,'-snapshot-path',$snapshot) -AllowFailure)

        $liveResultPath = Join-Path $liveRoot 'stage-g-shadow-session.json'
        $livePassed = Test-Path -LiteralPath $liveResultPath -PathType Leaf
        if ($livePassed) {
            $liveResult = Get-Content -LiteralPath $liveResultPath -Raw | ConvertFrom-Json
            $livePassed = ($liveResult.Status -eq 'PASS' -and $liveResult.actual_order_submits -eq 0 -and $liveResult.future_observations -eq 0)
            $liveEvidence = $liveResult
        }
        if (-not $livePassed) {
            Add-Result 'snapshot-restore' 'BLOCKED' 'live runtime did not produce a valid isolated snapshot' $liveResultPath
            Add-Result 'http-api-integration' 'BLOCKED' 'live runtime did not produce a valid isolated snapshot' $liveResultPath
        } else {
            $restoreRoot = Join-Path $liveRoot 'restore'
            [void](Invoke-Logged 'snapshot-restore-command' 'go' @('run','./cmd/livevalidation','-mode','stage-g','-root',$restoreRoot,'-duration','2m','-snapshot-path',$snapshot) -AllowFailure)
            $restorePath = Join-Path $restoreRoot 'stage-g-shadow-session.json'
            $restorePass = Test-Path -LiteralPath $restorePath -PathType Leaf
            if ($restorePass) {
                $restoreEvidence = Get-Content -LiteralPath $restorePath -Raw | ConvertFrom-Json
                $restorePass = ($restoreEvidence.snapshot_restored -and $restoreEvidence.restore_applied -and -not $restoreEvidence.full_bootstrap_executed -and $restoreEvidence.feature_decisions -gt 0 -and $restoreEvidence.canonical_id_gaps -eq 0 -and $restoreEvidence.duplicate_events -eq 0 -and $restoreEvidence.reverse_events -eq 0 -and $restoreEvidence.future_observations -eq 0 -and $restoreEvidence.actual_order_submits -eq 0)
            }
            $restoreCommandResult = $results[$results.Count-1]
            if ($restorePass) {
                $restoreCommandResult.status = 'PASS'
                $restoreCommandResult.detail = 'SNAPSHOT_RESTORE applied; continuity and decision growth verified'
                Add-Result 'snapshot-restore' 'PASS' ("decisions=" + $restoreEvidence.feature_decisions) $restorePath
            } else {
                Add-Result 'snapshot-restore' 'FAIL' 'restore identity, continuity, or decision-growth gate failed' $restorePath
            }
            $exe = Join-Path $runRoot 'traderui-verification.exe'
            [void](Invoke-Logged 'build-test-traderui' 'go' @('build','-o',$exe,'./cmd/traderui') -AllowFailure)
            $warmup = Join-Path $liveRoot 'capture-state.json'
            $autoState = Join-Path $liveRoot 'auto-execution-state.json'
            $stdout = Join-Path $runRoot 'traderui.stdout.log'
            $stderr = Join-Path $runRoot 'traderui.stderr.log'
            $arguments = @('-listen','127.0.0.1:18081','-run-duration',($ApiSeconds.ToString() + 's'),'-warmup-path',$warmup,'-live-snapshot-path',$snapshot,'-auto-state-path',$autoState)
            $process = Start-Process -FilePath $exe -ArgumentList $arguments -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
            $apiLog = Join-Path $runRoot 'http-api.json'
            $apiRows = New-Object System.Collections.ArrayList
            $deadline = [DateTime]::UtcNow.AddSeconds([math]::Min(60,$ApiSeconds))
            while ([DateTime]::UtcNow -lt $deadline) {
                try {
                    foreach ($path in @('/api/status','/api/system','/api/models','/api/market','/api/auto-trading?environment=TESTNET','/api/entry-blockers?environment=TESTNET','/api/logs')) {
                        $response = Invoke-RestMethod -Method Get -Uri ('http://127.0.0.1:18081' + $path) -TimeoutSec 5
                        [void]$apiRows.Add([ordered]@{ timestamp=[DateTime]::UtcNow.ToString('o'); path=$path; response=$response })
                    }
                    Start-Sleep -Seconds 10
                } catch {
                    Start-Sleep -Seconds 2
                }
            }
            $apiRows | ConvertTo-Json -Depth 15 | Out-File -LiteralPath $apiLog -Encoding utf8
            $process.WaitForExit([math]::Max(30000,($ApiSeconds + 30) * 1000)) | Out-Null
            if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
            $systems = @($apiRows | Where-Object { $_.path -eq '/api/system' } | ForEach-Object { $_.response })
            $models = @($apiRows | Where-Object { $_.path -eq '/api/models' } | ForEach-Object { $_.response })
            $autos = @($apiRows | Where-Object { $_.path -like '/api/auto-trading*' } | ForEach-Object { $_.response })
            $markets = @($apiRows | Where-Object { $_.path -eq '/api/market' } | ForEach-Object { $_.response })
            $apiPass = ($apiRows.Count -ge 7 -and $systems.Count -gt 0 -and $models.Count -gt 0 -and $autos.Count -gt 0 -and $markets.Count -gt 0)
            if ($apiPass) {
                $lastSystem = $systems[$systems.Count-1]
                $lastAuto = $autos[$autos.Count-1]
                $lastMarket = $markets[$markets.Count-1]
                $apiPass = ($lastSystem.counters.actual_order_submits -eq 0 -and $lastSystem.counters.future_observations -eq 0 -and $lastSystem.frozen.feature_registry_hash -eq 'a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef' -and -not $lastAuto.running -and -not $lastAuto.readiness.orders_enabled -and -not $lastAuto.readiness.auto_orders_enabled -and $lastMarket.connected)
                $apiEvidence = [ordered]@{ checks=$apiRows.Count; actual_order_submits=$lastSystem.counters.actual_order_submits; future_observations=$lastSystem.counters.future_observations; feature_decisions=$lastSystem.counters.feature_decisions; market_connected=$lastMarket.connected; orders_enabled=$lastAuto.readiness.orders_enabled; auto_orders_enabled=$lastAuto.readiness.auto_orders_enabled }
            }
            if ($apiPass) { Add-Result 'http-api-integration' 'PASS' ("api_checks=" + $apiRows.Count) $apiLog } else { Add-Result 'http-api-integration' 'FAIL' 'API structure, frozen identity, market, or order-disable gate failed' $apiLog }
        }
    }
} catch {
    $_ | Out-String | Out-File -LiteralPath (Join-Path $runRoot 'unhandled-error.log') -Encoding utf8
    Add-Result 'verification-script' 'FAIL' $_.Exception.Message (Join-Path $runRoot 'unhandled-error.log')
} finally {
    Write-Summaries
}

$summaryObject = Get-Content -LiteralPath (Join-Path $runRoot 'verification-summary.json') -Raw | ConvertFrom-Json
Write-Host ("VERIFICATION " + $summaryObject.status + " " + $runRoot)
if ($summaryObject.status -eq 'FAIL') { exit 1 }
if ($summaryObject.status -eq 'BLOCKED') { exit 2 }
exit 0
