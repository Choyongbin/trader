# Read-only check; never sends an order or starts/stops automatic trading.
param([int]$Port = 18080)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$base = 'http://127.0.0.1:' + $Port
$auto = Invoke-RestMethod -Uri ($base + '/api/auto-trading?environment=TESTNET') -Method Get -TimeoutSec 20
$system = Invoke-RestMethod -Uri ($base + '/api/system') -Method Get -TimeoutSec 20
$positions = Invoke-RestMethod -Uri ($base + '/api/positions?environment=TESTNET') -Method Get -TimeoutSec 20
$orders = Invoke-RestMethod -Uri ($base + '/api/orders?environment=TESTNET') -Method Get -TimeoutSec 20
[pscustomobject]@{
    AutoRunning = $auto.running
    AutoState = $auto.state
    AutoReadiness = $auto.readiness.overall
    AutoBlockers = (@($auto.blockers | Where-Object { $null -ne $_ } | ForEach-Object { $_.code }) -join ',')
    FeatureDecisions = $system.counters.feature_decisions
    EligibleDecisions = $system.counters.eligible_decisions
    AutoIntents = $system.counters.auto_execution_intents
    DemoSubmissionAttempts = $system.counters.actual_order_submits
    LastFeatureReason = $system.feature_diagnostics.last_eligibility_reason
    MissingLookbacks = ($system.feature_diagnostics.lookback_missing -join ',')
    OpenPositions = @($positions.positions | Where-Object { $null -ne $_ }).Count
    OpenOrders = @($orders.orders | Where-Object { $null -ne $_ }).Count
} | Format-List
