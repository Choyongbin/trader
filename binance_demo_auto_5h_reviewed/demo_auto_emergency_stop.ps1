# Manual fallback to disable NEW Binance Futures DEMO automatic entries.
# Does NOT close a position, cancel protective orders, or terminate Trader UI.
param([int]$Port = 18080)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$base = 'http://127.0.0.1:' + $Port
$status = Invoke-RestMethod -Uri ($base + '/api/status') -Method Get -TimeoutSec 20
if ($status.execution_mode -notmatch '^TESTNET LIVE') {
    throw ('Unexpected execution mode: ' + $status.execution_mode + '. Refusing control request.')
}
if ([string]::IsNullOrWhiteSpace([string]$status.csrf_token)) {
    throw 'Local CSRF token missing; cannot send emergency stop.'
}
$headers = @{ 'X-CSRF-Token' = [string]$status.csrf_token }
$null = Invoke-RestMethod -Uri ($base + '/api/auto-trading/emergency-stop?environment=TESTNET') -Method Post -ContentType 'application/json' -Headers $headers -Body '{}' -TimeoutSec 90
$state = Invoke-RestMethod -Uri ($base + '/api/auto-trading?environment=TESTNET') -Method Get -TimeoutSec 20
if ($state.running -ne $false) { throw 'Emergency stop was not confirmed. Inspect Demo account immediately.' }
Write-Host ('NEW ENTRIES DISABLED. Auto state=' + $state.state)
Write-Warning 'Existing positions and TP/SL are NOT closed. Keep Trader UI running until you check the Demo exchange account.'
