param(
    [string]$Root = "F:\binance_trader\history_external\raw\binance",
    [datetime]$StartDate = [datetime]"2024-01-01",
    [datetime]$EndDate   = [datetime]"2025-12-31",
    [switch]$SkipSpotAggTrades
)

$ErrorActionPreference = "Stop"

$BaseUrl = "https://data.binance.vision/data"
$Symbol  = "BTCUSDT"
$Failures = [System.Collections.Generic.List[string]]::new()

function Ensure-Dir {
    param([string]$Path)
    if (!(Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Get-RemoteFile {
    param([string]$RelativePath, [string]$Destination)

    Ensure-Dir (Split-Path -Parent $Destination)
    $Url = "$BaseUrl/$RelativePath"
    $Tmp = "$Destination.part"

    if (Test-Path -LiteralPath $Tmp) {
        Remove-Item -LiteralPath $Tmp -Force
    }

    Write-Host "[DOWNLOAD] $RelativePath"

    & curl.exe --fail --location --silent --show-error --retry 5 --retry-delay 2 --connect-timeout 20 --output $Tmp $Url

    if ($LASTEXITCODE -ne 0) {
        if (Test-Path -LiteralPath $Tmp) {
            Remove-Item -LiteralPath $Tmp -Force
        }
        throw "download failed: $Url"
    }

    Move-Item -LiteralPath $Tmp -Destination $Destination -Force
}

function Get-ExpectedSha256 {
    param([string]$ChecksumPath)

    $Text = (Get-Content -LiteralPath $ChecksumPath -Raw).Trim()
    if ([string]::IsNullOrWhiteSpace($Text)) {
        throw "empty checksum file: $ChecksumPath"
    }

    $Token = ($Text -split "\s+")[0].Trim().ToLowerInvariant()

    if ($Token -notmatch '^[0-9a-f]{64}$') {
        throw "invalid checksum format: $ChecksumPath"
    }

    return $Token
}

function Test-ZipChecksum {
    param([string]$ZipPath, [string]$ChecksumPath)

    if (!(Test-Path -LiteralPath $ZipPath)) { return $false }
    if (!(Test-Path -LiteralPath $ChecksumPath)) { return $false }

    $Expected = Get-ExpectedSha256 $ChecksumPath
    $Actual = (Get-FileHash -LiteralPath $ZipPath -Algorithm SHA256).Hash.ToLowerInvariant()

    return ($Expected -eq $Actual)
}

function Download-Archive {
    param([string]$RelativeZip)

    $ZipPath = Join-Path $Root $RelativeZip
    $ChecksumPath = "$ZipPath.CHECKSUM"
    $RelativeChecksum = "$RelativeZip.CHECKSUM"

    try {
        if (!(Test-Path -LiteralPath $ChecksumPath)) {
            Get-RemoteFile -RelativePath $RelativeChecksum -Destination $ChecksumPath
        }

        if (Test-ZipChecksum -ZipPath $ZipPath -ChecksumPath $ChecksumPath) {
            Write-Host "[OK] $RelativeZip"
            return
        }

        if (Test-Path -LiteralPath $ZipPath) {
            Write-Warning "기존 ZIP checksum 불일치. 다시 다운로드합니다: $RelativeZip"
            Remove-Item -LiteralPath $ZipPath -Force
        }

        Get-RemoteFile -RelativePath $RelativeZip -Destination $ZipPath

        if (!(Test-ZipChecksum -ZipPath $ZipPath -ChecksumPath $ChecksumPath)) {
            Remove-Item -LiteralPath $ChecksumPath -Force
            Get-RemoteFile -RelativePath $RelativeChecksum -Destination $ChecksumPath

            if (!(Test-ZipChecksum -ZipPath $ZipPath -ChecksumPath $ChecksumPath)) {
                throw "SHA256 mismatch after download: $RelativeZip"
            }
        }

        Write-Host "[OK] $RelativeZip"
    }
    catch {
        Write-Warning $_
        $Failures.Add($RelativeZip)
    }
}

function Get-MonthStartList {
    param([datetime]$From, [datetime]$To)

    $Current = Get-Date -Year $From.Year -Month $From.Month -Day 1
    $Last = Get-Date -Year $To.Year -Month $To.Month -Day 1

    while ($Current -le $Last) {
        $Current
        $Current = $Current.AddMonths(1)
    }
}

Ensure-Dir $Root

Write-Host ""
Write-Host "============================================================"
Write-Host "BTCUSDT Feature V2 historical raw download"
Write-Host "Period : $($StartDate.ToString('yyyy-MM-dd')) ~ $($EndDate.ToString('yyyy-MM-dd'))"
Write-Host "Root   : $Root"
Write-Host "============================================================"

Write-Host ""
Write-Host "[1/6] USD-M Futures metrics"

$Day = $StartDate.Date
while ($Day -le $EndDate.Date) {
    $YMD = $Day.ToString("yyyy-MM-dd")
    Download-Archive "futures/um/daily/metrics/$Symbol/$Symbol-metrics-$YMD.zip"
    $Day = $Day.AddDays(1)
}

$Months = @(Get-MonthStartList -From $StartDate -To $EndDate)

Write-Host ""
Write-Host "[2/6] USD-M markPriceKlines 1m"
foreach ($Month in $Months) {
    $YM = $Month.ToString("yyyy-MM")
    Download-Archive "futures/um/monthly/markPriceKlines/$Symbol/1m/$Symbol-1m-$YM.zip"
}

Write-Host ""
Write-Host "[3/6] USD-M indexPriceKlines 1m"
foreach ($Month in $Months) {
    $YM = $Month.ToString("yyyy-MM")
    Download-Archive "futures/um/monthly/indexPriceKlines/$Symbol/1m/$Symbol-1m-$YM.zip"
}

Write-Host ""
Write-Host "[4/6] USD-M premiumIndexKlines 1m"
foreach ($Month in $Months) {
    $YM = $Month.ToString("yyyy-MM")
    Download-Archive "futures/um/monthly/premiumIndexKlines/$Symbol/1m/$Symbol-1m-$YM.zip"
}

Write-Host ""
Write-Host "[5/6] USD-M fundingRate"
foreach ($Month in $Months) {
    $YM = $Month.ToString("yyyy-MM")
    Download-Archive "futures/um/monthly/fundingRate/$Symbol/$Symbol-fundingRate-$YM.zip"
}

if (!$SkipSpotAggTrades) {
    Write-Host ""
    Write-Host "[6/6] Spot BTCUSDT aggTrades"

    foreach ($Month in $Months) {
        $YM = $Month.ToString("yyyy-MM")
        Download-Archive "spot/monthly/aggTrades/$Symbol/$Symbol-aggTrades-$YM.zip"
    }
}
else {
    Write-Host ""
    Write-Host "[6/6] Spot BTCUSDT aggTrades SKIPPED"
}

Write-Host ""
Write-Host "============================================================"

if ($Failures.Count -gt 0) {
    $FailureFile = Join-Path $Root "download_failures.txt"
    $Failures | Sort-Object -Unique | Set-Content -LiteralPath $FailureFile -Encoding UTF8

    Write-Warning "다운로드 실패가 있습니다."
    Write-Warning "Failure count: $($Failures.Count)"
    Write-Warning "목록: $FailureFile"
    Write-Host "같은 스크립트를 다시 실행하면 정상 파일은 checksum 검증 후 건너뜁니다."
    exit 1
}

$FailureFile = Join-Path $Root "download_failures.txt"
if (Test-Path -LiteralPath $FailureFile) {
    Remove-Item -LiteralPath $FailureFile -Force
}

$ZipFiles = @(Get-ChildItem -LiteralPath $Root -Recurse -File -Filter *.zip)
$ChecksumFiles = @(Get-ChildItem -LiteralPath $Root -Recurse -File -Filter *.CHECKSUM)
$PartFiles = @(Get-ChildItem -LiteralPath $Root -Recurse -File -Filter *.part)

$TotalBytes = ($ZipFiles | Measure-Object -Property Length -Sum).Sum
if ($null -eq $TotalBytes) { $TotalBytes = 0 }

Write-Host "ALL DOWNLOADS COMPLETE"
Write-Host "ZIP count      : $($ZipFiles.Count)"
Write-Host "CHECKSUM count : $($ChecksumFiles.Count)"
Write-Host "PART count     : $($PartFiles.Count)"
Write-Host ("ZIP size       : {0:N2} GiB" -f ($TotalBytes / 1GB))
Write-Host "Root           : $Root"
Write-Host ""
Write-Host "중요:"
Write-Host "- ZIP을 풀지 마세요."
Write-Host "- SHA-256만 검증했으며 time-series gap/duplicate audit은 아직 하지 않았습니다."
Write-Host "- Spot aggTrades는 2025-01-01부터 timestamp가 microseconds입니다."
Write-Host "============================================================"
