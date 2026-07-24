$startDate = Get-Date "2025-07-23"
$endDate = Get-Date "2026-07-22"

$currentDate = $startDate
while ($currentDate -le $endDate) {
    $dateStr = $currentDate.ToString("yyyy-MM-dd")
    Write-Host "Running for date: $dateStr"
    python -m sg_air_quality.main --date $dateStr

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed on date: $dateStr (exit code $LASTEXITCODE)" -ForegroundColor Red
        # Uncomment the next line if you want the script to stop on first failure
        # break
    }

    $currentDate = $currentDate.AddDays(1)
}

Write-Host "Backfill complete."