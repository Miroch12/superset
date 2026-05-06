$DownloadDir = "C:\Users\hp\Desktop\airflow\data"
$TargetFile  = "$DownloadDir\fatf_data.xlsx"

$Urls = @(
    "https://www.fatf-gafi.org/content/dam/fatf-gafi/Global-Network/4th-Round-Ratings.xlsx.coredownload.inline.xlsx",
    "https://www.fatf-gafi.org/content/dam/fatf/documents/4th-Round-Ratings.xlsx.coredownload.inline.xlsx"
)

$Headers = @{
    "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    "Referer"    = "https://www.fatf-gafi.org/en/publications/Mutualevaluations/Assessment-ratings.html"
    "Accept"     = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*"
}

$Success = $false

foreach ($Url in $Urls) {
    try {
        Write-Host "Essai : $Url"
        Invoke-WebRequest -Uri $Url -Headers $Headers -OutFile $TargetFile -TimeoutSec 60
        $Size = (Get-Item $TargetFile).Length / 1KB
        if ($Size -gt 50) {
            Write-Host "OK : $([math]::Round($Size,1)) KB"
            $Success = $true
            break
        } else {
            Write-Host "Fichier trop petit ($([math]::Round($Size,1)) KB)"
        }
    } catch {
        Write-Host "Echec : $_"
    }
}

if (-not $Success) {
    Write-Host "ERREUR: toutes les URLs ont echoue"
    exit 1
}

Write-Host "SUCCES"
exit 0