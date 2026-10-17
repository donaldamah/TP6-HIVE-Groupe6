# ============================================================================
# 00_preparer_donnees.ps1
# TP6 - Hive et Data Warehouse distribue - Groupe 6
#
# Verifie la presence des trois volumes du fil conducteur dans data\ et
# reconstitue ce qui manque :
#   - small et medium sont copies depuis le dossier fourni par l'enseignant ;
#   - big (5 000 000 lignes) est genere par le generateur de l'enseignant,
#     puis compresse en .gz, format prevu par l'enonce.
#
# Usage :  .\scripts\00_preparer_donnees.ps1
# ============================================================================
$ErrorActionPreference = "Stop"
$projet  = Split-Path -Parent $PSScriptRoot
$data    = Join-Path $projet "data"
$source  = Join-Path (Split-Path -Parent $projet) "Donnees_TP_BigData"

New-Item -ItemType Directory -Force -Path $data | Out-Null

Write-Host "Projet : $projet"
Write-Host "Source enseignant : $source`n"

foreach ($f in @("transactions_small.csv", "transactions_medium.csv")) {
    $cible = Join-Path $data $f
    if (Test-Path $cible) {
        Write-Host "[OK]    $f deja present"
    } elseif (Test-Path (Join-Path $source $f)) {
        Copy-Item (Join-Path $source $f) $cible
        Write-Host "[COPIE] $f"
    } else {
        Write-Warning "$f introuvable dans $source"
    }
}

# --- volume big ------------------------------------------------------------
$big   = Join-Path $data "transactions_big.csv"
$biggz = "$big.gz"
if (Test-Path $biggz) {
    Write-Host "[OK]    transactions_big.csv.gz deja present"
} else {
    $gen = Join-Path $source "generateur_donnees.py"
    if (-not (Test-Path $gen)) { throw "Generateur introuvable : $gen" }
    Write-Host "[GEN]   generation de 5 000 000 lignes (environ 2 minutes)..."
    python $gen --n 5000000 --seed 303 --output $big
    Write-Host "[GZIP]  compression..."
    # Compression native .NET : produit un gzip standard, lu tel quel par Hive
    $entree = [System.IO.File]::OpenRead($big)
    $sortie = [System.IO.File]::Create($biggz)
    $gz     = New-Object System.IO.Compression.GzipStream($sortie, [System.IO.Compression.CompressionMode]::Compress)
    $entree.CopyTo($gz)
    $gz.Close(); $sortie.Close(); $entree.Close()
    Remove-Item $big
    Write-Host "[OK]    transactions_big.csv.gz genere"
}

Write-Host "`n--- Contenu de data\ ---"
Get-ChildItem $data | Select-Object Name, @{N="Taille (Mo)";E={[math]::Round($_.Length/1MB,1)}} | Format-Table -AutoSize

Write-Host "RAPPEL : les jeux small et medium fournis par l'enseignant ne sont pas"
Write-Host "reproductibles avec le seed 303 du generateur. Les trois volumes sont"
Write-Host "donc des tirages independants de la meme loi, et non des sous-ensembles"
Write-Host "les uns des autres. Voir docs\anomalies_et_divergences.md."
