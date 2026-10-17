# ============================================================================
# 01_demarrer_cluster.ps1
# TP6 - Hive et Data Warehouse distribue - Groupe 6
#
# Demarre le cluster Hadoop + Hive et attend que HiveServer2 reponde
# reellement. Le port 10000 est publie par Docker des le demarrage du
# conteneur, alors que HiveServer2 met encore 1 a 3 minutes a ecouter : un
# simple test de port renvoie donc un faux positif. On interroge donc le
# metastore via la ligne de commande Hive, qui ne repond que lorsque le
# service est pret.
#
# Usage :  .\scripts\01_demarrer_cluster.ps1
# ============================================================================
# IMPORTANT : "Continue" et non "Stop".
# docker, hdfs et hive ecrivent leurs avertissements (SLF4J, WARN Java) sur la
# sortie d'erreur. Avec $ErrorActionPreference = "Stop", PowerShell transforme
# la moindre de ces lignes en erreur terminante et interrompt le script alors
# que la commande a parfaitement reussi. Les echecs reels sont donc detectes
# ici par le code de retour $LASTEXITCODE, pas par le flux d'erreur.
$ErrorActionPreference = "Continue"
$projet = Split-Path -Parent $PSScriptRoot
Set-Location $projet

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker est introuvable. Installer Docker Desktop, puis le lancer une fois."
}

Write-Host "Demarrage du cluster..."
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up a echoue" }

Write-Host "`nEtat des conteneurs :"
docker compose ps

Write-Host "`nAttente de HiveServer2 (jusqu'a 4 minutes)..."
# On interroge HiveServer2 par beeline sur le port 10000, et non par la ligne
# de commande hive : celle-ci parle directement au metastore et repondrait
# meme si HiveServer2 n'ecoutait pas encore. Or c'est bien le port 10000 dont
# PyHive a besoin. L'URL porte auth=noSasl, conformement a hadoop-hive.env.
$urlHive = "jdbc:hive2://localhost:10000/default;auth=noSasl"
$pret = $false
$derniereSortie = ""
for ($i = 1; $i -le 24; $i++) {
    $derniereSortie = (docker exec hive-server beeline -u $urlHive -e "SHOW DATABASES;" 2>&1 | Out-String)
    if ($LASTEXITCODE -eq 0 -and $derniereSortie -match "database_name|default") { $pret = $true; break }
    Write-Host ("  [{0}/24] pas encore pret, nouvelle tentative dans 10 s..." -f $i)
    Start-Sleep -Seconds 10
}

if (-not $pret) {
    Write-Warning "HiveServer2 ne repond pas. Derniere sortie de beeline :"
    Write-Host $derniereSortie
    Write-Host "Diagnostic :"
    Write-Host "  docker compose ps"
    Write-Host "  docker logs hive-server --tail 40"
    Write-Host "  docker logs hive-metastore --tail 40"
    exit 1
}

Write-Host "`nHiveServer2 est pret."
Write-Host "  NameNode      http://localhost:9870"
Write-Host "  DataNode      http://localhost:9864"
Write-Host "  HiveServer2   http://localhost:10002  (Thrift sur le port 10000)"
Write-Host "`nEtape suivante : .\scripts\02_charger_hdfs.ps1"
