# ============================================================================
# 03_executer_hive.ps1
# TP6 - Hive et Data Warehouse distribue - Groupe 6
#
# Execute sql\analyse.hql sur le cluster, conserve le journal complet, mesure
# le cout de stockage TEXTFILE contre ORC, puis rapatrie les resultats depuis
# HDFS vers resultats\hive_hdfs\.
#
# Les CSV rapatries ici sont les fichiers BRUTS du Data Lake, sans ligne
# d'en-tete : c'est ce que le Groupe 7 lira. Les memes resultats avec en-tete,
# destines a la comparaison et aux graphiques, sont produits par
# python\tp6_hive.py.
#
# Usage :  .\scripts\03_executer_hive.ps1
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
$resultats = Join-Path $projet "resultats"
New-Item -ItemType Directory -Force -Path $resultats, (Join-Path $resultats "hive_hdfs") | Out-Null

$journal = Join-Path $resultats "analyse_hive.log"

Write-Host "--- 1. Execution de analyse.hql (plusieurs minutes) ---"
Write-Host "    journal : $journal`n"
# /projet est le dossier du projet monte dans le conteneur hive-server
docker exec hive-server hive -f /projet/sql/analyse.hql 2>&1 | Tee-Object -FilePath $journal
$codeHive = $LASTEXITCODE

Write-Host "`n--- 2. Cout de stockage : TEXTFILE contre ORC ---"
Write-Host "    (meme contenu, 500 000 transactions, deux formats)"
docker exec namenode hdfs dfs -du -s -h /datalake/raw/transactions/medium
docker exec namenode hdfs dfs -du -s -h /datalake/processed/transactions_part

Write-Host "`n--- 3. Partitions creees ---"
docker exec namenode hdfs dfs -ls /datalake/processed/transactions_part

Write-Host "`n--- 4. Rapatriement des resultats depuis HDFS ---"
$dossiers = docker exec namenode hdfs dfs -ls -C /datalake/results/hive
foreach ($d in $dossiers) {
    $d = $d.Trim()
    if (-not $d) { continue }
    $nom = Split-Path $d -Leaf
    $cible = "/projet/resultats/hive_hdfs/$nom.csv"
    # getmerge concatene les fichiers de sortie du dossier en un seul CSV
    docker exec namenode hdfs dfs -getmerge $d $cible
    if ($LASTEXITCODE -eq 0) { Write-Host "  recupere : $nom.csv" }
}

Write-Host "`n--- 5. Controle du journal ---"
# On cherche les echecs de Hive, pas le bruit : les avertissements SLF4J et les
# messages que PowerShell ajoute autour des commandes natives ne sont pas des
# erreurs de requete et produisaient des faux positifs.
$erreurs = Select-String -Path $journal -Pattern "FAILED|ParseException|SemanticException|Execution Error" |
           Where-Object { $_.Line -notmatch "SLF4J|NativeCommandError|RemoteException|FullyQualifiedErrorId|CategoryInfo" }
if ($erreurs) {
    Write-Warning "$($erreurs.Count) ligne(s) d'erreur dans le journal :"
    $erreurs | Select-Object -First 10 | ForEach-Object { Write-Host "  $($_.Line)" }
} else {
    Write-Host "  aucune erreur detectee dans le journal."
}

if ($codeHive -ne 0) { Write-Warning "hive -f a renvoye le code $codeHive" }
Write-Host "`nEtape suivante : python python\tp6_hive.py"
