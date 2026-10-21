# ============================================================================
# 02_charger_hdfs.ps1
# TP6 - Hive et Data Warehouse distribue - Groupe 6
#
# Cree l'arborescence du Data Lake et y charge les trois volumes.
#
# ANOMALIE DE TRANSMISSION : cette arborescence est normalement livree par le
# Groupe 2. Au 13/10/2026 aucun livrable amont n'avait ete transmis, le Groupe 6
# la recree donc a l'identique de la structure imposee dans l'enonce. La
# reconstruction est documentee, elle n'est pas silencieuse.
#
# Mise a jour du 20/10/2026 : le rendu du Groupe 5 emploie exactement les memes
# chemins (/datalake/raw/transactions/{small,medium,big}/). La structure recreee
# ici est donc conforme a celle utilisee en amont.
#
#   /datalake/
#      raw/transactions/{small,medium,big}/   donnees sources, jamais modifiees
#      processed/                             donnees retravaillees (ORC partitionne)
#      results/{mapreduce,hive,spark,ml}/     sorties de chaque groupe
#
# Les fichiers sont lus depuis /projet/data, le dossier du projet etant monte
# dans le conteneur namenode par docker-compose.yml : aucun docker cp n'est
# necessaire.
#
# Usage :  .\scripts\02_charger_hdfs.ps1
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

# $args est la variable automatique qui recoit les arguments non lies : on ne
# la declare donc PAS dans un bloc param(), ce que PowerShell refuserait.
function Hdfs {
    docker exec namenode hdfs @args
    if ($LASTEXITCODE -ne 0) { throw ("echec : hdfs " + ($args -join " ")) }
}

Write-Host "--- 1. Arborescence du Data Lake ---"
foreach ($d in @(
    "/datalake/raw/transactions/small",
    "/datalake/raw/transactions/medium",
    "/datalake/raw/transactions/big",
    "/datalake/processed",
    "/datalake/results/mapreduce",
    "/datalake/results/hive",
    "/datalake/results/spark",
    "/datalake/results/ml")) {
    Hdfs dfs -mkdir -p $d
    Write-Host "  cree : $d"
}

Write-Host "`n--- 2. Chargement des trois volumes ---"
$volumes = @(
    @{ Fichier = "transactions_small.csv";     Cible = "/datalake/raw/transactions/small/"  },
    @{ Fichier = "transactions_medium.csv";    Cible = "/datalake/raw/transactions/medium/" },
    @{ Fichier = "transactions_big.csv.gz";    Cible = "/datalake/raw/transactions/big/"    }
)
foreach ($v in $volumes) {
    $local = Join-Path (Join-Path $projet "data") $v.Fichier
    if (-not (Test-Path $local)) {
        Write-Warning "$($v.Fichier) absent de data\ - lancer d'abord 00_preparer_donnees.ps1"
        continue
    }
    Write-Host "  chargement de $($v.Fichier)..."
    Hdfs dfs -put -f "/projet/data/$($v.Fichier)" $v.Cible
}

Write-Host "`n--- 3. Verification du contenu ---"
Hdfs dfs -ls -R /datalake

Write-Host "`n--- 4. Occupation : taille logique et taille avec replication ---"
# La deuxieme colonne de -du est l'espace reellement consomme (taille x facteur
# de replication). Avec dfs.replication=1 les deux colonnes sont egales : c'est
# le prix d'un cluster a un seul DataNode, et cela doit etre dit.
Hdfs dfs -du -h /datalake/raw/transactions

Write-Host "`n--- 5. Blocs et replication (fsck) ---"
# Nombre de blocs attendu : plafond(taille du fichier / taille de bloc),
# avec dfs.blocksize = 128 Mo (134217728 octets), conformement au calcul
# demande au Groupe 2.
foreach ($v in $volumes) {
    $local = Join-Path (Join-Path $projet "data") $v.Fichier
    if (Test-Path $local) {
        $taille  = (Get-Item $local).Length
        $attendu = [math]::Ceiling($taille / 134217728)
        Write-Host ("  {0} : {1:N0} octets -> {2} bloc(s) attendu(s)" -f $v.Fichier, $taille, $attendu)
    }
}
Hdfs fsck /datalake/raw/transactions -files -blocks

Write-Host "`nEtape suivante : .\scripts\03_executer_hive.ps1"
