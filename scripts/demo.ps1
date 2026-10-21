# ============================================================================
# demo.ps1
# TP6 - Hive et Data Warehouse distribue - Groupe 6
#
# Sequence de demonstration en soutenance, en 8 etapes. Chaque etape affiche
# ce qu'elle va montrer, la commande executee, puis le resultat attendu, et
# attend une touche avant de passer a la suivante.
#
# PREREQUIS : le cluster doit DEJA tourner et les tables doivent exister.
#   .\scripts\01_demarrer_cluster.ps1   (plusieurs minutes : a lancer AVANT
#   .\scripts\02_charger_hdfs.ps1        d'entrer dans la salle)
#   .\scripts\03_executer_hive.ps1
#
# Usage :
#   .\scripts\demo.ps1                 toute la sequence, avec pauses
#   .\scripts\demo.ps1 -Etape 5        rejoue la seule etape 5
#   .\scripts\demo.ps1 -Etape 3,4,5    rejoue les etapes 3 a 5
#   .\scripts\demo.ps1 -SansPause      enchaine tout (repetition a blanc)
#   .\scripts\demo.ps1 -Verifier       controle seulement que tout est pret
# ============================================================================
param(
    [int[]]$Etape,
    [switch]$SansPause,
    [switch]$Verifier
)

# "Continue" et non "Stop" : docker, hdfs et hive ecrivent leurs avertissements
# (SLF4J, WARN Java) sur la sortie d'erreur. En mode "Stop", PowerShell en
# ferait des erreurs terminantes alors que la commande a reussi.
$ErrorActionPreference = "Continue"

$projet = Split-Path -Parent $PSScriptRoot
Set-Location $projet
# analyse.hql cree toutes les tables dans la base datawarehouse : s'y connecter
# directement, sinon SHOW TABLES et les requetes ne voient rien.
$urlHive = "jdbc:hive2://localhost:10000/datawarehouse;auth=noSasl"

# ---------------------------------------------------------------------------
# Presentation
# ---------------------------------------------------------------------------
function Titre($numero, $texte) {
    Write-Host ""
    Write-Host ("=" * 76) -ForegroundColor DarkCyan
    Write-Host ("  ETAPE {0} - {1}" -f $numero, $texte) -ForegroundColor Cyan
    Write-Host ("=" * 76) -ForegroundColor DarkCyan
}

function Commande($texte) {
    Write-Host "`n  > $texte" -ForegroundColor Yellow
    Write-Host ""
}

function Attendu($texte) {
    Write-Host "`n  Attendu : $texte" -ForegroundColor DarkGray
}

function Pause-Demo {
    if ($SansPause) { return }
    Write-Host "`n  [Entree] pour continuer, [Ctrl+C] pour arreter" -ForegroundColor DarkGray
    Read-Host | Out-Null
}

function Hive($requete) {
    docker exec hive-server beeline -u $urlHive --silent=true --outputformat=table -e $requete 2>&1 |
        Where-Object { $_ -notmatch 'SLF4J|^\s*$|Connected to|Driver:|Transaction isolation' }
}

# Meme chose, sans mise en tableau. Indispensable pour EXPLAIN et DESCRIBE
# FORMATTED : le format "table" aligne les colonnes sur la largeur du terminal
# et coupe les lignes longues en plein milieu, si bien qu'une recherche de
# "Num rows" ne trouve plus rien.
function HiveBrut($requete) {
    docker exec hive-server beeline -u $urlHive --silent=true --outputformat=tsv2 -e $requete 2>&1 |
        Where-Object { $_ -notmatch 'SLF4J|^\s*$|Connected to|Driver:|Transaction isolation' }
}

# ---------------------------------------------------------------------------
# Controle de l'etat du cluster (toujours execute en premier)
# ---------------------------------------------------------------------------
function Controler {
    Write-Host "`nControle de l'etat du cluster..." -ForegroundColor Cyan
    $pret = $true

    $conteneurs = (docker compose ps --services --filter "status=running" 2>&1) -split "`n" |
                  ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $attendus = @("namenode", "datanode", "hive-server", "hive-metastore", "hive-metastore-postgresql")
    foreach ($c in $attendus) {
        if ($conteneurs -contains $c) {
            Write-Host "  [OK]      $c"
        } else {
            Write-Host "  [ABSENT]  $c" -ForegroundColor Red
            $pret = $false
        }
    }

    $bases = (Hive "SHOW DATABASES;" | Out-String)
    if ($bases -match "datawarehouse") {
        Write-Host "  [OK]      HiveServer2 repond, base datawarehouse presente"
    } else {
        Write-Host "  [KO]      HiveServer2 ne repond pas, ou la base datawarehouse est absente" -ForegroundColor Red
        $pret = $false
    }

    $tables = (Hive "SHOW TABLES;" | Out-String)
    foreach ($t in @("transactions", "transactions_part")) {
        if ($tables -match $t) {
            Write-Host "  [OK]      table $t"
        } else {
            Write-Host "  [ABSENTE] table $t - relancer 03_executer_hive.ps1" -ForegroundColor Red
            $pret = $false
        }
    }

    if ($pret) {
        Write-Host "`nTout est pret pour la demonstration." -ForegroundColor Green
    } else {
        Write-Host "`nLe cluster n'est pas pret. Reprendre a 01_demarrer_cluster.ps1." -ForegroundColor Red
    }
    return $pret
}

# ---------------------------------------------------------------------------
# Les huit etapes
# ---------------------------------------------------------------------------
function Etape1 {
    Titre 1 "Le cluster : cinq conteneurs, un seul DataNode"
    Commande "docker compose ps"
    docker compose ps
    Attendu "5 conteneurs en Up : namenode, datanode, hive-server, hive-metastore, hive-metastore-postgresql."
    Write-Host "  A dire  : un cluster a un seul DataNode. La replication vaut 1 :" -ForegroundColor DarkGray
    Write-Host "            on demontre le fonctionnement, pas la tolerance aux pannes." -ForegroundColor DarkGray
    Pause-Demo
}

function Etape2 {
    Titre 2 "Le Data Lake recu en amont"
    Commande "hdfs dfs -ls -R /datalake | les trois volumes bruts"
    docker exec namenode hdfs dfs -ls -R /datalake/raw/transactions
    Commande "hdfs dfs -du -h /datalake/raw/transactions"
    docker exec namenode hdfs dfs -du -h /datalake/raw/transactions
    Attendu "small 1,3 Mo, medium 62,9 Mo, big 138,5 Mo compresse (5 000 000 lignes)."
    Write-Host "  A dire  : couche raw en lecture seule. Hive n'y touche pas : c'est la" -ForegroundColor DarkGray
    Write-Host "            raison de la table EXTERNAL, montree a l'etape suivante." -ForegroundColor DarkGray
    Pause-Demo
}

function Etape3 {
    Titre 3 "La table externe : le schema sans les donnees"
    Commande "DESCRIBE FORMATTED transactions"
    HiveBrut "DESCRIBE FORMATTED transactions;" |
        Select-String -Pattern "Location:|Table Type:|^EXTERNAL|InputFormat:" |
        ForEach-Object { "  " + ($_ -replace '\s+', ' ').Trim() }
    Attendu "Table Type : EXTERNAL_TABLE, Location : hdfs://namenode:9000/datalake/raw/transactions/medium"
    Write-Host "  A dire  : DROP TABLE effacerait le schema, pas les fichiers. Les groupes" -ForegroundColor DarkGray
    Write-Host "            7 et 8 lisent les memes donnees : une table interne les mettrait" -ForegroundColor DarkGray
    Write-Host "            en danger." -ForegroundColor DarkGray
    Pause-Demo
}

function Etape4 {
    Titre 4 "Le partitionnement : 24 dossiers dans HDFS"
    Commande "SHOW PARTITIONS transactions_part"
    Hive "SHOW PARTITIONS transactions_part;"
    Commande "hdfs dfs -ls /datalake/processed/transactions_part/annee=2025"
    # Le niveau superieur ne contient que annee=2024 et annee=2025 : c'est un
    # cran plus bas que les douze dossiers de mois deviennent visibles.
    docker exec namenode hdfs dfs -ls /datalake/processed/transactions_part/annee=2025
    Attendu "24 partitions : annee=2024 et annee=2025, douze mois chacune."
    Write-Host "  A dire  : la partition n'est pas une colonne du fichier, c'est un dossier." -ForegroundColor DarkGray
    Write-Host "            La valeur est ecrite dans le chemin. Decoupage par mois et non" -ForegroundColor DarkGray
    Write-Host "            par ville : 24 dossiers equilibres contre 8 dossiers dont un" -ForegroundColor DarkGray
    Write-Host "            contient 40 % des lignes (Lome)." -ForegroundColor DarkGray
    Pause-Demo
}

function Etape5 {
    Titre 5 "La preuve du partition pruning"
    function PlanLignes($requete) {
        $lignes = HiveBrut $requete | Select-String -Pattern "Num rows" | Select-Object -First 1
        if ($lignes) { $lignes | ForEach-Object { "  " + ($_ -replace '\s+', ' ').Trim() } }
        else { Write-Host "  (plan non lu : voir resultats\explain_pruning.txt)" -ForegroundColor Yellow }
    }

    Write-Host "`n  Sans filtre de partition :" -ForegroundColor White
    Commande "EXPLAIN SELECT COUNT(*) FROM transactions_part"
    PlanLignes "EXPLAIN SELECT COUNT(*) FROM transactions_part;"

    Write-Host "`n  Avec filtre sur annee et mois :" -ForegroundColor White
    Commande "EXPLAIN SELECT COUNT(*) FROM transactions_part WHERE annee=2025 AND mois=3"
    PlanLignes "EXPLAIN SELECT COUNT(*) FROM transactions_part WHERE annee = 2025 AND mois = 3;"

    Attendu "sans filtre : environ 500 000 lignes (les 24 partitions). Avec filtre : 21 127, soit exactement le nombre de transactions de mars 2025."
    Write-Host "  A dire  : Hive lit le nom des dossiers et ignore les 23 autres AVANT de" -ForegroundColor DarkGray
    Write-Host "            lire la moindre donnee. C'est le plan d'execution qui le dit," -ForegroundColor DarkGray
    Write-Host "            pas nous." -ForegroundColor DarkGray
    Pause-Demo
}

function Etape6 {
    Titre 6 "Le cout de stockage : TEXTFILE contre ORC"
    Commande "hdfs dfs -du -s -h sur les deux emplacements"
    Write-Host "  TEXTFILE (raw, medium) :"
    docker exec namenode hdfs dfs -du -s -h /datalake/raw/transactions/medium
    Write-Host "  ORC partitionne (processed) :"
    docker exec namenode hdfs dfs -du -s -h /datalake/processed/transactions_part
    Attendu "62,9 Mo contre 11,1 Mo : un facteur 5,7 pour un contenu identique."
    Write-Host "  A dire  : et la nuance qui compte - ORC ne rend PAS un balayage complet" -ForegroundColor DarkGray
    Write-Host "            plus rapide ici : 2,79 s contre 2,61 s en TEXTFILE. Mais il" -ForegroundColor DarkGray
    Write-Host "            divise par 1,6 le temps d'une requete filtree : 1,62 s contre" -ForegroundColor DarkGray
    Write-Host "            2,57 s. Le gain est dans le stockage et dans les requetes" -ForegroundColor DarkGray
    Write-Host "            selectives. (Source : resultats\benchmark_hive.csv)" -ForegroundColor DarkGray
    Pause-Demo
}

function Etape7 {
    Titre 7 "Une requete metier en direct"
    Commande "SELECT ville, format_number(SUM(montant),0) FROM transactions GROUP BY ville"
    # format_number evite la notation scientifique (1.838294035E9), illisible
    # pour un jury. Le tri se fait sur la somme, pas sur la chaine formatee.
    Hive ("SELECT ville, format_number(SUM(montant), 0) AS ca_fcfa FROM transactions " +
          "GROUP BY ville ORDER BY SUM(montant) DESC;")
    Attendu "8 villes, Lome en tete avec 1 838 294 035 FCFA, total 4 596 094 050 FCFA."
    Write-Host "  A dire  : Lome represente 40 % des transactions par construction du jeu." -ForegroundColor DarkGray
    Write-Host "            C'est un parametre du generateur, pas une decouverte metier." -ForegroundColor DarkGray
    Pause-Demo
}

function Etape8 {
    Titre 8 "La validation croisee avec le Groupe 5"
    Commande "python python\comparer_groupe5.py"
    # Le .venv du projet porte duckdb, pandas et pyhive ; le python systeme non.
    # comparer_groupe5.py n'utilise que la bibliotheque standard, mais on reste
    # coherent avec le reste du projet.
    $py = Join-Path $projet ".venv\Scripts\python.exe"
    if (-not (Test-Path $py)) { $py = "python" }
    & $py (Join-Path $projet "python\comparer_groupe5.py")
    Attendu "15/15 concordances sur small, medium et big."
    Write-Host "  A dire  : ces references viennent des jobs MapReduce du Groupe 3," -ForegroundColor DarkGray
    Write-Host "            implementes par le Groupe 4 et reellement executes sous YARN" -ForegroundColor DarkGray
    Write-Host "            par le Groupe 5. Deux chaines techniques independantes," -ForegroundColor DarkGray
    Write-Host "            le meme resultat au centime." -ForegroundColor DarkGray
    Pause-Demo
}

# ---------------------------------------------------------------------------
# Deroulement
# ---------------------------------------------------------------------------
$pret = Controler
if ($Verifier) { exit ([int](-not $pret)) }
if (-not $pret) {
    Write-Host "`nDemonstration interrompue : corriger l'etat du cluster d'abord." -ForegroundColor Red
    exit 1
}

$aJouer = if ($Etape) { $Etape } else { 1..8 }
Pause-Demo
foreach ($n in $aJouer) {
    switch ($n) {
        1 { Etape1 }
        2 { Etape2 }
        3 { Etape3 }
        4 { Etape4 }
        5 { Etape5 }
        6 { Etape6 }
        7 { Etape7 }
        8 { Etape8 }
        default { Write-Warning "Etape $n inconnue (1 a 8)" }
    }
}

Write-Host "`n"
Write-Host ("=" * 76) -ForegroundColor DarkCyan
Write-Host "  Fin de la demonstration." -ForegroundColor Cyan
Write-Host ("=" * 76) -ForegroundColor DarkCyan
