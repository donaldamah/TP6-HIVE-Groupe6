# TP6 – Hive et Data Warehouse distribué (Groupe 6)

**Big Data, Hadoop et applications Python** – Exposé 6
Université Catholique de l'Afrique de l'Ouest – Unité Universitaire au Togo
École Supérieure d'Ingénieurs – Master 1 Big Data – Année 2025/2026

**Enseignant :** Dr. Sena Apeke

**Groupe 6 :** AMAH-TCHOUTCHOUI Ekué Donald · FIOKLOU Donia · MINGANE Gracia · MOUN-RE Maxime

---

## Objet

Construire une couche analytique SQL — un Data Warehouse distribué — au-dessus des données HDFS du fil conducteur : transactions commerciales de huit villes du Togo, en trois volumes (10 000, 500 000 et 5 000 000 lignes).

Le TP couvre la création des tables externes, l'organisation partitionnée au format ORC, les sept blocs de requêtes d'analyse, le dépôt des résultats dans le Data Lake pour le Groupe 7, et le contrôle de ces résultats par deux moteurs indépendants.

## Démarrage rapide

```powershell
# Environnement virtuel : PyHive et Thrift ne servent qu'à ce TP et n'ont rien
# à faire dans le Python système. À activer dans chaque session PowerShell,
# car les scripts appellent `python`.
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

.\scripts\00_preparer_donnees.ps1    # vérifie small et medium, génère big (~2 min)
.\scripts\01_demarrer_cluster.ps1    # docker compose + attente de HiveServer2
.\scripts\02_charger_hdfs.ps1        # arborescence /datalake + chargement
.\scripts\03_executer_hive.ps1       # exécute sql\analyse.hql
python python\tp6_hive.py            # export, benchmark, contrôles, graphiques
```

Sans cluster, pour régénérer graphiques et comparaisons depuis les CSV déjà présents :

```powershell
python python\tp6_hive.py --sans-hive
```

## Organisation du dépôt

```
TP6_Hive_Groupe6/
├── docker-compose.yml                   cluster : NameNode, DataNode, HiveServer2, metastore, PostgreSQL
├── hadoop-hive.env                      configuration Hadoop et Hive
├── requirements.txt
├── sql/
│   └── analyse.hql                      tables, partitionnement ORC, pruning, 7 blocs de requêtes
├── python/
│   ├── tp6_hive.py                      pilotage Hive, benchmark, contrôles, graphiques
│   ├── valider_sql.py                   mêmes requêtes sous DuckDB : les résultats attendus
│   ├── comparer_resultats.py            comparateur générique de dossiers de CSV
│   ├── mapper.py / reducer.py           référence MapReduce (Hadoop Streaming)
│   └── lancer_reference_mapreduce.sh    Map → Shuffle/Sort → Reduce en pipes Unix
├── scripts/                             00 à 03, préparation puis exécution
├── data/                                les trois volumes (non versionnés, reproductibles)
├── resultats/
│   ├── hive/                            sorties Hive avec en-tête
│   ├── hive_hdfs/                       sorties brutes rapatriées de HDFS
│   ├── attendus_sql/{small,medium,big}/ résultats attendus, calculés hors Hive
│   ├── reference_mapreduce/             sorties des 5 jobs MapReduce
│   ├── graphiques/                      figures du rapport
│   ├── benchmark_hive.csv               durées par représentation de stockage
│   ├── explain_pruning.txt              plans d'exécution, preuve du partition pruning
│   └── comparaison_*.csv                rapports de contrôle
└── docs/
    ├── anomalies_et_divergences.md      sections 3 et 14 de l'énoncé
    └── dictionnaire_resultats.md        ce que reçoit le Groupe 7
```

## Architecture du cluster

| Service | Image | Rôle |
|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | NameNode HDFS (9870, 9000) |
| `datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | DataNode HDFS (9864) |
| `hive-server` | `bde2020/hive:2.3.2-postgresql-metastore` | HiveServer2 (10000, 10002) |
| `hive-metastore` | `bde2020/hive:2.3.2-postgresql-metastore` | Metastore (9083) |
| `hive-metastore-postgresql` | `bde2020/hive-metastore-postgresql:2.3.0` | base du metastore |

HiveServer2 tourne en **NOSASL**, ce qui évite d'installer le module Python `sasl`, impossible à compiler sous Windows sans chaîne C++. Le dossier du projet est monté sous `/projet` dans `namenode` et `hive-server` : les scripts lisent `/projet/data` et `/projet/sql` directement, sans `docker cp`.

## Le Data Lake

L'arborescence respecte celle imposée au Groupe 2 :

```
/datalake/
   raw/transactions/{small,medium,big}/   données sources, jamais modifiées
   processed/transactions_part/           ORC partitionné par (année, mois), 24 partitions
   results/{mapreduce,hive,spark,ml}/     sorties de chaque groupe
```

**Elle a été créée par le Groupe 6**, le Groupe 2 n'ayant transmis aucun livrable. Cette reconstruction est documentée dans `docs/anomalies_et_divergences.md`, comme l'exige la règle de transmission.

## Modèle de données

Table externe `transactions` (TEXTFILE) sur le CSV brut, puis table externe `transactions_part` au format **ORC partitionné par `(annee, mois)`**.

Ce découpage donne 24 partitions d'environ 20 800 lignes : assez de partitions pour que le filtrage serve, assez de lignes par partition pour éviter le problème des petits fichiers. Partitionner par ville (8 valeurs très inégales, Lomé pesant 40 %) ou par identifiant de transaction serait inexploitable.

Le partitionnement est **démontré, pas seulement déclaré** : `analyse.hql` compare les plans d'exécution de la même question — le CA de mars 2025 — posée à la table partitionnée (une partition lue) et à la table TEXTFILE (balayage complet). `tp6_hive.py` en chronomètre l'écart.

## Contrôle des résultats

La consigne 14 de l'énoncé impose que toute valeur soit contrôlée par rapport à une autre technologie. Trois calculs indépendants sont confrontés automatiquement :

| Calcul | Outil | Rôle |
|---|---|---|
| Hive | HiveQL sur le cluster | le livrable |
| SQL de contrôle | DuckDB sur le CSV brut, mêmes requêtes | résultats attendus |
| MapReduce | `mapper.py` \| `sort` \| `reducer.py` | référence du modèle Map/Reduce |

`python/comparer_resultats.py` compare deux dossiers de CSV ligne à ligne, avec une tolérance de 0,01 FCFA, et produit un rapport `PASS`/`FAIL` par fichier.

**Résultats vérifiés par exécution** (jeu medium, 500 000 transactions, 13/10/2026) :

| Contrôle | Résultat |
|---|---|
| Qualité des données, mesurée dans Hive | 500 000 lignes, 500 000 identifiants distincts, 0 champ nul, 0 montant incohérent, 0 remise hors bornes, 0 stock incohérent |
| `montant` contre `quantité × prix × (1 − remise)` | écart maximal 7 × 10⁻¹² |
| Conservation à la conversion ORC | 500 000 lignes et 4 596 094 050 FCFA, identiques au TEXTFILE |
| MapReduce contre SQL | 5 jobs sur 5 concordants, écart nul |
| **Hive contre SQL** | **17 fichiers sur 17 concordants, écart nul** |
| **Hive contre MapReduce** | **5 fichiers sur 5 concordants, écart nul** |

Les rapports détaillés sont dans `resultats/comparaison_hive_vs_sql.csv` et `resultats/comparaison_hive_vs_mapreduce.csv`.

## Performances mesurées

Stockage, même contenu de 500 000 transactions :

| Format | Taille HDFS |
|---|---|
| TEXTFILE (CSV brut) | 62,9 Mo |
| ORC partitionné, compression ZLIB | **11,1 Mo** |

Soit un facteur **5,7**. Durées des requêtes (meilleure de deux exécutions) :

| Requête | TEXTFILE | ORC partitionné |
|---|---|---|
| CA par ville, toutes partitions | 2,72 s | 2,85 s |
| CA de mars 2025, filtré | 2,67 s | **1,65 s** |
| Comptage des lignes | 0,15 s | 0,20 s |
| CA par ville sur le jeu big (5 M lignes, gzip) | 13,63 s | — |

**À lire correctement, c'est le résultat le plus instructif du TP :** l'ORC partitionné n'accélère pas une agrégation qui balaie tout — il est même très légèrement plus lent, le gain de décompression étant absorbé par l'ouverture de 24 fichiers au lieu d'un. Son bénéfice est ailleurs : la taille divisée par 5,7, et le filtrage par partition, où la lecture annoncée par le plan d'exécution tombe de 250 000 à **21 127 lignes**, soit exactement le contenu de la partition de mars 2025.

Les deux méthodes donnent d'ailleurs le même chiffre pour mars 2025 : 21 127 transactions, 194 982 547,5 FCFA.

## Principaux résultats (jeu medium)

Chiffre d'affaires total : **4 596 094 050 FCFA** sur 500 000 transactions, soit 9 192 FCFA par transaction.

| Constat | Mesure |
|---|---|
| Lomé | 40 % des transactions et du chiffre d'affaires (1,84 Md FCFA) |
| Panier moyen | stable d'une ville à l'autre, autour de 9 200 FCFA |
| Promotions | 109 678 transactions (21,9 %), remise moyenne 13,17 %, coût 136,9 M FCFA |
| Panier moyen avec promotion | 8 231 FCFA contre 9 462 FCFA sans |
| Catégorie dominante | Électronique, 2,47 Md FCFA |

**Trois lectures à ne pas faire**, détaillées dans `docs/anomalies_et_divergences.md` :

1. les 21 produits sont tirés de façon équiprobable — le Top 10 classe les prix unitaires, pas les préférences d'achat ;
2. la quantité est tirée indépendamment de la promotion — l'égalité des quantités moyennes (0,06 % d'écart) est une propriété du générateur, pas un résultat commercial ;
3. les dates sont uniformes — les 12 % d'écart entre mois tombent à 3,9 % une fois ramenés au jour calendaire, et la dispersion résiduelle (0,79 %) est au niveau du bruit de Poisson attendu (0,69 %) : **il n'y a aucune saisonnalité**.

## Transmission au Groupe 7

Tables Hive dans la base `datawarehouse`, fichiers CSV dans `/datalake/results/hive/`, schémas et conventions de calcul dans `docs/dictionnaire_resultats.md`. Source recommandée pour PySpark : `spark.table("datawarehouse.transactions_part")`, qui permet la lecture par partition.

## Dépannage

| Symptôme | Cause et remède |
|---|---|
| `TSocket read 0 bytes` | HiveServer2 n'écoute pas encore : attendre 1 à 3 minutes, les scripts réessaient seuls |
| `docker` non reconnu | Docker Desktop absent ou non démarré |
| Conteneurs qui redémarrent en boucle | mémoire insuffisante : allouer 6 Go à WSL2 dans `C:\Users\<nom>\.wslconfig` |
| `ORDER BY` refusé sur une colonne absente du `SELECT` | limite de Hive 2.3 : la colonne de tri doit figurer dans le `SELECT` externe (cas de `ordre` en Q6) |
| Une requête Hive renvoie des `NULL` partout | l'en-tête du CSV est lu comme une donnée : vérifier `skip.header.line.count` |
| Le jeu big est très lent | gzip n'est pas splittable : un seul mapper traite tout le fichier |
| `ModuleNotFoundError: pyhive` | environnement virtuel non activé dans cette session : `.\.venv\Scripts\Activate.ps1` |
| `Activate.ps1 ... n'est pas signé` | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force` |

## Historique

Ce dépôt reprend et prolonge le travail initial de **MOUN-RE Maxime** ([Maxmoun/tp6-hive-groupe6](https://github.com/Maxmoun/tp6-hive-groupe6)), dont le cluster Docker, le choix du mode NOSASL et les premières requêtes HiveQL constituent la base. Les résultats qu'il avait produits ont été recalculés et confirmés au centime près par un moteur indépendant.

Ajouts de cette version : contrôles qualité dans Hive, démonstration effective du partition pruning, dépôt des résultats dans le Data Lake pour le Groupe 7, référence MapReduce réellement exécutée en Hadoop Streaming, benchmark interprétable par représentation de stockage, jeu big de 5 millions de lignes, documentation des anomalies et des divergences.
