# Fiche de démonstration — TP6 Hive, Groupe 6

Exposé 6 · *Big Data, Hadoop et applications Python* · Dr. Sena Apeke
Groupe 6 : AMAH-TCHOUTCHOUI Ekué Donald · FIOKLOU Donia · MINGANE Gracia · MOUN-RE Maxime

Protocole de la démonstration réalisée en soutenance : préparation, commandes, sorties attendues et lecture de ces sorties. Il est rédigé pour être **reproductible** par un lecteur qui n'a pas assisté à la séance.

Durée de la démonstration : environ 8 minutes, en direct sur le cluster.

---

## 1. Objet

La démonstration établit trois propositions, une par preuve :

| Proposition | Établie à l'étape | Preuve |
|---|---|---|
| Hive décrit des fichiers HDFS sans en être propriétaire | 3 | `Table Type : EXTERNAL_TABLE` |
| Le partitionnement réduit le volume réellement lu | 5 | le plan d'exécution : 21 127 lignes au lieu de ~500 000 |
| Les résultats sont justes au-delà de notre propre chaîne | 8 | 15 concordances sur 15 avec les sorties YARN du Groupe 5 |

Les étapes 1, 2, 4, 6 et 7 installent le contexte nécessaire à ces trois preuves.

---

## 2. Prérequis

### 2.1 Environnement

| Élément | Version ou valeur |
|---|---|
| Système | Windows 11, PowerShell 7 |
| Docker Desktop | lancé et stabilisé avant toute commande |
| Cluster | 5 conteneurs, voir `docker-compose.yml` |
| Python | celui du `.venv` du projet (`pyhive`, `duckdb`, `pandas`, `matplotlib`) |
| Base Hive | `datawarehouse` — **pas** `default` |

### 2.2 Séquence de mise en route (T-30 minutes)

```powershell
cd C:\Users\donal\Downloads\Hadoop\TP6_Hive_Groupe6
.\.venv\Scripts\Activate.ps1          # sinon : « No module named 'pyhive' »

.\scripts\01_demarrer_cluster.ps1     # 2 à 4 min — attend que HiveServer2 réponde
.\scripts\02_charger_hdfs.ps1         # 2 min — arborescence /datalake + les 3 volumes
.\scripts\03_executer_hive.ps1        # 4 min — exécute sql\analyse.hql
```

`03_executer_hive.ps1` doit se terminer par **« aucune erreur detectee dans le journal »** et lister les deux dossiers d'année. Il propose ensuite `python python\tp6_hive.py` : cette étape régénère les graphiques du rapport et **ne fait pas partie de la démonstration**.

### 2.3 Contrôle avant passage

```powershell
.\scripts\demo.ps1 -Verifier
```

Sortie attendue — huit `[OK]`, puis « Tout est pret pour la demonstration » :

```
  [OK]      namenode
  [OK]      datanode
  [OK]      hive-server
  [OK]      hive-metastore
  [OK]      hive-metastore-postgresql
  [OK]      HiveServer2 repond, base datawarehouse presente
  [OK]      table transactions
  [OK]      table transactions_part
```

### 2.4 Conditions matérielles

| Point | Raison |
|---|---|
| Terminal en plein écran, police ≥ 16 | lisibilité depuis le fond de la salle |
| Mise en veille désactivée | une question longue suffit à éteindre l'écran |
| Dossier `Rendu groupe 5` à côté du projet | l'étape 8 le lit par chemin relatif |

---

## 3. Exécution

```powershell
.\scripts\demo.ps1
```

Le script marque une pause après chaque étape ; l'opérateur donne le rythme.

| Usage | Commande |
|---|---|
| Rejouer une étape | `.\scripts\demo.ps1 -Etape 5` |
| Rejouer un groupe d'étapes | `.\scripts\demo.ps1 -Etape 3,4,5` |
| Répétition sans pauses | `.\scripts\demo.ps1 -SansPause` |
| Contrôle seul | `.\scripts\demo.ps1 -Verifier` |

Les huit étapes sont **indépendantes** : un incident sur l'une n'empêche pas les suivantes.

---

# 4. Les huit étapes

---

## Étape 1 — Le cluster

### Commande

```
docker compose ps
```

### Sortie attendue

Cinq conteneurs en `Up`. Chacun porte un rôle du cluster :

| Conteneur | Rôle | Ports publiés |
|---|---|---|
| `namenode` | annuaire HDFS : quel fichier, quels blocs, sur quelle machine. Ne contient aucune donnée. | 9000, 9870 (web) |
| `datanode` | les blocs eux-mêmes, sur le disque | 9864 |
| `hive-server` | HiveServer2 : reçoit le SQL, le traduit en jobs MapReduce | **10000**, 10002 (web) |
| `hive-metastore` | dictionnaire : tables, colonnes, types, partitions | 9083 |
| `hive-metastore-postgresql` | base PostgreSQL où ce dictionnaire est écrit | 5432 (non publié) |

La mention `(healthy)` sur `namenode` et `datanode` provient d'un test de santé défini dans l'image Docker.

### Lecture

Les deux premiers conteneurs constituent HDFS, les trois suivants la couche Hive. Ce sont des services distincts qui communiquent par le réseau : l'architecture est celle d'un cluster réel.

**Limite à relever :** un seul DataNode, donc `dfs.replication = 1`. Le dispositif démontre le fonctionnement du système, non sa tolérance aux pannes.

### Choix technique documenté

Docker a été retenu plutôt qu'une installation native : l'installation manuelle sous Windows exige Java, des variables d'environnement et SSH, et ne garantit pas un environnement identique entre les quatre membres du groupe.

---

## Étape 2 — Le Data Lake

### Commandes

```
hdfs dfs -ls -R /datalake/raw/transactions
hdfs dfs -du -h  /datalake/raw/transactions
```

### Sortie attendue

| Dossier | Fichier | Taille | Lignes |
|---|---|---|---|
| `raw/transactions/small/` | `transactions_small.csv` | 1,3 Mo | 10 000 |
| `raw/transactions/medium/` | `transactions_medium.csv` | 62,9 Mo | 500 000 |
| `raw/transactions/big/` | `transactions_big.csv.gz` | 138,5 Mo | 5 000 000 |

### Lecture

`hdfs dfs -du` affiche **deux colonnes** : la taille logique du fichier, puis l'espace réellement consommé sur le cluster, c'est-à-dire taille × facteur de réplication. Elles sont égales ici parce que la réplication vaut 1 ; sur un cluster à trois copies, la seconde colonne vaudrait le triple de la première.

**Nombre de blocs** — taille de bloc 128 Mo, soit ⌈taille ÷ 134 217 728⌉ :

| Fichier | Octets | Blocs attendus |
|---|---|---|
| `transactions_small.csv` | 1 319 956 | 1 |
| `transactions_medium.csv` | 65 977 458 | 1 |
| `transactions_big.csv.gz` | 145 205 662 | 2 |

Le calcul est effectué et vérifié par `hdfs fsck` dans `02_charger_hdfs.ps1`.

La couche `raw` est en lecture seule : ces fichiers servent également aux Groupes 7 (Spark) et 8 (Machine Learning).

---

## Étape 3 — La table externe

### Commande

```sql
DESCRIBE FORMATTED transactions
```

La commande renvoie la fiche descriptive de la table dans le metastore, non son contenu.

### Sortie attendue

Quatre lignes retenues par le filtre du script :

| Ligne | Lecture |
|---|---|
| `Location: hdfs://namenode:9000/datalake/raw/transactions/medium` | Emplacement des fichiers. C'est un **dossier** : tout CSV déposé dedans entre dans la table. |
| `Table Type: EXTERNAL_TABLE` | Hive n'est pas propriétaire des fichiers. |
| `EXTERNAL TRUE` | Propriété portant ce comportement. |
| `InputFormat: ...TextInputFormat` | Lecture ligne à ligne. La table ORC de l'étape 4 déclare `OrcInputFormat`. |

### Lecture

La création de cette table **n'a déplacé aucun octet** : Hive a seulement enregistré, dans son metastore, l'emplacement du dossier, les 19 colonnes à y lire et le séparateur de champs. C'est le principe du *schema on read* — le schéma s'applique à la lecture, et non à l'écriture comme dans une base relationnelle.

### Table externe contre table interne

| | Interne (managed) | Externe |
|---|---|---|
| Propriétaire des fichiers | Hive | HDFS, un autre groupe |
| `DROP TABLE` | efface schéma **et** données | efface le schéma seulement |
| Emplacement | `/user/hive/warehouse/` imposé | `LOCATION` libre |

Le choix de `EXTERNAL` est contraint par le projet : les données sont communes aux huit groupes, une table interne mal supprimée effacerait le Data Lake de la promotion.

### Détail d'implémentation

`TBLPROPERTIES ("skip.header.line.count"="1")` écarte la ligne de titres du CSV. Sans cette propriété, elle serait lue comme une transaction et fausserait tous les totaux.

---

## Étape 4 — Le partitionnement

### Commandes

```sql
SHOW PARTITIONS transactions_part
```
```
hdfs dfs -ls /datalake/processed/transactions_part/annee=2025
```

### Sortie attendue

`SHOW PARTITIONS` renvoie **24 entrées**, de `annee=2024/mois=1` à `annee=2025/mois=12`. L'ordre est alphabétique et non chronologique (`mois=10` précède `mois=2`) : les noms de dossiers sont comparés comme du texte.

La commande HDFS montre les douze dossiers `mois=…` de l'année 2025.

### Lecture

Une partition n'est pas une colonne stockée dans les fichiers : c'est un **chemin**. La valeur est portée par le nom du dossier, ce qui rend le partitionnement gratuit en espace et rend possible l'élagage démontré à l'étape 5.

Les 24 noms n'ont pas été écrits à la main : ils sont déduits des dates par le partitionnement dynamique (`PARTITION (annee, mois)` sans valeurs).

### Justification du choix de la clé

Partitionnement **par mois**, et non par ville, sur trois critères :

1. **Usage** — une clé de partition n'a d'effet que si les requêtes filtrent dessus. Les analyses demandées sont temporelles (CA mensuel, CA annuel) ; aucune ne filtre sur une ville seule.
2. **Équilibre** — les 24 partitions comptent environ 20 800 lignes chacune. Par ville, Lomé en concentrerait 40 % et Aného 5 %, soit un rapport de huit et une charge mal répartie.
3. **Nombre** — par jour : 730 partitions de 685 lignes ; par transaction : 500 000 partitions d'une ligne. Le problème des petits fichiers de HDFS serait atteint, chaque fichier occupant une entrée en mémoire du namenode quelle que soit sa taille.

### Piège d'implémentation

Dans l'`INSERT`, les colonnes de partition doivent être **les dernières du SELECT** : Hive les identifie par position et non par nom. Une inversion produit des partitions erronées sans lever d'erreur.

---

## Étape 5 — Le partition pruning

### Commandes

```sql
EXPLAIN SELECT COUNT(*) FROM transactions_part;
EXPLAIN SELECT COUNT(*) FROM transactions_part WHERE annee = 2025 AND mois = 3;
```

`EXPLAIN` **n'exécute pas** la requête : il renvoie le plan d'exécution, c'est-à-dire ce que le moteur compte faire et sur combien de lignes, avant toute lecture de données.

### Sortie attendue

Le script extrait la ligne `TableScan` de chaque plan :

| Requête | `Num rows` annoncé |
|---|---|
| sans filtre | ~500 000 — les 24 partitions |
| avec `WHERE annee = 2025 AND mois = 3` | **21 127** |

### Lecture

21 127 n'est pas une estimation : c'est **exactement** le nombre de transactions de mars 2025, vérifiable dans `resultats/hive/Q1a_ca_mensuel.csv` (ligne `2025,3,21127`). Hive a donc identifié la partition par le nom de son dossier et écarté les 23 autres **avant d'ouvrir le moindre fichier de données**.

La nature de la preuve importe : un temps d'exécution peut refléter un cache ou une machine chargée. Le plan d'exécution est une déclaration du moteur antérieure à tout calcul.

### Comparaison avec la table non partitionnée

Enregistrée dans `resultats/explain_pruning.txt`. Sur la table TEXTFILE avec `WHERE SUBSTR(datetime,1,7) = '2025-03'`, le `TableScan` annonce **500 000 lignes** : le filtre porte sur le contenu d'une colonne et non sur un chemin, aucune élimination préalable n'est possible.

Le même plan affiche plus bas `Num rows: 250000`. Cette valeur est l'estimation de Hive **après** l'opérateur de filtrage : ne pouvant évaluer la sélectivité du prédicat, il applique une règle par défaut et divise par deux. Ce n'est pas un comptage. **Le nombre de lignes effectivement lues est celui du `TableScan` : 500 000.**

Les deux requêtes renvoient le même résultat — 194 982 547,5 FCFA. Le partitionnement modifie le chemin d'accès, jamais le résultat.

---

## Étape 6 — Coût de stockage : TEXTFILE contre ORC

### Commandes

```
hdfs dfs -du -s -h /datalake/raw/transactions/medium
hdfs dfs -du -s -h /datalake/processed/transactions_part
```

### Sortie attendue

| Emplacement | Format | Taille |
|---|---|---|
| `raw/.../medium` | CSV (TEXTFILE) | 62,9 Mo |
| `processed/transactions_part` | ORC partitionné | 11,1 Mo |

Contenu identique — les mêmes 500 000 transactions. **Facteur 5,7.**

### Lecture

ORC est un format **orienté colonne** : chaque colonne ne contient que des valeurs de même nature, souvent répétées (huit villes sur 500 000 lignes), ce qui se compresse efficacement. Une requête ne portant que sur deux colonnes ne décompresse pas les dix-sept autres.

### Résultat contre-intuitif à signaler

Le gain d'ORC **n'est pas un gain de vitesse universel**. Mesures de `resultats/benchmark_hive.csv`, deux exécutions par cas, valeur minimale retenue :

| Requête | TEXTFILE | ORC partitionné | Effet |
|---|---|---|---|
| CA par ville (balayage complet) | 2,61 s | **2,79 s** | ORC légèrement plus lent — 24 fichiers à ouvrir au lieu d'un |
| CA de mars 2025 (requête filtrée) | 2,57 s | **1,62 s** | ORC 37 % plus rapide |

Le gain d'ORC est donc dans le **stockage** et dans les **requêtes sélectives**, non dans le balayage complet.

**Limite statistique :** deux exécutions par configuration ne permettent aucune conclusion de significativité. L'écart de 0,18 s sur le balayage complet est du même ordre que la variabilité de la machine. Le facteur 5,7 sur le stockage, lui, ne dépend d'aucune exécution.

### Le cas du jeu `big`

La même requête prend **13,58 s** sur le jeu de 5 millions de lignes, parce qu'il est stocké en gzip et **qu'un fichier gzip n'est pas splittable** : il est impossible de commencer la décompression au milieu du fichier. Un seul mapper traite l'ensemble pendant que le reste du cluster est inactif. C'est l'argument déterminant en faveur de la conversion en ORC.

---

## Étape 7 — Requête métier

### Commande

```sql
SELECT ville, format_number(SUM(montant), 0) AS ca_fcfa
FROM transactions
GROUP BY ville
ORDER BY SUM(montant) DESC;
```

`format_number` évite la notation scientifique (`1.838294035E9`). Le tri porte sur la somme et non sur la chaîne formatée.

### Sortie attendue

| Ville | CA (FCFA) |
|---|---|
| Lomé | 1 838 294 035 |
| Kara | 550 959 268 |
| Sokodé | 503 559 238 |
| Kpalimé | 416 192 485 |
| Atakpamé | 411 258 220 |
| Tsévié | 323 521 683 |
| Dapaong | 321 225 313 |
| Aného | 231 083 810 |

Total : **4 596 094 050 FCFA** sur 500 000 transactions, soit 9 192 FCFA de panier moyen.

La requête s'exécute en quelques secondes : chaque requête Hive déclenche un job. L'ordre de grandeur est la seconde, jamais la milliseconde.

### Lecture — réserves d'interprétation

Le jeu de données est **synthétique**. La lecture du code du générateur fourni invalide trois conclusions tentantes :

| Conclusion apparente | Ce que montre le générateur |
|---|---|
| Lomé est le marché dominant | `CITY_P = [.40, ...]` : 40 % des transactions lui sont attribuées par construction. |
| L'électronique se vend mieux | Les 21 produits sont tirés uniformément ; leurs quantités vendues tiennent dans 1,7 %. L'écart de chiffre d'affaires vient des prix (Powerbank 12 000 FCFA contre stylo 250). |
| Les promotions stimulent les ventes | La quantité suit une loi de Poisson tirée indépendamment de la promotion. L'égalité mesurée (3,201 contre 3,199) vérifie le générateur. |

Détail complet dans `docs/anomalies_et_divergences.md`.

---

## Étape 8 — Validation croisée avec le Groupe 5

### Commande

```
python python\comparer_groupe5.py
```

### Origine des références

Chaîne de production du projet :

> Groupe 3 conçoit les cinq jobs MapReduce → Groupe 4 les implémente en Hadoop Streaming → **Groupe 5 les exécute sous YARN** et livre les sorties dans `references_groupe3/{small,medium,big}/`.

Ces sorties ont été **confrontées** aux nôtres, non reprises.

### Sortie attendue

Quinze lignes `PASS`, « ecart max 0 », puis `15/15 concordances`. Rapport écrit dans `resultats/comparaison_hive_vs_groupe5.csv`.

Le moteur indiqué entre crochets est celui qui a produit **notre** chiffre :

| Volume | Notre moteur | Raison |
|---|---|---|
| `medium` | **Hive** | jeu de travail, sur lequel le cluster a tourné |
| `small` et `big` | **DuckDB** | Hive n'a tourné que sur `medium` ; les deux autres volumes sont comparés via le moteur de contrôle indépendant |

Le script signale en fin d'exécution ce qu'il ne compare pas : la colonne « CA brut » publiée par le Groupe 5, que nos jobs de référence ne produisent pas, et la colonne `nb_transactions` du job 3.

### Lecture

Une chaîne technique entièrement distincte — de vrais jobs MapReduce exécutés sous YARN, sur une autre machine, par un autre groupe — produit les mêmes nombres, à écart nul et non « sous tolérance ».

### Contrôle d'identité des jeux de données

```
python python\comparer_groupe5.py --md5
```

Recalcule les empreintes MD5 de nos trois jeux et les compare à celles publiées dans le `config.sh` du Groupe 5. Les trois concordent, y compris `transactions_big.csv.gz` que le Groupe 6 a généré lui-même (générateur de l'enseignant, graine 303). Durée : 20 à 30 secondes.

Ce contrôle conditionne le précédent : sans identité des fichiers sources, la concordance des agrégats ne démontrerait rien.

### Réserve

Le rendu du Groupe 5 **n'a pas encore été présenté** au jury et n'est donc pas validé. La concordance constatée est une vérification croisée, non une validation.

---

# 5. Annexes

## A. Résultats de référence

Valeurs permettant de contrôler qu'une exécution est conforme.

| Grandeur | Valeur | Source |
|---|---|---|
| Transactions (jeu `medium`) | 500 000 | `resultats/hive/Q7_transactions_par_ville.csv` |
| Chiffre d'affaires total | 4 596 094 050 FCFA | `resultats/hive/Q7_ca_par_ville.csv` |
| Panier moyen | 9 192 FCFA | calculé |
| Part de Lomé | 40 % | idem |
| Partitions créées | 24, ≈ 20 800 lignes chacune | `SHOW PARTITIONS` |
| Transactions de mars 2025 | 21 127 | `resultats/hive/Q1a_ca_mensuel.csv` |
| Stockage TEXTFILE → ORC | 62,9 Mo → 11,1 Mo (×5,7) | `hdfs dfs -du -s -h` |
| Balayage complet | 2,61 s / 2,79 s | `resultats/benchmark_hive.csv` |
| Requête filtrée | 2,57 s / 1,62 s | idem |
| Jeu `big`, même requête | 13,58 s | idem |
| Contrôles Hive ↔ DuckDB | 17 / 17, écart 0 | `resultats/comparaison_hive_vs_sql.csv` |
| Contrôles Hive ↔ MapReduce | 5 / 5, écart 0 | `resultats/comparaison_hive_vs_mapreduce.csv` |
| Contrôles Hive ↔ Groupe 5 | 15 / 15, écart 0 | `resultats/comparaison_hive_vs_groupe5.csv` |

## B. Incidents et traitement

| Incident | Cause | Traitement |
|---|---|---|
| `[ABSENTE] table transactions` | les tables sont dans la base `datawarehouse`, pas `default` | vérifier la ligne `$urlHive` de `demo.ps1` |
| `No module named 'pyhive'` | `.venv` non activé | `.\.venv\Scripts\Activate.ps1`, puis relancer |
| Un conteneur arrêté | — | `docker compose up -d` dans un second terminal, puis reprendre à l'étape suivante |
| HiveServer2 ne répond plus | — | `docker logs hive-server --tail 40` ; l'étape 8 reste disponible, elle ne sollicite pas Hive |
| L'étape 5 n'affiche aucun plan | mise en forme de beeline | les deux plans sont enregistrés dans `resultats/explain_pruning.txt` |
| Requête anormalement lente | — | requête `medium` : 2 à 3 s ; `COUNT` sur `big` : 8 à 14 s |
| Cluster indisponible | — | `resultats/` contient les CSV, graphiques et journaux de la dernière exécution ; la démonstration devient un commentaire de ces fichiers, ce qui doit être annoncé |

## C. Limites du dispositif

| Limite | Portée |
|---|---|
| Un seul DataNode, réplication 1 | aucune tolérance aux pannes démontrée |
| Cinq conteneurs sur une machine | maquette fonctionnelle, non cluster réparti |
| Deux exécutions par configuration | les écarts de durée sont indicatifs, non significatifs |
| Jeu de données synthétique | aucune conclusion commerciale valide |
| HiveServer2 en mode NOSASL | acceptable en TP local, exclu en production (Kerberos requis) |
| Références du Groupe 5 non encore validées par le jury | vérification croisée, non validation |
