# Anomalies, divergences et hypothèses

**TP6 – Hive et Data Warehouse distribué – Groupe 6**
M1 Big Data, UCAO-UUT – Dr. Sena Apeke – 2025/2026

Ce document répond à deux obligations de l'énoncé :

- **Section 3 – Règle de transmission** : « Il est interdit de reconstruire silencieusement une sortie manquante : toute correction doit être documentée. »
- **Section 14 – Consigne importante** : « Une valeur obtenue avec une technologie doit être contrôlée par rapport aux résultats antérieurs. Les divergences doivent être expliquées. »

Tout ce qui suit a été **vérifié par exécution**, pas supposé. Chaque constat indique comment le reproduire.

---

## 1. Anomalies de transmission entre groupes

### 1.1 Le livrable amont est arrivé le 20/10/2026, après la construction du projet

**Mise à jour du 20/10/2026.** Le Groupe 5 a transmis son rendu (`group5_yarn`) : les références MapReduce des Groupes 3 et 4 pour les trois volumes, les empreintes MD5 des jeux de données, le benchmark de ressources YARN et ses journaux. Ce rendu n'a pas encore été présenté au jury : il n'est donc pas validé, et nous le traitons comme une vérification croisée, pas comme une autorité.

Le contrôle est automatisé par `python/comparer_groupe5.py` et donne **15 concordances sur 15** (small, medium, big ; 5 jobs par volume ; écart maximal 0). L'option `--md5` ajoute la vérification que nos trois jeux de données sont **identiques bit à bit** à ceux du Groupe 3, y compris le `big` que nous avions généré nous-mêmes. Rapport : `resultats/comparaison_hive_vs_groupe5.csv`.

Ce qui suit décrit la situation telle qu'elle était au moment de la construction du projet, et reste la justification de nos choix.

À la date de construction (13/10/2026), les Groupes 1 à 5 n'avaient transmis aucun livrable : ni Data Lake HDFS (Groupe 2), ni résultats de référence MapReduce (Groupe 3), ni sorties HDFS des jobs Hadoop Streaming (Groupe 4), ni `benchmark_yarn.csv` (Groupe 5). Le Groupe 4 n'avait pas encore présenté.

**Ce que le Groupe 6 a fait, et qui doit être su :**

| Élément manquant | Ce qui a été fait | Où |
|---|---|---|
| Arborescence `/datalake` (Groupe 2) | Recréée à l'identique de la structure imposée dans l'énoncé, sans l'inventer | `scripts/02_charger_hdfs.ps1` |
| Résultats de référence MapReduce (Groupes 3 et 4) | Réimplémentés en Hadoop Streaming réel : `mapper.py`, `reducer.py`, exécutés en Map → Shuffle/Sort → Reduce | `python/mapper.py`, `python/reducer.py` |
| Jeu `transactions_big` | Généré avec le générateur fourni par l'enseignant, seed 303 | `scripts/00_preparer_donnees.ps1` |

**Cette limite est levée depuis le 20/10/2026.** Nos références MapReduce restaient les nôtres : elles validaient la *logique analytique* de Hive, pas la chaîne Hadoop réelle. Les sorties du Groupe 5, produites par de vrais jobs Hadoop Streaming sous YARN, concordent avec les nôtres sur les trois volumes — la chaîne complète est donc vérifiée, et pas seulement notre raisonnement. La seule réserve qui demeure : ce travail n'est pas encore passé devant le jury.

**Pourquoi un vrai Hadoop Streaming et non une agrégation pandas :** une agrégation pandas aurait comparé Hive à pandas. Le pipeline `mapper.py | sort | reducer.py` reproduit le modèle de calcul du Groupe 3 et le script est exécutable tel quel sur Hadoop (`-mapper mapper.py -combiner reducer.py -reducer reducer.py`) : les chiffres seront donc directement confrontables.

---

## 2. Divergences entre le dictionnaire des variables et les données

### 2.1 Modes de paiement : 5 modalités au lieu de 4

Le dictionnaire annonce « Espèces, Mobile Money, carte ou virement ». Les données contiennent **cinq** modalités : `Especes`, `TMoney`, `Flooz`, `Carte`, `Virement`.

Le « Mobile Money » du dictionnaire est éclaté en ses deux opérateurs togolais, **TMoney et Flooz, qui pèsent ensemble 45 % des transactions** (25 % + 20 % dans le générateur).

**Décision : aucune correction.** Regrouper TMoney et Flooz modifierait les données du fil conducteur et romprait la comparabilité avec les autres groupes. La divergence est signalée, les données restent intactes. Si une analyse « Mobile Money » est demandée, le regroupement doit être fait dans la requête, explicitement.

### 2.2 Villes sans accents

Le dictionnaire, l'énoncé et les rapports écrivent « Lomé, Sokodé, Kpalimé, Atakpamé, Tsévié, Aného ». Les données contiennent `Lome, Sokode, Kpalime, Atakpame, Tsevie, Aneho`, **sans accents**.

**Conséquence pratique :** tout filtre ou toute jointure sur un libellé accentué renvoie zéro ligne. À signaler au Groupe 7, qui joindra ces libellés dans PySpark.

---

## 3. Propriétés du jeu de données qui conditionnent l'interprétation

Ces points ne sont pas des défauts : ce sont des propriétés du générateur (`Donnees_TP_BigData/generateur_donnees.py`) qui **invalident certaines conclusions qu'on serait tenté de tirer**. Les ignorer serait une faute d'analyse.

### 3.1 Les produits sont équiprobables : le Top 10 mesure les prix, pas la demande

Le générateur tire l'indice produit uniformément parmi 21 produits. Les quantités vendues sont donc quasi identiques (75 158 à 76 416 sur le jeu medium, soit 1,7 % d'écart).

**Conséquence :** « l'Électronique domine le chiffre d'affaires » ne traduit aucun comportement d'achat, seulement l'échelle des prix unitaires (Powerbank 12 000 FCFA contre Stylo 250 FCFA). Un classement par quantité vendue serait du pur bruit.

### 3.2 Les promotions n'ont aucun effet sur les quantités, par construction

Dans le générateur, `quantite = Poisson(2.2) + 1` est tiré **indépendamment** de `promotion = Bernoulli(0.22)`.

Mesuré : quantité moyenne de 3,201 sans promotion contre 3,199 avec — **0,06 % d'écart**.

**Conséquence :** conclure que « les promotions ne font pas vendre davantage » serait une erreur. Le jeu ne contient aucun mécanisme d'élasticité ; l'égalité observée est la vérification que le générateur fait ce qu'il annonce, rien de plus.

### 3.3 Il n'y a pas de saisonnalité : les écarts mensuels sont la longueur des mois

Le chiffre d'affaires mensuel varie de 12 % entre le mois le plus fort et le plus faible, ce qui ressemble à une saisonnalité. **Ce n'en est pas une.**

En ramenant au nombre de transactions **par jour calendaire** :

| Mesure | Amplitude |
|---|---|
| Transactions par mois | 12,0 % |
| Transactions par jour calendaire | 3,9 % |
| Écart-type relatif par jour | 0,79 % |
| Bruit de Poisson attendu (1/√n) | 0,69 % |

La dispersion résiduelle est **au niveau du bruit statistique**. Les dates sont tirées uniformément sur 730 jours : il ne peut y avoir ni saisonnalité, ni effet de fin de mois, ni tendance. Les creux de février (28 ou 29 jours) sont un artefact calendaire.

Reproduction : `resultats/attendus_sql/medium/Q1a_ca_mensuel.csv`, divisé par le nombre de jours de chaque mois.

### 3.4 Décembre 2025 est tronqué

La dernière transaction est datée du **30/12/2025 23:55**. Le générateur tire sur `2 × 365 = 730` jours à partir du 01/01/2024 : le 31 décembre 2025 n'existe pas dans les données. Décembre 2025 compte donc 30 jours, pas 31 — à prendre en compte dans toute comparaison mensuelle.

### 3.5 Les trois volumes ne sont pas emboîtés

Les jeux `small` et `medium` fournis par l'enseignant **ne sont pas reproductibles** avec le générateur et son seed par défaut (303). Vérifié sur douze seeds usuels (303, 42, 0, 1, 7, 100, 123, 2024, 2025, 777, 1234, 2026) : aucun ne reproduit la première ligne du fichier fourni.

**Conséquence :** `small` n'est pas un sous-ensemble de `medium`, qui n'est pas un préfixe de `big`. Les trois sont des tirages **indépendants de la même loi**. Leurs totaux ne sont donc pas proportionnels au nombre de lignes, mais statistiquement comparables :

| Volume | Transactions | CA total (FCFA) | CA moyen par transaction |
|---|---:|---:|---:|
| small | 10 000 | 91 926 192,5 | 9 192,6 |
| medium | 500 000 | 4 596 094 050,0 | 9 192,2 |
| big | 5 000 000 | 45 979 512 357,5 | 9 196,0 |

Le jeu `big` du Groupe 6 a été généré avec le seed documenté 303 : **tout groupe relançant `python generateur_donnees.py --n 5000000 --seed 303` obtiendra un fichier identique au nôtre.** Vérifié le 20/10/2026 : l'empreinte MD5 de notre `big` décompressé (`ea57ad7e97c0b4f6ca49296cb204e933`) est celle que le Groupe 5 publie pour le fichier du Groupe 3. Les empreintes de `small` et `medium` concordent également. Les trois jeux sont donc les mêmes fichiers, pas seulement des tirages comparables — ce qui donne son sens à la concordance des totaux.

---

## 4. Points techniques à signaler aux groupes suivants

### 4.1 Un fichier gzip n'est pas splittable

`transactions_big.csv.gz` (145 Mo compressés, 660 Mo en clair) occupe 2 blocs HDFS de 128 Mo, mais gzip ne permet pas de démarrer la lecture au milieu d'un fichier : **un seul mapper traite tout le fichier**, quel que soit le nombre de blocs. C'est un goulet d'étranglement que le Groupe 5 doit observer sur YARN, et la raison pour laquelle la couche `processed` est convertie en ORC, qui est splittable et compressé par bandes.

### 4.2 Réplication à 1

Le cluster ne comporte qu'un seul DataNode : `dfs.replication=1`. L'espace consommé est donc égal à la taille logique, et aucune résilience n'est démontrable. Sur un cluster réel à réplication 3, l'occupation serait triplée. À dire explicitement plutôt que de laisser croire que `hdfs dfs -du` prouve quoi que ce soit sur la réplication.

### 4.3 Hive CLI et les apostrophes dans les commentaires

Piège rencontré à la première exécution, qui mérite d'être connu des groupes suivants. Hive CLI retire les lignes de commentaire avec `line.startsWith("--")`, **sans `trim()` préalable** : un commentaire indenté n'est donc pas retiré et part au parseur. Or Hive suit l'état des apostrophes pour découper les instructions sur `;`. Une seule apostrophe isolée dans un commentaire indenté — « l'en-tête » — ouvre une chaîne fictive, le `;` suivant est ignoré, deux instructions fusionnent, et l'erreur de syntaxe signalée désigne une ligne parfaitement correcte située plus loin :

```
FAILED: ParseException line 25:17 cannot recognize input near 'transactions' ';' 'SELECT' in table name
```

Aucun commentaire de `analyse.hql` ne porte donc d'apostrophe.

### 4.4 Le CSV n'est pas protégé par des guillemets

Vérifié : aucun guillemet dans les trois volumes, et toutes les lignes comportent exactement 19 champs. La table Hive peut donc utiliser `ROW FORMAT DELIMITED FIELDS TERMINATED BY ','` sans SerDe CSV. **Si un libellé contenait une virgule, cette définition casserait silencieusement** et décalerait toutes les colonnes suivantes. Le contrôle `champs_numeriques_nuls` de la section 2 de `analyse.hql` détecterait ce cas.

---

## 5. Contrôles de cohérence effectués

Trois moteurs de calcul indépendants ont produit les mêmes chiffres, à l'écart nul :

| Contrôle | Méthode | Résultat |
|---|---|---|
| Qualité des données | DuckDB sur le CSV brut | 0 doublon, 0 valeur manquante, 0 montant incohérent, 0 remise hors bornes, 0 stock incohérent |
| `montant` = quantité × prix × (1 − remise) | Recalcul sur 500 000 lignes | écart maximal 7 × 10⁻¹² (arrondi machine) |
| MapReduce réel contre SQL | `comparer_resultats.py` | 5/5 jobs concordants, écart 0 |
| Hive contre SQL | `comparer_resultats.py` | **17/17 fichiers concordants, écart 0** (13/10/2026) |
| Hive contre MapReduce | `comparer_resultats.py` | **5/5 fichiers concordants, écart 0** (13/10/2026) |
| Qualité mesurée dans Hive | section 2 de `analyse.hql` | mêmes nombres que DuckDB : 500 000 lignes, 500 000 identifiants distincts, zéro anomalie |
| Conservation à la conversion ORC | `analyse.hql`, section 3 | 500 000 lignes et 4 596 094 050 FCFA avant comme après |

Tolérance de comparaison : 0,01 FCFA, pour absorber les différences d'arrondi entre moteurs.
