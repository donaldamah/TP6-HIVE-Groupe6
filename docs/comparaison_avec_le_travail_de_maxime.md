# Ce qui change entre la version de Maxime et la nôtre

Document interne au Groupe 6. Il compare le dépôt `Maxmoun/tp6-hive-groupe6`
(ci-après « version initiale ») au projet `TP6_Hive_Groupe6` que nous rendons.

Chaque différence est donnée avec sa source : fichier et section de la version
initiale, fichier et section de la nôtre. Rien ici n'est une impression.

**À dire d'emblée : la version initiale est juste.** Ses douze résultats ont été
recalculés indépendamment et concordent au centime. Ses CSV sources sont
identiques (md5) à ceux fournis par l'enseignant. Ce qui suit ne corrige pas des
chiffres faux ; cela comble des écarts avec la consigne et ajoute les preuves
qu'un jury demandera.

---

## 1. La table partitionnée n'atterrissait pas dans le Data Lake

**Version initiale** — `analyse_transactions.hql`, section 3 :

```sql
CREATE TABLE IF NOT EXISTS transactions_part ( ... )
PARTITIONED BY (annee INT, mois INT)
STORED AS ORC;
```

Pas de `EXTERNAL`, pas de `LOCATION`. Hive range donc la table dans son propre
entrepôt, `/user/hive/warehouse/transactions_part`.

**Notre version** — `sql/analyse.hql`, section 4 :

```sql
CREATE EXTERNAL TABLE transactions_part ( ... )
PARTITIONED BY (annee INT, mois INT)
STORED AS ORC
LOCATION '/datalake/processed/transactions_part';
```

**Pourquoi c'est important.** La consigne impose l'arborescence
`/datalake/{raw,processed,results}` héritée du Groupe 2. `processed` est
précisément la couche des données retravaillées : notre table ORC est ce qu'on y
attend. Second effet : sur une table *managed*, `DROP TABLE` efface les données
elles-mêmes ; sur une table *external*, il n'efface que la description.

---

## 2. Les résultats restaient sur le poste, pas dans HDFS

**Version initiale** — `tp6_hive_python.py`, fonction `requete()` :

```python
df.to_csv(os.path.join(DOSSIER_RESULTATS, nom + ".csv"), index=False)
```

Les douze CSV sont écrits dans le dossier local `resultats_hive/`. Rien n'est
déposé dans `/datalake/results/hive/`.

**Notre version** — `sql/analyse.hql`, section 7 : chaque résultat est
matérialisé en table externe sous `/datalake/results/hive/<Qxx>/`, et le script
Python se contente de *lire* ces tables.

**Pourquoi c'est important.** C'est la différence la plus lourde vis-à-vis de la
consigne. Le Groupe 7 (Spark) doit reprendre nos sorties. S'il doit nous
demander un dossier sur un PC, la chaîne est rompue : le livrable attendu vit
dans HDFS, pas sur une machine.

---

## 3. La « référence MapReduce » n'était pas du MapReduce

**Version initiale** — `tp6_hive_python.py`, fonction `reference_locale()` :

```python
paires = [kv for ligne in csv.DictReader(f) for kv in mapper(ligne, ...)]  # MAP
paires.sort(key=itemgetter(0))                                             # SHUFFLE
return {k: sum(v for _, v in g) for k, g in groupby(paires, ...)}          # REDUCE
```

C'est une simulation en mémoire, dans un seul processus, dans le même script que
Hive. Le fichier `comparaison_hive_mapreduce.csv` le dit lui-même : les cinq
lignes portent `origine_reference = reconstruite localement`.

**Notre version** — `python/mapper.py` et `python/reducer.py` sont de vrais
programmes Hadoop Streaming, lus sur `stdin`, écrits sur `stdout`, lancés par
`python/lancer_reference_mapreduce.sh` :

```bash
JOB=ca_par_ville python3 mapper.py < transactions_medium.csv \
  | LC_ALL=C sort -k1,1 \
  | JOB=ca_par_ville python3 reducer.py
```

Le `reducer` sert aussi de *combiner*, ce qui impose qu'il n'arrondisse pas.

**Pourquoi c'est important.** La consigne demande de comparer Hive à MapReduce.
Un calcul fait dans le même script, dans le même langage, au même moment, n'est
pas une seconde technologie : c'est la même main qui vérifie son propre travail.

---

## 4. Un seul contrôle croisé, contre vingt-sept

| | Version initiale | Notre version |
|---|---|---|
| Hive contre MapReduce | 5 comparaisons | 5 comparaisons |
| Hive contre DuckDB | — | 17 comparaisons |
| MapReduce contre DuckDB | — | 5 comparaisons |
| **Total** | **5** | **27, écart nul** |

DuckDB (`python/valider_sql.py`) rejoue les mêmes questions sur le CSV brut, sans
Hadoop, sans Hive. C'est un troisième moteur réellement indépendant.

---

## 5. Le jeu « big » n'était pas traité

**Version initiale** — deux tables seulement : `transactions` (medium, 500 000
lignes) et `transactions_small` (10 000).

**Notre version** — `transactions_big` également, 5 000 000 de lignes en
TEXTFILE gzip, et la mesure qui va avec : 13,58 s pour un `SUM` par ville contre
2,6 s sur le medium.

**Pourquoi c'est important.** C'est le seul endroit du TP où l'on voit un effet
Big Data réel, et il donne l'argument le plus solide de l'exposé : gzip n'est pas
*splittable*, donc un seul *mapper* traite les 5 millions de lignes pendant que
le reste du cluster attend. C'est ce qui justifie ORC, pas une préférence de
style.

---

## 6. Le partitionnement était affirmé, pas démontré

**Version initiale** — `analyse_transactions.hql`, commentaire de la section 3 :

> « 24 partitions (2 ans x 12 mois) : le filtre WHERE annee=... AND mois=... ne
> lit qu'une partition (partition pruning) »

L'affirmation est exacte. Mais aucun `EXPLAIN` n'apparaît dans le script, et
`benchmark_hive.csv` ne compare pas la table partitionnée à la table brute : il
mesure douze requêtes, toutes sur la table partitionnée.

**Notre version** — `sql/analyse.hql`, section 5 : la même question (« chiffre
d'affaires de mars 2025 ») est posée deux fois, une fois avec
`WHERE annee=2025 AND mois=3`, une fois avec
`WHERE SUBSTR(datetime,1,7)='2025-03'`, et `EXPLAIN` est lu dans les deux cas.

| | Lignes annoncées par le plan | Durée |
|---|---|---|
| Table partitionnée ORC | 21 127 | 1,62 s |
| Table TEXTFILE | 250 000 | 2,57 s |

21 127 est exactement le nombre de transactions de mars 2025 : Hive n'ouvre
qu'un dossier sur vingt-quatre. Les deux requêtes rendent le même montant,
194 982 547,5 FCFA.

**Pourquoi c'est important.** C'est la question que le jury posera. « Montrez-moi
que le partitionnement sert à quelque chose » appelle un plan d'exécution, pas
une phrase.

---

## 7. Sept contrôles qualité avant la première analyse

**Version initiale** — `analyse_transactions.hql`, section 2, « Vérification
rapide » : `SELECT * LIMIT 10`, `COUNT(*)`, `SUM(montant)`.

**Notre version** — `sql/analyse.hql`, section 3 : nuls par champ numérique,
identifiants distincts, `montant` contre `quantité × prix × (1 − remise)`,
remises hors de [0 ; 1], promotion nulle avec remise non nulle, stocks
incohérents. Les sept renvoient zéro.

**Pourquoi c'est important.** Un chiffre faux calculé sur des données mal lues
ressemble exactement à un chiffre juste. Si l'en-tête du CSV était lu comme une
transaction, `SUM(montant)` resterait plausible.

---

## 8. Le chargement ne passe plus par `docker cp`

**Version initiale** — `README.md`, lignes 114-118 : `docker cp` du CSV dans le
conteneur, puis `hdfs dfs -put`. Le fichier est donc copié deux fois.

**Notre version** — `docker-compose.yml` monte `./:/projet` dans `namenode` et
`hive-server`. Les scripts PowerShell `01` à `03` enchaînent le démarrage, le
chargement et l'exécution sans copie intermédiaire.

---

## 9. Ce que nous avons ajouté et qui n'existait pas

- `docs/anomalies_et_divergences.md` — les trois pièges d'interprétation du jeu
  de données, démontrés par lecture du générateur fourni : le Top 10 mesure les
  prix et non la demande ; les promotions ne peuvent pas faire vendre plus (la
  quantité suit une loi de Poisson tirée indépendamment) ; il n'y a pas de
  saisonnalité (les 12 % d'écart entre mois tombent à 3,9 % une fois ramenés au
  jour calendaire).
- `docs/dictionnaire_resultats.md` — colonnes, types et conventions de calcul des
  douze sorties, pour le Groupe 7.
- `python/comparer_resultats.py` — le comparateur PASS/FAIL générique.
- Le mode `--sans-hive` de `python/tp6_hive.py`, qui rejoue les contrôles sans
  cluster.
- Le rapport et les slides.

---

## 10. Ce que nous avons gardé tel quel

- Le `docker-compose.yml` à cinq services et les images `bde2020` : le choix est
  bon et il est stable.
- Le mode d'authentification NOSASL de HiveServer2 : sous Windows, le module
  Python `sasl` exige un compilateur C++. Acceptable en TP, à dire en soutenance.
- Les dix-neuf colonnes et leurs types, repris à l'identique.
- La logique des douze requêtes. Elles étaient justes ; nous ne les avons pas
  réécrites pour le plaisir de réécrire.

---

## Une réserve sur les temps mesurés

`benchmark_hive.csv` de la version initiale donne 6,0 s à 23,6 s par requête ;
nos mesures vont de 0,1 s à 13,6 s. **Ces deux séries ne sont pas comparables** :
machines différentes, et surtout une exécution unique chez lui contre la
meilleure de plusieurs exécutions chez nous. Il ne faut pas en conclure que notre
version est plus rapide — ce serait une comparaison malhonnête. La seule
comparaison valide est celle de notre section 6, où les deux requêtes tournent
sur la même machine dans la même session.
