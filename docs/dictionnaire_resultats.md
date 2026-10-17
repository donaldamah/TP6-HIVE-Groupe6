# Dictionnaire des résultats transmis au Groupe 7

**TP6 – Hive et Data Warehouse distribué – Groupe 6**

Chaque requête de `sql/analyse.hql` est matérialisée de deux façons :

- une **table Hive externe** `res_<requête>` dans la base `datawarehouse`, interrogeable en SQL ;
- les **fichiers CSV** correspondants dans `/datalake/results/hive/<Requête>/` sur HDFS, lisibles directement par Spark.

Les CSV déposés dans HDFS **n'ont pas de ligne d'en-tête** (Hive n'en écrit pas) : les noms et types de colonnes sont ceux de ce dictionnaire et de la table Hive. Les copies locales produites par `python/tp6_hive.py` dans `resultats/hive/`, elles, portent un en-tête.

## Accès depuis PySpark (Groupe 7)

```python
spark.sql("USE datawarehouse")
# source recommandée : table ORC partitionnée, lecture par partition possible
df = spark.table("datawarehouse.transactions_part")
mars = df.filter((df.annee == 2025) & (df.mois == 3))   # une seule partition lue

# résultats de référence du Groupe 6
ca_ville = spark.table("datawarehouse.res_q2_ca_panier_ville")
```

## Tables de données

| Table | Format | Partitions | Emplacement HDFS | Lignes |
|---|---|---|---|---|
| `transactions` | TEXTFILE | aucune | `/datalake/raw/transactions/medium` | 500 000 |
| `transactions_small` | TEXTFILE | aucune | `/datalake/raw/transactions/small` | 10 000 |
| `transactions_big` | TEXTFILE gzip | aucune | `/datalake/raw/transactions/big` | 5 000 000 |
| `transactions_part` | ORC (ZLIB) | `(annee, mois)`, 24 | `/datalake/processed/transactions_part` | 500 000 |

Les 19 colonnes sont celles du dictionnaire des variables de l'énoncé. `transactions_part` y ajoute les deux colonnes de partition `annee INT` et `mois INT`, dérivées de `datetime`.

## Tables de résultats

| Table Hive | Fichier | Colonnes (dans l'ordre) | Source |
|---|---|---|---|
| `res_q1a_ca_mensuel` | `Q1a_ca_mensuel` | `annee INT, mois INT, nb_transactions BIGINT, ca_mensuel DOUBLE` | `transactions_part` |
| `res_q1b_ca_annuel` | `Q1b_ca_annuel` | `annee INT, nb_transactions BIGINT, ca_annuel DOUBLE` | `transactions_part` |
| `res_q2_ca_panier_ville` | `Q2_ca_panier_ville` | `ville STRING, nb_transactions BIGINT, ca_total DOUBLE, panier_moyen DOUBLE` | `transactions_part` |
| `res_q3_top10_produits` | `Q3_top10_produits` | `produit STRING, categorie STRING, quantite_vendue BIGINT, ca_total DOUBLE` | `transactions_part` |
| `res_q4_ventes_categorie_paiement` | `Q4_ventes_categorie_paiement` | `categorie STRING, mode_paiement STRING, nb_transactions BIGINT, quantite_vendue BIGINT, ca_total DOUBLE` | `transactions_part` |
| `res_q5_impact_promotions` | `Q5_impact_promotions` | `promotion INT, nb_transactions BIGINT, ca_net DOUBLE, ca_brut DOUBLE, cout_remises DOUBLE, panier_moyen DOUBLE, quantite_moyenne DOUBLE, remise_moyenne DOUBLE` | `transactions_part` |
| `res_q6_activite_jour_semaine` | `Q6_activite_jour_semaine` | `jour_semaine STRING, nb_transactions BIGINT, ca_total DOUBLE, panier_moyen DOUBLE, ordre INT` | `transactions_part` |
| `res_q7_transactions_par_ville` | `Q7_transactions_par_ville` | `ville STRING, nb_transactions BIGINT` | `transactions` |
| `res_q7_ca_par_ville` | `Q7_ca_par_ville` | `ville STRING, ca DOUBLE` | `transactions` |
| `res_q7_quantite_par_produit` | `Q7_quantite_par_produit` | `produit STRING, quantite_vendue BIGINT` | `transactions` |
| `res_q7_ca_par_categorie` | `Q7_ca_par_categorie` | `categorie STRING, ca DOUBLE` | `transactions` |
| `res_q7_transactions_par_paiement` | `Q7_transactions_par_paiement` | `mode_paiement STRING, nb_transactions BIGINT` | `transactions` |

## Conventions de calcul

| Grandeur | Définition retenue |
|---|---|
| Chiffre d'affaires (Q1 à Q6) | `SUM(montant)`, montant étant la colonne nette fournie |
| Chiffre d'affaires (Q7) | `SUM(quantite × prix_unitaire × (1 − remise))`, formule de l'énoncé — recalcule le montant et le contrôle au passage |
| Panier moyen | `AVG(montant)` : une ligne = une transaction = un panier |
| CA brut | `SUM(quantite × prix_unitaire)`, avant remise |
| Coût des remises | CA brut − CA net |
| Année et mois | `SUBSTR(datetime, 1, 4)` et `SUBSTR(datetime, 6, 2)`, `datetime` étant une chaîne |

Les deux définitions du chiffre d'affaires donnent le même total : l'écart maximal mesuré entre `montant` et `quantite × prix_unitaire × (1 − remise)` est de 7 × 10⁻¹² sur 500 000 lignes.

## Avertissements d'interprétation

Trois propriétés du générateur conditionnent la lecture de ces résultats et sont détaillées dans `anomalies_et_divergences.md` : les produits sont équiprobables (le Top 10 mesure les prix, pas la demande), les quantités sont indépendantes des promotions (aucune élasticité dans le jeu), et les dates sont uniformes (aucune saisonnalité).
