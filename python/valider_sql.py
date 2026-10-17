#!/usr/bin/env python3
"""
valider_sql.py
TP6 - Hive et Data Warehouse distribue - Groupe 6 (M1 Big Data, UCAO-UUT)

Rejoue les controles qualite et les sept blocs de requetes du TP avec DuckDB,
directement sur le CSV brut, SANS passer par Hive ni Hadoop.

Pourquoi : la consigne 14 de l'enonce demande que toute valeur obtenue avec une
technologie soit controlee par rapport a une autre. Les sorties de ce script
sont les resultats ATTENDUS. Quand le cluster Hive tourne, comparer_resultats.py
confronte les sorties Hive a ces attendus et tout ecart doit etre explique.

Les requetes sont volontairement ecrites dans la meme forme que celles de
analyse.hql : meme SUBSTR sur datetime traite en chaine (comme le STRING de
Hive), meme ROUND a deux decimales, memes noms de colonnes. Deux moteurs SQL,
une seule logique analytique.

Usage :
    python3 valider_sql.py <fichier.csv|.csv.gz> <dossier_sortie>
Exemples :
    python3 valider_sql.py ../data/transactions_medium.csv  ../resultats/attendus_sql/medium
    python3 valider_sql.py ../data/transactions_big.csv.gz  ../resultats/attendus_sql/big
"""

import os
import shutil
import sys
import tempfile
import time

import duckdb

# Les 19 colonnes du fil conducteur, typees comme dans analyse.hql.
# datetime reste une chaine : c'est le type STRING de la table Hive, et les
# requetes en extraient l'annee et le mois par SUBSTR.
SCHEMA = {
    "transaction_id": "VARCHAR", "datetime": "VARCHAR", "client_id": "VARCHAR",
    "ville": "VARCHAR", "magasin_id": "VARCHAR", "categorie": "VARCHAR",
    "produit": "VARCHAR", "quantite": "INTEGER", "prix_unitaire": "DOUBLE",
    "remise": "DOUBLE", "mode_paiement": "VARCHAR", "age_client": "INTEGER",
    "type_client": "VARCHAR", "stock_avant": "INTEGER", "stock_apres": "INTEGER",
    "promotion": "INTEGER", "jour_semaine": "VARCHAR", "livraison": "INTEGER",
    "montant": "DOUBLE",
}

REQUETES = {
    # ----- Controles qualite : rien ne doit etre suppose, tout doit etre verifie
    "Q0_controles": """
        SELECT COUNT(*)                                   AS nb_lignes,
               COUNT(DISTINCT transaction_id)             AS nb_ids_distincts,
               SUM(CASE WHEN ABS(montant - quantite*prix_unitaire*(1-remise)) > 0.01
                        THEN 1 ELSE 0 END)                AS montants_incoherents,
               SUM(CASE WHEN remise < 0 OR remise > 0.30 THEN 1 ELSE 0 END)
                                                          AS remises_hors_bornes,
               SUM(CASE WHEN promotion = 0 AND remise > 0 THEN 1 ELSE 0 END)
                                                          AS promo0_avec_remise,
               SUM(CASE WHEN stock_apres > stock_avant THEN 1 ELSE 0 END)
                                                          AS stocks_incoherents,
               COUNT(DISTINCT ville)                      AS nb_villes,
               COUNT(DISTINCT produit)                    AS nb_produits,
               COUNT(DISTINCT mode_paiement)              AS nb_modes_paiement,
               MIN(datetime)                              AS premiere_transaction,
               MAX(datetime)                              AS derniere_transaction,
               ROUND(SUM(montant), 2)                     AS ca_total
        FROM t""",

    # ----- Q1a : chiffre d'affaires mensuel (cle de partitionnement Hive)
    "Q1a_ca_mensuel": """
        SELECT CAST(SUBSTR(datetime, 1, 4) AS INT) AS annee,
               CAST(SUBSTR(datetime, 6, 2) AS INT) AS mois,
               COUNT(*)               AS nb_transactions,
               ROUND(SUM(montant), 2) AS ca_mensuel
        FROM t GROUP BY 1, 2 ORDER BY 1, 2""",

    # ----- Q1b : chiffre d'affaires annuel
    "Q1b_ca_annuel": """
        SELECT CAST(SUBSTR(datetime, 1, 4) AS INT) AS annee,
               COUNT(*)               AS nb_transactions,
               ROUND(SUM(montant), 2) AS ca_annuel
        FROM t GROUP BY 1 ORDER BY 1""",

    # ----- Q2 : CA et panier moyen par ville
    #   une ligne = une transaction = un panier, donc panier moyen = AVG(montant)
    "Q2_ca_panier_ville": """
        SELECT ville,
               COUNT(*)               AS nb_transactions,
               ROUND(SUM(montant), 2) AS ca_total,
               ROUND(AVG(montant), 2) AS panier_moyen
        FROM t GROUP BY ville ORDER BY ca_total DESC""",

    # ----- Q3 : top 10 des produits par chiffre d'affaires
    "Q3_top10_produits": """
        SELECT produit, categorie,
               SUM(quantite)          AS quantite_vendue,
               ROUND(SUM(montant), 2) AS ca_total
        FROM t GROUP BY produit, categorie ORDER BY ca_total DESC LIMIT 10""",

    # ----- Q4 : ventes par categorie et mode de paiement
    "Q4_ventes_categorie_paiement": """
        SELECT categorie, mode_paiement,
               COUNT(*)               AS nb_transactions,
               SUM(quantite)          AS quantite_vendue,
               ROUND(SUM(montant), 2) AS ca_total
        FROM t GROUP BY categorie, mode_paiement ORDER BY categorie, ca_total DESC""",

    # ----- Q5 : impact des promotions (cout des remises = CA brut - CA net)
    "Q5_impact_promotions": """
        SELECT promotion,
               COUNT(*)                                        AS nb_transactions,
               ROUND(SUM(montant), 2)                          AS ca_net,
               ROUND(SUM(quantite*prix_unitaire), 2)           AS ca_brut,
               ROUND(SUM(quantite*prix_unitaire)-SUM(montant), 2) AS cout_remises,
               ROUND(AVG(montant), 2)                          AS panier_moyen,
               ROUND(AVG(quantite), 3)                         AS quantite_moyenne,
               ROUND(AVG(remise), 4)                           AS remise_moyenne
        FROM t GROUP BY promotion ORDER BY promotion""",

    # ----- Q6 : activite par jour de la semaine, dans l'ordre calendaire
    "Q6_activite_jour_semaine": """
        SELECT jour_semaine,
               COUNT(*)               AS nb_transactions,
               ROUND(SUM(montant), 2) AS ca_total,
               ROUND(AVG(montant), 2) AS panier_moyen,
               CASE jour_semaine
                    WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
                    ELSE 7 END        AS ordre
        FROM t GROUP BY jour_semaine ORDER BY ordre""",

    # ----- Q7 : les cinq equivalents des jobs MapReduce du Groupe 3.
    #   Le CA est recalcule par SUM(q*p*(1-r)) et non par SUM(montant) : c'est
    #   la formule de l'enonce, et cela controle au passage la colonne montant.
    "Q7_transactions_par_ville":
        "SELECT ville, COUNT(*) AS nb_transactions FROM t GROUP BY ville ORDER BY ville",
    "Q7_ca_par_ville":
        "SELECT ville, ROUND(SUM(quantite*prix_unitaire*(1-remise)), 2) AS ca "
        "FROM t GROUP BY ville ORDER BY ville",
    "Q7_quantite_par_produit":
        "SELECT produit, SUM(quantite) AS quantite_vendue FROM t GROUP BY produit ORDER BY produit",
    "Q7_ca_par_categorie":
        "SELECT categorie, ROUND(SUM(quantite*prix_unitaire*(1-remise)), 2) AS ca "
        "FROM t GROUP BY categorie ORDER BY categorie",
    "Q7_transactions_par_paiement":
        "SELECT mode_paiement, COUNT(*) AS nb_transactions FROM t GROUP BY mode_paiement "
        "ORDER BY mode_paiement",
}

# Jobs MapReduce correspondants, pour produire aussi des fichiers ref_*.csv
# directement comparables aux sorties de lancer_reference_mapreduce.sh
CORRESPONDANCE_MAPREDUCE = {
    "Q7_transactions_par_ville": "ref_transactions_par_ville",
    "Q7_ca_par_ville": "ref_ca_par_ville",
    "Q7_quantite_par_produit": "ref_quantite_par_produit",
    "Q7_ca_par_categorie": "ref_ca_par_categorie",
    "Q7_transactions_par_paiement": "ref_transactions_par_paiement",
}


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    chemin, sortie = sys.argv[1], sys.argv[2]
    if not os.path.exists(chemin):
        sys.exit("Fichier introuvable : " + chemin)
    os.makedirs(sortie, exist_ok=True)

    # Base temporaire sur le disque local de la machine (et non dans le dossier
    # du projet) : DuckDB peut y deborder sans saturer la memoire sur le jeu big.
    tmp = tempfile.mkdtemp(prefix="tp6_duckdb_")
    con = duckdb.connect(os.path.join(tmp, "base.duckdb"))
    con.execute("SET memory_limit='2GB'")
    colonnes = ", ".join("'%s': '%s'" % (c, t) for c, t in SCHEMA.items())
    # CREATE TABLE et non CREATE VIEW : la table est lue une seule fois, alors
    # qu'une vue ferait relire et redecompresser le CSV a chaque requete.
    # DuckDB lit les .csv et les .csv.gz de la meme facon.
    t0 = time.time()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM read_csv('%s', header=true, columns={%s})"
        % (chemin.replace("'", "''"), colonnes)
    )
    print("Chargement : %.1fs pour %s lignes\n"
          % (time.time() - t0, con.execute("SELECT COUNT(*) FROM t").fetchone()[0]))

    print("Source : %s" % chemin)
    print("Sortie : %s\n" % sortie)
    temps = []
    for nom, sql in REQUETES.items():
        t0 = time.time()
        df = con.execute(sql).df()
        duree = time.time() - t0
        df.to_csv(os.path.join(sortie, nom + ".csv"), index=False)
        if nom in CORRESPONDANCE_MAPREDUCE:
            df.to_csv(os.path.join(sortie, CORRESPONDANCE_MAPREDUCE[nom] + ".csv"), index=False)
        temps.append((nom, round(duree, 2), len(df)))
        print("  %-32s %6.2fs  %4d ligne(s)" % (nom, duree, len(df)))

    import pandas as pd
    pd.DataFrame(temps, columns=["requete", "duree_s", "nb_lignes"]).to_csv(
        os.path.join(sortie, "benchmark_duckdb.csv"), index=False)

    print("\n--- Controles qualite ---")
    print(con.execute(REQUETES["Q0_controles"]).df().T.to_string(header=False))
    con.close()
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
