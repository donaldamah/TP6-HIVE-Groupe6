#!/usr/bin/env python3
"""
mapper.py - Mapper Hadoop Streaming
TP6 - Hive et Data Warehouse distribue - Groupe 6 (M1 Big Data, UCAO-UUT)

Reimplemente les 5 jobs concus par le Groupe 3 pour produire une reference
de comparaison aux resultats Hive. Le Groupe 4 n'ayant pas encore presente au
13/10/2026, ces sorties tiennent lieu de reference provisoire : elles sont
produites par le VRAI schema Map -> Shuffle/Sort -> Reduce (et non par une
agregation pandas), donc directement comparables aux sorties Hadoop du
Groupe 4 quand elles seront disponibles.

Le job est choisi par la variable d'environnement JOB :
    transactions_par_ville, ca_par_ville, quantite_par_produit,
    ca_par_categorie, transactions_par_paiement

Execution locale (pipes Unix - ce que fait Hadoop Streaming) :
    JOB=ca_par_ville python3 mapper.py < transactions_medium.csv \
      | sort -k1,1 \
      | JOB=ca_par_ville python3 reducer.py

Execution sur Hadoop :
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
      -D mapreduce.job.name=ca_par_ville \
      -files mapper.py,reducer.py \
      -cmdenv JOB=ca_par_ville \
      -input  /datalake/raw/transactions/medium \
      -output /datalake/results/mapreduce/ca_par_ville \
      -mapper mapper.py -combiner reducer.py -reducer reducer.py

Contrat de sortie : une ligne "cle<TAB>valeur" par transaction retenue.
Les lignes malformees sont comptees dans les compteurs Hadoop (stderr), jamais
ignorees silencieusement : la consigne 14 de l'enonce exige que toute
divergence soit explicable.
"""

import os
import sys

NB_CHAMPS = 19

# Position des colonnes dans le CSV du fil conducteur (dictionnaire des variables)
VILLE, CATEGORIE, PRODUIT = 3, 5, 6
QUANTITE, PRIX_UNITAIRE, REMISE = 7, 8, 9
MODE_PAIEMENT = 10

# job -> (colonne servant de cle, mesure emise)
JOBS = {
    "transactions_par_ville":    (VILLE,         "compte"),
    "ca_par_ville":              (VILLE,         "ca"),
    "quantite_par_produit":      (PRODUIT,       "quantite"),
    "ca_par_categorie":          (CATEGORIE,     "ca"),
    "transactions_par_paiement": (MODE_PAIEMENT, "compte"),
}


def main():
    job = os.environ.get("JOB", "")
    if job not in JOBS:
        sys.exit(
            "JOB manquant ou inconnu : %r\nValeurs admises : %s"
            % (job, ", ".join(sorted(JOBS)))
        )
    idx_cle, mesure = JOBS[job]

    mappees = rejetees = entetes = 0
    ecrire = sys.stdout.write

    for ligne in sys.stdin:
        ligne = ligne.strip()
        if not ligne:
            continue
        champs = ligne.split(",")

        # Mauvais nombre de colonnes : ligne tronquee ou separateur inattendu
        if len(champs) != NB_CHAMPS:
            rejetees += 1
            continue

        # En-tete : present dans le premier split du fichier, et dans chaque
        # fichier si plusieurs CSV coexistent dans le repertoire d'entree.
        # Hive gere ce cas par skip.header.line.count ; ici c'est explicite.
        if champs[0] == "transaction_id":
            entetes += 1
            continue

        try:
            if mesure == "compte":
                valeur = "1"
            elif mesure == "quantite":
                valeur = str(int(champs[QUANTITE]))
            else:  # chiffre d'affaires : CA = somme de q * p * (1 - r)
                q = int(champs[QUANTITE])
                p = float(champs[PRIX_UNITAIRE])
                r = float(champs[REMISE])
                valeur = repr(q * p * (1.0 - r))
        except ValueError:
            rejetees += 1
            continue

        cle = champs[idx_cle]
        if not cle:
            rejetees += 1
            continue

        mappees += 1
        ecrire(cle + "\t" + valeur + "\n")

    # Compteurs Hadoop : visibles dans les logs du job et dans l'interface YARN
    err = sys.stderr.write
    err("reporter:counter:TP6,lignes_mappees,%d\n" % mappees)
    err("reporter:counter:TP6,lignes_rejetees,%d\n" % rejetees)
    err("reporter:counter:TP6,lignes_entete_ignorees,%d\n" % entetes)


if __name__ == "__main__":
    main()
