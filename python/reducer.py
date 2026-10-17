#!/usr/bin/env python3
"""
reducer.py - Reducer Hadoop Streaming
TP6 - Hive et Data Warehouse distribue - Groupe 6 (M1 Big Data, UCAO-UUT)

Entree : "cle<TAB>valeur" TRIEE par cle (le tri est assure par la phase
Shuffle/Sort de Hadoop, ou par `sort -k1,1` en execution locale).
Sortie : "cle<TAB>somme", une ligne par cle.

La somme etant associative et commutative, ce meme script sert de COMBINER
(-combiner reducer.py) : c'est pourquoi il n'arrondit pas. L'arrondi a deux
decimales est applique une seule fois, a l'export CSV, par
lancer_reference_mapreduce.sh - exactement comme Hive applique ROUND() en
sortie et non dans ses agregats intermediaires.
"""

import os
import sys

JOBS_ENTIERS = {
    "transactions_par_ville",
    "quantite_par_produit",
    "transactions_par_paiement",
}


def main():
    job = os.environ.get("JOB", "")
    if not job:
        sys.exit("JOB manquant (doit valoir la meme valeur que pour le mapper)")
    entier = job in JOBS_ENTIERS

    cle_courante = None
    cumul = 0
    cles = 0
    ecrire = sys.stdout.write

    def vider():
        if cle_courante is not None:
            ecrire("%s\t%s\n" % (cle_courante, cumul if entier else repr(cumul)))

    for ligne in sys.stdin:
        ligne = ligne.rstrip("\n")
        if not ligne:
            continue
        cle, _, valeur = ligne.partition("\t")
        try:
            v = int(valeur) if entier else float(valeur)
        except ValueError:
            sys.stderr.write("reporter:counter:TP6,valeurs_illisibles,1\n")
            continue

        if cle != cle_courante:
            vider()
            cle_courante, cumul = cle, 0 if entier else 0.0
            cles += 1
        cumul += v

    vider()
    sys.stderr.write("reporter:counter:TP6,cles_reduites,%d\n" % cles)


if __name__ == "__main__":
    main()
