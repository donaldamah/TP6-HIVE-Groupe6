#!/usr/bin/env python3
"""
comparer_resultats.py
TP6 - Hive et Data Warehouse distribue - Groupe 6 (M1 Big Data, UCAO-UUT)

Compare deux dossiers de resultats CSV portant les memes noms de fichiers :
par exemple les sorties Hive et les resultats attendus calcules par un autre
moteur, ou les sorties Hive et les references MapReduce.

Met en oeuvre la consigne 14 de l'enonce : une valeur obtenue avec une
technologie doit etre controlee par rapport aux resultats anterieurs, et les
divergences expliquees (types numeriques, separateur, en-tete, valeurs
manquantes, arrondis, doublons, logique analytique).

Methode :
  - les colonnes non numeriques forment la cle de la ligne ;
  - les deux tables doivent avoir le meme ensemble de cles ;
  - l'ecart absolu maximal est calcule sur chaque colonne numerique ;
  - verdict PASS si memes cles ET ecart maximal <= tolerance, sinon FAIL.

Usage :
    python3 comparer_resultats.py <dossier_a> <dossier_b> [tolerance] [rapport.csv]
Exemple :
    python3 comparer_resultats.py ../resultats/hive ../resultats/attendus_sql/medium \
            0.01 ../resultats/comparaison_hive_vs_sql.csv

Code de retour : 0 si tout concorde, 1 sinon (utilisable dans un script).
"""

import os
import sys

import pandas as pd

TOLERANCE_DEFAUT = 0.01


def colonnes_cles(df):
    return [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]


def comparer(fichier_a, fichier_b, tolerance):
    a = pd.read_csv(fichier_a)
    b = pd.read_csv(fichier_b)

    if list(a.columns) != list(b.columns):
        return "FAIL", "colonnes differentes : %s vs %s" % (list(a.columns), list(b.columns)), None

    cles = colonnes_cles(a)
    numeriques = [c for c in a.columns if c not in cles]

    # Table d'une seule ligne (controles globaux) : pas de cle, comparaison directe
    if not cles:
        if len(a) != len(b):
            return "FAIL", "%d ligne(s) vs %d" % (len(a), len(b)), None
        ecart = max((abs(a[c].iloc[0] - b[c].iloc[0]) for c in numeriques), default=0.0)
        return ("PASS" if ecart <= tolerance else "FAIL"), "%d ligne" % len(a), ecart

    a2 = a.set_index(cles).sort_index()
    b2 = b.set_index(cles).sort_index()

    if set(a2.index) != set(b2.index):
        absentes_b = len(set(a2.index) - set(b2.index))
        absentes_a = len(set(b2.index) - set(a2.index))
        return ("FAIL",
                "cles differentes (absentes de B : %d, de A : %d)" % (absentes_b, absentes_a),
                None)

    b2 = b2.loc[a2.index]
    ecart = max((float((a2[c] - b2[c]).abs().max()) for c in numeriques), default=0.0)
    return ("PASS" if ecart <= tolerance else "FAIL"), "%d cles" % len(a2), ecart


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    dossier_a, dossier_b = sys.argv[1], sys.argv[2]
    tolerance = float(sys.argv[3]) if len(sys.argv) > 3 else TOLERANCE_DEFAUT
    rapport = sys.argv[4] if len(sys.argv) > 4 else None

    csv_a = set(f for f in os.listdir(dossier_a) if f.endswith(".csv"))
    csv_b = set(f for f in os.listdir(dossier_b) if f.endswith(".csv"))
    communs = sorted(csv_a & csv_b)
    if not communs:
        sys.exit("Aucun fichier CSV de meme nom dans les deux dossiers.")

    print("A : %s" % dossier_a)
    print("B : %s" % dossier_b)
    print("Tolerance : %s\n" % tolerance)
    print("%-34s %-6s %-18s %s" % ("fichier", "statut", "perimetre", "ecart_max"))
    print("-" * 78)

    lignes = []
    for fichier in communs:
        statut, detail, ecart = comparer(os.path.join(dossier_a, fichier),
                                        os.path.join(dossier_b, fichier),
                                        tolerance)
        print("%-34s %-6s %-18s %s" % (fichier, statut, detail,
                                       "-" if ecart is None else "%.6g" % ecart))
        lignes.append({"fichier": fichier, "statut": statut,
                       "perimetre": detail, "ecart_max": ecart,
                       "tolerance": tolerance})

    seuls_a = sorted(csv_a - csv_b)
    seuls_b = sorted(csv_b - csv_a)
    if seuls_a:
        print("\nPresents seulement dans A : %s" % ", ".join(seuls_a))
    if seuls_b:
        print("Presents seulement dans B : %s" % ", ".join(seuls_b))

    nb_fail = sum(1 for l in lignes if l["statut"] == "FAIL")
    print("\n%d/%d fichiers concordants." % (len(lignes) - nb_fail, len(lignes)))
    if rapport:
        os.makedirs(os.path.dirname(os.path.abspath(rapport)), exist_ok=True)
        pd.DataFrame(lignes).to_csv(rapport, index=False)
        print("Rapport ecrit : %s" % rapport)
    return 1 if nb_fail else 0


if __name__ == "__main__":
    sys.exit(main())
