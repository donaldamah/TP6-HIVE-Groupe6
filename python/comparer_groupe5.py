#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
comparer_groupe5.py — TP6 Hive, Groupe 6

Confronte nos resultats aux references du Groupe 3 telles que le Groupe 5 les
a rejouees sur Hadoop/YARN, sur les trois volumes.

Chaine amont : Groupe 3 concoit les jobs MapReduce -> Groupe 4 les implemente
en Hadoop Streaming -> Groupe 5 les execute sous YARN et livre les sorties de
reference dans references_groupe3/{small,medium,big}/.

Cote Groupe 6 :
  - medium : sorties Hive reelles           (resultats/hive/ref_*.csv)
  - small et big : contre-calcul DuckDB     (resultats/attendus_sql/<vol>/ref_*.csv)
    Hive n'a tourne que sur medium ; small et big sont compares par le moteur
    independant, ce qui reste un controle valable de la logique analytique.

Sortie : resultats/comparaison_hive_vs_groupe5.csv  + code de retour 0/1.

Usage :
    python python/comparer_groupe5.py
    python python/comparer_groupe5.py --references "../Rendu groupe 5/group5_yarn/references_groupe3"
    python python/comparer_groupe5.py --tolerance 0.01
"""

import argparse
import csv
import gzip
import hashlib
import sys
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent

# nom du job cote Groupe 5 -> (notre nom de fichier, colonne comparee de chaque cote)
CORRESPONDANCE = {
    "job1_transactions_par_ville":         ("ref_transactions_par_ville",  "nb_transactions"),
    "job2_ca_par_ville":                   ("ref_ca_par_ville",            "CA net (FCFA)"),
    "job3_quantite_par_produit":           ("ref_quantite_par_produit",    "quantite vendue"),
    "job4_ca_par_categorie":               ("ref_ca_par_categorie",        "CA net (FCFA)"),
    "job5_transactions_par_mode_paiement": ("ref_transactions_par_paiement", "nb_transactions"),
}

# ou se trouvent nos resultats, par volume, et par quel moteur ils ont ete produits
NOS_SORTIES = {
    "small":  ("resultats/attendus_sql/small", "DuckDB"),
    "medium": ("resultats/hive",               "Hive"),
    "big":    ("resultats/attendus_sql/big",   "DuckDB"),
}


# Empreintes publiees par le Groupe 5 dans scripts/config.sh : elles prouvent que
# les deux chaines ont travaille sur des fichiers identiques bit a bit, et pas
# seulement sur des jeux "du meme genre".
MD5_GROUPE5 = {
    "small":  ("data/transactions_small.csv",  "0f01deb58373ff458abc83166cdac37c"),
    "medium": ("data/transactions_medium.csv", "4403708511a26fe2360288e074db37e1"),
    "big":    ("data/transactions_big.csv.gz", "ea57ad7e97c0b4f6ca49296cb204e933"),
}


def md5_du_contenu(chemin):
    """MD5 du contenu decompresse : le Groupe 5 compare le flux, pas l'archive."""
    ouvrir = gzip.open if chemin.suffix == ".gz" else open
    empreinte = hashlib.md5()
    with ouvrir(chemin, "rb") as f:
        for bloc in iter(lambda: f.read(1 << 20), b""):
            empreinte.update(bloc)
    return empreinte.hexdigest()


def controler_md5():
    """Compare nos trois jeux aux empreintes publiees. Retourne True si tout concorde."""
    print("Empreintes MD5 des jeux de donnees (plusieurs secondes pour le big)")
    tout_bon = True
    for volume, (relatif, attendu) in MD5_GROUPE5.items():
        chemin = RACINE / relatif
        if not chemin.exists():
            print(f"  {volume:7} ABSENT    {relatif}")
            tout_bon = False
            continue
        obtenu = md5_du_contenu(chemin)
        concorde = obtenu == attendu
        tout_bon &= concorde
        print(f"  {volume:7} {'IDENTIQUE' if concorde else 'DIFFERENT':9} {obtenu}")
    print()
    return tout_bon


def lire(chemin):
    """Lit un CSV cle;valeur -> dict {cle: float(colonne 2)}. La 1re colonne est
    la cle, la 2e la mesure comparee. Les colonnes suivantes sont ignorees :
    le Groupe 5 publie aussi le CA brut, que nos jobs ne produisent pas."""
    valeurs = {}
    with open(chemin, encoding="utf-8") as f:
        lecteur = csv.reader(f)
        entete = next(lecteur, None)
        if entete is None:
            raise ValueError(f"{chemin} est vide")
        for ligne in lecteur:
            if not ligne or not ligne[0].strip():
                continue
            valeurs[ligne[0].strip()] = float(ligne[1])
    return valeurs


def comparer(ref, nous, tolerance):
    """Retourne (ecart_max, liste des cles en desaccord)."""
    desaccords, ecart_max = [], 0.0
    for cle in sorted(set(ref) | set(nous)):
        if cle not in ref:
            desaccords.append(f"{cle} absent de la reference")
            continue
        if cle not in nous:
            desaccords.append(f"{cle} absent de nos resultats")
            continue
        ecart = abs(ref[cle] - nous[cle])
        ecart_max = max(ecart_max, ecart)
        if ecart > tolerance:
            desaccords.append(f"{cle} : {ref[cle]} vs {nous[cle]}")
    return ecart_max, desaccords


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--references", default="../Rendu groupe 5/group5_yarn/references_groupe3",
                    help="dossier des references livrees par le Groupe 5 "
                         "(defaut : %(default)s, relatif a la racine du projet)")
    ap.add_argument("--tolerance", type=float, default=0.01,
                    help="ecart absolu tolere sur une valeur (defaut : %(default)s)")
    ap.add_argument("--md5", action="store_true",
                    help="controle en plus que nos jeux de donnees ont les empreintes "
                         "publiees par le Groupe 5 (lit les trois fichiers : ~30 s)")
    args = ap.parse_args()

    dossier_ref = Path(args.references)
    if not dossier_ref.is_absolute():
        dossier_ref = (RACINE / dossier_ref).resolve()

    if not dossier_ref.is_dir():
        print(f"Dossier de references introuvable : {dossier_ref}", file=sys.stderr)
        print("Indiquer le bon chemin avec --references.", file=sys.stderr)
        return 2

    print(f"References Groupe 5 : {dossier_ref}")
    print(f"Tolerance           : {args.tolerance}\n")

    md5_ok = controler_md5() if args.md5 else True

    lignes, total, reussis, manquants = [], 0, 0, 0

    for volume, (sous_dossier, moteur) in NOS_SORTIES.items():
        for job, (notre_nom, mesure) in CORRESPONDANCE.items():
            chemin_ref = dossier_ref / volume / f"{job}.csv"
            chemin_nous = RACINE / sous_dossier / f"{notre_nom}.csv"

            if not chemin_ref.exists() or not chemin_nous.exists():
                absent = chemin_ref if not chemin_ref.exists() else chemin_nous
                print(f"{volume:7} {job:36} MANQUANT  ({absent.name})")
                lignes.append([volume, job, moteur, "MANQUANT", "", mesure, absent.name])
                manquants += 1
                continue

            reference, notres = lire(chemin_ref), lire(chemin_nous)
            ecart_max, desaccords = comparer(reference, notres, args.tolerance)
            total += 1
            statut = "PASS" if not desaccords else "FAIL"
            reussis += statut == "PASS"

            print(f"{volume:7} {job:36} {statut:4}  {len(reference):>2} cles  "
                  f"ecart max {ecart_max:g}  [{moteur}]"
                  + ("" if not desaccords else "  -> " + " | ".join(desaccords[:3])))
            lignes.append([volume, job, moteur, statut, f"{ecart_max:g}", mesure,
                           f"{len(reference)} cles"])

    sortie = RACINE / "resultats" / "comparaison_hive_vs_groupe5.csv"
    sortie.parent.mkdir(parents=True, exist_ok=True)
    with open(sortie, "w", encoding="utf-8", newline="") as f:
        ecrivain = csv.writer(f)
        ecrivain.writerow(["volume", "job_groupe5", "moteur_groupe6", "statut",
                           "ecart_max", "mesure_comparee", "perimetre"])
        ecrivain.writerows(lignes)

    print(f"\n{reussis}/{total} concordances" + (f", {manquants} manquant(s)" if manquants else ""))
    print(f"Rapport : {sortie}")

    # Rappel honnete : ce que cette comparaison ne couvre pas.
    print("\nNon compare : la colonne CA brut du job2 (le Groupe 5 la publie, nos "
          "jobs de reference ne la produisent pas) et la colonne nb_transactions "
          "du job3.")
    return 0 if (reussis == total and manquants == 0 and md5_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
