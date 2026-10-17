#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tp6_hive.py
TP6 - Hive et Data Warehouse distribue - Groupe 6 (M1 Big Data, UCAO-UUT)

Pilote Hive depuis Python et produit les livrables chiffres du TP :

  1. export des tables de resultats creees par sql/analyse.hql, en CSV avec
     en-tete, dans resultats/hive/ ;
  2. benchmark des REPRESENTATIONS de stockage : la meme requete executee sur
     le TEXTFILE non partitionne, sur l'ORC partitionne, sur une seule
     partition et sur le volume big en gzip ;
  3. mesure et preuve du partition pruning (EXPLAIN + chronometrage) ;
  4. comparaison automatique des resultats Hive aux attendus SQL et aux
     references MapReduce ;
  5. graphiques du rapport.

CHOIX DE CONCEPTION : ce script ne reecrit AUCUNE des requetes d'analyse.
Elles sont definies une seule fois, dans sql/analyse.hql, qui materialise ses
resultats dans des tables res_*. Le script les lit. Il n'y a donc aucun risque
de divergence entre le SQL du livrable et le SQL du rapport. Les seules
requetes ecrites ici sont celles du benchmark, qui n'existent nulle part
ailleurs.

Pre-requis sur la machine hote :
    pip install pyhive thrift future pandas matplotlib
    NE PAS installer pyhive[hive] ni sasl : le module sasl exige un compilateur
    C++ sous Windows, et il est inutile car HiveServer2 est en mode NOSASL.

Pre-requis cote cluster, dans l'ordre :
    .\\scripts\\00_preparer_donnees.ps1
    .\\scripts\\01_demarrer_cluster.ps1
    .\\scripts\\02_charger_hdfs.ps1
    .\\scripts\\03_executer_hive.ps1

Usage :
    python tp6_hive.py                  execution complete sur le cluster
    python tp6_hive.py --sans-hive      graphiques et comparaisons seulement,
                                        depuis des CSV deja presents (permet de
                                        travailler sans cluster)
    python tp6_hive.py --source <dir>   dossier CSV a utiliser avec --sans-hive
"""

import argparse
import os
import sys
import time

import pandas as pd

ICI = os.path.dirname(os.path.abspath(__file__))
PROJET = os.path.dirname(ICI)
RESULTATS = os.path.join(PROJET, "resultats")
DOSSIER_HIVE = os.path.join(RESULTATS, "hive")
DOSSIER_GRAPHIQUES = os.path.join(RESULTATS, "graphiques")
ATTENDUS_MEDIUM = os.path.join(RESULTATS, "attendus_sql", "medium")
REFERENCE_MAPREDUCE = os.path.join(RESULTATS, "reference_mapreduce")

BASE = "datawarehouse"
TOLERANCE = 0.01

# ---------------------------------------------------------------------------
# Style des graphiques
#
# Palette categorielle validee (lightness, chroma, separation pour les
# daltonismes protan/deutan/tritan, contraste) : les quatre premiers slots
# servent aux series multiples, le slot 1 seul aux series uniques. Les hues
# sont assignees dans un ordre FIXE, jamais cycle : un mode de paiement garde
# la meme couleur d'un graphique a l'autre.
# Le controle de contraste signale que l'aqua, le jaune et le magenta passent
# sous 3:1 sur fond blanc : la regle de relief impose alors une legende et une
# vue tableau, toutes deux fournies (legende sur le graphique, CSV a cote).
# ---------------------------------------------------------------------------
SURFACE = "#ffffff"
TEXTE_PRINCIPAL = "#0b0b0b"
TEXTE_SECONDAIRE = "#52514e"
GRILLE = "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]


def appliquer_style():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "axes.edgecolor": GRILLE,
        "axes.labelcolor": TEXTE_SECONDAIRE,
        "axes.titlecolor": TEXTE_PRINCIPAL,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",   # DejaVu Sans ne fournit pas semibold
        "axes.titlelocation": "left",
        "axes.titlepad": 12,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.axisbelow": True,
        "grid.color": GRILLE,
        "grid.linewidth": 0.8,
        "xtick.color": TEXTE_SECONDAIRE,
        "ytick.color": TEXTE_SECONDAIRE,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "text.color": TEXTE_PRINCIPAL,
        "legend.frameon": False,
        "legend.fontsize": 9,
        "font.size": 10,
        "figure.dpi": 130,
        "savefig.bbox": "tight",
    })
    return plt


def fcfa(valeur, _position=None):
    """Formate un montant en FCFA de facon lisible sur un axe."""
    for seuil, suffixe in ((1e9, " Md"), (1e6, " M"), (1e3, " k")):
        if abs(valeur) >= seuil:
            return ("%.1f%s" % (valeur / seuil, suffixe)).replace(".0", "")
    return "%.0f" % valeur


# ---------------------------------------------------------------------------
# 1. Connexion a HiveServer2
# ---------------------------------------------------------------------------
def connecter(hote="localhost", port=10000, base=BASE, tentatives=30, pause=10):
    """Connexion NOSASL avec reessais.

    Docker publie le port 10000 des le demarrage du conteneur, mais
    HiveServer2 met encore une a trois minutes a ecouter reellement : la
    connexion est alors acceptee puis coupee ('TSocket read 0 bytes'). On
    reessaie donc, avec un delai par tentative pour ne jamais rester bloque.
    """
    from pyhive import hive
    from thrift.transport import TSocket, TTransport

    derniere = None
    for i in range(1, tentatives + 1):
        try:
            sock = TSocket.TSocket(hote, port)
            sock.setTimeout(15000)
            conn = hive.Connection(
                thrift_transport=TTransport.TBufferedTransport(sock),
                username="root",
                database=base,
            )
            sock.setTimeout(None)   # les requetes peuvent durer des minutes
            print("Connecte a HiveServer2 (%s:%d, base %s)." % (hote, port, base))
            return conn
        except Exception as e:                      # noqa: BLE001
            derniere = e
            print("  [%d/%d] HiveServer2 pas encore pret (%s), nouvel essai dans %ds..."
                  % (i, tentatives, type(e).__name__, pause))
            time.sleep(pause)

    sys.exit(
        "\nImpossible de se connecter a HiveServer2 (derniere erreur : %r).\n"
        "Verifier dans PowerShell :\n"
        "  1) docker compose ps                       -> hive-server doit etre Up\n"
        "  2) docker logs hive-server --tail 40       -> chercher 'Starting HiveServer2'\n"
        "  3) docker exec hive-server sh -c \"grep -r -A1 server2.authentication /opt/hive/conf\"\n"
        "                                             -> doit afficher NOSASL\n"
        "  4) la base '%s' existe-t-elle ? Executer d'abord scripts\\03_executer_hive.ps1\n"
        % (derniere, base))


def executer(conn, sql):
    """Execute une requete et renvoie un DataFrame aux colonnes nettoyees."""
    curseur = conn.cursor()
    curseur.execute(sql)
    colonnes = [c[0].split(".")[-1] for c in curseur.description]
    donnees = curseur.fetchall()
    curseur.close()
    return pd.DataFrame(donnees, columns=colonnes)


def mesurer(conn, sql, repetitions=2):
    """Chronometre une requete plusieurs fois et renvoie (durees, nb_lignes).

    La premiere execution paie le demarrage du job MapReduce et le cache froid
    du systeme de fichiers : on la conserve mais on la distingue des suivantes,
    au lieu de presenter une mesure unique comme si elle etait stable.
    """
    durees = []
    nb = 0
    for _ in range(repetitions):
        t0 = time.time()
        df = executer(conn, sql)
        durees.append(round(time.time() - t0, 2))
        nb = len(df)
    return durees, nb


# ---------------------------------------------------------------------------
# 2. Export des tables de resultats produites par analyse.hql
# ---------------------------------------------------------------------------
# Les cinq requetes Q7 sont aussi ecrites sous le nom ref_<job>.csv, afin
# d'etre directement comparables aux sorties de lancer_reference_mapreduce.sh,
# qui utilise cette convention de nommage.
CORRESPONDANCE_MAPREDUCE = {
    "Q7_transactions_par_ville": "ref_transactions_par_ville",
    "Q7_ca_par_ville": "ref_ca_par_ville",
    "Q7_quantite_par_produit": "ref_quantite_par_produit",
    "Q7_ca_par_categorie": "ref_ca_par_categorie",
    "Q7_transactions_par_paiement": "ref_transactions_par_paiement",
}


def nom_fichier(table):
    """res_q1a_ca_mensuel -> Q1a_ca_mensuel (nom attendu par les comparaisons)."""
    morceaux = table[len("res_"):].split("_")
    morceaux[0] = morceaux[0].capitalize()
    return "_".join(morceaux)


def exporter_resultats(conn, dossier):
    """Exporte chaque table res_* en CSV avec en-tete.

    Les tables sont decouvertes par SHOW TABLES : aucune liste n'est recopiee
    ici, donc ajouter une requete dans analyse.hql suffit pour qu'elle soit
    exportee, sans toucher a ce script.
    """
    os.makedirs(dossier, exist_ok=True)
    tables = [t for t in executer(conn, "SHOW TABLES").iloc[:, 0].tolist()
              if t.startswith("res_")]
    if not tables:
        print("ATTENTION : aucune table res_* trouvee. "
              "Executer d'abord scripts\\03_executer_hive.ps1")
        return []

    print("\n--- Export des resultats (%d tables) ---" % len(tables))
    exportes = []
    for table in sorted(tables):
        df = executer(conn, "SELECT * FROM %s" % table)
        nom = nom_fichier(table)
        df.to_csv(os.path.join(dossier, nom + ".csv"), index=False)
        if nom in CORRESPONDANCE_MAPREDUCE:
            df.to_csv(os.path.join(dossier, CORRESPONDANCE_MAPREDUCE[nom] + ".csv"),
                      index=False)
        print("  %-34s %4d ligne(s) -> %s.csv" % (table, len(df), nom))
        exportes.append(nom)
    return exportes


# ---------------------------------------------------------------------------
# 3. Benchmark des representations de stockage
#
# C'est le point que le benchmark initial du groupe ne permettait pas de lire :
# il melangeait des durees mesurees sur l'ORC partitionne (Q1 a Q6) et sur le
# TEXTFILE (Q7) dans un meme tableau, sans le dire. Ici, LA MEME requete est
# executee sur plusieurs representations, et la colonne format precise laquelle.
# Seules ces requetes de benchmark sont ecrites dans ce fichier ; les requetes
# d'analyse vivent uniquement dans sql/analyse.hql.
# ---------------------------------------------------------------------------
BENCHMARK = [
    # (requete, table, format, filtre_partition, volume, sql)
    ("ca_par_ville", "transactions", "TEXTFILE", "sans objet", "medium",
     "SELECT ville, ROUND(SUM(montant), 2) AS ca FROM transactions GROUP BY ville"),
    ("ca_par_ville", "transactions_part", "ORC", "aucun (24 partitions lues)", "medium",
     "SELECT ville, ROUND(SUM(montant), 2) AS ca FROM transactions_part GROUP BY ville"),
    ("ca_par_ville", "transactions_big", "TEXTFILE gzip", "sans objet", "big",
     "SELECT ville, ROUND(SUM(montant), 2) AS ca FROM transactions_big GROUP BY ville"),

    ("ca_mars_2025", "transactions", "TEXTFILE", "balayage complet", "medium",
     "SELECT COUNT(*) AS nb, ROUND(SUM(montant), 2) AS ca FROM transactions "
     "WHERE SUBSTR(datetime, 1, 7) = '2025-03'"),
    ("ca_mars_2025", "transactions_part", "ORC", "1 partition lue", "medium",
     "SELECT COUNT(*) AS nb, ROUND(SUM(montant), 2) AS ca FROM transactions_part "
     "WHERE annee = 2025 AND mois = 3"),

    ("comptage", "transactions", "TEXTFILE", "sans objet", "medium",
     "SELECT COUNT(*) AS nb FROM transactions"),
    ("comptage", "transactions_part", "ORC", "aucun", "medium",
     "SELECT COUNT(*) AS nb FROM transactions_part"),
    ("comptage", "transactions_big", "TEXTFILE gzip", "sans objet", "big",
     "SELECT COUNT(*) AS nb FROM transactions_big"),
]


def executer_benchmark(conn, dossier, repetitions=2):
    os.makedirs(dossier, exist_ok=True)
    print("\n--- Benchmark des representations (%d cas, %d executions chacun) ---"
          % (len(BENCHMARK), repetitions))
    lignes = []
    for requete, table, format_, filtre, volume, sql in BENCHMARK:
        try:
            durees, nb = mesurer(conn, sql, repetitions)
        except Exception as e:                       # noqa: BLE001
            print("  %-14s %-18s ECHEC : %s" % (requete, table, type(e).__name__))
            continue
        lignes.append({
            "requete": requete, "table_source": table, "format": format_,
            "filtre_partition": filtre, "volume": volume,
            "duree_1_s": durees[0],
            "duree_2_s": durees[1] if len(durees) > 1 else None,
            "duree_min_s": min(durees),
            "nb_lignes_resultat": nb,
        })
        print("  %-14s %-18s %-14s %-26s 1re %6.2fs  min %6.2fs"
              % (requete, table, format_, filtre, durees[0], min(durees)))

    chemin = os.path.join(dossier, "benchmark_hive.csv")
    pd.DataFrame(lignes).to_csv(chemin, index=False)
    print("  -> %s" % chemin)
    return pd.DataFrame(lignes)


# ---------------------------------------------------------------------------
# 4. Preuve du partition pruning par le plan d'execution
# ---------------------------------------------------------------------------
def expliquer_pruning(conn, dossier):
    """Enregistre les deux plans EXPLAIN et compte les partitions lues.

    Un chronometrage seul ne prouve rien : il peut refleter un cache. Le plan
    d'execution, lui, enonce noir sur blanc combien de repertoires de partition
    Hive a l'intention de lire.
    """
    os.makedirs(dossier, exist_ok=True)
    cas = [
        ("ORC partitionne, filtre sur annee et mois",
         "EXPLAIN SELECT COUNT(*) AS nb, ROUND(SUM(montant), 2) AS ca "
         "FROM transactions_part WHERE annee = 2025 AND mois = 3"),
        ("TEXTFILE non partitionne, filtre sur une sous-chaine",
         "EXPLAIN SELECT COUNT(*) AS nb, ROUND(SUM(montant), 2) AS ca "
         "FROM transactions WHERE SUBSTR(datetime, 1, 7) = '2025-03'"),
    ]
    import re
    chemin = os.path.join(dossier, "explain_pruning.txt")
    resume = []
    with open(chemin, "w", encoding="utf-8") as f:
        for libelle, sql in cas:
            plan = executer(conn, sql)
            texte = "\n".join(str(v) for v in plan.iloc[:, 0].tolist())
            # La preuve chiffree est la statistique du TableScan : c'est le
            # nombre de lignes que Hive prevoit de lire. Sur la table
            # partitionnee il vaut la taille d'UNE partition ; sur la table
            # TEXTFILE il porte sur la table entiere.
            # (Le plan EXPLAIN simple de Hive 2.3 ne nomme pas les partitions :
            #  il faudrait EXPLAIN EXTENDED, dont la sortie est illisible.)
            trouve = re.search(r"TableScan.*?Num rows:\s*(\d+)", texte, re.S)
            lignes_plan = int(trouve.group(1)) if trouve else None
            f.write("=" * 78 + "\n%s\n%s\n" % (libelle, "=" * 78))
            f.write(sql + "\n\n" + texte + "\n\n")
            f.write("-> lignes annoncees par le TableScan : %s\n\n"
                    % ("non trouve" if lignes_plan is None else "{:,}".format(lignes_plan).replace(",", " ")))
            resume.append((libelle, lignes_plan))
            print("  %-52s lignes annoncees par le plan : %s"
                  % (libelle, "?" if lignes_plan is None else "{:,}".format(lignes_plan).replace(",", " ")))
    valeurs = [v for _, v in resume if v]
    if len(valeurs) == 2 and min(valeurs) > 0:
        print("  -> le filtrage par partition divise la lecture annoncee par %.1f"
              % (max(valeurs) / float(min(valeurs))))
    print("  -> %s" % chemin)
    return resume


# ---------------------------------------------------------------------------
# 5. Graphiques du rapport
#
# Regles appliquees :
#   - la forme suit le role de la donnee : courbe pour une evolution, barres
#     horizontales des que les libelles sont longs, barres empilees pour une
#     composition, petits multiples plutot qu'un second axe ;
#   - AUCUN graphique a deux axes y ;
#   - les barres partent toujours de zero, meme quand cela aplatit les ecarts :
#     c'est precisement l'information dans le cas des jours de la semaine ;
#   - les couleurs sont assignees dans un ordre fixe par entite, jamais cyclees,
#     et une legende accompagne tout graphique a deux series ou plus ;
#   - le texte reste en encre neutre, la couleur ne porte que les marques ;
#   - labels directs selectifs, jamais une valeur sur chaque point.
# ---------------------------------------------------------------------------
def _note(ax, texte):
    ax.annotate(texte, xy=(0, -0.16), xycoords="axes fraction",
                fontsize=8, color=TEXTE_SECONDAIRE, annotation_clip=False)


def graphiques(dossier_csv, dossier_png, volume="medium"):
    plt = appliquer_style()
    from matplotlib.ticker import FuncFormatter
    from matplotlib.patches import Patch
    os.makedirs(dossier_png, exist_ok=True)
    source = "Source : Hive, jeu %s. Donnees : %s" % (volume, os.path.basename(dossier_csv))
    faits = []

    def lire(nom):
        chemin = os.path.join(dossier_csv, nom + ".csv")
        return pd.read_csv(chemin) if os.path.exists(chemin) else None

    # --- 1. Evolution mensuelle : une courbe, deux reperes annotes -----------
    df = lire("Q1a_ca_mensuel")
    if df is not None:
        etiquettes = (df["annee"].astype(str) + "-"
                      + df["mois"].astype(str).str.zfill(2)).tolist()
        fig, ax = plt.subplots(figsize=(9.5, 4.2))
        ax.plot(etiquettes, df["ca_mensuel"], color=SERIES[0], linewidth=2,
                marker="o", markersize=5, markerfacecolor=SURFACE,
                markeredgecolor=SERIES[0], markeredgewidth=1.6)
        ax.set_title("Chiffre d'affaires mensuel")
        ax.yaxis.set_major_formatter(FuncFormatter(fcfa))
        ax.set_ylim(0, df["ca_mensuel"].max() * 1.15)
        ax.set_xticks(range(0, len(etiquettes), 3))
        ax.set_xticklabels([etiquettes[i] for i in range(0, len(etiquettes), 3)])
        ax.grid(axis="x", visible=False)
        # labels directs selectifs : seulement le mois le plus haut et le plus bas
        for idx, position in ((df["ca_mensuel"].idxmax(), 12), (df["ca_mensuel"].idxmin(), -16)):
            ax.annotate(fcfa(df["ca_mensuel"][idx]),
                        (idx, df["ca_mensuel"][idx]), textcoords="offset points",
                        xytext=(0, position), ha="center", fontsize=9,
                        color=TEXTE_PRINCIPAL, fontweight="bold")
        _note(ax, source + " - seuls le mois le plus fort et le plus faible sont etiquetes.")
        fig.savefig(os.path.join(dossier_png, "ca_mensuel.png"))
        plt.close(fig)
        ecart = (df["ca_mensuel"].max() - df["ca_mensuel"].min()) / df["ca_mensuel"].mean()
        faits.append("amplitude mensuelle : %.1f %% autour de la moyenne" % (100 * ecart))

    # --- 2. Chiffre d'affaires annuel ---------------------------------------
    df = lire("Q1b_ca_annuel")
    if df is not None:
        fig, ax = plt.subplots(figsize=(5, 4))
        barres = ax.bar(df["annee"].astype(str), df["ca_annuel"],
                        color=SERIES[0], width=0.55)
        ax.set_title("Chiffre d'affaires annuel")
        ax.yaxis.set_major_formatter(FuncFormatter(fcfa))
        ax.bar_label(barres, labels=[fcfa(v) for v in df["ca_annuel"]],
                     padding=4, fontsize=9, color=TEXTE_PRINCIPAL)
        ax.set_ylim(0, df["ca_annuel"].max() * 1.18)
        ax.grid(axis="x", visible=False)
        _note(ax, source)
        fig.savefig(os.path.join(dossier_png, "ca_annuel.png"))
        plt.close(fig)

    # --- 3. Chiffre d'affaires par ville ------------------------------------
    df = lire("Q2_ca_panier_ville")
    if df is not None:
        d = df.sort_values("ca_total")
        fig, ax = plt.subplots(figsize=(8, 4.4))
        barres = ax.barh(d["ville"], d["ca_total"], color=SERIES[0], height=0.65)
        ax.set_title("Chiffre d'affaires par ville")
        ax.xaxis.set_major_formatter(FuncFormatter(fcfa))
        ax.bar_label(barres, labels=[fcfa(v) for v in d["ca_total"]],
                     padding=4, fontsize=9, color=TEXTE_PRINCIPAL)
        ax.set_xlim(0, d["ca_total"].max() * 1.16)
        ax.grid(axis="y", visible=False)
        part = 100 * d["ca_total"].max() / d["ca_total"].sum()
        _note(ax, source + " - %s concentre %.0f %% du chiffre d'affaires."
              % (d["ville"].iloc[-1], part))
        fig.savefig(os.path.join(dossier_png, "ca_par_ville.png"))
        plt.close(fig)
        faits.append("%s pese %.0f %% du CA" % (d["ville"].iloc[-1], part))

    # --- 4. Top 10 des produits, colore par categorie -----------------------
    df = lire("Q3_top10_produits")
    if df is not None:
        d = df.sort_values("ca_total")
        categories = sorted(d["categorie"].unique())
        couleur = {c: SERIES[i % len(SERIES)] for i, c in enumerate(categories)}
        fig, ax = plt.subplots(figsize=(8.5, 4.8))
        barres = ax.barh(d["produit"], d["ca_total"],
                         color=[couleur[c] for c in d["categorie"]], height=0.68)
        ax.set_title("Top 10 des produits par chiffre d'affaires")
        ax.xaxis.set_major_formatter(FuncFormatter(fcfa))
        ax.bar_label(barres, labels=[fcfa(v) for v in d["ca_total"]],
                     padding=4, fontsize=9, color=TEXTE_PRINCIPAL)
        ax.set_xlim(0, d["ca_total"].max() * 1.18)
        ax.grid(axis="y", visible=False)
        ax.legend(handles=[Patch(facecolor=couleur[c], label=c) for c in categories],
                  loc="lower right", ncol=2)
        _note(ax, source + " - les 21 produits sont tires de maniere equiprobable : "
                           "ce classement reflete les prix unitaires, pas les preferences d'achat.")
        fig.savefig(os.path.join(dossier_png, "top10_produits.png"))
        plt.close(fig)

    # --- 5. Composition : categorie x mode de paiement ----------------------
    df = lire("Q4_ventes_categorie_paiement")
    if df is not None:
        pivot = df.pivot(index="categorie", columns="mode_paiement", values="ca_total").fillna(0)
        pivot = pivot.loc[pivot.sum(axis=1).sort_values().index]
        modes = list(pivot.columns)          # ordre alphabetique, donc stable
        fig, ax = plt.subplots(figsize=(9, 4.4))
        gauche = pd.Series(0.0, index=pivot.index)
        for i, mode in enumerate(modes):
            ax.barh(pivot.index, pivot[mode], left=gauche, height=0.62,
                    color=SERIES[i % len(SERIES)], label=mode,
                    edgecolor=SURFACE, linewidth=1.4)   # separateur de 2 px
            gauche += pivot[mode]
        ax.set_title("Chiffre d'affaires par categorie et mode de paiement")
        ax.xaxis.set_major_formatter(FuncFormatter(fcfa))
        ax.grid(axis="y", visible=False)
        ax.legend(loc="lower right", ncol=3, title=None)
        _note(ax, source + " - valeurs detaillees dans Q4_ventes_categorie_paiement.csv.")
        fig.savefig(os.path.join(dossier_png, "ca_categorie_paiement.png"))
        plt.close(fig)

    # --- 6. Promotions : petits multiples, jamais deux axes y ---------------
    df = lire("Q5_impact_promotions")
    if df is not None and len(df) == 2:
        d = df.sort_values("promotion")
        libelles = ["Sans promotion", "Avec promotion"]
        couleurs = [SERIES[0], SERIES[1]]
        fig, axes = plt.subplots(1, 2, figsize=(9, 4))
        for ax, colonne, titre, fmt in (
                (axes[0], "panier_moyen", "Panier moyen (FCFA)", "{:,.0f}"),
                (axes[1], "quantite_moyenne", "Quantite moyenne par transaction", "{:.3f}")):
            barres = ax.bar(libelles, d[colonne], color=couleurs, width=0.55)
            ax.set_title(titre, fontsize=11)
            ax.bar_label(barres, labels=[fmt.format(v).replace(",", " ") for v in d[colonne]],
                         padding=4, fontsize=9, color=TEXTE_PRINCIPAL)
            ax.set_ylim(0, d[colonne].max() * 1.2)
            ax.grid(axis="x", visible=False)
        q0, q1 = d["quantite_moyenne"].tolist()
        ecart = 100 * abs(q1 - q0) / q0
        # Note ancree au pied de la FIGURE et non d'un axe : avec deux
        # sous-graphiques, une note ancree a l'axe recouvre les etiquettes de
        # l'abscisse (defaut constate en relisant le rendu).
        fig.subplots_adjust(bottom=0.32)
        fig.text(0.02, 0.02,
                 source + " - ecart de quantite moyenne : %.2f %%. Dans ce jeu, la quantite\n"
                 "est tiree independamment de la promotion : cette egalite est une propriete\n"
                 "du generateur, pas un resultat commercial." % ecart,
                 fontsize=8, color=TEXTE_SECONDAIRE, va="bottom")
        fig.savefig(os.path.join(dossier_png, "impact_promotions.png"))
        plt.close(fig)
        faits.append("quantite moyenne : ecart de %.2f %% entre avec et sans promotion" % ecart)

    # --- 7. Jours de la semaine : axe a zero, l'uniformite est le message ---
    df = lire("Q6_activite_jour_semaine")
    if df is not None:
        d = df.sort_values("ordre") if "ordre" in df.columns else df
        fig, ax = plt.subplots(figsize=(8.5, 4))
        ax.bar(d["jour_semaine"], d["nb_transactions"], color=SERIES[0], width=0.6)
        ax.set_title("Nombre de transactions par jour de la semaine")
        ax.grid(axis="x", visible=False)
        ax.set_ylim(0, d["nb_transactions"].max() * 1.15)
        amplitude = 100 * (d["nb_transactions"].max() - d["nb_transactions"].min()) / d["nb_transactions"].mean()
        _note(ax, source + " - axe partant de zero : l'ecart entre le jour le plus et le moins\n"
                           "actif n'est que de %.1f %%, l'activite est donc uniforme sur la semaine." % amplitude)
        fig.savefig(os.path.join(dossier_png, "activite_jour_semaine.png"))
        plt.close(fig)
        faits.append("amplitude hebdomadaire : %.1f %%" % amplitude)

    # --- 8. Benchmark des representations -----------------------------------
    chemin_bench = os.path.join(os.path.dirname(dossier_csv.rstrip(os.sep)), "benchmark_hive.csv")
    if not os.path.exists(chemin_bench):
        chemin_bench = os.path.join(RESULTATS, "benchmark_hive.csv")
    if os.path.exists(chemin_bench):
        b = pd.read_csv(chemin_bench)
        if {"requete", "table_source", "duree_min_s"} <= set(b.columns):
            b = b.sort_values(["requete", "duree_min_s"])
            etiquettes = b["requete"] + "  |  " + b["table_source"] + " (" + b["format"] + ")"
            fig, ax = plt.subplots(figsize=(9.5, 0.45 * len(b) + 1.8))
            barres = ax.barh(etiquettes, b["duree_min_s"], color=SERIES[0], height=0.6)
            ax.invert_yaxis()
            ax.set_title("Duree des requetes selon la representation de stockage")
            ax.set_xlabel("secondes (meilleure des executions)")
            ax.bar_label(barres, fmt="%.1f s", padding=4, fontsize=9, color=TEXTE_PRINCIPAL)
            ax.set_xlim(0, b["duree_min_s"].max() * 1.15)
            ax.grid(axis="y", visible=False)
            _note(ax, "Meme requete, representations differentes. Les lignes d'une meme requete\n"
                      "sont comparables entre elles ; deux requetes differentes ne le sont pas.")
            fig.savefig(os.path.join(dossier_png, "benchmark_formats.png"))
            plt.close(fig)

    produits = sorted(f for f in os.listdir(dossier_png) if f.endswith(".png"))
    print("\n--- Graphiques (%d) ---" % len(produits))
    for f in produits:
        print("  " + f)
    if faits:
        print("\n--- Faits mesures ---")
        for f in faits:
            print("  - " + f)
    return produits


# ---------------------------------------------------------------------------
# 6. Comparaisons automatiques
# ---------------------------------------------------------------------------
def lancer_comparaison(dossier_a, dossier_b, rapport, titre):
    """Delegue a comparer_resultats.py, qui reste utilisable seul."""
    import subprocess
    print("\n--- %s ---" % titre)
    if not (os.path.isdir(dossier_a) and os.path.isdir(dossier_b)):
        print("  dossier manquant, comparaison ignoree :")
        print("    %s" % dossier_a)
        print("    %s" % dossier_b)
        return None
    code = subprocess.call([sys.executable,
                            os.path.join(ICI, "comparer_resultats.py"),
                            dossier_a, dossier_b, str(TOLERANCE), rapport])
    return code == 0


# ---------------------------------------------------------------------------
# 7. Programme principal
# ---------------------------------------------------------------------------
def main():
    analyseur = argparse.ArgumentParser(
        description="TP6 Groupe 6 - pilotage de Hive, benchmark, controles et graphiques")
    analyseur.add_argument("--sans-hive", action="store_true",
                           help="ne pas se connecter au cluster : graphiques et comparaisons "
                                "a partir de CSV deja presents")
    analyseur.add_argument("--source", default=None,
                           help="dossier de CSV a utiliser avec --sans-hive "
                                "(defaut : resultats/attendus_sql/medium)")
    analyseur.add_argument("--hote", default="localhost")
    analyseur.add_argument("--port", type=int, default=10000)
    analyseur.add_argument("--base", default=BASE)
    analyseur.add_argument("--repetitions", type=int, default=2,
                           help="nombre d'executions par cas de benchmark (defaut 2)")
    options = analyseur.parse_args()

    os.makedirs(RESULTATS, exist_ok=True)

    # ---- Mode hors cluster -------------------------------------------------
    if options.sans_hive:
        source = options.source or ATTENDUS_MEDIUM
        if not os.path.isdir(source):
            sys.exit("Dossier source introuvable : %s" % source)
        print("MODE SANS CLUSTER : graphiques et comparaisons depuis %s\n" % source)
        graphiques(source, DOSSIER_GRAPHIQUES,
                   volume=os.path.basename(source.rstrip(os.sep)))
        lancer_comparaison(REFERENCE_MAPREDUCE, source,
                           os.path.join(RESULTATS, "comparaison_mapreduce_vs_sql.csv"),
                           "Reference MapReduce contre resultats SQL attendus")
        print("\nTermine (sans cluster). Les durees Hive et le pruning exigent le cluster.")
        return 0

    # ---- Mode complet ------------------------------------------------------
    conn = connecter(options.hote, options.port, options.base)
    try:
        exportes = exporter_resultats(conn, DOSSIER_HIVE)
        if not exportes:
            sys.exit("Aucun resultat a exporter : executer scripts\\03_executer_hive.ps1 "
                     "avant ce script.")
        executer_benchmark(conn, RESULTATS, options.repetitions)
        print("\n--- Partition pruning : plans d'execution ---")
        expliquer_pruning(conn, RESULTATS)
    finally:
        conn.close()

    graphiques(DOSSIER_HIVE, DOSSIER_GRAPHIQUES, volume="medium")

    verdicts = [
        lancer_comparaison(DOSSIER_HIVE, ATTENDUS_MEDIUM,
                           os.path.join(RESULTATS, "comparaison_hive_vs_sql.csv"),
                           "Hive contre resultats SQL attendus (second moteur, meme logique)"),
        lancer_comparaison(DOSSIER_HIVE, REFERENCE_MAPREDUCE,
                           os.path.join(RESULTATS, "comparaison_hive_vs_mapreduce.csv"),
                           "Hive contre reference MapReduce (Map / Shuffle-Sort / Reduce)"),
    ]

    print("\n" + "=" * 70)
    print("RECAPITULATIF")
    print("=" * 70)
    print("  resultats CSV        : %s" % DOSSIER_HIVE)
    print("  graphiques           : %s" % DOSSIER_GRAPHIQUES)
    print("  benchmark            : %s" % os.path.join(RESULTATS, "benchmark_hive.csv"))
    print("  plans d'execution    : %s" % os.path.join(RESULTATS, "explain_pruning.txt"))
    print("  rapports de controle : comparaison_hive_vs_sql.csv, comparaison_hive_vs_mapreduce.csv")
    if all(v is True for v in verdicts):
        print("\n  Tous les controles concordent.")
        return 0
    print("\n  AU MOINS UN CONTROLE A ECHOUE : lire les rapports de comparaison et")
    print("  expliquer chaque divergence dans docs\\anomalies_et_divergences.md,")
    print("  comme l'exige la consigne 14 de l'enonce.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
