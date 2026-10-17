#!/usr/bin/env bash
# ============================================================================
# lancer_reference_mapreduce.sh
# TP6 - Hive et Data Warehouse distribue - Groupe 6 (M1 Big Data, UCAO-UUT)
#
# Execute les 5 jobs MapReduce de reference en pipes Unix, selon le schema
#   MAP  ->  SHUFFLE/SORT  ->  REDUCE
# c'est-a-dire exactement ce que Hadoop Streaming enchaine, mais sur une seule
# machine. Sert a produire les references de comparaison aux resultats Hive
# tant que le Groupe 4 n'a pas livre ses sorties HDFS.
#
#   LC_ALL=C sort -k1,1   reproduit le tri par octets de la phase Shuffle/Sort
#                          de Hadoop (sans lui, l'ordre depend de la locale).
#
# Usage :
#   ./lancer_reference_mapreduce.sh <fichier.csv> <dossier_sortie>
# Exemple :
#   ./lancer_reference_mapreduce.sh ../data/transactions_medium.csv \
#                                   ../resultats/reference_mapreduce
# ============================================================================
set -euo pipefail

ENTREE="${1:?Usage: $0 <fichier.csv> <dossier_sortie>}"
SORTIE="${2:?Usage: $0 <fichier.csv> <dossier_sortie>}"
ICI="$(cd "$(dirname "$0")" && pwd)"

[ -f "$ENTREE" ] || { echo "Fichier introuvable : $ENTREE" >&2; exit 1; }
mkdir -p "$SORTIE"

# job -> "colonne_cle colonne_valeur" (noms alignes sur les requetes Hive)
declare -A ENTETES=(
  [transactions_par_ville]="ville,nb_transactions"
  [ca_par_ville]="ville,ca"
  [quantite_par_produit]="produit,quantite_vendue"
  [ca_par_categorie]="categorie,ca"
  [transactions_par_paiement]="mode_paiement,nb_transactions"
)
# Les jobs de CA sont arrondis a 2 decimales a l'export, comme le ROUND() de Hive
declare -A ARRONDI=( [ca_par_ville]=1 [ca_par_categorie]=1 )

BENCH="$SORTIE/benchmark_reference_mapreduce.csv"
echo "job,nb_cles,duree_s,lignes_mappees,lignes_rejetees,fichier_entree" > "$BENCH"

NB_LIGNES=$(( $(wc -l < "$ENTREE") - 1 ))
echo "Entree  : $ENTREE ($NB_LIGNES transactions)"
echo "Sortie  : $SORTIE"
echo

for JOB in transactions_par_ville ca_par_ville quantite_par_produit ca_par_categorie transactions_par_paiement; do
  BRUT="$SORTIE/ref_${JOB}.tsv"
  CSV="$SORTIE/ref_${JOB}.csv"
  LOG="$SORTIE/ref_${JOB}.log"

  DEBUT=$(date +%s.%N)
  # MAP                                   SHUFFLE/SORT          REDUCE
  JOB="$JOB" python3 "$ICI/mapper.py" < "$ENTREE" 2> "$LOG" \
    | LC_ALL=C sort -k1,1 \
    | JOB="$JOB" python3 "$ICI/reducer.py" 2>> "$LOG" \
    | LC_ALL=C sort -k1,1 > "$BRUT"
  FIN=$(date +%s.%N)

  DUREE=$(awk -v a="$DEBUT" -v b="$FIN" 'BEGIN{printf "%.2f", b-a}')
  NB_CLES=$(wc -l < "$BRUT")
  MAPPEES=$(grep -o 'lignes_mappees,[0-9]*'  "$LOG" | head -1 | cut -d, -f2)
  REJETEES=$(grep -o 'lignes_rejetees,[0-9]*' "$LOG" | head -1 | cut -d, -f2)

  # Export CSV : en-tete + arrondi des montants (une seule fois, en sortie)
  { echo "${ENTETES[$JOB]}"
    if [ "${ARRONDI[$JOB]:-0}" = "1" ]; then
      awk -F'\t' '{printf "%s,%.2f\n", $1, $2}' "$BRUT"
    else
      awk -F'\t' '{printf "%s,%s\n", $1, $2}' "$BRUT"
    fi
  } > "$CSV"

  printf "%-28s %3s cles  %7ss  %8s lignes mappees  %s rejetees\n" \
         "$JOB" "$NB_CLES" "$DUREE" "$MAPPEES" "$REJETEES"
  echo "$JOB,$NB_CLES,$DUREE,$MAPPEES,$REJETEES,$(basename "$ENTREE")" >> "$BENCH"
done

echo
echo "References ecrites dans $SORTIE (ref_*.csv), benchmark dans $(basename "$BENCH")"
