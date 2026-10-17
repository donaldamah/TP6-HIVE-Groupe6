-- ============================================================================
-- analyse.hql
-- TP6 - Hive et Data Warehouse distribue - GROUPE 6
-- Big Data, Hadoop et applications Python - Dr. Sena Apeke
-- M1 Big Data, Ecole Superieure d Ingenieurs, UCAO-UUT - 2025/2026
--
-- Groupe 6 : AMAH-TCHOUTCHOUI Ekue Donald, FIOKLOU Donia, MINGANE Gracia,
--            MOUN-RE Maxime
--
-- OBJET
--   Construire une couche analytique SQL (Data Warehouse) au-dessus des
--   donnees HDFS du fil conducteur, et deposer les resultats attendus par le
--   Groupe 7 dans /datalake/results/hive/.
--
-- EXECUTION
--   docker cp analyse.hql hive-server:/analyse.hql
--   docker exec -it hive-server hive -f /analyse.hql 2>&1 | tee analyse_hive.log
--
-- PREALABLE : les trois volumes charges dans HDFS (scripts/02_charger_hdfs.ps1)
--   /datalake/raw/transactions/small/   transactions_small.csv
--   /datalake/raw/transactions/medium/  transactions_medium.csv
--   /datalake/raw/transactions/big/     transactions_big.csv.gz
--
-- ANOMALIE DE TRANSMISSION (enonce, section 3)
--   Au 13/10/2026, les Groupes 1 a 5 n ont transmis aucun livrable : ni Data
--   Lake HDFS, ni sorties MapReduce, ni benchmark YARN. L arborescence
--   /datalake a donc ete creee par le Groupe 6 en respectant la structure
--   imposee au Groupe 2. Cette reconstruction est documentee dans
--   docs/anomalies_et_divergences.md : elle n est pas silencieuse.
-- ============================================================================

-- NOTE TECHNIQUE : aucun commentaire de ce fichier ne porte d apostrophe.
-- Hive CLI retire les lignes de commentaire par line.startsWith("--"), SANS
-- trim prealable : un commentaire INDENTE part donc au parseur. Or Hive suit
-- les apostrophes pour decouper les instructions sur le point-virgule. Une
-- seule apostrophe isolee dans un commentaire ouvre une chaine fictive, le
-- point-virgule suivant est ignore, et deux instructions fusionnent. On
-- obtient alors une erreur de syntaxe qui designe une ligne parfaitement
-- correcte, situee plus loin dans le fichier (constate le 13/10/2026 sur la
-- clause FROM de la section 2). Les apostrophes des commentaires ont donc
-- ete remplacees par une espace, et il faut les y laisser.

-- ----------------------------------------------------------------------------
-- 0. Reglages de session
-- ----------------------------------------------------------------------------
SET hive.cli.print.header=true;

-- Partitionnement dynamique : les valeurs de (annee, mois) sont calculees par
-- la requete et non ecrites a la main. Le mode nonstrict est obligatoire
-- des lors qu aucune partition n est fournie en statique.
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=100;

-- Compression ORC explicite : c est deja la valeur par defaut, on l ecrit
-- pour que la mesure de taille de la section 5 soit reproductible.
SET hive.exec.orc.default.compress=ZLIB;

-- Un Data Warehouse ne vit pas dans la base default .
CREATE DATABASE IF NOT EXISTS datawarehouse
  COMMENT 'TP6 Groupe 6 - couche analytique Hive sur le Data Lake';
USE datawarehouse;

-- ----------------------------------------------------------------------------
-- 1. Tables EXTERNES sur HDFS, une par volume
--
--    EXTERNAL : Hive ne detient que le schema, les fichiers restent la
--    propriete du Data Lake. Un DROP TABLE ne detruit donc aucune donnee,
--    ce qui est la regle pour la couche  raw .
--
--    skip.header.line.count : les CSV du fil conducteur portent une ligne
--    d en-tete. Sans cette propriete, elle est lue comme une transaction et
--    tous ses champs numeriques valent NULL (controle en section 2).
-- ----------------------------------------------------------------------------

-- 1.a Volume medium (500 000 lignes) : developpement et validation.
--     C est la table  transactions  au sens de l enonce.
DROP TABLE IF EXISTS transactions;
CREATE EXTERNAL TABLE transactions (
    transaction_id STRING  COMMENT 'identifiant unique de la transaction',
    datetime       STRING  COMMENT 'date et heure, format YYYY-MM-DD HH:MM:SS',
    client_id      STRING  COMMENT 'identifiant du client',
    ville          STRING  COMMENT 'ville du magasin',
    magasin_id     STRING  COMMENT 'identifiant du point de vente',
    categorie      STRING  COMMENT 'categorie commerciale du produit',
    produit        STRING  COMMENT 'produit achete',
    quantite       INT     COMMENT 'nombre d unites achetees',
    prix_unitaire  DOUBLE  COMMENT 'prix unitaire en FCFA',
    remise         DOUBLE  COMMENT 'taux de remise, entre 0 et 0,30',
    mode_paiement  STRING  COMMENT 'Especes, TMoney, Flooz, Carte ou Virement',
    age_client     INT     COMMENT 'age du client',
    type_client    STRING  COMMENT 'Particulier, Professionnel ou Administration',
    stock_avant    INT     COMMENT 'stock disponible avant la transaction',
    stock_apres    INT     COMMENT 'stock restant apres la transaction',
    promotion      INT     COMMENT 'indicateur 0/1 de promotion',
    jour_semaine   STRING  COMMENT 'jour de la semaine, en anglais',
    livraison      INT     COMMENT 'indicateur 0/1 de livraison',
    montant        DOUBLE  COMMENT 'montant net = quantite x prix_unitaire x (1 - remise)'
)
COMMENT 'Transactions commerciales multisites - volume medium (500 000 lignes)'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/raw/transactions/medium'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 1.b Volume small (10 000 lignes) : tests rapides.
DROP TABLE IF EXISTS transactions_small;
CREATE EXTERNAL TABLE transactions_small (
    transaction_id STRING  COMMENT 'identifiant unique de la transaction',
    datetime       STRING  COMMENT 'date et heure, format YYYY-MM-DD HH:MM:SS',
    client_id      STRING  COMMENT 'identifiant du client',
    ville          STRING  COMMENT 'ville du magasin',
    magasin_id     STRING  COMMENT 'identifiant du point de vente',
    categorie      STRING  COMMENT 'categorie commerciale du produit',
    produit        STRING  COMMENT 'produit achete',
    quantite       INT     COMMENT 'nombre d unites achetees',
    prix_unitaire  DOUBLE  COMMENT 'prix unitaire en FCFA',
    remise         DOUBLE  COMMENT 'taux de remise, entre 0 et 0,30',
    mode_paiement  STRING  COMMENT 'Especes, TMoney, Flooz, Carte ou Virement',
    age_client     INT     COMMENT 'age du client',
    type_client    STRING  COMMENT 'Particulier, Professionnel ou Administration',
    stock_avant    INT     COMMENT 'stock disponible avant la transaction',
    stock_apres    INT     COMMENT 'stock restant apres la transaction',
    promotion      INT     COMMENT 'indicateur 0/1 de promotion',
    jour_semaine   STRING  COMMENT 'jour de la semaine, en anglais',
    livraison      INT     COMMENT 'indicateur 0/1 de livraison',
    montant        DOUBLE  COMMENT 'montant net = quantite x prix_unitaire x (1 - remise)'
)
COMMENT 'Transactions commerciales multisites - volume small (10 000 lignes)'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/raw/transactions/small'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 1.c Volume big (5 000 000 lignes, gzip) : experimentation Big Data.
--     Hive decompresse le .gz de maniere transparente sur une table TEXTFILE.
--     POINT CLE : un fichier gzip n est PAS splittable. Le fichier entier est
--     donc traite par UN SEUL mapper, quel que soit le nombre de blocs HDFS.
--     C est precisement le goulet d etranglement que le Groupe 5 doit observer
--     sur YARN, et la raison de convertir ensuite en ORC, qui est splittable
--     et compresse par bandes.
DROP TABLE IF EXISTS transactions_big;
CREATE EXTERNAL TABLE transactions_big (
    transaction_id STRING  COMMENT 'identifiant unique de la transaction',
    datetime       STRING  COMMENT 'date et heure, format YYYY-MM-DD HH:MM:SS',
    client_id      STRING  COMMENT 'identifiant du client',
    ville          STRING  COMMENT 'ville du magasin',
    magasin_id     STRING  COMMENT 'identifiant du point de vente',
    categorie      STRING  COMMENT 'categorie commerciale du produit',
    produit        STRING  COMMENT 'produit achete',
    quantite       INT     COMMENT 'nombre d unites achetees',
    prix_unitaire  DOUBLE  COMMENT 'prix unitaire en FCFA',
    remise         DOUBLE  COMMENT 'taux de remise, entre 0 et 0,30',
    mode_paiement  STRING  COMMENT 'Especes, TMoney, Flooz, Carte ou Virement',
    age_client     INT     COMMENT 'age du client',
    type_client    STRING  COMMENT 'Particulier, Professionnel ou Administration',
    stock_avant    INT     COMMENT 'stock disponible avant la transaction',
    stock_apres    INT     COMMENT 'stock restant apres la transaction',
    promotion      INT     COMMENT 'indicateur 0/1 de promotion',
    jour_semaine   STRING  COMMENT 'jour de la semaine, en anglais',
    livraison      INT     COMMENT 'indicateur 0/1 de livraison',
    montant        DOUBLE  COMMENT 'montant net = quantite x prix_unitaire x (1 - remise)'
)
COMMENT 'Transactions commerciales multisites - volume big (5 000 000 lignes, gzip)'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/raw/transactions/big'
TBLPROPERTIES ("skip.header.line.count"="1");

SHOW TABLES;
DESCRIBE FORMATTED transactions;

-- ----------------------------------------------------------------------------
-- 2. CONTROLES QUALITE (Q0), avant toute analyse
--
--    Rien n est suppose : on verifie le volume, l unicite des identifiants,
--    la coherence de la colonne montant avec sa definition, les bornes de la
--    remise, la coherence des stocks, et l absence de NULL de parsing.
--    Les memes controles sont rejoues hors Hive par python/valider_sql.py :
--    les deux moteurs doivent donner les memes nombres.
-- ----------------------------------------------------------------------------
SELECT 'medium'                         AS volume,
       COUNT(*)                         AS nb_lignes,
       COUNT(DISTINCT transaction_id)   AS nb_ids_distincts,
 -- un NULL ici signalerait une ligne mal decoupee, ou l en-tete non ignore
       SUM(CASE WHEN quantite IS NULL OR prix_unitaire IS NULL
                  OR remise IS NULL OR montant IS NULL
                THEN 1 ELSE 0 END)      AS champs_numeriques_nuls,
       -- montant est-il bien egal a quantite x prix_unitaire x (1 - remise) ?
       SUM(CASE WHEN ABS(montant - quantite * prix_unitaire * (1 - remise)) > 0.01
                THEN 1 ELSE 0 END)      AS montants_incoherents,
       SUM(CASE WHEN remise < 0 OR remise > 0.30 THEN 1 ELSE 0 END)
                                        AS remises_hors_bornes,
       SUM(CASE WHEN promotion = 0 AND remise > 0 THEN 1 ELSE 0 END)
                                        AS promo0_avec_remise,
       SUM(CASE WHEN stock_apres > stock_avant THEN 1 ELSE 0 END)
                                        AS stocks_incoherents,
       COUNT(DISTINCT ville)            AS nb_villes,
       COUNT(DISTINCT produit)          AS nb_produits,
       COUNT(DISTINCT mode_paiement)    AS nb_modes_paiement,
       MIN(datetime)                    AS premiere_transaction,
       MAX(datetime)                    AS derniere_transaction,
       ROUND(SUM(montant), 2)           AS ca_total
FROM transactions;

-- Le meme controle sur le volume small, qui doit etre coherent en ordre de
-- grandeur (les trois volumes sont des tirages independants de la meme loi,
-- voir docs/anomalies_et_divergences.md).
SELECT 'small'                          AS volume,
       COUNT(*)                         AS nb_lignes,
       COUNT(DISTINCT transaction_id)   AS nb_ids_distincts,
       ROUND(SUM(montant), 2)           AS ca_total
FROM transactions_small;

-- Modalites reellement presentes, a confronter au dictionnaire des variables.
-- Le dictionnaire annonce "Especes, Mobile Money, carte ou virement", soit
-- 4 modalites ; les donnees en contiennent 5, le Mobile Money etant eclate en
-- TMoney et Flooz. Divergence signalee et NON corrigee : corriger reviendrait
-- a modifier les donnees du fil conducteur.
SELECT mode_paiement, COUNT(*) AS nb_transactions
FROM transactions GROUP BY mode_paiement ORDER BY nb_transactions DESC;

-- Villes : le dictionnaire et les rapports ecrivent "Lome, Sokode, Kpalime"
-- avec accents, les donnees sont sans accents. Toute jointure ou tout filtre
-- sur un libelle accentue renverrait donc zero ligne.
SELECT ville, COUNT(*) AS nb_transactions
FROM transactions GROUP BY ville ORDER BY nb_transactions DESC;

-- ----------------------------------------------------------------------------
-- 3. ORGANISATION PARTITIONNEE : (annee, mois) au format ORC
--
--    POURQUOI CE DECOUPAGE
--    Les analyses du TP sont presque toutes temporelles (CA mensuel, CA annuel)
--    et les donnees couvrent 24 mois, soit 24 partitions d environ 20 800
--    lignes sur le volume medium. C est un bon compromis : assez de partitions
--    pour que le filtrage soit utile, assez de lignes par partition pour ne pas
--    tomber dans le probleme des petits fichiers de HDFS.
--    Partitionner par ville (8 valeurs tres inegales, Lome pesant 40 %) ou par
--    transaction_id (500 000 valeurs distinctes) serait au contraire
--    inexploitable.
--
--    POURQUOI ORC
--    Stockage en colonnes : une requete qui ne lit que  montant  et  ville  ne
--    decompresse pas les 17 autres colonnes. ORC porte en outre des index et
--    des statistiques min/max par bande, et il est splittable - contrairement
--    au gzip du volume big.
--
--    LOCATION dans /datalake/processed/ : la couche raw reste intacte, la
--    couche processed recoit les donnees retravaillees, conformement a
--    l arborescence imposee au Groupe 2.
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS transactions_part;
CREATE EXTERNAL TABLE transactions_part (
    transaction_id STRING,
    datetime       STRING,
    client_id      STRING,
    ville          STRING,
    magasin_id     STRING,
    categorie      STRING,
    produit        STRING,
    quantite       INT,
    prix_unitaire  DOUBLE,
    remise         DOUBLE,
    mode_paiement  STRING,
    age_client     INT,
    type_client    STRING,
    stock_avant    INT,
    stock_apres    INT,
    promotion      INT,
    jour_semaine   STRING,
    livraison      INT,
    montant        DOUBLE
)
COMMENT 'Transactions medium, format ORC, partitionnees par annee et mois'
PARTITIONED BY (annee INT, mois INT)
STORED AS ORC
LOCATION '/datalake/processed/transactions_part'
TBLPROPERTIES ("orc.compress"="ZLIB");

-- Insertion dynamique : annee et mois sont derives de datetime. Les colonnes
-- de partition doivent etre les DERNIERES du SELECT, dans l ordre declare par
-- PARTITIONED BY.
INSERT OVERWRITE TABLE transactions_part PARTITION (annee, mois)
SELECT transaction_id, datetime, client_id, ville, magasin_id,
       categorie, produit, quantite, prix_unitaire, remise,
       mode_paiement, age_client, type_client, stock_avant, stock_apres,
       promotion, jour_semaine, livraison, montant,
       CAST(SUBSTR(datetime, 1, 4) AS INT) AS annee,
       CAST(SUBSTR(datetime, 6, 2) AS INT) AS mois
FROM transactions;

-- Doit afficher 24 partitions : annee=2024/mois=1 ... annee=2025/mois=12
SHOW PARTITIONS transactions_part;

-- Controle de conservation : la conversion ne doit perdre ni ligne ni franc.
-- Les deux lignes de ce resultat doivent etre identiques.
SELECT 'transactions (TEXTFILE)' AS source,
       COUNT(*) AS nb_lignes, ROUND(SUM(montant), 2) AS ca_total
FROM transactions
UNION ALL
SELECT 'transactions_part (ORC)'  AS source,
       COUNT(*) AS nb_lignes, ROUND(SUM(montant), 2) AS ca_total
FROM transactions_part;

-- Statistiques du metastore : indispensables pour que DESCRIBE FORMATTED
-- renvoie numRows et totalSize, utilises a la section 5.
ANALYZE TABLE transactions COMPUTE STATISTICS;
ANALYZE TABLE transactions_part PARTITION(annee, mois) COMPUTE STATISTICS;

-- ----------------------------------------------------------------------------
-- 4. DEMONSTRATION DU PARTITION PRUNING
--
--    Le partitionnement ne sert a rien si aucune requete ne filtre sur la cle.
--    Les requetes ci-dessous calculent LE MEME chiffre - le CA de mars 2025 -
--    de deux facons :
--      A) sur la table ORC partitionnee, en filtrant annee et mois : Hive ne
--         lit qu un seul repertoire de partition ;
--      B) sur la table TEXTFILE, en filtrant une sous-chaine de datetime :
--         Hive doit lire et decouper les 500 000 lignes.
--    Comparer les deux plans EXPLAIN : le premier annonce une seule partition,
--    le second un balayage complet. python/tp6_hive.py en mesure les temps.
-- ----------------------------------------------------------------------------
EXPLAIN
SELECT COUNT(*) AS nb_transactions, ROUND(SUM(montant), 2) AS ca
FROM transactions_part
WHERE annee = 2025 AND mois = 3;

SELECT COUNT(*) AS nb_transactions, ROUND(SUM(montant), 2) AS ca
FROM transactions_part
WHERE annee = 2025 AND mois = 3;

EXPLAIN
SELECT COUNT(*) AS nb_transactions, ROUND(SUM(montant), 2) AS ca
FROM transactions
WHERE SUBSTR(datetime, 1, 7) = '2025-03';

SELECT COUNT(*) AS nb_transactions, ROUND(SUM(montant), 2) AS ca
FROM transactions
WHERE SUBSTR(datetime, 1, 7) = '2025-03';

-- Filtrage sur un trimestre : plusieurs partitions lues, toujours pas de
-- balayage complet. Le pruning fonctionne aussi sur un intervalle.
SELECT annee, mois, COUNT(*) AS nb_transactions, ROUND(SUM(montant), 2) AS ca
FROM transactions_part
WHERE annee = 2025 AND mois BETWEEN 1 AND 3
GROUP BY annee, mois
ORDER BY annee, mois;

-- ----------------------------------------------------------------------------
-- 5. COUT DE STOCKAGE : TEXTFILE contre ORC
--
--    DESCRIBE FORMATTED expose numRows et totalSize (en octets), renseignes par
--    ANALYZE TABLE. Le gain d ORC vient de la compression ZLIB appliquee
--    colonne par colonne : les colonnes repetitives (ville, categorie,
--    jour_semaine) se compressent beaucoup mieux qu en texte.
--    La mesure cote HDFS est faite en parallele par
--    scripts/03_executer_hive.ps1 (hdfs dfs -du -h).
-- ----------------------------------------------------------------------------
DESCRIBE FORMATTED transactions;
DESCRIBE FORMATTED transactions_part PARTITION (annee=2025, mois=3);

-- ============================================================================
-- 6. LES SEPT BLOCS DE REQUETES D ANALYSE
--
--    Chaque resultat est materialise dans une table EXTERNE dont les fichiers
--    sont deposes dans /datalake/results/hive/<requete>/, c est-a-dire a
--    l emplacement prevu par l arborescence du Groupe 2 - par symetrie avec
--    /datalake/results/mapreduce/ attendu du Groupe 4.
--
--    Pourquoi des tables externes plutot qu un INSERT OVERWRITE DIRECTORY :
--    le Groupe 7 herite ainsi d un SCHEMA NOMME et TYPE, interrogeable en SQL
--    (SELECT * FROM datawarehouse.res_q2_ca_panier_ville) ET de fichiers CSV
--    lisibles directement par Spark. Un INSERT OVERWRITE DIRECTORY ne
--    produirait que des colonnes anonymes.
--
--    SOURCE DES REQUETES - difference a ne pas confondre dans le benchmark :
--      Q1 a Q6 interrogent transactions_part, la table ORC PARTITIONNEE ;
--      Q7.a a Q7.e interrogent transactions, la table TEXTFILE NON partitionnee,
--        pour etre strictement comparables aux jobs MapReduce qui lisent le
--        meme CSV brut. Les durees des deux series ne sont donc PAS comparables
--        entre elles : la colonne  table_source  du benchmark le precise.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Q1a - Chiffre d affaires mensuel
--   source : transactions_part
--   sortie : /datalake/results/hive/Q1a_ca_mensuel/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q1a_ca_mensuel;
CREATE EXTERNAL TABLE res_q1a_ca_mensuel (
    annee INT, mois INT, nb_transactions BIGINT, ca_mensuel DOUBLE
)
COMMENT 'Q1a - Chiffre d''affaires mensuel'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q1a_ca_mensuel';

INSERT OVERWRITE TABLE res_q1a_ca_mensuel
SELECT annee, mois,
       COUNT(*)               AS nb_transactions,
       ROUND(SUM(montant), 2) AS ca_mensuel
FROM transactions_part
GROUP BY annee, mois
ORDER BY annee, mois;

SELECT * FROM res_q1a_ca_mensuel ORDER BY annee, mois;

-- ----------------------------------------------------------------------------
-- Q1b - Chiffre d affaires annuel
--   source : transactions_part
--   sortie : /datalake/results/hive/Q1b_ca_annuel/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q1b_ca_annuel;
CREATE EXTERNAL TABLE res_q1b_ca_annuel (
    annee INT, nb_transactions BIGINT, ca_annuel DOUBLE
)
COMMENT 'Q1b - Chiffre d''affaires annuel'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q1b_ca_annuel';

INSERT OVERWRITE TABLE res_q1b_ca_annuel
SELECT annee,
       COUNT(*)               AS nb_transactions,
       ROUND(SUM(montant), 2) AS ca_annuel
FROM transactions_part
GROUP BY annee
ORDER BY annee;

SELECT * FROM res_q1b_ca_annuel ORDER BY annee;

-- ----------------------------------------------------------------------------
-- Q2 - Chiffre d affaires et panier moyen par ville
--   source : transactions_part
--   sortie : /datalake/results/hive/Q2_ca_panier_ville/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q2_ca_panier_ville;
CREATE EXTERNAL TABLE res_q2_ca_panier_ville (
    ville STRING, nb_transactions BIGINT, ca_total DOUBLE, panier_moyen DOUBLE
)
COMMENT 'Q2 - Chiffre d''affaires et panier moyen par ville'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q2_ca_panier_ville';

INSERT OVERWRITE TABLE res_q2_ca_panier_ville
-- Une ligne = une transaction = un panier : le panier moyen est donc
-- AVG(montant), et non SUM(montant) divise par le nombre de clients.
SELECT ville,
       COUNT(*)               AS nb_transactions,
       ROUND(SUM(montant), 2) AS ca_total,
       ROUND(AVG(montant), 2) AS panier_moyen
FROM transactions_part
GROUP BY ville
ORDER BY ca_total DESC;

SELECT * FROM res_q2_ca_panier_ville ORDER BY ca_total DESC;

-- ----------------------------------------------------------------------------
-- Q3 - Top 10 des produits par chiffre d affaires
--   source : transactions_part
--   sortie : /datalake/results/hive/Q3_top10_produits/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q3_top10_produits;
CREATE EXTERNAL TABLE res_q3_top10_produits (
    produit STRING, categorie STRING, quantite_vendue BIGINT, ca_total DOUBLE
)
COMMENT 'Q3 - Top 10 des produits par chiffre d''affaires'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q3_top10_produits';

INSERT OVERWRITE TABLE res_q3_top10_produits
-- Classement par CA. ATTENTION a l interpretation : les 21 produits du
-- catalogue sont tires de maniere equiprobable par le generateur, donc leurs
-- quantites vendues sont quasi identiques. Le classement par CA ne traduit
-- donc PAS une preference d achat, seulement l echelle des prix unitaires.
-- Un classement par quantite_vendue serait, sur ce jeu, du pur bruit.
SELECT produit, categorie,
       SUM(quantite)          AS quantite_vendue,
       ROUND(SUM(montant), 2) AS ca_total
FROM transactions_part
GROUP BY produit, categorie
ORDER BY ca_total DESC
LIMIT 10;

SELECT * FROM res_q3_top10_produits ORDER BY ca_total DESC;

-- ----------------------------------------------------------------------------
-- Q4 - Ventes par categorie et mode de paiement
--   source : transactions_part
--   sortie : /datalake/results/hive/Q4_ventes_categorie_paiement/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q4_ventes_categorie_paiement;
CREATE EXTERNAL TABLE res_q4_ventes_categorie_paiement (
    categorie STRING, mode_paiement STRING, nb_transactions BIGINT, quantite_vendue BIGINT, ca_total DOUBLE
)
COMMENT 'Q4 - Ventes par categorie et mode de paiement'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q4_ventes_categorie_paiement';

INSERT OVERWRITE TABLE res_q4_ventes_categorie_paiement
SELECT categorie, mode_paiement,
       COUNT(*)               AS nb_transactions,
       SUM(quantite)          AS quantite_vendue,
       ROUND(SUM(montant), 2) AS ca_total
FROM transactions_part
GROUP BY categorie, mode_paiement
ORDER BY categorie, ca_total DESC;

SELECT * FROM res_q4_ventes_categorie_paiement ORDER BY categorie, ca_total DESC;

-- ----------------------------------------------------------------------------
-- Q5 - Impact des promotions
--   source : transactions_part
--   sortie : /datalake/results/hive/Q5_impact_promotions/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q5_impact_promotions;
CREATE EXTERNAL TABLE res_q5_impact_promotions (
    promotion INT, nb_transactions BIGINT, ca_net DOUBLE, ca_brut DOUBLE, cout_remises DOUBLE, panier_moyen DOUBLE, quantite_moyenne DOUBLE, remise_moyenne DOUBLE
)
COMMENT 'Q5 - Impact des promotions'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q5_impact_promotions';

INSERT OVERWRITE TABLE res_q5_impact_promotions
-- cout_remises = CA brut (sans remise) - CA net (encaisse).
-- A INTERPRETER AVEC PRUDENCE : dans ce jeu, la quantite est tiree
-- independamment de la promotion. La quantite moyenne est donc identique avec
-- et sans promotion, et cette egalite est une propriete du generateur, pas un
-- resultat commercial. Conclure que "les promotions ne font pas vendre plus"
-- serait une faute d analyse.
SELECT promotion,
       COUNT(*)                                             AS nb_transactions,
       ROUND(SUM(montant), 2)                               AS ca_net,
       ROUND(SUM(quantite * prix_unitaire), 2)              AS ca_brut,
       ROUND(SUM(quantite * prix_unitaire) - SUM(montant), 2) AS cout_remises,
       ROUND(AVG(montant), 2)                               AS panier_moyen,
       ROUND(AVG(quantite), 3)                              AS quantite_moyenne,
       ROUND(AVG(remise), 4)                                AS remise_moyenne
FROM transactions_part
GROUP BY promotion
ORDER BY promotion;

SELECT * FROM res_q5_impact_promotions ORDER BY promotion;

-- ----------------------------------------------------------------------------
-- Q6 - Activite par jour de la semaine
--   source : transactions_part
--   sortie : /datalake/results/hive/Q6_activite_jour_semaine/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q6_activite_jour_semaine;
CREATE EXTERNAL TABLE res_q6_activite_jour_semaine (
    jour_semaine STRING, nb_transactions BIGINT, ca_total DOUBLE, panier_moyen DOUBLE, ordre INT
)
COMMENT 'Q6 - Activite par jour de la semaine'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q6_activite_jour_semaine';

INSERT OVERWRITE TABLE res_q6_activite_jour_semaine
-- La colonne ordre doit figurer dans le SELECT de la requete qui trie :
-- Hive 2.3 refuse un ORDER BY portant sur une colonne absente du SELECT.
-- Elle est conservee dans la table de resultats pour que le Groupe 7 puisse
-- retrouver l ordre calendaire sans recoder le CASE.
SELECT jour_semaine, nb_transactions, ca_total, panier_moyen, ordre
FROM (
    SELECT jour_semaine,
           COUNT(*)               AS nb_transactions,
           ROUND(SUM(montant), 2) AS ca_total,
           ROUND(AVG(montant), 2) AS panier_moyen,
           CASE jour_semaine
                WHEN 'Monday'    THEN 1 WHEN 'Tuesday'  THEN 2
                WHEN 'Wednesday' THEN 3 WHEN 'Thursday' THEN 4
                WHEN 'Friday'    THEN 5 WHEN 'Saturday' THEN 6
                ELSE 7 END        AS ordre
    FROM transactions_part
    GROUP BY jour_semaine
) t
ORDER BY ordre;

SELECT * FROM res_q6_activite_jour_semaine ORDER BY ordre;

-- ----------------------------------------------------------------------------
-- Q7.a - Nombre de transactions par ville
--   source : transactions
--   sortie : /datalake/results/hive/Q7_transactions_par_ville/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q7_transactions_par_ville;
CREATE EXTERNAL TABLE res_q7_transactions_par_ville (
    ville STRING, nb_transactions BIGINT
)
COMMENT 'Q7.a - Nombre de transactions par ville'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q7_transactions_par_ville';

INSERT OVERWRITE TABLE res_q7_transactions_par_ville
SELECT ville, COUNT(*) AS nb_transactions
FROM transactions
GROUP BY ville
ORDER BY ville;

SELECT * FROM res_q7_transactions_par_ville ORDER BY ville;

-- ----------------------------------------------------------------------------
-- Q7.b - Chiffre d affaires par ville
--   source : transactions
--   sortie : /datalake/results/hive/Q7_ca_par_ville/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q7_ca_par_ville;
CREATE EXTERNAL TABLE res_q7_ca_par_ville (
    ville STRING, ca DOUBLE
)
COMMENT 'Q7.b - Chiffre d''affaires par ville'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q7_ca_par_ville';

INSERT OVERWRITE TABLE res_q7_ca_par_ville
SELECT ville, ROUND(SUM(quantite * prix_unitaire * (1 - remise)), 2) AS ca
FROM transactions
GROUP BY ville
ORDER BY ville;

SELECT * FROM res_q7_ca_par_ville ORDER BY ville;

-- ----------------------------------------------------------------------------
-- Q7.c - Quantite vendue par produit
--   source : transactions
--   sortie : /datalake/results/hive/Q7_quantite_par_produit/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q7_quantite_par_produit;
CREATE EXTERNAL TABLE res_q7_quantite_par_produit (
    produit STRING, quantite_vendue BIGINT
)
COMMENT 'Q7.c - Quantite vendue par produit'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q7_quantite_par_produit';

INSERT OVERWRITE TABLE res_q7_quantite_par_produit
SELECT produit, SUM(quantite) AS quantite_vendue
FROM transactions
GROUP BY produit
ORDER BY produit;

SELECT * FROM res_q7_quantite_par_produit ORDER BY produit;

-- ----------------------------------------------------------------------------
-- Q7.d - Chiffre d affaires par categorie
--   source : transactions
--   sortie : /datalake/results/hive/Q7_ca_par_categorie/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q7_ca_par_categorie;
CREATE EXTERNAL TABLE res_q7_ca_par_categorie (
    categorie STRING, ca DOUBLE
)
COMMENT 'Q7.d - Chiffre d''affaires par categorie'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q7_ca_par_categorie';

INSERT OVERWRITE TABLE res_q7_ca_par_categorie
SELECT categorie, ROUND(SUM(quantite * prix_unitaire * (1 - remise)), 2) AS ca
FROM transactions
GROUP BY categorie
ORDER BY categorie;

SELECT * FROM res_q7_ca_par_categorie ORDER BY categorie;

-- ----------------------------------------------------------------------------
-- Q7.e - Nombre de transactions par mode de paiement
--   source : transactions
--   sortie : /datalake/results/hive/Q7_transactions_par_paiement/
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS res_q7_transactions_par_paiement;
CREATE EXTERNAL TABLE res_q7_transactions_par_paiement (
    mode_paiement STRING, nb_transactions BIGINT
)
COMMENT 'Q7.e - Nombre de transactions par mode de paiement'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/results/hive/Q7_transactions_par_paiement';

INSERT OVERWRITE TABLE res_q7_transactions_par_paiement
SELECT mode_paiement, COUNT(*) AS nb_transactions
FROM transactions
GROUP BY mode_paiement
ORDER BY mode_paiement;

SELECT * FROM res_q7_transactions_par_paiement ORDER BY mode_paiement;

-- ============================================================================
-- 7. LIVRABLES TRANSMIS AU GROUPE 7
--
--   Tables Hive, base  datawarehouse  :
--     transactions                  table externe TEXTFILE, volume medium
--     transactions_small            table externe TEXTFILE, volume small
--     transactions_big              table externe TEXTFILE gzip, volume big
--     transactions_part             table externe ORC partitionnee (annee, mois)
--                                   -> a privilegier comme source PySpark :
--                                      spark.table("datawarehouse.transactions_part")
--     res_q1a_... a res_q7_...      resultats de reference, un par requete
--
--   Fichiers HDFS :
--     /datalake/raw/transactions/{small,medium,big}/   donnees sources, intactes
--     /datalake/processed/transactions_part/           ORC partitionne, 24 partitions
--     /datalake/results/hive/<requete>/                resultats en CSV
--
--   Le dictionnaire des sorties (nom, colonnes, types, source) est dans
--   docs/dictionnaire_resultats.md. Les chiffres a retrouver, calcules par un
--   second moteur SQL independant, sont dans resultats/attendus_sql/.
-- ============================================================================
SHOW TABLES;
