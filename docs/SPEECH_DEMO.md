# Discours de la démonstration — TP6 Hive, Groupe 6

Document personnel, à ne pas joindre au rendu.

Ce qui est à dire, étape par étape, pendant les 8 minutes de démonstration. Le protocole technique est dans `FICHE_DEMO.md`.

**Trois règles pour tenir le fil :**

1. Vous parlez **pendant** que la commande s'exécute, pas après. Le silence d'attente est le pire moment d'une démo.
2. Vous annoncez **ce que le jury va voir avant qu'il le voie**. Sinon il lit l'écran au lieu de vous écouter.
3. Les étapes 5 et 8 sont celles qui rapportent des points. Les six autres, vous les traversez.

---

## Ouverture — avant la première commande

> « La démonstration tourne en direct sur le cluster. Il est déjà démarré : son lancement prend plusieurs minutes, nous ne vous le ferons pas attendre.
>
> Huit étapes. Elles répondent à trois questions : qu'est-ce que Hive décrit exactement, est-ce que notre façon de ranger les données sert à quelque chose, et est-ce que nos chiffres sont justes. »

*Appuyez sur Entrée.*

---

## Étape 1 — Le cluster · 30 secondes

**Pendant que la commande s'exécute :**

> « Je demande à Docker la liste des conteneurs. »

**Quand les cinq lignes s'affichent :**

> « Cinq services. Les deux premiers forment HDFS : le *namenode* est l'annuaire — il sait quel fichier est découpé en quels blocs et où ils sont, mais il ne contient aucune donnée. Le *datanode*, lui, contient les blocs.
>
> Les trois suivants forment Hive : le serveur qui reçoit nos requêtes SQL, le metastore qui décrit les tables, et la base PostgreSQL où ce dictionnaire est écrit.
>
> Un point que nous disons tout de suite plutôt que d'attendre qu'on nous le demande : **un seul DataNode**. La réplication vaut donc 1. Nous démontrons le fonctionnement du système, pas sa tolérance aux pannes. »

**Transition :**
> « Voyons ce que ce cluster contient. »

---

## Étape 2 — Le Data Lake · 45 secondes

**Pendant l'exécution :**

> « Je liste la couche *raw* du Data Lake — les données sources, jamais modifiées. »

**Quand les trois dossiers s'affichent :**

> « Trois volumes du même jeu : dix mille lignes pour les tests, cinq cent mille pour le travail, cinq millions pour l'expérimentation. Le dernier est compressé, il pèse 138 mégaoctets.
>
> Un détail sur la seconde commande : `du` affiche deux colonnes, et elles sont identiques. Ce n'est pas une répétition — la première est la taille du fichier, la seconde l'espace réellement consommé, taille multipliée par la réplication. Elles sont égales parce que notre réplication vaut 1. Sur un cluster à trois copies, la seconde vaudrait le triple. »

**Transition — elle prépare l'étape 3 :**
> « Ces fichiers ne nous appartiennent pas : les Groupes 7 et 8 les liront aussi. C'est exactement la raison du choix que je montre maintenant. »

---

## Étape 3 — La table externe · 1 minute

**Pendant l'exécution :**

> « `DESCRIBE FORMATTED` ne montre pas le contenu de la table. Il montre ce que Hive en sait. »

**Quand les quatre lignes s'affichent — montrez `EXTERNAL_TABLE` du doigt :**

> « Et ce qu'il en sait tient en deux lignes. D'abord l'emplacement : un dossier dans HDFS, pas un fichier — tout CSV déposé dedans entrerait automatiquement dans la table. Ensuite le type : **table externe**.
>
> Externe veut dire que `DROP TABLE` effacerait cette fiche descriptive, et rien d'autre. Les fichiers resteraient. C'était obligatoire ici : les Groupes 7 et 8 lisent les mêmes données, une table interne mal supprimée effacerait le Data Lake de toute la promotion.
>
> Et voici le point que nous aimerions faire retenir : **créer cette table n'a lu ni copié aucune donnée**. Pas un octet n'a bougé. Hive a écrit une fiche dans son metastore, c'est tout. C'est ce qu'on appelle le *schema on read* — le schéma s'applique à la lecture, pas à l'écriture. »

**Transition :**
> « Maintenant, comment nous avons rangé ces données pour nous. »

---

## Étape 4 — Les partitions · 1 minute

**Pendant l'exécution :**

> « Je demande à Hive la liste de ses partitions. »

**Quand les 24 lignes s'affichent :**

> « Vingt-quatre partitions : deux années, douze mois. L'ordre est alphabétique, pas chronologique — mois 10 avant mois 2 — parce que ce sont des noms de dossiers, comparés comme du texte.
>
> Et c'est bien là le point. »

**Quand la commande HDFS s'affiche :**

> « Regardez ce qu'elles sont réellement dans HDFS : **des dossiers**. La valeur de la partition n'est écrite nulle part dans les fichiers, elle est dans le chemin. C'est pour cela que partitionner ne coûte aucun espace. Et personne n'a écrit ces vingt-quatre noms : Hive les a déduits des dates. »

**Enchaînez sans attendre la question — c'est la question la plus probable de la soutenance :**

> « Pourquoi par mois et pas par ville ? Trois raisons, dans l'ordre.
>
> L'usage d'abord : une clé de partition ne sert que si les requêtes filtrent dessus, et les analyses demandées sont temporelles — chiffre d'affaires mensuel, chiffre d'affaires annuel. Aucune ne filtre sur une ville seule.
>
> L'équilibre ensuite : nos vingt-quatre partitions font environ vingt mille huit cents lignes chacune. Par ville, Lomé en concentrerait quarante pour cent et Aného cinq — un rapport de huit, et un travail mal réparti.
>
> Le nombre enfin : par jour, ce serait sept cent trente partitions de six cent quatre-vingt-cinq lignes. Par transaction, cinq cent mille partitions d'une ligne. On tomberait en plein dans le problème des petits fichiers de HDFS. »

**Transition — annoncez l'importance :**
> « Et maintenant la preuve que ce découpage sert à quelque chose. C'est le cœur de notre travail. »

---

## Étape 5 — Le partition pruning · 1 minute 30

### C'est votre meilleure étape. Ralentissez.

**Avant de lancer :**

> « Deux fois la même question, posée au moteur. Et attention : `EXPLAIN` **n'exécute pas** la requête. Il demande à Hive ce qu'il *compte* faire, et sur combien de lignes — avant d'avoir lu quoi que ce soit. »

**Quand le premier plan s'affiche :**

> « Sans filtre : environ cinq cent mille lignes. Les vingt-quatre partitions, toute la table. »

**Quand le second s'affiche — marquez un temps :**

> « Avec `WHERE annee égale 2025 et mois égale 3` : **vingt et un mille cent vingt-sept**.
>
> Et ce nombre n'est pas une estimation. C'est exactement le nombre de transactions de mars 2025 dans nos résultats — vous pouvez le vérifier dans le fichier `Q1a_ca_mensuel.csv`.
>
> Autrement dit : Hive a lu le **nom des dossiers**, éliminé les vingt-trois autres, et il annonce qu'il ne lira qu'un vingt-quatrième de la table. Tout cela avant d'ouvrir le moindre fichier de données. C'est le *partition pruning*. »

**La phrase sur la méthode — c'est elle qui distingue votre groupe :**

> « Nous insistons sur la nature de cette preuve. Nous ne prouvons pas par le chronomètre : un temps d'exécution peut refléter un cache, une machine chargée, un hasard. Nous prouvons par le plan d'exécution, que le moteur produit avant tout calcul. C'est une preuve d'une autre nature. »

**Si on vous demande ce que ça donne sans partitionnement :**

> « Nous l'avons mesuré, c'est dans `explain_pruning.txt`. Sur la table TEXTFILE, avec un filtre sur le contenu de la colonne date, le `TableScan` annonce cinq cent mille lignes : Hive doit tout lire. Le filtre porte sur une valeur, pas sur un chemin — il ne peut rien éliminer d'avance. »

**Si on vous fait remarquer le 250 000 plus bas dans le plan :**

> « Bonne lecture. Ce nombre-là est l'estimation de Hive *après* l'opérateur de filtrage : ne sachant pas combien de lignes vont passer le filtre, il applique une règle par défaut et divise par deux. C'est une estimation statistique, pas un comptage. Le nombre qui compte est celui du `TableScan`, et c'est cinq cent mille. »

**Transition :**
> « Le partitionnement, c'est le rangement. Voyons le format. »

---

## Étape 6 — TEXTFILE contre ORC · 1 minute

**Quand les deux tailles s'affichent :**

> « Même contenu, les mêmes cinq cent mille transactions. Soixante-trois mégaoctets en CSV, onze en ORC. Un facteur 5,7.
>
> ORC range les données par colonne et non par ligne. Une colonne ne contient que des valeurs de même nature, souvent répétées — huit villes sur cinq cent mille lignes — donc elle se compresse très bien. »

### Puis la nuance. Ne la sautez jamais.

> « Et maintenant le résultat contre-intuitif, parce que nous l'avons mesuré au lieu de le supposer.
>
> **ORC ne rend pas tout plus rapide.** Sur une requête qui balaie toute la table, il est même légèrement plus lent chez nous : 2,79 secondes contre 2,61 en CSV. Il faut ouvrir vingt-quatre fichiers au lieu d'un.
>
> Là où il gagne vraiment, c'est sur une requête filtrée : 1,62 seconde contre 2,57. Un tiers de temps en moins.
>
> Donc : le gain d'ORC est dans le stockage, et dans les requêtes sélectives. Pas dans le balayage complet. »

**Ajoutez la limite vous-même — c'est ce qui donne du crédit au reste :**

> « Deux exécutions par cas seulement. L'écart de 0,18 seconde sur le balayage complet est du même ordre que la variabilité de la machine : nous donnons un ordre de grandeur, pas un résultat statistique. Le facteur 5,7 sur le stockage, lui, ne dépend d'aucune exécution. »

**Si on vous interroge sur le jeu big :**

> « Treize secondes et demie pour la même requête. Parce qu'il est en gzip, et **qu'un fichier gzip n'est pas splittable** : impossible de commencer à décompresser au milieu. Un seul mapper traite les cinq millions de lignes pendant que le reste du cluster attend. C'est l'argument décisif en faveur d'ORC. »

**Transition :**
> « Assez de plomberie. Une vraie question métier. »

---

## Étape 7 — Requête métier · 1 minute

**Pendant que la requête tourne — vous avez quelques secondes, occupez-les :**

> « Chaque requête Hive déclenche un job. On compte en secondes, jamais en millisecondes : ce n'est pas une base transactionnelle. »

**Quand le tableau s'affiche :**

> « Huit villes. Lomé en tête avec un milliard huit cent trente-huit millions de francs CFA, soit quarante pour cent du total. Quatre milliards six cent millions au total, pour un panier moyen de neuf mille cent quatre-vingt-douze francs. »

### Et tout de suite l'honnêteté. C'est elle qui rapporte des points.

> « Mise en garde immédiate : **ce n'est pas une découverte**. Lomé pèse quarante pour cent parce que le générateur de données lui attribue quarante pour cent des transactions. Nous avons lu son code.
>
> C'est vrai de tout ce jeu, qui est synthétique. Nous avons documenté trois conclusions tentantes qu'il ne permet pas de tirer. »

**Si on vous demande lesquelles :**

> « Premièrement, le classement des produits mesure les prix, pas la demande : les vingt et un produits sont tirés uniformément, leurs quantités vendues tiennent dans 1,7 pour cent. Si l'électronique domine le chiffre d'affaires, c'est qu'un Powerbank coûte douze mille francs et un stylo deux cent cinquante.
>
> Deuxièmement, les promotions ne peuvent pas faire vendre plus : la quantité suit une loi de Poisson tirée indépendamment de la promotion. L'égalité que nous mesurons vérifie le générateur, elle ne dit rien du commerce.
>
> Troisièmement, il n'y a aucune saisonnalité : les douze pour cent d'écart entre mois tombent à 3,9 une fois ramenés au jour calendaire, au niveau du bruit attendu. »

**Transition — annoncez l'importance :**
> « Dernière étape, et c'est celle à laquelle nous tenons le plus. »

---

## Étape 8 — La validation croisée · 1 minute 30

### Votre seconde meilleure étape. Racontez la chaîne avant de montrer le résultat.

**Avant de lancer :**

> « Jusqu'ici, nos chiffres sont cohérents avec nous-mêmes. La vraie question est : sont-ils justes ?
>
> Le Groupe 5 nous a livré son rendu. Il contient les cinq jobs MapReduce conçus par le Groupe 3, implémentés par le Groupe 4, et **réellement exécutés sous YARN** par le Groupe 5. Une chaîne technique complètement différente de la nôtre.
>
> Nous n'avons pas repris ces chiffres. Nous les avons confrontés aux nôtres, et c'est ce script qui le fait. »

**Quand les quinze lignes s'affichent :**

> « Quinze comparaisons, sur les trois volumes. Écart maximal : **zéro**. Pas « inférieur à la tolérance » — zéro.
>
> La mention entre crochets dit quel moteur a produit *notre* chiffre : Hive pour le jeu medium, celui sur lequel le cluster a tourné, et DuckDB — notre moteur de contrôle indépendant — pour les deux autres volumes. »

**Ajoutez le contrôle MD5, peu de groupes y pensent :**

> « Et nous avons vérifié quelque chose de plus. Les empreintes MD5 de nos trois jeux de données sont identiques aux leurs, bit à bit — y compris le jeu de cinq millions de lignes que nous avons généré nous-mêmes, avec la graine documentée. Sans cette vérification, la concordance des totaux ne voudrait rien dire : on pourrait comparer deux tirages différents du même générateur. »

### La réserve. Énoncez-la vous-même, avant qu'on vous la pose.

> « Une réserve que nous préférons donner nous-mêmes : **le Groupe 5 n'a pas encore présenté**. Son travail n'est pas validé par le jury. Nous parlons donc de vérification croisée, pas de label. »

---

## Clôture de la démonstration

> « Voilà. En résumé : Hive ne stocke rien, il décrit des fichiers qui existent déjà. Notre partitionnement lui fait lire un vingt-quatrième des données sur une requête filtrée, et nous l'avons prouvé par le plan d'exécution. Et nos résultats concordent avec ceux d'une chaîne technique entièrement indépendante.
>
> Nous sommes à votre disposition. »

---

# Les trois phrases à ne jamais prononcer

| Ne dites pas | Dites |
|---|---|
| « ORC est plus rapide. » | « ORC est plus rapide **sur une requête filtrée**, et plus compact en stockage. Sur un balayage complet il est même légèrement plus lent chez nous. » |
| « Nos résultats sont validés par le Groupe 5. » | « Nos chiffres **concordent** avec les leurs. Leur travail n'est pas encore passé devant le jury. » |
| « Le cluster est distribué. » | « Cinq conteneurs sur une seule machine, un seul DataNode. C'est une maquette fidèle du fonctionnement, pas un cluster réparti. » |

---

# Si quelque chose casse devant le jury

**La règle :** vous l'annoncez, vous ne le masquez pas. Un incident expliqué calmement coûte moins qu'un silence.

| Ce qui arrive | Ce que vous dites, et ce que vous faites |
|---|---|
| Une requête traîne | « Pendant qu'elle tourne — … » et vous reprenez l'architecture. **Ne l'interrompez pas.** |
| Un conteneur est tombé | « Un conteneur s'est arrêté, je le relance et je continue : les étapes sont indépendantes. » Puis `docker compose up -d` dans un second terminal. |
| L'étape 5 n'affiche aucun plan | « L'affichage est tronqué, mais les deux plans sont enregistrés. » Puis vous ouvrez `resultats\explain_pruning.txt`. |
| Le cluster ne démarre pas du tout | « Le cluster ne répond pas ce matin. Je vous montre les résultats de notre dernière exécution, ils sont tous enregistrés. » Puis vous commentez les CSV et les graphiques de `resultats\`. |

---

# Les chiffres à savoir sans regarder

| | |
|---|---|
| Transactions | 500 000 |
| CA total | 4,60 milliards de FCFA |
| Panier moyen | 9 192 FCFA |
| Part de Lomé | 40 % |
| Partitions | 24, soit ~20 800 lignes chacune |
| Mars 2025 | 21 127 transactions |
| Stockage | 62,9 Mo → 11,1 Mo, facteur 5,7 |
| Balayage complet | 2,61 s CSV / 2,79 s ORC |
| Requête filtrée | 2,57 s CSV / 1,62 s ORC |
| Jeu big | 13,58 s |
| Concordances | 17/17 · 5/5 · 15/15, écart nul |
