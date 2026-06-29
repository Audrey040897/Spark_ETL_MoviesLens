# Spark_ETL

# 🎬 MovieLens ETL Pipeline — Projet Spark

Pipeline Spark complet : Ingestion → Nettoyage → Analyses → Optimisation

---

## 🎯 Vue d'ensemble

**Objectif:** Construire un pipeline ETL Spark avec le dataset MovieLens ml-latest-small

```
CSV brut → Nettoyage → Analyses → Optimisation → Rapport
```

**Dataset:** 123k lignes | 4 tables | 1995-2009 | 610 utilisateurs

**Livrables:**
- Code PySpark (5 scripts)
- Données nettoyées (Parquet)
- 3 analyses métier (CSV)
- Mesures optimisation
- Screenshots Spark UI
- Rapport académique

---

## 🏗️ Architecture: Bronze → Silver → Gold

**Bronze:** CSV brut (data/ml-latest-small/)
**Silver:** Parquet nettoyé (output/silver/)
**Gold:** Résultats analyses (output/gold/)

---

## ✅ Prérequis

```bash
python3 --version        # 3.8+
spark-shell --version    # 3.x
java -version            # Java 8+
curl --version           # Pour télécharger (ou wget)
```

---

## 📁 Structure

```
projet/
├── data-download.sh                  (Télécharge les données)
├── ingestion.py                   (Inspect données)
├── 02_cleaning.py                    (Nettoyage)
├── 03_JANALYSES.py             (3 Analyses)
├── 04_OPTIMISATION.py         (Cache)
├── output/                           (auto-créé)
├── images/                           (screenshots)
└── docs/
    ├── README.md
    ├── RAPPORT_FINAL.md
    └── CLEANING_PLAN.md
    └── DATA_DICTIONARY.md
```

---

## 🚀 Lancer tous les scripts

### 1. Télécharger les données (une fois)

```bash
bash data-download.sh
```

Télécharge et extrait le dataset MovieLens ml-latest-small dans data/

### 2. Exécuter les scripts

```bash
# Ingestion & inspection
python ingestion.py

# Nettoyage
python 02_cleaning.py

# Analyses (+ Spark UI screenshots)
python 03_ANALYSES.py
# Pendant: http://localhost:4040 + capturer 4 screenshots

# Optimisation cache
python 04_OPTIMISATION.py
```

**Timing total:** ~7-11 minutes (dont ~2-3 min pour le téléchargement)

---

## 📊 Sorties créées

### Après ingestion & nettoyage

```
output/silver/
├── movies/       (9,742 lignes)
├── ratings/      (100,836 lignes, 15 partitions)
├── tags/         (3,683 lignes)
└── links/        (9,742 lignes)
```

### Après analyses

```
output/gold/
├── analysis1_best_films/       (10 films)
├── analysis2_avg_by_genre/     (20 genres + gain broadcast)
└── analysis3_top3_by_genre/    (60 films)

images/
├── spark-ui-jobs.png
├── spark-ui-dag.png
├── spark-ui-stage-detail.png
└── spark-ui-tasks.png

observations_jour3_analyses.txt
```

### Après optimisation

```
observations_jour4_optimisation.txt
(cache timings + gain %)
```

---

## 📖 Documentation

| Fichier | Description |
|---------|-------------|
| **README.md** | Vue générale (ce fichier) |
| **SCRIPTS_EXECUTION.md** | Lancement détaillé de chaque script |
| **DATA_DICTIONARY.md** | Description des données |
| **CLEANING_PLAN.md** | Justification transformations |
| **RAPPORT_FINAL.md** | Template rapport |

**→ Pour le lancement détaillé, voir SCRIPTS_EXECUTION.md**

---

## ❓ FAQ

**"bash: command not found: bash"**
→ Utiliser: `sh data-download.sh` (au lieu de bash)

**"bash: data-download.sh: command not found"**
→ Rendre exécutable: `chmod +x data-download.sh`

**"curl: (7) Failed to connect"**
→ Vérifier connexion internet

**"File not found: data/ml-latest-small/movies.csv"**
→ Lancer: `bash data-download.sh`

**"Output directory already exists"**
→ Normal, Spark supprime auto

**Spark UI ne s'affiche pas**
→ Vérifier: http://localhost:4040 pendant l'exécution

**Screenshots floues**
→ Capturer PENDANT (pas après) l'exécution

---

## 🎓 Techniques maîtrisées

✅ Schema explicite (StructType)
✅ Nettoyage (dropDuplicates, na.drop, trim)
✅ Agrégation (groupBy, agg, avg, count)
✅ Jointures (regular vs broadcast)
✅ Window Functions (partitionBy, row_number)
✅ Optimisation (cache, broadcast, partitioning)
✅ I/O Parquet + partitionnement
✅ Bronze/Silver/Gold layers

---

## 📝 Prochaines étapes

1. Lancer: `bash data-download.sh`
2. Lancer scripts 01-04 (voir SCRIPTS_EXECUTION.md)
3. Capturer 4 screenshots Spark UI (03_JOUR3_ANALYSES.py)
4. Remplir RAPPORT_FINAL_TEMPLATE.md
5. Remise
---