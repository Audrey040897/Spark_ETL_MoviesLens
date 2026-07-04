# 🎬 MovieLens ETL Pipeline — Projet Spark Complet

Pipeline Spark complet : Ingestion → Nettoyage → Analyses → Optimisation → Exploration

---

## 🎯 Vue d'ensemble

**Objectif:** Construire un pipeline ETL Spark avec le dataset MovieLens ml-latest-small

```
CSV brut → Nettoyage → Analyses → Optimisation → Exploration → Rapport
```

**Dataset:** 123,983 lignes | 4 tables | 1995-2018 | 610 utilisateurs

**Livrables:**
- Code PySpark complet (6 scripts)
- Données nettoyées en Parquet (0% data loss)
- 3 analyses métier (CSV + visualisations)
- Mesures optimisation (cache, broadcast)
- Exploration au-delà du cours (cache sur séquence)
- Screenshots Spark UI + DAG
- Rapport académique détaillé

---

## 🏗️ Architecture: Bronze → Silver → Gold

```
Bronze: CSV brut (data/ml-latest-small/)
    ↓
Silver: Parquet nettoyé (output/silver/, 100% data quality)
    ├── movies/       (9,742 lignes)
    ├── ratings/      (100,836 lignes, 15 partitions par year)
    ├── tags/         (3,683 lignes)
    └── links/        (9,742 lignes)
    ↓
Gold: Analyses métier (output/gold/, CSV)
    ├── analysis1_best_films/       (10 films)
    ├── analysis2_avg_by_genre/     (20 genres)
    └── analysis3_top3_by_genre/    (60 films)
```

---

## ✅ Prérequis

```bash
python --version        # 3.8+
spark-shell --version    # 3.x
java -version            # Java 8+
curl --version           # Pour télécharger (ou wget)
```

---

## 📁 Structure du projet

```
projet/
├── data-download.sh                    (Télécharge les données)
├── Ingestion.py                     (Inspect données)
├── 02_cleaning.py                      (Nettoyage)
├── 03_ANALYSES.py               (3 Analyses métier)
├── 04_OPTIMISATION.py           (Cache benchmark)
├── 05_EXPLORATION.py                   (Cache sur séquence)
├── output/
│   ├── silver/                         (Parquet nettoyé)
│   │   ├── movies/
│   │   ├── ratings/ (partitionné par year)
│   │   ├── tags/
│   │   └── links/
│   └── gold/                           (Résultats analyses)
│       ├── analysis1_best_films/
│       ├── analysis2_avg_by_genre/
│       └── analysis3_top3_by_genre/
├── images/                             (Screenshots Spark UI)
└── docs/
    ├── README.md (ce fichier) 
    ├── DATA_DICTIONARY.md
    ├── CLEANING_PLAN.md
    ├── Rapport_Projet_Spark_MoviesLens.md
```

---

## 🚀 Lancer les scripts (Étapes 1-6)

### 1. Télécharger les données

```bash
bash data-download.sh
```

Télécharge et extrait MovieLens ml-latest-small dans `data/`

### 2. Ingestion & Inspection

```bash
python3 Ingestion.py
```

- Charge 4 tables CSV
- Inspecte schémas et données
- Crée dossiers output/

### 3. Nettoyage & Transformation

```bash
python3 02_CLEANING.py
```

**Transformations appliquées:**

| Table | Transformations | Résultat |
|-------|-----------------|----------|
| movies | dropDuplicates, na.drop, trim | 9,742 (0 perte) |
| ratings | dropDuplicates, na.drop, filter rating∈[0.5,5.0], conversions | 100,836 (0 perte) |
| tags | dropDuplicates, na.drop, trim | 3,683 (0 perte) |
| links | dropDuplicates, na.drop | 9,742 (0 perte) |

**Output:** Parquet partitionné dans `output/silver/`

### 4. Analyses Métier

```bash
python3 03_ANALYSES.py
```

**Pendant l'exécution:** Ouvrir http://localhost:4040 et capturer 4 screenshots

#### Analyse 1: Top 10 meilleurs films (≥50 votes)

```python
ratings.groupBy("movieId").agg(avg("rating"), count("rating"))
.filter(col("num_votes") >= 50)
.join(movies, "movieId").orderBy(desc("avg_rating")).limit(10)
```

**Résultat:**
| Rank | Film | Note | Votes |
|------|------|------|-------|
| 1 | Shawshank Redemption (1994) | 4.43 ⭐ | 317 |
| 2 | The Godfather (1972) | 4.39 ⭐ | 261 |
| 3 | The Dark Knight (2008) | 4.35 ⭐ | 254 |

**Timing:** ~0.916s

#### Analyse 2: Avg rating par genre + Optimisation Broadcast

```python
# Explode genres
movies_exploded = movies.select(..., explode(split("genres", "\\|")).alias("genre"))

# Jointure OPTIMISÉE
.join(F.broadcast(movies_exploded), "movieId", "left")  # <- BROADCAST
```

**Résultats comparatifs:**

| Stratégie | Temps |
|-----------|-------|
| Regular join | 0.503s |
| Broadcast join | 0.360s |
| **Gain** | **+28.5%** ✅ |

**Meilleur genre:** Documentary (3.78 ⭐)

**Timing:** ~0.360s (avec broadcast)

#### Analyse 3: Top 3 films par genre (Window Function)

```python
window_spec = Window.partitionBy("genre").orderBy(desc("avg_rating"))
.withColumn("rank", row_number().over(window_spec))
.filter(col("rank") <= 3)
```

**Résultats:**
- 20 genres couverts
- 58 films retournés (60 attendus)
- Genres avec <3 films filtrés

**Timing:** ~0.555s

### 5. Optimisation: Cache Benchmark (Requête isolée)

```bash
python3 04_JOUR4_OPTIMISATION.py
```

**Test:** Mesurer impact du cache sur 1 requête relancée

```python
# SANS cache
ratings.filter(col("year") == 2009).count()

# AVEC cache (1ère exécution)
ratings.cache()
ratings.filter(col("year") == 2009).count()

# AVEC cache (2ème exécution - warm)
ratings.filter(col("year") == 2009).count()
```

**Résultats:**

| Scenario | Temps |
|----------|-------|
| Sans cache | 0.083s |
| Cache (1ère exec) | 0.777s |
| Cache (warm) | 0.095s |
| **Gain** | **-14.2%** ⚠️ |

**Conclusion:** Sur ml-latest-small (petit volume), cache sur 1 requête pénalisant. L'overhead > gain pour usage ponctuel.

### 6. Exploration au-delà du cours: Cache sur séquence

```bash
python3 05_EXPLORATION.py
```

**Test:** Mesurer impact du cache sur 4 requêtes ENCHAÎNÉES (usage réel)

```python
# Sans cache: 4 requêtes séquentielles
ratings.filter(col("year") == 2009).count()
ratings.filter(col("rating") >= 4).count()
ratings.groupBy("year").count().collect()
ratings.groupBy("rating").count().collect()

# Avec cache: mêmes 4 requêtes, cache rempli au préalable
ratings.cache()
ratings.count()  # Remplir le cache
# (mêmes 4 requêtes)
```

**Résultats:**

| Scenario | Temps |
|----------|-------|
| Sans cache (4 requêtes) | 4.656s |
| Avec cache (4 requêtes) | 1.379s |
| **Gain** | **+70.4%** ✅ |

**Conclusion:** Cache TRÈS utile pour DataFrames réutilisés (70% de gain!). Le coût de matérialisation est amorti sur les 4 opérations.

**Insight clé:** Cache efficace pour usage réel (plusieurs requêtes), pas pour une seule requête isolée.

---

## 📊 Résultats créés

### Après analyses (Étape 4)

```
output/gold/
├── analysis1_best_films/part-*.csv         (10 films)
├── analysis2_avg_by_genre/part-*.csv       (20 genres)
└── analysis3_top3_by_genre/part-*.csv      (60 films)

images/
├── spark-ui-jobs.png                       (82 jobs, timings)
├── spark-ui-dag.png                        (DAG avec broadcast)
├── spark-ui-stages.png                     (82 stages, 74 skipped)
└── spark-ui-tasks.png                      (Task metrics)

observations_jour3_analyses.txt             (Résumé analyses)
```

### Après optimisation (Étape 5)

```
observations_apres_optimisation.txt         (Cache timings)
```

### Après exploration (Étape 6)

```
observations_exploration_cache.txt          (Cache sur séquence: +70.4%)
```

---

## 🔍 Spark UI Analysis

### Jobs & Stages

**82 jobs complétés** lors de l'exécution du script d'analyses

**DAG Visualization (Analyse 2):**
```
Scan Parquet (ratings)
    ↓
WholeStageCodegen
    ↓
Exchange (stage 154 - skipped)
    ↓
Exchange (stage 155)
    ↓
WholeStageCodegen → mapPartitionsInternal
```

### Shuffle & Exchange

**Au passage du groupBy("movieId") → groupBy("genre"):**
- Stage 153: 8/8 tasks, Input 59.5 KiB, Shuffle Write 472 B
- Stage 155: 1 task, Shuffle Read 472 B
- Duration: 3 ms (tâche unique, volume minuscule)
- Pas de skew détecté (20 genres bien équilibrés)

### Optimisations observées

✅ **Partition Pruning:** Parquet lu uniquement par year nécessaire
✅ **Task Locality:** Tasks exécutées NODE_LOCAL (données en cache)
✅ **Broadcast:** movies_exploded broadcast dans la jointure (28.5% gain)
✅ **Cache Reuse:** 74 stages marqués "skipped" (résultats réutilisés)

---

## 📈 Performance Summary

| Composant | Timing | Gain/Note |
|-----------|--------|-----------|
| Analyse 1 (agrégation) | 0.916s | Baseline |
| Analyse 2 (broadcast) | 0.360s | +28.5% vs regular |
| Analyse 3 (window) | 0.555s | Efficace top-N |
| Cache (1 requête) | -14.2% | Pénalisant |
| Cache (4 requêtes) | +70.4% | Très utile! |
| **Total pipeline** | ~1-2 min | Complet |

---

## 📚 Techniques Spark maîtrisées

### Core PySpark
✅ Schema explicite (StructType, IntegerType, DoubleType)
✅ Transformations (dropDuplicates, na.drop, trim, filter, select)
✅ Agrégations (groupBy, agg, avg, count, sum)
✅ Conversions (string→timestamp, unix_timestamp)
✅ Jointures (regular join vs broadcast join)
✅ Window Functions (partitionBy, row_number, orderBy, rank)

### Optimisations
✅ Cache management (cache, unpersist)
✅ Broadcast join (F.broadcast)
✅ Partitioning (partitionBy, coalesce)
✅ Partition pruning (year-based filtering)

### I/O & Architecture
✅ Parquet (columnar, compressed)
✅ CSV (read/write)
✅ Partitioned output (bronze→silver→gold)
✅ Medallion architecture

### Monitoring
✅ Spark UI (jobs, stages, tasks, DAG)
✅ Performance measurement (timings)
✅ Benchmark (SANS vs AVEC optimisation)

---

## 🎓 Leçons apprises

### Ce qui a marché

✅ Pipeline complet fonctionnel (5 scripts + 1 exploration)
✅ Schémas explicites évitent erreurs de typage
✅ Broadcast join: +28.5% de gain même sur petit volume
✅ Window function: classement par groupe en 1 passe (vs requêtes répétées)
✅ Cache sur séquence: +70.4% de gain (vs isolé: -14.2%)
✅ Data quality: 0% data loss (123,983 lignes nettoyées)

### Insights clés

🔑 **Cache n'est pas une balle magique:**
- Mauvais sur requête isolée (overhead > gain)
- Excellent sur usage réel (plusieurs requêtes)
- À réserver à DataFrames réutilisés +2 fois

🔑 **Broadcast join pertinent même sur petit volume**
- 28.5% de gain sur ml-latest-small
- Sur ml-latest (27M+), gain bien plus significatif

🔑 **Partitioning by year payant:**
- Partition pruning: lire juste l'année demandée
- Shuffle minuscule (472 B), pas de skew

🔑 **Window function > requêtes groupBy répétées:**
- row_number() en 1 passe
- Vs: loop par genre (inefficace)

---

## 📋 Data Quality & Nettoyage

**Résultat:** 100% data quality (0% data loss)

| Table | Avant | Après | Écartées | % |
|-------|-------|-------|----------|---|
| movies | 9,742 | 9,742 | 0 | 0.0% |
| ratings | 100,836 | 100,836 | 0 | 0.0% |
| tags | 3,683 | 3,683 | 0 | 0.0% |
| links | 9,742 | 9,742 | 0 | 0.0% |

**Interprétation:** ml-latest-small est déjà propre en amont. Le pipeline de nettoyage fonctionne, juste sans données à écarter (résultat attendu).

## 📖 Documentation complète

| Fichier | Description |
|---------|-------------|
| **README.md** | Vue générale (ce fichier) |
| **SCRIPTS_EXECUTION.md** | Lancement détaillé de chaque script |
| **DATA_DICTIONARY_FINAL.md** | Description complète des données |
| **CLEANING_PLAN.md** | Justification des transformations |
| **RAPPORT_FINAL_TEMPLATE.md** | Template rapport académique |
| **CV_COMPETENCES_SPARK.md** | Compétences Spark pour CV |
| **HOW_TO_RUN.md** | Guide détaillé supplémentaire |

---

## ❓ FAQ

**"bash: data-download.sh: command not found"**
```bash
chmod +x data-download.sh
bash data-download.sh
```

**"File not found: data/ml-latest-small/movies.csv"**
```bash
bash data-download.sh
```

**"Spark UI ne s'affiche pas"**
```
Vérifier: http://localhost:4040 PENDANT l'exécution
(se ferme 30 sec après la fin du job)
```

**"Output directory already exists"**
```
Normal, Spark supprime auto avec mode="overwrite"
```

**"Cache measurements trop rapides"**
```
Normal sur ml-latest-small (petit volume)
Sur ml-latest (27M+), gains plus visibles
```
---