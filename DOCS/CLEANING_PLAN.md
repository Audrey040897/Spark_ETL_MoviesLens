# 🧹 CLEANING PLAN — MovieLens ml-latest-small

**Basé sur l'analyse exploration du notebook `Data_mining_spark.ipynb`**  
---

## 🎯 Objectif

Transformer les données **BRUTES (Bronze)** en données **PROPRES (Silver)** :
- Supprimer les anomalies (doublons, NULL critiques)
- Valider les plages (ratings 0.5-5.0)
- Convertir les types pour Spark
- Dériver des colonnes utiles (year, month, etc.)
- Écrire en **Parquet partitionné** pour performance

---

## 📊 TABLE 1: MOVIES

### État initial (de l'EDA)
```
Lignes: 9,742
Doublons: 0 ✅
NULL: 0 ✅
Genres: Pipe-separated (ex: "Action|Adventure")
```

### Plan de nettoyage

#### 1️⃣ Deduplicate sur movieId
```python
.dropDuplicates(["movieId"])
```

**Justification:** Bien que l'EDA détecte 0 doublon, c'est une bonne pratique pour les clés primaires. Coût: ~0 lignes supprimées.

**Résultat attendu:** 9,742 → 9,742 lignes

---

#### 2️⃣ NA.drop sur movieId + title
```python
.na.drop(subset=["movieId", "title"])
```

**Justification:** L'EDA confirme 0 NULL, mais cette étape garantit l'intégrité des clés. Si une anomalie existe, on la détecte ici.

**Résultat attendu:** 9,742 → 9,742 lignes

---

#### 3️⃣ Trim whitespaces
```python
.withColumn("title", trim(col("title")))
.withColumn("genres", trim(col("genres")))
```

**Justification:** 
- Normalization du texte (espacements accidentels)
- Exemple: `" Toy Story (1995) "` → `"Toy Story (1995)"`
- Évite les problèmes de matching lors des jointures

**Résultat attendu:** 9,742 → 9,742 lignes (aucune suppression)

---

### Code Spark complet (Movies)

```python
movies = movies_raw \
    .dropDuplicates(["movieId"]) \
    .na.drop(subset=["movieId", "title"]) \
    .withColumn("title", trim(col("title"))) \
    .withColumn("genres", trim(col("genres")))

print(f"Movies: {movies_raw.count()} → {movies.count()}")
# Expected: Movies: 9742 → 9742
```

---

## ⭐ TABLE 2: RATINGS

### État initial (de l'EDA)
```
Lignes: 100,836
Doublons: 0 ✅
NULL: 0 ✅
Rating plage: 0.5 - 5.0 ✅ (valide)
Timestamp: Unix epoch (à convertir)
Utilisateurs: 610 uniques
```

### Plan de nettoyage

#### 1️⃣ Deduplicate sur (userId, movieId, timestamp)
```python
.dropDuplicates(["userId", "movieId", "timestamp"])
```

**Justification:** 
- Combinaison unique = clé naturelle
- L'EDA détecte 0 doublon, mais c'est une safeguard
- Même utilisateur peut noter film plusieurs fois (à des dates différentes)

**Résultat attendu:** 100,836 → 100,836 lignes

---

#### 2️⃣ NA.drop sur userId, movieId, rating
```python
.na.drop(subset=["userId", "movieId", "rating"])
```

**Justification:**
- L'EDA confirme 0 NULL
- Pas d'analyse possible sans ces 3 colonnes
- Supprime any lignes incomplètes

**Résultat attendu:** 100,836 → 100,836 lignes

---

#### 3️⃣ Filter rating entre 0.5 et 5.0
```python
.filter((col("rating") >= 0.5) & (col("rating") <= 5.0))
```

**Justification:**
- L'EDA montre: min=0.5, max=5.0
- Confirme la plage valide (incréments 0.5)
- Élimine any valeurs aberrantes (< 0.5 ou > 5.0)

**Résultat attendu:** 100,836 → 100,836 lignes

---

#### 4️⃣ Convertir timestamp en datetime
```python
.withColumn("timestamp", to_timestamp(col("timestamp")))
```

**Justification:**
- Timestamp Unix (entier) → DateTime (lisible)
- Permet analyses temporelles (par année, mois, etc.)
- Exemple: 964982703 → 2000-08-02 12:38:23

**Résultat attendu:** Aucune suppression, conversion de type

---

#### 5️⃣ Dériver colonnes temporelles
```python
.withColumn("year", year(col("timestamp"))) \
.withColumn("month", month(col("timestamp"))) \
.withColumn("day", dayofmonth(col("timestamp"))) \
.withColumn("dayOfWeek", dayofweek(col("timestamp"))) \
.withColumn("hour", hour(col("timestamp")))
```

**Justification:**
- Utile pour analyses par période
- Exemple: "Combien de ratings en 2020 ?"
- `dayOfWeek` utile pour patterns (weekend vs weekday)
- `hour` utile pour heatmaps temporelles

**Résultat attendu:** 100,836 lignes + 5 colonnes dérivées

---

#### 6️⃣ Partitionner par year
```python
.write.partitionBy("year").parquet("output/silver/ratings")
```

**Justification:**
- Optimisation Spark (partition pruning)
- Requêtes "ratings en 2020" scannent 1 partition
- Améliore performance de ~10-50% selon queries
- Données distribuées 1995-2009 (~15 ans)

**Résultat attendu:** Dossier ratings/ avec sous-dossiers year=1995, year=1996, ..., year=2009

---

### Code Spark complet (Ratings)

```python
ratings = ratings_raw \
    .dropDuplicates(["userId", "movieId", "timestamp"]) \
    .na.drop(subset=["userId", "movieId", "rating"]) \
    .filter((col("rating") >= 0.5) & (col("rating") <= 5.0)) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp"))) \
    .withColumn("dayOfWeek", dayofweek(col("timestamp"))) \
    .withColumn("hour", hour(col("timestamp")))

print(f"Ratings: {ratings_raw.count()} → {ratings.count()}")
# Expected: Ratings: 100836 → 100836

# Écrire avec partitionnement
ratings.write.mode("overwrite").partitionBy("year").parquet("output/silver/ratings")
```

---

## 🏷️ TABLE 3: TAGS

### État initial (de l'EDA)
```
Lignes: 3,683
Doublons: 0 ✅
NULL: 0 ✅
Texte libre (tags variés)
Timestamp: Unix epoch
```

### Plan de nettoyage

#### 1️⃣ Deduplicate sur (userId, movieId, tag, timestamp)
```python
.dropDuplicates(["userId", "movieId", "tag", "timestamp"])
```

**Justification:**
- Même utilisateur ne peut pas ajouter le même tag au même film à la même heure
- L'EDA détecte 0 doublon (bon signal)

**Résultat attendu:** 3,683 → 3,683 lignes

---

#### 2️⃣ NA.drop sur userId, movieId, tag
```python
.na.drop(subset=["userId", "movieId", "tag"])
```

**Justification:**
- L'EDA confirme 0 NULL
- Sans ces colonnes, tag n'a pas de contexte

**Résultat attendu:** 3,683 → 3,683 lignes

---

#### 3️⃣ Trim whitespaces
```python
.withColumn("tag", trim(col("tag")))
```

**Justification:**
- Normalisation (ex: `" funny "` → `"funny"`)
- Facilite le grouping par tag

**Résultat attendu:** 3,683 → 3,683 lignes

---

#### 4️⃣ Convertir timestamp
```python
.withColumn("timestamp", to_timestamp(col("timestamp")))
```

**Justification:** Idem ratings (lisibilité + analyses temporelles)

---

### Code Spark complet (Tags)

```python
tags = tags_raw \
    .dropDuplicates(["userId", "movieId", "tag", "timestamp"]) \
    .na.drop(subset=["userId", "movieId", "tag"]) \
    .withColumn("tag", trim(col("tag"))) \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

print(f"Tags: {tags_raw.count()} → {tags.count()}")
# Expected: Tags: 3683 → 3683
```

---

## 🔗 TABLE 4: LINKS

### État initial (de l'EDA)
```
Lignes: 9,742
Doublons: OUI dans tmdbId ⚠️
NULL: 1,128 dans tmdbId (11.6%) ⚠️
```

### Plan de nettoyage

#### 1️⃣ Deduplicate sur movieId
```python
.dropDuplicates(["movieId"])
```

**Justification:**
- movieId est la clé primaire
- Garantit 1 ligne par film

**Résultat attendu:** 9,742 → 9,742 lignes

---

#### 2️⃣ NA.drop sur movieId
```python
.na.drop(subset=["movieId"])
```

**Justification:**
- L'EDA confirme 0 NULL sur movieId
- movieId est clé de jointure

**Résultat attendu:** 9,742 → 9,742 lignes

---

#### ⚠️ Gérer tmdbId manquants

**Option A: Supprimer les lignes (recommandé)**
```python
.na.drop(subset=["tmdbId"])
```
- Résultat: 9,742 → 8,614 lignes (1,128 lignes perdues, 11.6%)
- Avantage: Données complètes pour intégration TMDB
- Désavantage: Perte d'info IMDb

**Option B: Tolérer les NULLs (alternative)**
```python
# Aucune action, garder les NULLs
```
- Résultat: 9,742 → 9,742 lignes
- Avantage: Aucune suppression
- Désavantage: tmdbId incomplet, à gérer en requêtes

**Décision:** **Option A (recommandée)** — TMDB ID optionnel, pas critique

---

### Code Spark complet (Links)

```python
links = links_raw \
    .dropDuplicates(["movieId"]) \
    .na.drop(subset=["movieId"])
    # .na.drop(subset=["tmdbId"])  # Option A: supprimer NULLs

print(f"Links: {links_raw.count()} → {links.count()}")
# Expected: Links: 9742 → 9742 (ou 8614 si tmdbId supprimé)
```

---

## 📈 RÉSUMÉ DES SUPPRESSIONS

| Table | Avant | Après | Supprimé | % Perte |
|-------|-------|-------|----------|---------|
| **movies** | 9,742 | 9,742 | 0 | 0% |
| **ratings** | 100,836 | 100,836 | 0 | 0% |
| **tags** | 3,683 | 3,683 | 0 | 0% |
| **links** | 9,742 | 9,742 | 0 | 0% |
| **TOTAL** | **123,983** | **123,983** | **0** | **0%** |

### Observation
✅ **Données de très bonne qualité** — Aucune suppression nécessaire (sauf tmdbId optionnel)

---

## 💾 ÉCRITURE SILVER LAYER

### Format & Localisation

```bash
output/silver/
├── movies/                          # Parquet coalesce(1)
│   └── part-00000-xxx.parquet
├── ratings/                         # Parquet partitionné par year
│   ├── year=1995/
│   │   └── part-00000-xxx.parquet
│   ├── year=1996/
│   │   └── part-00000-xxx.parquet
│   └── year=2009/
│       └── part-00000-xxx.parquet
├── tags/                            # Parquet coalesce(1)
│   └── part-00000-xxx.parquet
└── links/                           # Parquet coalesce(1)
    └── part-00000-xxx.parquet
```

### Code d'écriture

```python
# Movies
movies.coalesce(1).write.mode("overwrite").parquet("output/silver/movies")

# Ratings (partitionné par year)
ratings.write.mode("overwrite").partitionBy("year").parquet("output/silver/ratings")

# Tags
tags.coalesce(1).write.mode("overwrite").parquet("output/silver/tags")

# Links
links.coalesce(1).write.mode("overwrite").parquet("output/silver/links")
```

---

## ✅ CHECKLIST JOUR 2

- [ ] Lire ce plan
- [ ] Lancer `02_cleaning_simple.py`
- [ ] Vérifier que `output/silver/` existe
  ```bash
  ls output/silver/
  # movies/ ratings/ tags/ links/
  ```
- [ ] Vérifier counts
  ```bash
  # Movies: 9742
  # Ratings: 100836
  # Tags: 3683
  # Links: 9742
  ```
- [ ] Relire depuis silver (test Parquet)
  ```python
  m = spark.read.parquet("output/silver/movies")
  m.show(3)
  ```
- [ ] Sauvegarder observations_day2_audrey.txt

---

## 📖 RÉFÉRENCES

**EDA Source:** `Data_mining_spark.ipynb`

**Observations utilisées:**
- Explorer_dataset() results
- Sweetviz rapport
- Markdown analysis (null counts, duplicates, ranges)
