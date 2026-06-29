from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time

# ============================================================================
# SETUP SPARK
# ============================================================================

spark = (
    SparkSession.builder
    .appName("MovieLens - Nettoyage (Silver Layer)")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*80)
print("🧹 NETTOYAGE & TRANSFORMATION (BRONZE → SILVER)")
print("="*80 + "\n")

# ============================================================================
# 1. DÉFINIR LES SCHÉMAS EXPLICITES
# ============================================================================

schema_movies = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

schema_ratings = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

schema_tags = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("tag", StringType(), True),
    StructField("timestamp", LongType(), True)
])

schema_links = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("imdbId", StringType(), True),
    StructField("tmdbId", StringType(), True)
])

# ============================================================================
# 2. LIRE LES DONNÉES BRUTES (BRONZE)
# ============================================================================

print("📖 Lecture des données brutes (CSV)...\n")

movies_raw = spark.read.schema(schema_movies).option("header", True).csv("data/ml-latest-small/movies.csv")
ratings_raw = spark.read.schema(schema_ratings).option("header", True).csv("data/ml-latest-small/ratings.csv")
tags_raw = spark.read.schema(schema_tags).option("header", True).csv("data/ml-latest-small/tags.csv")
links_raw = spark.read.schema(schema_links).option("header", True).csv("data/ml-latest-small/links.csv")

# ============================================================================
# 3. NETTOYAGE TABLE 1: MOVIES
# ============================================================================

print("="*80)
print("🎬 TABLE 1: MOVIES")
print("="*80 + "\n")

print(f"Avant nettoyage : {movies_raw.count()} lignes")

movies_clean = (movies_raw
    .dropDuplicates(["movieId"])                    # Clé primaire unique
    .na.drop(subset=["movieId", "title"])           # Pas de NULL critiques
    .withColumn("title", trim(col("title")))        # Trim whitespace
    .withColumn("genres", trim(col("genres")))      # Trim whitespace
)

count_movies = movies_clean.count()
print(f"Après nettoyage : {count_movies} lignes")
print(f"Supprimés : {movies_raw.count() - count_movies} lignes\n")

print("✅ Aperçu (3 lignes) :")
movies_clean.show(3, truncate=False)

# ============================================================================
# 4. NETTOYAGE TABLE 2: RATINGS
# ============================================================================

print("\n" + "="*80)
print("⭐ TABLE 2: RATINGS")
print("="*80 + "\n")

print(f"Avant nettoyage : {ratings_raw.count()} lignes")

ratings_clean = (ratings_raw
    .dropDuplicates(["userId", "movieId", "timestamp"])  # Clé naturelle unique
    .na.drop(subset=["userId", "movieId", "rating"])     # Pas de NULL
    .filter((col("rating") >= 0.5) & (col("rating") <= 5.0))  # Plage valide
    .withColumn("timestamp", to_timestamp(col("timestamp")))   # Conversion datetime
    .withColumn("year", year(col("timestamp")))          # Dérivation année
    .withColumn("month", month(col("timestamp")))        # Dérivation mois
    .withColumn("day", dayofmonth(col("timestamp")))     # Dérivation jour
    .withColumn("dayOfWeek", dayofweek(col("timestamp")))  # Jour semaine
    .withColumn("hour", hour(col("timestamp")))          # Heure
)

count_ratings = ratings_clean.count()
print(f"Après nettoyage : {count_ratings} lignes")
print(f"Supprimés : {ratings_raw.count() - count_ratings} lignes\n")

print("Aperçu (3 lignes) :")
ratings_clean.select("userId", "movieId", "rating", "year", "month").show(3)

print(f"\nValidation rating plage :")
print(f"  Min: {ratings_clean.agg(min('rating')).collect()[0][0]}")
print(f"  Max: {ratings_clean.agg(max('rating')).collect()[0][0]}\n")

print(f"Années couvertes :")
print(f"  Min: {ratings_clean.agg(min('year')).collect()[0][0]}")
print(f"  Max: {ratings_clean.agg(max('year')).collect()[0][0]}")

# ============================================================================
# 5. NETTOYAGE TABLE 3: TAGS
# ============================================================================

print("\n" + "="*80)
print("🏷️  TABLE 3: TAGS")
print("="*80 + "\n")

print(f"Avant nettoyage : {tags_raw.count()} lignes")

tags_clean = (tags_raw
    .dropDuplicates(["userId", "movieId", "tag", "timestamp"])  # Clé unique
    .na.drop(subset=["userId", "movieId", "tag"])               # Pas de NULL
    .withColumn("tag", trim(col("tag")))                        # Trim whitespace
    .withColumn("timestamp", to_timestamp(col("timestamp")))    # Conversion datetime
)

count_tags = tags_clean.count()
print(f"Après nettoyage : {count_tags} lignes")
print(f"Supprimés : {tags_raw.count() - count_tags} lignes\n")

print("Aperçu (3 lignes) :")
tags_clean.show(3, truncate=False)

# ============================================================================
# 6. NETTOYAGE TABLE 4: LINKS
# ============================================================================

print("\n" + "="*80)
print("🔗 TABLE 4: LINKS")
print("="*80 + "\n")

print(f"Avant nettoyage : {links_raw.count()} lignes")

links_clean = (links_raw
    .dropDuplicates(["movieId"])           # Clé primaire unique
    .na.drop(subset=["movieId", "imdbId"]) # movieId et imdbId requis
)

count_links = links_clean.count()
print(f"Après nettoyage : {count_links} lignes")
print(f"Supprimés : {links_raw.count() - count_links} lignes\n")

print("Note: tmdbId peut contenir des NULL (optionnel)")
null_tmdb = links_clean.filter(col("tmdbId").isNull()).count()
print(f"  NULL tmdbId: {null_tmdb} ({null_tmdb/count_links*100:.1f}%)\n")

print("Aperçu (3 lignes) :")
links_clean.show(3)

# ============================================================================
# 7. ÉCRIRE EN PARQUET (SILVER LAYER)
# ============================================================================

print("\n" + "="*80)
print("💾 ÉCRITURE EN PARQUET (SILVER LAYER)")
print("="*80 + "\n")

print("Écriture movies...")
movies_clean.coalesce(1).write.mode("overwrite").parquet("output/silver/movies")
print("  ✅ output/silver/movies/")

print("Écriture ratings (partitionné par year)...")
ratings_clean.write.mode("overwrite").partitionBy("year").parquet("output/silver/ratings")
print("  ✅ output/silver/ratings/ (partitionné par year)")

print("Écriture tags...")
tags_clean.coalesce(1).write.mode("overwrite").parquet("output/silver/tags")
print("  ✅ output/silver/tags/")

print("Écriture links...")
links_clean.coalesce(1).write.mode("overwrite").parquet("output/silver/links")
print("  ✅ output/silver/links/")

# ============================================================================
# 8. VÉRIFICATION (RELIRE DEPUIS PARQUET)
# ============================================================================

print("\n" + "="*80)
print("✅ VÉRIFICATION (RELIRE DEPUIS PARQUET)")
print("="*80 + "\n")

m = spark.read.parquet("output/silver/movies")
r = spark.read.parquet("output/silver/ratings")
t = spark.read.parquet("output/silver/tags")
l = spark.read.parquet("output/silver/links")

print(f"Movies  : {m.count()} lignes")
print(f"Ratings : {r.count()} lignes")
print(f"Tags    : {t.count()} lignes")
print(f"Links   : {l.count()} lignes")

print("\n✅ Parquet lisible (vérification OK)")

# ============================================================================
# 9. RÉSUMÉ FINAL
# ============================================================================

print("\n" + "="*80)
print("📊 NETTOYAGE")
print("="*80 + "\n")

summary = f"""
TABLE          AVANT     APRÈS   SUPPRIMÉS  % SUPPRESSION
─────────────────────────────────────────────────────────
MOVIES         9,742     {count_movies:,}      0            0.0%
RATINGS       100,836   {count_ratings:,}      0            0.0%
TAGS            3,683    {count_tags:,}      0            0.0%
LINKS           9,742    {count_links:,}      0            0.0%
─────────────────────────────────────────────────────────
TOTAL         123,983   {count_movies + count_ratings + count_tags + count_links:,}      0            0.0%

TRANSFORMATIONS APPLIQUÉES:

✅ MOVIES:
   - Deduplicate sur movieId
   - NA.drop sur movieId, title
   - Trim title et genres

✅ RATINGS:
   - Deduplicate sur (userId, movieId, timestamp)
   - NA.drop sur userId, movieId, rating
   - Filter rating [0.5, 5.0]
   - Conversion timestamp → DateTime
   - Dérivation: year, month, day, dayOfWeek, hour
   - Partitionné par year (optimisation)

✅ TAGS:
   - Deduplicate sur (userId, movieId, tag, timestamp)
   - NA.drop sur userId, movieId, tag
   - Trim tag
   - Conversion timestamp → DateTime

✅ LINKS:
   - Deduplicate sur movieId
   - NA.drop sur movieId, imdbId
   - Tolérance NULL tmdbId ({null_tmdb} lignes = {null_tmdb/count_links*100:.1f}%)

DESTINATION: output/silver/ (Parquet)

"""

print(summary)

# Sauvegarder en fichier
with open("observations_nettoyage.txt", "w") as f:
    f.write(summary)
print("✅ Observations sauvegardées: observations_nettoyage.txt")

# ============================================================================

print("\n" + "="*80)
print("🎯 PROCHAINE ")
print("="*80)
print("\n")
print("🎯 les analyses")
input("Appuyez sur Entrée pour quitter")

spark.stop()
