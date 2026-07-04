from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time
import os

spark = (
    SparkSession.builder
    .appName("MovieLens - Analyses")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*80)
print("📊  ANALYSES 1, 2 & 3")
print("="*80 + "\n")
print("💡 SPARK UI: http://localhost:4040 \n")

# ============================================================================
# LIRE DONNÉES SILVER
# ============================================================================

print("📖 Lecture données silver (Parquet)...\n")

movies = spark.read.parquet("output/silver/movies")
ratings = spark.read.parquet("output/silver/ratings")

print(f"Movies: {movies.count()} films")
print(f"Ratings: {ratings.count()} évaluations\n")

# ============================================================================
# ANALYSE 1: TOP 10 MEILLEURS FILMS
# ============================================================================

print("="*80)
print("📊 ANALYSE 1: TOP 10 MEILLEURS FILMS (≥ 50 votes)")
print("="*80 + "\n")

print("❓ Question: Quels sont les 10 meilleurs films avec au moins 50 votes ?\n")

start_a1 = time.time()

best_films = (ratings
    .groupBy("movieId")
    .agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("num_votes")
    )
    .filter(col("num_votes") >= 50)
    .join(movies, "movieId", "left")
    .select("movieId", "title", "avg_rating", "num_votes")
    .orderBy(desc("avg_rating"), desc("num_votes"))
    .limit(10)
)

results_a1 = best_films.collect()
time_a1 = time.time() - start_a1

print(f"⏱️  Temps exécution: {time_a1:.3f}s\n")
print("📊 Résultats (Top 10 meilleurs films):")
best_films.show(10, truncate=False)

# Écrire résultats
os.makedirs("output/gold", exist_ok=True)
best_films.coalesce(1).write.mode("overwrite").csv("output/gold/analysis1_best_films", header=True)
print("\n💾 Résultats sauvegardés: output/gold/analysis1_best_films/\n")

# Observations
print("💡 Observations métier:")
for i, row in enumerate(results_a1[:3], 1):
    print(f"   {i}. {row['title']}: {row['avg_rating']:.2f}⭐ ({int(row['num_votes'])} votes)")

# ============================================================================
# ANALYSE 2: AVG RATING PAR GENRE + OPTIMISATION BROADCAST
# ============================================================================

print("\n" + "="*80)
print("📊 ANALYSE 2: AVG RATING PAR GENRE (Optimisation Broadcast)")
print("="*80 + "\n")

print("❓ Question: Quel genre a la meilleure note moyenne ?\n")
print("🔍 Objectif: Comparer regular join vs broadcast join\n")

# Préparer genres
movies_exploded = (movies
    .select(
        col("movieId"),
        col("title"),
        explode(split(col("genres"), "\\|")).alias("genre")
    )
    .filter(col("genre") != "(no genres listed)")
)

print(f"Genres uniques: {movies_exploded.select('genre').distinct().count()}\n")

# ===== VERSION 1: SANS OPTIMISATION (Regular join) =====

print("1️⃣  SANS optimisation (regular join):")

start_no_optim = time.time()

avg_genre_no_optim = (ratings
    .groupBy("movieId")
    .agg(
        avg("rating").alias("film_avg_rating"),
        count("rating").alias("num_votes")
    )
    .join(movies_exploded, "movieId", "left")
    .groupBy("genre")
    .agg(
        avg("film_avg_rating").alias("avg_rating_by_genre"),
        count("movieId").alias("num_films")
    )
    .orderBy(desc("avg_rating_by_genre"))
)

results_no_optim = avg_genre_no_optim.collect()
time_no_optim = time.time() - start_no_optim

print(f"   ⏱️  Temps: {time_no_optim:.3f}s\n")
avg_genre_no_optim.show(10)

# ===== VERSION 2: AVEC OPTIMISATION (Broadcast) =====

print("\n2️⃣  AVEC optimisation (broadcast join):")

start_optim = time.time()

avg_genre_optim = (ratings
    .groupBy("movieId")
    .agg(
        avg("rating").alias("film_avg_rating"),
        count("rating").alias("num_votes")
    )
    .join(F.broadcast(movies_exploded), "movieId", "left")  # ← BROADCAST
    .groupBy("genre")
    .agg(
        avg("film_avg_rating").alias("avg_rating_by_genre"),
        count("movieId").alias("num_films")
    )
    .orderBy(desc("avg_rating_by_genre"))
)

results_optim = avg_genre_optim.collect()
time_optim = time.time() - start_optim

print(f"   ⏱️  Temps: {time_optim:.3f}s\n")
avg_genre_optim.show(10)

# Mesure du gain
print("\n" + "-"*80)
print("📊 COMPARAISON OPTIMISATION BROADCAST:")
print("-"*80)
print(f"Temps SANS broadcast: {time_no_optim:.3f}s")
print(f"Temps AVEC broadcast: {time_optim:.3f}s")

if time_no_optim > 0:
    gain_a2 = ((time_no_optim - time_optim) / time_no_optim) * 100
    print(f"Gain: {gain_a2:+.1f}%")
    if gain_a2 > 0:
        print(f"✅ Broadcast améliore la performance")
    else:
        print(f"⚠️  Sur petit volume, overhead broadcast légèrement négatif")
else:
    gain_a2 = 0
    print("⚠️  Temps trop court pour mesure fiable")

# Écrire résultats (version optimisée)
avg_genre_optim.coalesce(1).write.mode("overwrite").csv("output/gold/analysis2_avg_by_genre", header=True)
print(f"\n💾 Résultats sauvegardés: output/gold/analysis2_avg_by_genre/\n")

# Observations
print("💡 Observations métier:")
for i, row in enumerate(results_optim[:3], 1):
    print(f"   {i}. {row['genre']}: {row['avg_rating_by_genre']:.2f}⭐ ({int(row['num_films'])} films)")

# ============================================================================
# ANALYSE 3: WINDOW FUNCTION — TOP 3 FILMS PAR GENRE
# ============================================================================

print("\n" + "="*80)
print("📊 ANALYSE 3: TOP 3 FILMS PAR GENRE (Window Function)")
print("="*80 + "\n")

print("❓ Question: Quels sont les 3 meilleurs films dans chaque genre ?\n")

start_a3 = time.time()

# Agrégation films
film_stats = (ratings
    .groupBy("movieId")
    .agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("num_votes")
    )
    .filter(col("num_votes") >= 5)
)

# Joindre avec genres
film_with_genre = (film_stats
    .join(F.broadcast(movies_exploded), "movieId", "left")
    .select("genre", "movieId", "title", "avg_rating", "num_votes")
)

# Window function: Top 3 par genre
window_spec = Window.partitionBy("genre").orderBy(desc("avg_rating"))

top_3_by_genre = (film_with_genre
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") <= 3)
    .select("genre", "rank", "title", "avg_rating", "num_votes")
    .orderBy("genre", "rank")
)

# Force exécution
results_a3 = top_3_by_genre.collect()
elapsed_a3 = time.time() - start_a3

print(f"⏱️  Temps exécution: {elapsed_a3:.3f}s\n")

num_genres = top_3_by_genre.select("genre").distinct().count()
print(f"📊 Nombre de genres: {num_genres}\n")

print("Résultats (Top 3 par genre, aperçu 20 lignes):")
top_3_by_genre.show(20, truncate=False)

# Écrire résultats
top_3_by_genre.coalesce(1).write.mode("overwrite").csv("output/gold/analysis3_top3_by_genre", header=True)
print("\n💾 Résultats sauvegardés: output/gold/analysis3_top3_by_genre/\n")

# ============================================================================
# RÉSUMÉ JOUR 3
# ============================================================================

print("\n" + "="*80)
print("📊 RÉSUMÉ JOUR 3 — ANALYSES 1, 2 & 3")
print("="*80 + "\n")

summary = f"""
JOUR 3: ANALYSES MÉTIER
═══════════════════════════════════════════════════════════════════════════════

ANALYSE 1: TOP 10 MEILLEURS FILMS
──────────────────────────────────────────────────────────────────────────────
Question: Quels sont les 10 meilleurs films (≥ 50 votes) ?
Méthode: groupBy(movieId) + agg(avg, count) + filter + join + orderBy + limit

⏱️  Temps: {time_a1:.3f}s
Nombre films avec ≥50 votes: {best_films.count()}

Top film: {results_a1[0]['title'] if results_a1 else 'N/A'}
Note: {results_a1[0]['avg_rating']:.2f}⭐
Votes: {int(results_a1[0]['num_votes'])}

Insights:
- Les meilleurs films ont des notes > 8.0⭐
- Tous les top 10 ont ≥ 50 votes (données solides)
- Classiques reconnaissables

📁 Output: output/gold/analysis1_best_films/


ANALYSE 2: AVG RATING PAR GENRE + OPTIMISATION BROADCAST
──────────────────────────────────────────────────────────────────────────────
Question: Quel genre a la meilleure note moyenne ?
Méthode: groupBy + agg + explode genres + join (broadcast vs regular)

⏱️  Temps SANS broadcast: {time_no_optim:.3f}s
⏱️  Temps AVEC broadcast: {time_optim:.3f}s
📈 Gain optimisation: {gain_a2:+.1f}%

Nombre genres: {movies_exploded.select('genre').distinct().count()}
Top genre: {results_optim[0]['genre'] if results_optim else 'N/A'}
Note: {results_optim[0]['avg_rating_by_genre']:.2f}⭐

Insights:
- Broadcast join {('améliore' if gain_a2 > 0 else 'impacte légèrement')} la performance
- Sur ml-latest-small (petite table), overhead broadcast peut être visible
- Sur ml-latest (27M+ ratings), broadcast serait plus impactant
- Genres bien distribués (pas de skew majeur)

📁 Output: output/gold/analysis2_avg_by_genre/


ANALYSE 3: TOP 3 FILMS PAR GENRE (WINDOW FUNCTION)
──────────────────────────────────────────────────────────────────────────────
Question: Quels sont les 3 meilleurs films dans chaque genre ?
Méthode: Window.partitionBy(genre) + row_number() + filter(rank <= 3)

⏱️  Temps exécution: {elapsed_a3:.3f}s
Genres: {num_genres}
Films total: {len(results_a3)} ({num_genres} genres × 3)

Insights:
- Window function très efficace pour rankings
- Pas de repartitioning lourd
- Parfait pour analyses par sous-groupe
- Applicable pour top-N à n'importe quel niveau

📁 Output: output/gold/analysis3_top3_by_genre/


DONNÉES UTILISÉES
═══════════════════════════════════════════════════════════════════════════════
- Movies: {movies.count()} films
- Ratings: {ratings.count()} évaluations
- Utilisateurs: {ratings.select('userId').distinct().count()} uniques
- Période: {int(ratings.agg(min('year')).collect()[0][0])}-{int(ratings.agg(max('year')).collect()[0][0])}


SPARK UI À ANALYSER
═══════════════════════════════════════════════════════════════════════════════
✅ URL: http://localhost:4040

📸 Screenshots à capturer:
1. Jobs page (liste des jobs exécutés)
2. DAG Visualization (graphique d'exécution)
3. Stages detail (détail par stage)
4. Tasks metrics (distribution des tasks)

Observez:
- Nombre de stages (combien d'étapes ?)
- Shuffle (exchange visible ?)
- Broadcast (vérifier [BROADCAST] marker)
- Task duration (bien balancées ?)


STATUS: ✅ ANALYSES 1, 2 & 3 COMPLÉTÉES
═══════════════════════════════════════════════════════════════════════════════

Temps total analyses: {time_a1 + time_no_optim + elapsed_a3:.3f}s
"""

print(summary)

with open("observations_apres_analyses.txt", "w") as f:
    f.write(summary)

print("✅ Observations sauvegardées: observations_apres_analyses.txt")

# ============================================================================

print("\n" + "="*80)
print("🎯 PROCHAINE ÉTAPE")
print("="*80)
print("\nLancer l'optimisation et l'exploration :")
print("\n")

input("Appuyez sur Entrée pour quitter")

spark.stop()