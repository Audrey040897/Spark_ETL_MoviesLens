from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import os

spark = (
    SparkSession.builder
    .appName("MovieLens - Optimisation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*80)
print("⚡ OPTIMISATION")
print("="*80 + "\n")

# ============================================================================
# LIRE DONNÉES SILVER
# ============================================================================

print("📖 Lecture données silver (Parquet)...\n")

ratings = spark.read.parquet("output/silver/ratings")

print(f"Ratings: {ratings.count()} évaluations\n")

# ============================================================================
# OPTIMISATION: CACHE
# ============================================================================

print("="*80)
print("⚡ OPTIMISATION: IMPACT DU CACHE")
print("="*80 + "\n")

print("Hypothèse: Cacher un DataFrame réutilisé accélère les requêtes\n")

# SANS CACHE
print("1️⃣  SANS CACHE:")
start = time.time()
result1 = ratings.filter(col("year") == 2009).count()
time_no_cache = time.time() - start
print(f"   Résultat: {result1} ratings en 2009")
print(f"   ⏱️  Temps: {time_no_cache:.3f}s")

# AVEC CACHE (1ère exécution)
print("\n2️⃣  AVEC CACHE (1ère exécution):")
ratings.cache()
start = time.time()
result2 = ratings.filter(col("year") == 2009).count()
time_cache_1 = time.time() - start
print(f"   Résultat: {result2} ratings en 2009")
print(f"   ⏱️  Temps: {time_cache_1:.3f}s")

# 2ème exécution (cache warm)
print("\n3️⃣  AVEC CACHE (2ème exécution - cache warm):")
start = time.time()
result3 = ratings.filter(col("year") == 2009).count()
time_cache_2 = time.time() - start
print(f"   Résultat: {result3} ratings en 2009")
print(f"   ⏱️  Temps: {time_cache_2:.3f}s")

# Résultats
print("\n" + "-"*80)
print("📊 RÉSULTATS OPTIMISATION CACHE:")
print("-"*80)
print(f"Sans cache:          {time_no_cache:.3f}s")
print(f"Cache (1ère):        {time_cache_1:.3f}s")
print(f"Cache (warm):        {time_cache_2:.3f}s")

if time_no_cache > 0:
    gain_cache = ((time_no_cache - time_cache_2) / time_no_cache) * 100
    print(f"Gain (cache warm):   {gain_cache:+.1f}%")
    if gain_cache > 0:
        print(f"✅ Cache améliore la performance")
    else:
        print(f"⚠️  Sur petit volume, overhead cache légèrement négatif")
else:
    gain_cache = 0
    print("⚠️  Temps trop court pour mesure fiable")

ratings.unpersist()

# ============================================================================
# RÉSUMÉ JOUR 4
# ============================================================================

print("\n" + "="*80)
print("📊 OPTIMISATION CACHE")
print("="*80 + "\n")

summary = f"""
OPTIMISATION CACHE
═══════════════════════════════════════════════════════════════════════════════

OPTIMISATION: IMPACT DU CACHE
──────────────────────────────────────────────────────────────────────────────
Hypothèse: Cacher un DataFrame réutilisé accélère les requêtes

Résultats:
- Sans cache:           {time_no_cache:.3f}s
- Cache (1ère exéc):    {time_cache_1:.3f}s
- Cache (warm):         {time_cache_2:.3f}s

Gain (cache warm): {gain_cache:+.1f}%

Observations:
- Cache peut améliorer si DF est réutilisé plusieurs fois
- Sur ml-latest-small, effet modéré (petit volume)
- Sur ml-latest (27M+ ratings), cache serait plus impactant
- Recommandation: Utiliser pour DataFrames réutilisés > 2x

Conclusion: {'✅ Cache améliore la performance' if gain_cache > 0 else '⚠️ Overhead minimal sur petit volume'}


DONNÉES ANALYSÉES:
═══════════════════════════════════════════════════════════════════════════════
- Ratings total: {ratings.count():,} évaluations
- Ratings 2009: {result1:,} évaluations
- Période: {int(ratings.agg(min('year')).collect()[0][0])}-{int(ratings.agg(max('year')).collect()[0][0])}




STATUS: ✅  OPTIMISATION TERMINÉE 
═══════════════════════════════════════════════════════════════════════════════
"""

print(summary)

with open("observations_apres_optimisation.txt", "w") as f:
    f.write(summary)

print("✅ Observations sauvegardées: observations_apres_optimisation.txt")

# ============================================================================

print("\n" + "="*80)
print("\n")

input("Appuyez sur Entrée pour quitter")

spark.stop()