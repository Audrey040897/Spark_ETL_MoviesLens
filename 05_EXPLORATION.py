from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year
import time

spark = (
    SparkSession.builder
    .appName("MovieLens - Exploration Cache")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ratings = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/ml-latest-small/ratings.csv")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("year", year(col("timestamp")))
)

print("=" * 70)
print("EXPLORATION : IMPACT DU CACHE SUR PLUSIEURS REQUÊTES")
print("=" * 70)

# Sans cache
t0 = time.time()

ratings.filter(col("year") == 2009).count()
ratings.filter(col("rating") >= 4).count()
ratings.groupBy("year").count().collect()
ratings.groupBy("rating").count().collect()

temps_sans_cache = time.time() - t0

print(f"Temps sans cache : {temps_sans_cache:.3f}s")

# Avec cache
ratings.cache()
ratings.count()  # remplit le cache

t0 = time.time()

ratings.filter(col("year") == 2009).count()
ratings.filter(col("rating") >= 4).count()
ratings.groupBy("year").count().collect()
ratings.groupBy("rating").count().collect()

temps_avec_cache = time.time() - t0

print(f"Temps avec cache : {temps_avec_cache:.3f}s")

gain = ((temps_sans_cache - temps_avec_cache) / temps_sans_cache) * 100

print(f"Gain : {gain:.1f}%")

summary = f"""
EXPLORATION AU-DELÀ DU COURS : CACHE
===================================

Question :
Le cache améliore-t-il les performances lorsqu'un DataFrame est réutilisé plusieurs fois ?

Protocole :
Nous avons exécuté les mêmes requêtes sur ratings :
- filtre sur l'année 2009
- filtre sur les notes >= 4
- agrégation par année
- agrégation par note

Mesures :
- Sans cache : {temps_sans_cache:.3f}s
- Avec cache : {temps_avec_cache:.3f}s
- Gain : {gain:.1f}%

Conclusion :
Sur MovieLens Small, le cache a un effet limité car le volume est faible.
Il devient plus pertinent sur un DataFrame volumineux réutilisé plusieurs fois.
"""

print(summary)

with open("observations_exploration_cache.txt", "w", encoding="utf-8") as f:
    f.write(summary)

ratings.unpersist()
spark.stop()