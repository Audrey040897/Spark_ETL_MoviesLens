from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Setup Spark
spark = (
    SparkSession.builder
    .appName("MovieLens - Ingestion")
    .master("local[*]")
    .getOrCreate()
)

# Réduire le bruit dans la console
spark.sparkContext.setLogLevel("WARN")

print("Version de Spark :", spark.version)
print("Master :", spark.sparkContext.master)

# ============================================================================
# Définir les schémas explicites
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
# Lire les données MovieLens
# ============================================================================

movies = spark.read.schema(schema_movies).option("header", True).csv("data/ml-latest-small/movies.csv")
ratings = spark.read.schema(schema_ratings).option("header", True).csv("data/ml-latest-small/ratings.csv")
tags = spark.read.schema(schema_tags).option("header", True).csv("data/ml-latest-small/tags.csv")
links = spark.read.schema(schema_links).option("header", True).csv("data/ml-latest-small/links.csv")

# ============================================================================
# MOVIES
# ============================================================================

print("\n" + "="*80)
print("MOVIES")
print("="*80)
print(f"Nombre de lignes : {movies.count()}")
movies.printSchema()
movies.show(5, truncate=False)

# ============================================================================
# RATINGS
# ============================================================================

print("\n" + "="*80)
print("RATINGS")
print("="*80)
print(f"Nombre de lignes : {ratings.count()}")
ratings.printSchema()
ratings.show(5, truncate=False)
ratings.describe().show()

# ============================================================================
# TAGS
# ============================================================================

print("\n" + "="*80)
print("TAGS")
print("="*80)
print(f"Nombre de lignes : {tags.count()}")
tags.printSchema()
tags.show(5, truncate=False)

# ============================================================================
# LINKS
# ============================================================================

print("\n" + "="*80)
print("LINKS")
print("="*80)
print(f"Nombre de lignes : {links.count()}")
links.printSchema()
links.show(3, truncate=False)

# ============================================================================
# Spark UI
# ============================================================================

input("Spark UI : http://localhost:4040 (Appuyez sur Entrée pour quitter)")

spark.stop()