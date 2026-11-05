"""P1: % de cada Nutri-Score (A a E) por categoría."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MIN_PRODUCTS = 100      # mínimo de productos por categoría

spark = SparkSession.builder.appName("off-q01").getOrCreate()

# Leer el Parquet limpio
df = spark.read.parquet("hdfs:///data/clean/products")

# Solo productos con categoría y Nutri-Score
df = df.where(F.col("category").isNotNull() & F.col("nutriscore_grade").isNotNull())

# % de cada nota por categoría
grade = F.col("nutriscore_grade")
result = df.groupBy("category").agg(
    F.count("*").alias("products"),
    F.round(100 * F.avg((grade == "a").cast("int")), 1).alias("pct_a"),
    F.round(100 * F.avg((grade == "b").cast("int")), 1).alias("pct_b"),
    F.round(100 * F.avg((grade == "c").cast("int")), 1).alias("pct_c"),
    F.round(100 * F.avg((grade == "d").cast("int")), 1).alias("pct_d"),
    F.round(100 * F.avg((grade == "e").cast("int")), 1).alias("pct_e"),
)

# Quitar categorías pequeñas y ordenar por % de A y B
result = (result.where(F.col("products") >= MIN_PRODUCTS)
                .orderBy(F.desc(F.col("pct_a") + F.col("pct_b"))))

# Mostrar y guardar en un CSV
result.show(20, truncate=False)
result.coalesce(1).write.mode("overwrite").option("header", True).csv("hdfs:///results/q01-nutriscore-by-category")

spark.stop()
