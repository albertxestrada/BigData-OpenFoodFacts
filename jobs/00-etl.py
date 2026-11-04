"""ETL: lee el CSV en bruto, limpia las 16 columnas que usan las preguntas y las guarda en Parquet.

Uso: ./run.sh jobs/00-etl.py hdfs:///data/raw/muestra.csv   (muestra)
     ./run.sh jobs/00-etl.py                                 (CSV completo)
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/raw/openfoodfacts.csv"
OUTPUT = "hdfs:///data/clean/products"

spark = SparkSession.builder.appName("off-00-etl").getOrCreate()


# Leer el CSV separado por tabuladores, todo como texto; quote="" evita que una comilla descoloque columnas
raw = spark.read.csv(INPUT, sep="\t", header=True, quote="")


def tag_list(col):
    """Convierte 'en:France,en:Spain' en ['france', 'spain']."""
    return F.split(F.regexp_replace(F.lower(col), "[a-z]{2}:", ""), ",")


def grams(col):
    """Convierte texto en número y anula los valores fuera de 0 a 100 g (errores de entrada)."""
    x = F.col(col).cast("double")
    return F.when(x.between(0, 100), x)


# Categoría en un solo formato: 'en:Salted-snacks' pasa a 'salted snacks'; 'en:null' y 'en:undefined' a nulo
category = F.lower("main_category_en")
category = F.regexp_replace(category, "^[a-z]{2}:", "")
category = F.regexp_replace(category, "-", " ")
category = F.when(~category.isin("undefined", "null"), category)

# Estado vegano: 'vegan', 'non-vegan' o nulo si no se sabe
analysis = F.split("ingredients_analysis_tags", ",")
vegan_status = F.when(F.array_contains(analysis, "en:non-vegan"), "non-vegan").when(
    F.array_contains(analysis, "en:vegan"), "vegan"
)


# Elegir y limpiar las columnas
clean = raw.select(
    F.col("code"),
    F.col("product_name"),
    tag_list("brands_tags")[0].alias("brand"),  # primera marca
    category.alias("category"),
    F.array_remove(tag_list("countries_tags"), "world").alias(
        "countries"
    ),  # 'world' no es un país
    F.year(F.from_unixtime("created_t")).alias("created_year"),  # fecha Unix a año
    F.when(
        F.col("nutriscore_grade").isin("a", "b", "c", "d", "e"),  # 'unknown' a nulo
        F.col("nutriscore_grade"),
    ).alias("nutriscore_grade"),
    F.col("nutriscore_score").cast("int").alias("nutriscore_score"),
    F.col("nova_group").cast("int").alias("nova_group"),  # de 1 a 4
    grams("sugars_100g").alias("sugars_100g"),
    grams("salt_100g").alias("salt_100g"),
    grams("proteins_100g").alias("proteins_100g"),
    tag_list("additives_tags").alias("additives"),
    tag_list("allergens").alias("allergens"),
    vegan_status.alias("vegan_status"),
    F.col("completeness").cast("double").alias("completeness"),  # de 0 a 1
)

# Quitar filas sin código de barras y productos repetidos
clean = clean.where(F.col("code").isNotNull()).dropDuplicates(["code"])

# Guardar en Parquet
clean.write.mode("overwrite").parquet(OUTPUT)

# Comprobar: filas leídas y cuántos productos tienen cada dato (count no cuenta los nulos)
clean = spark.read.parquet(OUTPUT)
print("Filas del CSV:      ", raw.count())
print("Productos limpios:  ", clean.count())
clean.select([F.count(c).alias(c) for c in clean.columns]).show(vertical=True)

spark.stop()
