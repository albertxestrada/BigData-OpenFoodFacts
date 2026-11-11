# limpieza.py · aplica las reglas de docs/EDA.md y las columnas añadidas por Mauro; guarda un Parquet en HDFS
# Uso: spark-submit src/client_ingesta/limpieza.py [entrada] [salida]
import sys
from pyspark.sql import SparkSession, Window, functions as F

entrada = sys.argv[1] if len(sys.argv) > 1 else "hdfs://hadoop-master:9000/data/raw/openfoodfacts.csv"
salida = sys.argv[2] if len(sys.argv) > 2 else "hdfs://hadoop-master:9000/data/clean/off_limpio"

spark = SparkSession.builder.appName("limpieza_off").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

# 1. Leer como texto para no perder ceros a la izquierda en el código de barras
df = spark.read.csv(entrada, sep="\t", header=True, quote="")

def c(nombre):
    return F.col(f"`{nombre}`")

def num(nombre, lo, hi):
    x = c(nombre).cast("double")
    return F.when((x >= lo) & (x <= hi), x)

def sin_desconocidos(nombre):
    return F.when(~c(nombre).isin("unknown", "not-applicable", "undefined", ""), c(nombre))

# 2. Deduplicar por código conservando la fecha de edición más reciente
orden = Window.partitionBy("code").orderBy(c("last_modified_t").cast("long").desc_nulls_last())
df = df.withColumn("_rn", F.row_number().over(orden)).filter("_rn = 1").drop("_rn")

# Transformaciones de listas y texto
etiq_analisis = F.split(c("ingredients_analysis_tags"), ",")
paises = F.filter(F.split(c("countries_tags"), ","), lambda p: p != "en:world")
marcas = F.transform(F.split(c("brands_tags"), ","),
                     lambda b: F.regexp_replace(F.lower(F.trim(b)), r"^[a-z]{2}:", ""))
marcas = F.array_distinct(F.filter(marcas, lambda b: b != ""))
envases = F.split(c("packaging_tags"), ",")
aditivos_list = F.split(c("additives_tags"), ",")

limpio = df.select(
    F.col("code"),
    (~F.length("code").isin(8, 13)).alias("code_raro"),
    c("product_name").alias("product_name"),
    F.year(F.from_unixtime(c("created_t").cast("long"))).alias("anio_alta"),
    c("created_t").cast("long").alias("created_t"),
    c("last_modified_t").cast("long").alias("last_modified_t"),
    sin_desconocidos("pnns_groups_1").alias("grupo_pnns"),
    sin_desconocidos("pnns_groups_2").alias("categoria_pnns"),
    sin_desconocidos("categories_tags").alias("categories_tags"),
    F.when(c("nutriscore_grade").isin("a", "b", "c", "d", "e"), c("nutriscore_grade")).alias("nutriscore_grade"),
    F.when(c("nova_group").isin("1", "2", "3", "4"), c("nova_group").cast("int")).alias("nova_group"),
    sin_desconocidos("environmental_score_grade").alias("environmental_score_grade"),
    c("additives_n").cast("int").alias("additives_n"),
    F.when(F.size(aditivos_list) > 0, aditivos_list).alias("additives_tags"),
    F.when(F.array_contains(etiq_analisis, "en:vegan"), True)
     .when(F.array_contains(etiq_analisis, "en:non-vegan"), False).alias("es_vegano"),
    F.when(F.array_contains(etiq_analisis, "en:vegetarian"), True)
     .when(F.array_contains(etiq_analisis, "en:non-vegetarian"), False).alias("es_vegetariano"),
    F.when(F.size(paises) > 0, paises).alias("paises"),
    F.when(F.size(marcas) > 0, marcas).alias("marcas"),
    F.when(F.size(marcas) > 0, F.element_at(marcas, 1)).alias("marca_principal"),
    F.when(F.size(envases) > 0, envases).alias("packaging_tags"),
    F.when(c("serving_quantity").cast("double") > 0, c("serving_quantity").cast("double")).alias("serving_quantity"),
    # Nutrientes en gramos (0 a 100) y energía (0 a 900 kcal)
    num("energy-kcal_100g", 0, 900).alias("energy_kcal_100g"),
    num("fat_100g", 0, 100).alias("fat_100g"),
    num("saturated-fat_100g", 0, 100).alias("saturated_fat_100g"),
    num("carbohydrates_100g", 0, 100).alias("carbohydrates_100g"),
    num("sugars_100g", 0, 100).alias("sugars_100g"),
    num("fiber_100g", 0, 100).alias("fiber_100g"),
    num("proteins_100g", 0, 100).alias("proteins_100g"),
    num("salt_100g", 0, 100).alias("salt_100g"),
    # Micronutrientes para preguntas específicas
    num("iron_100g", 0, 100).alias("iron_100g"),
    num("calcium_100g", 0, 100).alias("calcium_100g"),
    num("vitamin-c_100g", 0, 100).alias("vitamin_c_100g"),
    c("data_quality_errors_tags").isNotNull().alias("_error_off"),
)

# Indicadores de calidad y consistencia nutricional
nutrientes = ["energy_kcal_100g", "fat_100g", "carbohydrates_100g", "sugars_100g", "proteins_100g", "salt_100g"]
tiene_datos = F.coalesce(F.greatest(*[F.col(n).isNotNull().cast("int") for n in nutrientes]), F.lit(0)) == 1

incoherente = (
    F.coalesce(F.col("saturated_fat_100g") > F.col("fat_100g"), F.lit(False))
    | F.coalesce(F.col("sugars_100g") > F.col("carbohydrates_100g"), F.lit(False))
    | F.coalesce(F.col("fat_100g") + F.col("carbohydrates_100g") + F.col("proteins_100g") > 105, F.lit(False))
    | F.col("_error_off")
)

limpio = (limpio.withColumn("tiene_datos_nutricionales", tiene_datos)
                .withColumn("nutrientes_incoherentes", incoherente)
                .drop("_error_off"))

limpio.repartition(12).write.mode("overwrite").parquet(salida)
print("ESCRITO", salida)
