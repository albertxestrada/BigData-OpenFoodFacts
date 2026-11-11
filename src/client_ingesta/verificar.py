# verificar.py · comprueba que el Parquet cumple las reglas y repite los conteos de las preguntas de referencia
# Uso: spark-submit src/client_ingesta/verificar.py [entrada] [salida]
import sys
from functools import reduce
from pyspark.sql import SparkSession, functions as F

entrada = sys.argv[1] if len(sys.argv) > 1 else "hdfs://hadoop-master:9000/data/raw/openfoodfacts.csv"
salida = sys.argv[2] if len(sys.argv) > 2 else "hdfs://hadoop-master:9000/data/clean/off_limpio"

spark = SparkSession.builder.appName("verificar_limpieza").getOrCreate()
bruto = spark.read.csv(entrada, sep="\t", header=True, quote="")
limpio = spark.read.parquet(salida)
fallos = []


def comprobar(nombre, ok, detalle):
    print(("OK    " if ok else "FALLO ") + nombre + " -> " + str(detalle))
    if not ok:
        fallos.append(nombre)


# 1. Filas: las que faltan deben ser exactamente las copias sobrantes de códigos repetidos
n_bruto, n_limpio = bruto.count(), limpio.count()
sobrantes = bruto.groupBy("code").count().filter("count > 1").agg(F.sum(F.col("count") - 1)).first()[0] or 0
comprobar("filas = bruto - copias sobrantes", n_bruto - n_limpio == sobrantes,
          f"bruto={n_bruto} limpio={n_limpio} sobrantes={sobrantes}")
comprobar("code único", limpio.select("code").distinct().count() == n_limpio, "distinct == filas")

# 2. Valores desconocidos y rangos
comprobar("sin 'unknown' en grupo/categoría",
          limpio.filter(F.col("grupo_pnns").isin("unknown", "not-applicable")
                        | F.col("categoria_pnns").isin("unknown", "not-applicable")).count() == 0, "0 filas")
comprobar("nutriscore solo a-e",
          limpio.filter(F.col("nutriscore_grade").isNotNull() & ~F.col("nutriscore_grade").isin("a", "b", "c", "d", "e")).count() == 0, "0 filas")
comprobar("nova_group solo 1-4",
          limpio.filter(F.col("nova_group").isNotNull() & ~F.col("nova_group").between(1, 4)).count() == 0, "0 filas")
g100 = ["fat_100g", "saturated_fat_100g", "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", "salt_100g"]
r = limpio.agg(*[F.min(x).alias("min_" + x) for x in g100], *[F.max(x).alias("max_" + x) for x in g100],
               F.min("energy_kcal_100g").alias("min_kcal"), F.max("energy_kcal_100g").alias("max_kcal")).first()
comprobar("nutrientes en [0, 100]", all(r["min_" + x] >= 0 and r["max_" + x] <= 100 for x in g100 if r["max_" + x] is not None),
          {x: (r["min_" + x], r["max_" + x]) for x in g100})
comprobar("energy_kcal en [0, 900]", r["min_kcal"] >= 0 and r["max_kcal"] <= 900, (r["min_kcal"], r["max_kcal"]))

# 3. Coherencia de las marcas y de las columnas derivadas
comprobar("marcas en minúsculas y sin prefijo",
          limpio.filter(F.exists("marcas", lambda b: (b != F.lower(b)) | b.rlike("^[a-z]{2}:"))).count() == 0, "0 filas")
comprobar("paises sin en:world",
          limpio.filter(F.array_contains("paises", "en:world")).count() == 0, "0 filas")
comprobar("incoherentes con saturada > grasa siempre marcadas",
          limpio.filter((F.col("saturated_fat_100g") > F.col("fat_100g")) & ~F.col("nutrientes_incoherentes")).count() == 0, "0 filas")

# 4. Conteos de las 5 preguntas (la estimación sale de la muestra de 100.000 filas)
esperado = {"q1_vegano": 11768, "q1_vegetariano": 7996, "q2_azucar": 26141,
            "q3_nutriscore_pais": 29144, "q4_aditivos_nova": 23089, "q5_marcas_pnns": 19127}


def nn(*cols):   # cuenta las filas donde todas las columnas indicadas tienen valor
    return F.sum(reduce(lambda a, b: a & b, [F.col(x).isNotNull() for x in cols]).cast("int"))


q = limpio.agg(nn("categoria_pnns", "es_vegano").alias("q1_vegano"),
               nn("categoria_pnns", "es_vegetariano").alias("q1_vegetariano"),
               nn("categoria_pnns", "sugars_100g").alias("q2_azucar"),
               nn("nutriscore_grade", "paises").alias("q3_nutriscore_pais"),
               nn("additives_n", "nova_group").alias("q4_aditivos_nova"),
               nn("marcas", "grupo_pnns", "nutriscore_grade").alias("q5_marcas_pnns")).first().asDict()
for k, v in q.items():          # solo aviso: la estimación sale de una muestra, no es una regla
    est = esperado[k] * n_limpio / 100000
    print(("OK    " if abs(v - est) <= 0.15 * est else "AVISO ") + f"{k} real={v} estimado={int(est)} (±15 %)")

print("\nRESUMEN:", "TODO OK" if not fallos else f"{len(fallos)} FALLO(S): {fallos}")
sys.exit(1 if fallos else 0)
