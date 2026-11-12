# perfil_completo.py · % de filas con valor en cada columna del CSV completo
# Uso: spark-submit src/client_ingesta/perfil_completo.py [entrada] [salida]
import sys
from collections import Counter
from pyspark.sql import SparkSession, functions as F

entrada = sys.argv[1] if len(sys.argv) > 1 else "hdfs://hadoop-master:9000/data/raw/openfoodfacts.csv"
salida = sys.argv[2] if len(sys.argv) > 2 else "hdfs://hadoop-master:9000/data/clean/perfil_columnas"

spark = SparkSession.builder.appName("perfil_columnas").getOrCreate()
# Todo como texto (sin inferSchema). quote="" evita que una comilla suelta una dos líneas.
# En Spark una celda vacía se lee como null, así que count(columna) cuenta las filas con valor.
df = spark.read.csv(entrada, sep="\t", header=True, quote="")

agg = df.agg(F.count(F.lit(1)).alias("__filas"),
             *[F.count(F.col(f"`{c}`")).alias(c) for c in df.columns]).first().asDict()
filas = agg.pop("__filas")

perfil = [(i + 1, c, agg[c], round(100.0 * agg[c] / filas, 2)) for i, c in enumerate(df.columns)]
(spark.createDataFrame(perfil, ["n", "columna", "filas_con_valor", "pct_con_valor"])
      .coalesce(1).write.mode("overwrite").option("header", True).option("sep", "\t").csv(salida))


def banda(p):
    if p >= 90: return "1) 90-100 %"
    if p >= 50: return "2) 50-90 %"
    if p >= 10: return "3) 10-50 %"
    if p > 0: return "4) 0-10 %"
    return "5) 0 % (vacías)"


print("FILAS_TOTALES", filas)
print("COLUMNAS", len(perfil))
for b, k in sorted(Counter(banda(p[3]) for p in perfil).items()):
    print(b, k)
print("COLUMNAS_VACIAS:", [p[1] for p in perfil if p[2] == 0])
