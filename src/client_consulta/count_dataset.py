"""Cuenta los productos del dataset (cliente de consulta)."""

import argparse

from pyspark.sql import SparkSession


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input", nargs="?",
        default="hdfs://hadoop-master:9000/data/raw/openfoodfacts.csv",
        help="Ruta del CSV en HDFS o file:///ruta/a/muestra.csv.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("consulta-count-dataset").getOrCreate()
    try:
        df = spark.read.csv(args.input, sep="\t", header=True)
        print("TOTAL_PRODUCTOS:", df.count())
        print("TOTAL_COLUMNAS:", len(df.columns))
    finally:
        spark.stop()
