"""Genera una gráfica PNG por cada pregunta que tenga resultado. Las que no existen se saltan.

Uso: python3 viz/plots.py results figures
"""
import glob
import os
import sys

import matplotlib
matplotlib.use("Agg")             # sin ventana, solo guarda los PNG
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

RESULTS = sys.argv[1] if len(sys.argv) > 1 else "results"
OUT = sys.argv[2] if len(sys.argv) > 2 else "figures"
os.makedirs(OUT, exist_ok=True)
sns.set_theme(style="whitegrid")


def exists(name):
    """True si Spark dejó el resultado de esa pregunta en results/<name>/."""
    found = bool(glob.glob(f"{RESULTS}/{name}/part-*.csv"))
    if not found:
        print("Sin resultado, se salta:", name)
    return found


def load(name):
    """Lee el CSV que Spark deja en results/<name>/part-*.csv."""
    return pd.read_csv(glob.glob(f"{RESULTS}/{name}/part-*.csv")[0])


def save(name, title, xlabel="", ylabel=""):
    """Pone título y ejes y guarda la gráfica como PNG."""
    plt.title(title, loc="left", fontweight="bold")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(f"{OUT}/{name}.png", dpi=150)
    plt.close()
    print("OK", f"{OUT}/{name}.png")


def ranking(name, y, x, title, xlabel):
    """Barras horizontales con las 15 primeras filas del resultado."""
    if not exists(name):
        return
    plt.figure(figsize=(9, 6))
    sns.barplot(data=load(name).head(15), y=y, x=x, color="#2e86c1")
    save(name, title, xlabel)


# P1: barras apiladas al 100 % con los colores oficiales del Nutri-Score
name = "q01-nutriscore-by-category"
if exists(name):
    df = load(name).head(15).set_index("category")[["pct_a", "pct_b", "pct_c", "pct_d", "pct_e"]]
    df.iloc[::-1].plot.barh(stacked=True, figsize=(9, 6), width=0.8,
                            color=["#038141", "#85bb2f", "#fecb02", "#ee8100", "#e63e11"])
    plt.legend(["A", "B", "C", "D", "E"], title="Nutri-Score", bbox_to_anchor=(1, 1))
    save(name, "P1. Reparto del Nutri-Score por categoría", "% de productos")

# P2: ranking
ranking("q02-ultraprocessed-by-category", "category", "pct_nova4",
        "P2. % de ultraprocesados (NOVA 4) por categoría", "% de productos")

# P3: un punto por categoría (azúcar en x, sal en y) con su nombre
name = "q03-sugar-salt-by-category"
if exists(name):
    df = load(name).head(15)
    plt.figure(figsize=(9, 6))
    sns.scatterplot(data=df, x="avg_sugars_g", y="avg_salt_g", size="products", sizes=(40, 400), legend=False)
    for _, row in df.iterrows():
        plt.annotate(row["category"], (row["avg_sugars_g"], row["avg_salt_g"]), fontsize=8)
    save(name, "P3. Azúcar y sal medios por categoría", "Azúcar (g/100 g)", "Sal (g/100 g)")

# P4: ranking
ranking("q04-worst-brands-nutriscore", "brand", "avg_nutriscore_score",
        "P4. Marcas con peor Nutri-Score medio", "Puntuación media (más alta es peor)")

# P5: una línea por país a lo largo de los años
name = "q05-products-per-year-country"
if exists(name):
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=load(name), x="created_year", y="new_products", hue="country", marker="o")
    plt.legend(bbox_to_anchor=(1, 1))
    save(name, "P5. Productos añadidos por año y país", "Año", "Productos nuevos")

# P6 y P8: mapa de calor de categoría por aditivo (o alérgeno) con el %
for name, column, title in [
    ("q06-top-additives-by-category", "additive", "P6. Aditivos más usados por categoría (%)"),
    ("q08-top-allergens-by-category", "allergen", "P8. Alérgenos más frecuentes por categoría (%)"),
]:
    if exists(name):
        df = load(name)
        top_categories = df["category"].unique()[:15]            # ya vienen ordenadas de mayor a menor
        df = df[df["category"].isin(top_categories)]
        table = df.pivot(index="category", columns=column, values="pct_products")
        plt.figure(figsize=(11, 6))
        sns.heatmap(table, annot=True, fmt=".0f", cmap="Reds")
        save(name, title)

# P7, P9 y P10: rankings
ranking("q07-vegan-by-country", "country", "pct_vegan",
        "P7. % de productos veganos por país", "% de productos")
ranking("q09-protein-by-category", "category", "avg_proteins_g",
        "P9. Proteína media por categoría", "Proteína (g/100 g)")
ranking("q10-completeness-by-country", "country", "avg_completeness",
        "P10. Completitud media de los datos por país", "Completitud (0 a 1)")
