# Preguntas del análisis

Las 11 preguntas propuestas por el equipo y revisadas con los datos de la exploración.
Cinco de ellas (1 a 5) son las preguntas de referencia sobre las que se hicieron los scripts de limpieza
y verificación; las otras seis (6 a 11) usan las columnas que Mauro añadió al Parquet (micronutrientes, fibra,
fechas, aditivos y envases).

- **Datos de entrada:** Parquet limpio `/data/clean/off_limpio` (ver [LIMPIEZA.md](LIMPIEZA.md)).
- **Dónde se ejecutan:** en el cliente de consulta (`hadoop-client-consulta`), con el código en `src/client_consulta/`
  (ver [README](../README.md#sincronizar-y-ejecutar-el-código-de-src-en-los-clientes)).
- **Resultados:** se guardan en `/results/<pregunta>` (HDFS). Solo el cliente de consulta escribe ahí.

## Índice

- [Resumen](#resumen)
- [Cómo leer los datos](#cómo-leer-los-datos)
- [1. Vegano/vegetariano por categoría](#1-veganovegetariano-por-categoría)
- [2. Productos altos en azúcar según el semáforo nutricional](#2-productos-altos-en-azúcar-según-el-semáforo-nutricional)
- [3. Distribución del Nutri-Score por país](#3-distribución-del-nutri-score-por-país)
- [4. Aditivos frente a nivel de procesamiento (NOVA)](#4-aditivos-frente-a-nivel-de-procesamiento-nova)
- [5. Marcas con mejor y peor perfil nutricional dentro de cada grupo](#5-marcas-con-mejor-y-peor-perfil-nutricional-dentro-de-cada-grupo)
- [6. Evolución anual de la cantidad y calidad de los datos](#6-evolución-anual-de-la-cantidad-y-calidad-de-los-datos)
- [7. Coherencia entre la energía declarada y la calculada, por país](#7-coherencia-entre-la-energía-declarada-y-la-calculada-por-país)
- [8. Alegaciones de proteínas y fibra (Reglamento UE 1924/2006)](#8-alegaciones-de-proteínas-y-fibra-reglamento-ue-19242006)
- [9. Aditivos más frecuentes y su presencia por país](#9-aditivos-más-frecuentes-y-su-presencia-por-país)
- [10. Mejores categorías como fuente de hierro, calcio y vitamina C](#10-mejores-categorías-como-fuente-de-hierro-calcio-y-vitamina-c)
- [11. Reparto de la energía entre grasas, carbohidratos y proteínas](#11-reparto-de-la-energía-entre-grasas-carbohidratos-y-proteínas)

## Resumen

| N.º | Pregunta | Propuesta de | Filas útiles en el archivo completo |
|---|---|---|---|
| 1 | Vegano/vegetariano por categoría | Katherine | ≈ 574.000 (vegano), ≈ 390.000 (vegetariano) |
| 2 | Altos en azúcar según el semáforo nutricional | Katherine | ≈ 1,28 M |
| 3 | Nutri-Score por país | Katherine | ≈ 1,42 M |
| 4 | Aditivos frente a NOVA | Katherine | ≈ 1,13 M |
| 5 | Marcas con mejor y peor perfil por grupo | Alberto | ≈ 0,93 M |
| 6 | Evolución anual de la calidad de los datos | Alberto | por medir |
| 7 | Energía declarada frente a calculada, por país | Alberto | por medir |
| 8 | Alegaciones de proteínas y fibra | Jorge | por medir |
| 9 | Aditivos más frecuentes por país | Jorge | por medir |
| 10 | Fuentes de hierro, calcio y vitamina C | Jorge | por medir |
| 11 | Reparto de macronutrientes frente a rangos | Jorge | por medir |

Las cifras de las preguntas 1 a 5 son estimaciones a partir de la muestra de 100.000 filas
(ver [EDA.md](EDA.md)), y `verificar.py` las comprueba sobre el archivo completo. Para las demás, las columnas
existen en el Parquet, pero falta contar las filas con todos los datos a la vez.

El responsable de cada pregunta y su tanda se fijan en la definición de las 10 preguntas finales y 2 de reserva; hay 11 propuestas para 12 huecos.

## Cómo leer los datos

El Parquet limpio tiene 34 columnas. Las que usan las preguntas:

| Columna | Contenido |
|---|---|
| `grupo_pnns`, `categoria_pnns` | Grupos PNNS 1 y 2 (sin `unknown`) |
| `categories_tags` | Lista de categorías |
| `es_vegano`, `es_vegetariano` | `true`, `false` o nulo si no se sabe |
| `paises` | Lista de países (sin `en:world`) |
| `marcas`, `marca_principal` | Marcas en minúsculas y sin prefijo de idioma |
| `nutriscore_grade`, `nova_group` | Letra `a`-`e`; entero 1-4 |
| `additives_n`, `additives_tags` | Número y lista de aditivos |
| `anio_alta`, `created_t` | Año y fecha de alta |
| `energy_kcal_100g`, `fat_100g`, `saturated_fat_100g`, `carbohydrates_100g`, `sugars_100g`, `fiber_100g`, `proteins_100g`, `salt_100g` | Nutrientes por 100 g |
| `iron_100g`, `calcium_100g`, `vitamin_c_100g` | Micronutrientes por 100 g |
| `tiene_datos_nutricionales`, `nutrientes_incoherentes` | Indicadores de calidad |

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pregunta").getOrCreate()
df = spark.read.parquet("hdfs://hadoop-master:9000/data/clean/off_limpio")
# ... transformaciones de la pregunta ...
# resultado.write.mode("overwrite").parquet("hdfs://hadoop-master:9000/results/p01_vegano")
```

## 1. Vegano/vegetariano por categoría

- **Enunciado:** qué porcentaje de productos son veganos, vegetarianos, no veganos o de estado desconocido, y cómo varía según la categoría de alimento.
- **Utilidad:** foto rápida de cuánta oferta vegana hay en Open Food Facts y en qué categorías se concentra.
- **Columnas:** `es_vegano`, `es_vegetariano`, `categoria_pnns` (o `categories_tags`).
- **Operaciones de Spark:** `groupBy`/`agg` con `count` y porcentaje por categoría, `when`/`otherwise` para el estado.
- **Datos:** ≈ 574.000 productos con estado vegano conocido y categoría; ≈ 390.000 con estado vegetariano (41 categorías). Es la más ajustada de las cinco.

## 2. Productos altos en azúcar según el semáforo nutricional

- **Enunciado:** qué porcentaje de productos superan el umbral de "alto en azúcar" (más de 22,5 g/100 g, criterio del semáforo nutricional basado en el Reglamento UE 1169/2011) y qué categorías concentran más casos.
- **Utilidad:** pregunta con respaldo regulatorio, no con un umbral inventado.
- **Columnas:** `sugars_100g`, `categoria_pnns` (o `grupo_pnns`).
- **Operaciones de Spark:** `withColumn` con `when`, `groupBy`/`agg`, ordenación por porcentaje.
- **Datos:** ≈ 1,28 M productos con azúcar válido y categoría. El Parquet no incluye `nutrient_levels_tags` ni una columna que distinga líquidos (umbral de 27 g/100 ml), así que el cálculo se hace con `sugars_100g` y el umbral de 22,5 g/100 g, y esa simplificación debe constar en la respuesta.

## 3. Distribución del Nutri-Score por país

- **Enunciado:** cómo se reparte la calificación Nutri-Score (A-E) entre los países donde se vende el producto.
- **Utilidad:** compara la calidad nutricional del mercado de alimentos envasados entre países.
- **Columnas:** `nutriscore_grade`, `paises`.
- **Operaciones de Spark:** `explode` de `paises` (un producto puede venderse en varios), `groupBy`/`pivot` por letra, filtro por mínimo de productos por país.
- **Datos:** ≈ 1,42 M productos con Nutri-Score y país.

## 4. Aditivos frente a nivel de procesamiento (NOVA)

- **Enunciado:** a mayor número de aditivos (`additives_n`), ¿es mayor la probabilidad de que el producto esté en el grupo NOVA 4 (ultraprocesado)?
- **Utilidad:** valida con datos una intuición común.
- **Columnas:** `additives_n`, `nova_group`.
- **Operaciones de Spark:** `groupBy` por número de aditivos (agrupando los valores altos), `avg` de `nova_group == 4`, tabla de proporciones.
- **Datos:** ≈ 1,13 M productos con ambos datos. NOVA 4 se define en parte por los aditivos, así que la relación será algo circular y debe señalarse.

## 5. Marcas con mejor y peor perfil nutricional dentro de cada grupo

- **Enunciado:** dentro de cada grupo PNNS, ¿qué 5 marcas con al menos N productos tienen la media más baja y la más alta de azúcar, grasa saturada y sal?
- **Utilidad:** ayuda a consumidores y distribuidores a ver qué marcas de cada categoría tienen mejores productos nutricionalmente y cuáles destacan por lo contrario; no se comparan bebidas con aceites.
- **Columnas:** `marcas`, `grupo_pnns`, `sugars_100g`, `saturated_fat_100g`, `salt_100g`.
- **Operaciones de Spark:** `explode` de `marcas`, `groupBy`/`agg` (`avg`, `count`), filtro por mínimo de productos, ventana `row_number()` con `Window.partitionBy(...).orderBy(...)`.
- **Dificultad:** 3.
- **Datos:** ≈ 0,93 M productos con marca, grupo y Nutri-Score. Las marcas blancas (`hacendado`, `marque-repere`, `u`) cuentan como marca (decisión provisional de la [limpieza](LIMPIEZA.md#decisiones-provisionales)).

## 6. Evolución anual de la cantidad y calidad de los datos

- **Enunciado:** por año de alta, ¿cuántos productos se añadieron, qué porcentaje tiene energía y macronutrientes informados y cómo crece cada año respecto al anterior?
- **Utilidad:** decide de qué años se pueden fiar las demás preguntas y justifica filtros de limpieza.
- **Columnas:** `anio_alta`, `energy_kcal_100g`, `fat_100g`, `carbohydrates_100g`, `proteins_100g`.
- **Operaciones de Spark:** `groupBy`/`agg` con `avg(when(col.isNotNull(),1).otherwise(0))`, ventana con `lag` para la variación interanual.
- **Dificultad:** 2.
- **Riesgo de datos:** la energía y los macronutrientes tienen un 28-29 % de nulos, que es justamente la métrica medida. 2026 es un año incompleto y debe indicarse.

## 7. Coherencia entre la energía declarada y la calculada, por país

- **Enunciado:** ¿qué porcentaje de productos tiene una energía declarada que difiere más de un 20 % de la calculada con `4·carbohidratos + 4·proteínas + 9·grasa`, y cómo varía según el país?
- **Utilidad:** detecta etiquetados incoherentes y muestra qué mercados tienen datos fiables.
- **Columnas:** `energy_kcal_100g`, `carbohydrates_100g`, `proteins_100g`, `fat_100g`, `paises`.
- **Operaciones de Spark:** `withColumn` con expresión aritmética, `when`/`otherwise`, `explode` de `paises`, `groupBy`/`agg`, filtro por mínimo de productos por país.
- **Dificultad:** 2.
- **Riesgo de datos:** hay que medir cuántas filas tienen las cuatro columnas a la vez (cada una ronda el 28 % de nulos).

## 8. Alegaciones de proteínas y fibra (Reglamento UE 1924/2006)

- **Enunciado:** ¿qué porcentaje de productos cumple los umbrales legales para declararse "fuente de proteínas" o "alto contenido de proteínas", y "fuente de fibra" o "alto contenido de fibra", y en qué grupos se concentran?
- **Utilidad:** los umbrales vienen de un reglamento europeo, no de un criterio inventado.
- **Umbrales:**
  - Fuente de proteínas: al menos el 12 % de la energía viene de proteínas (`proteins_100g · 4 / energy_kcal_100g ≥ 0,12`).
  - Alto contenido de proteínas: al menos el 20 % de la energía.
  - Fuente de fibra: ≥ 3 g/100 g (o ≥ 1,5 g/100 kcal).
  - Alto contenido de fibra: ≥ 6 g/100 g (o ≥ 3 g/100 kcal).
- **Columnas:** `proteins_100g`, `energy_kcal_100g`, `fiber_100g`, `grupo_pnns`.
- **Operaciones de Spark:** `withColumn` para el % de energía de las proteínas, `when`/`otherwise` para asignar cada alegación, `groupBy`/`agg` por grupo.
- **Dificultad:** 2.
- **Riesgo de datos:** `fiber_100g` tiene un 61 % de nulos, así que la parte de fibra se calcula solo sobre los productos que la informan.

## 9. Aditivos más frecuentes y su presencia por país

- **Enunciado:** ¿cuáles son los 10 aditivos (E-números) más usados y en qué países aparecen proporcionalmente más?
- **Utilidad:** muestra diferencias de regulación o de hábitos de fabricación entre mercados.
- **Columnas:** `additives_tags`, `paises`.
- **Operaciones de Spark:** doble `explode` (aditivos × países), `groupBy`/`count`, normalización por el total de productos con aditivos informados de cada país, filtro por mínimo de productos, ventana `row_number()` con `Window.partitionBy("pais")` para el top N.
- **Dificultad:** 3. El doble `explode` multiplica las filas: conviene `repartition` y vigilar la memoria de los executors en YARN.
- **Riesgo de datos:** hay que distinguir "sin aditivos" de "no informado" (se espera un % de nulos parecido al de `additives_n`, ≈ 72 %).

## 10. Mejores categorías como fuente de hierro, calcio y vitamina C

- **Enunciado:** ¿qué categorías aportan más hierro, calcio y vitamina C por 100 g y por 100 kcal, y qué proporción de sus productos informa esos nutrientes?
- **Utilidad:** ayuda a quien necesita esos nutrientes (por ejemplo, dietas veganas) a ver qué categorías de producto envasado le aportan más.
- **Columnas:** `iron_100g`, `calcium_100g`, `vitamin_c_100g`, `energy_kcal_100g`, `categories_tags`.
- **Operaciones de Spark:** `explode` de categorías, `withColumn` para el valor por 100 kcal, `groupBy`/`agg` (`avg`, `percentile_approx`, `count`, porcentaje de no nulos), filtro por mínimo de productos con dato, `rank` con ventana por nutriente.
- **Dificultad:** 2.
- **Riesgo de datos:** alto; hierro 84 %, calcio 84,2 % y vitamina C 88,8 % de nulos. Solo se interpreta sobre los productos que informan el nutriente, y el porcentaje de cobertura forma parte de la respuesta.

## 11. Reparto de la energía entre grasas, carbohidratos y proteínas

- **Enunciado:** ¿qué porcentaje de los productos de cada categoría reparte su energía dentro de los rangos de referencia (grasa 20-35 % y carbohidratos 45-60 % de la energía, según EFSA/OMS) y cuáles se desvían más?
- **Utilidad:** muestra qué categorías, por su composición, empujan la dieta hacia el exceso de grasa o de carbohidratos.
- **Columnas:** `fat_100g`, `carbohydrates_100g`, `proteins_100g`, `energy_kcal_100g`, `categories_tags`.
- **Operaciones de Spark:** `withColumn` para el % de energía de cada macronutriente (`9·grasa`, `4·carbohidratos`, `4·proteínas`, sobre la suma), filtro de filas incoherentes (`nutrientes_incoherentes`), `when`/`otherwise` para "dentro/fuera de rango", `explode` de categorías, `groupBy`/`agg`.
- **Dificultad:** 2.
- **Riesgo de datos:** hay que medir las filas con los cuatro nutrientes a la vez. Hay que confirmar la fuente exacta de los rangos antes de publicar.
