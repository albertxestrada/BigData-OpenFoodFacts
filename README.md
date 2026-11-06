# BigData-OpenFoodFacts

Análisis de la información nutricional de Open Food Facts con **Spark sobre Hadoop
(HDFS + YARN) en AWS Academy**. El clúster tiene un master, tres workers y un cliente
desde el que se desarrollan y envían los trabajos al master.

Este README reúne las instrucciones de uso diario y desarrollo del proyecto.
La creación y preparación inicial se documentan en [la guía de despliegue](DEPLOYMENT.md).
El trabajo original de [Semana 0](docs/Semana0.md) se conserva como registro histórico.
La planificación del equipo está en [issues.md](docs/issues.md).

> Contains data from Open Food Facts, available under the
> [Open Database License](https://opendatacommons.org/licenses/odbl/1.0/).
> Fuente: <https://world.openfoodfacts.org/data>.

## Índice

- [Estructura del proyecto](#estructura-del-proyecto)
- [Requisitos](#requisitos)
- [Crear el clúster](#crear-el-clúster)
- [Sincronizar y ejecutar el código de src/ en master y cliente](#sincronizar-y-ejecutar-el-código-de-src-en-master-y-cliente)
- [Preguntas candidatas](#preguntas-candidatas)

## Estructura del proyecto

```text
README.md                    # Uso del proyecto
DEPLOYMENT.md                # Creación y preparación del clúster
scripts/
  cluster.sh                 # Despliegue: ver DEPLOYMENT.md
  sync.sh                    # Copiar código al master y al cliente
ansible/
  deploy.yml                 # Configuración y datos del despliegue
config/
  cluster.env.example        # Ejemplo de configuración local
src/
  master/                    # Limpieza de datos en el master
  client/                    # Desarrollo de preguntas en el cliente
docs/                        # Semana 0, imágenes y planificación
```

## Requisitos

Los dos scripts se ejecutan **en tu ordenador**, desde Linux, WSL o macOS, con Bash,
AWS CLI v2, OpenSSH y rsync. Los comandos `apt` del despliegue son para Linux/WSL.
Para instalar AWS CLI, seguir las [instrucciones oficiales para tu sistema](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

```bash
aws --version
ssh -V
rsync --version
```

Renovar las credenciales de AWS Academy en `~/.aws/credentials` (puedes usar nano ` nano ~/.aws/credentials`) al empezar la sesión.
La clave del laboratorio debe estar en `~/.ssh/vockey.pem` con permisos `600`.

## Crear el clúster

Para crear y preparar el clúster, seguir [DEPLOYMENT.md](DEPLOYMENT.md).
La guía documenta la infraestructura, la configuración y la carga de datos paso a paso.

## Sincronizar y ejecutar el código de `src/` en master y cliente

Encender y apagar las instancias desde la consola de AWS. Para sincronizar `src/`
desde tu ordenador:

```bash
./scripts/sync.sh all
```

El código se organiza según el nodo donde se ejecutará:

| Carpeta local | Nodo de destino | Carpeta remota |
|---|---|---|
| `src/master/` | Master: limpieza de datos | `~/BigData-OpenFoodFacts/src/master/` |
| `src/client/` | Cliente: desarrollo de las preguntas | `~/BigData-OpenFoodFacts/src/client/` |

El driver se ejecuta en el master con modo `client`; los executors procesan en los workers.

Ambas carpetas incluyen un `count_dataset.py` de prueba: lee el CSV de HDFS
con separador tabulador y cabecera, e imprime `TOTAL_PRODUCTOS` y `TOTAL_COLUMNAS`.
El de `master` sirve como comprobación previa a la limpieza; ambos solo leen datos.

En el master, ejecutar el conteo de `master`:

```bash
source ~/.bashrc
cd ~/BigData-OpenFoodFacts
ls src/master/

spark-submit --master yarn --deploy-mode client src/master/count_dataset.py
```

Desde el cliente, copiar las preguntas al master y ejecutarlas allí por SSH:

```bash
source ~/.bashrc
cd ~/BigData-OpenFoodFacts
ls src/client/

rsync -av src/client hadoop-master:~/BigData-OpenFoodFacts/src/
ssh hadoop-master 'source ~/.bashrc; cd ~/BigData-OpenFoodFacts; spark-submit --master yarn --deploy-mode client src/client/count_dataset.py'
```

Por defecto leen `hdfs://hadoop-master:9000/data/raw/openfoodfacts.csv`.
Para probar con la muestra local, ejecutar en el master (también sirve para `client`):

```bash
spark-submit --master 'local[2]' --deploy-mode client src/master/count_dataset.py file:///home/ec2-user/muestra.csv
```

La salida del driver aparece en la terminal. Los logs de executors se consultan
desde el master con `yarn logs -applicationId <APPLICATION_ID>`.

## Preguntas candidatas

1. **Vegano/vegetariano por categoría.**
   - Objetivo: qué porcentaje de productos son veganos, vegetarianos, no-veganos o de estado
     desconocido, y cómo varía según la categoría de alimento.
   - Utilidad: foto rápida de cuánta oferta "vegana" hay realmente en Open Food Facts y en qué
     categorías se concentra.
   - Columnas: `ingredients_analysis_tags` (`en:vegan-status-unknown`, `en:non-vegan`,
     `en:maybe-vegan`...), `food_groups_en`/`categories_en` para agrupar.

2. **Productos "altos en azúcar" según el semáforo nutricional.**
   - Objetivo: qué porcentaje de productos superan el umbral de "alto en azúcar" (>22.5 g/100g,
     o >27 g/100ml en líquidos, el mismo criterio del Reglamento UE 1169/2011 / semáforo
     nutricional), y qué categorías concentran más casos.
   - Utilidad: pregunta con respaldo regulatorio real, no un umbral inventado.
   - Columnas: `sugars_100g` o `nutrient_levels_tags` (ya trae `en:sugars-in-high-quantity`
     precalculado), más `categories_en`/`food_groups_en` para agrupar.

3. **Distribución del Nutri-Score por país.**
   - Objetivo: cómo se reparte la calificación Nutri-Score (A–E) entre los distintos países
     donde se vende el producto.
   - Utilidad: compara la "calidad nutricional" percibida del mercado de alimentos envasados
     entre países.
   - Columnas: `nutriscore_grade`, `countries_en`/`countries_tags` (hay que explotar la lista,
     un producto puede venderse en varios países).

4. **Aditivos vs. nivel de procesamiento (NOVA).**
   - Objetivo: a mayor número de aditivos (`additives_n`), ¿mayor es la probabilidad de que el
     producto esté en el grupo NOVA 4 (ultraprocesado)?
   - Utilidad: valida con datos una intuición común (más aditivos = más ultraprocesado).
   - Columnas: `additives_n`, `nova_group`.

**5. Marcas con mejor y peor perfil nutricional dentro de cada grupo de alimentos**
- **Enunciado:** dentro de cada grupo PNNS (`pnns_groups_1`), ¿qué 5 marcas con al menos N productos tienen la media más baja y la más alta de azúcar, grasa saturada y sal?
- **Objetivo:** clasificar las marcas por su perfil nutricional dentro de su propio grupo, para no comparar bebidas con aceites.
- **Utilidad:** ayuda a consumidores y a distribuidores a identificar qué marcas de cada categoría tienen mejores productos nutricionalmente y cuáles destacan por lo contrario.
- **Columnas:** `brands`, `pnns_groups_1`, `sugars_100g`, `saturated-fat_100g`, `salt_100g`.
- **Operaciones de Spark:** `split` + `explode` (un producto puede tener varias marcas), `groupBy`/`agg` (`avg`, `count`), `filter` por mínimo de productos, ventana `row_number()` con `Window.partitionBy(...).orderBy(...)`.
- **Dificultad:** 3.
- **Riesgo de datos:** `brands` 35,9 % nulos; `sugars` 33 %, `saturated-fat` 34,6 %, `salt` 37,5 %. `pnns_groups_1` contiene `unknown` (por limpiar).

**6. Evolución anual de la cantidad y calidad de los datos nutricionales**
- **Enunciado:** por año de alta del producto (`created_t`), ¿cuántos productos se añadieron, qué porcentaje tiene energía y macronutrientes informados, y cómo crece cada año respecto al anterior?
- **Objetivo:** medir cómo ha mejorado la completitud de Open Food Facts con el tiempo.
- **Utilidad:** decide de qué años se pueden fiar las demás preguntas, y justifica filtros de limpieza.
- **Columnas:** `created_t`, `energy-kcal_100g`, `fat_100g`, `carbohydrates_100g`, `proteins_100g`.
- **Operaciones de Spark:** `from_unixtime`/`year`, `groupBy`/`agg` con `avg(when(col.isNotNull(),1).otherwise(0))`, ventana con `lag` para la variación interanual.
- **Dificultad:** 2.
- **Riesgo de datos:** `created_t` 0 % nulos; la energía y los macros tienen 28-29 % de nulos, que es justamente la métrica medida.

**7. Coherencia entre la energía declarada y la calculada, por país**
- **Enunciado:** ¿qué porcentaje de productos tiene una energía declarada que difiere más de un 20 % de la calculada con `4·carbohidratos + 4·proteínas + 9·grasa`, y cómo varía según el país?
- **Objetivo:** detectar etiquetados incoherentes o errores de entrada, y ver en qué países son más frecuentes.
- **Utilidad:** distingue qué mercados tienen datos fiables y alimenta las reglas de limpieza de valores imposibles.
- **Columnas:** `energy-kcal_100g`, `carbohydrates_100g`, `proteins_100g`, `fat_100g`, `countries_tags`.
- **Operaciones de Spark:** `withColumn` con expresión aritmética, `when`/`otherwise`, `explode(split(...))` sobre países, `groupBy`/`agg`, filtro por mínimo de productos por país.
- **Dificultad:** 2.
- **Riesgo de datos:** `energy` 28 %, `carbohydrates` 28,7 %, `proteins` 28,6 %, `fat` 28,7 % nulos; hay que medir cuántas filas tienen las cuatro a la vez. `countries_tags` 0,4 % nulos.

**8. Productos que cumplen las alegaciones "fuente de / alto en proteínas y fibra" (Reglamento UE 1924/2006)**
- **Enunciado:** ¿qué porcentaje de productos cumple los umbrales legales para declararse "fuente de proteínas" o "alto contenido de proteínas", y "fuente de fibra" o "alto contenido de fibra", y en qué grupos de alimentos se concentran?
- **Objetivo:** cuantificar cuántos productos podrían anunciarse con una alegación nutricional según el criterio legal y en qué grupos aparecen.
- **Utilidad:** los umbrales vienen de un reglamento europeo, así que la pregunta no se apoya en un criterio inventado.
- **Umbrales (Anexo del Reglamento 1924/2006):**
  - Fuente de proteínas: al menos el 12 % de la energía viene de proteínas (`proteins_100g · 4 / energy-kcal_100g ≥ 0,12`).
  - Alto contenido de proteínas: al menos el 20 % de la energía.
  - Fuente de fibra: ≥ 3 g/100g (o ≥ 1,5 g/100 kcal).
  - Alto contenido de fibra: ≥ 6 g/100g (o ≥ 3 g/100 kcal).
- **Columnas:** `proteins_100g`, `energy-kcal_100g`, `fiber_100g`, `pnns_groups_1`.
- **Operaciones de Spark:** `withColumn` para calcular el % de energía que aportan las proteínas, `when`/`otherwise` para asignar cada alegación, `filter` de valores imposibles (nutrientes > 100 g, energía ≤ 0 o > 900 kcal), `groupBy`/`agg` por grupo PNNS.
- **Dificultad:** 2.
- **Riesgo de datos:**
  - `fiber_100g` tiene un 61 % de nulos, así que la parte de fibra se calcula solo sobre los productos que la informan.
  - `proteins_100g` y `energy-kcal_100g` rondan el 28-29 % de nulos.
  - `pnns_groups_1` contiene `unknown`, que hay que tratar como nulo.

**9. Aditivos más frecuentes y su presencia por país**
- **Enunciado:** ¿cuáles son los 10 aditivos (E-números) más usados en los productos, y en qué países aparecen proporcionalmente más?
- **Objetivo:** pasar del número de aditivos de cada producto a saber cuáles son y cómo cambia su uso entre mercados.
- **Utilidad:** permite ver diferencias de regulación o de hábitos de fabricación entre países (un aditivo frecuente en un país y casi ausente en otro).
- **Columnas:** `additives_tags` (lista tipo `en:e330,en:e322i`), `countries_tags`.
- **Operaciones de Spark:** doble `explode(split(...))` (aditivos × países), `groupBy`/`count`, normalizar por el total de productos con aditivos informados de cada país, `filter` por mínimo de productos por país, ventana `row_number()` con `Window.partitionBy("pais")` para el top N.
- **Dificultad:** 3. El doble `explode` multiplica las filas, así que conviene `repartition` y vigilar la memoria de los executors en YARN.
- **Riesgo de datos:** `additives_tags` no está en la tabla de columnas de la muestra; hay que confirmar su nombre con `grep -w "additives_tags" columnas_relleno.tsv`. Se espera un % de nulos parecido al de `additives_n` (72 %), y hay que distinguir "sin aditivos" de "no informado".

**10. Mejores categorías como fuente de hierro, calcio y vitamina C**
- **Enunciado:** ¿qué categorías aportan más hierro, calcio y vitamina C por 100 g y por 100 kcal, y qué proporción de sus productos informa esos nutrientes?
- **Objetivo:** identificar dónde están las fuentes alimentarias de esos micronutrientes y hasta qué punto se dispone de datos.
- **Utilidad:** ayuda a quien necesita más hierro, calcio o vitamina C (por ejemplo, dietas veganas) a ver qué categorías de producto envasado le aportan más.
- **Columnas:** `iron_100g`, `calcium_100g`, `vitamin-c_100g`, `energy-kcal_100g`, `categories_tags`.
- **Operaciones de Spark:** `explode` de categorías, `withColumn` para el valor por 100 kcal, `groupBy`/`agg` (`avg`, `percentile_approx`, `count`, porcentaje de no nulos), filtro por mínimo de productos con dato y `rank` con ventana por nutriente.
- **Dificultad:** 2.
- **Riesgo de datos:** alto; hierro 84 %, calcio 84,2 % y vitamina C 88,8 % de nulos. Solo se interpreta sobre los productos que informan el nutriente, y el porcentaje de cobertura forma parte de la respuesta.

**11. Reparto de la energía entre grasas, carbohidratos y proteínas frente a los rangos recomendados**
- **Enunciado:** ¿qué porcentaje de los productos de cada categoría reparte su energía dentro de los rangos de referencia (grasa 20-35 % y carbohidratos 45-60 % de la energía, según EFSA/OMS), y cuáles se desvían más?
- **Objetivo:** medir el equilibrio de macronutrientes de cada categoría respecto a recomendaciones oficiales, sin inventar umbrales.
- **Utilidad:** muestra qué categorías, por su composición, empujan la dieta hacia el exceso de grasa o de carbohidratos.
- **Columnas:** `fat_100g`, `carbohydrates_100g`, `proteins_100g`, `energy-kcal_100g`, `categories_tags`.
- **Operaciones de Spark:** `withColumn` para el % de energía de cada macronutriente (`9·grasa`, `4·carbohidratos`, `4·proteínas`, sobre la suma), filtro de filas incoherentes, `when`/`otherwise` para "dentro/fuera de rango", `explode` de categorías, `groupBy`/`agg`.
- **Dificultad:** 2.
- **Riesgo de datos:** `energy` 28 %, `fat` 28,7 %, `carbohydrates` 28,7 %, `proteins` 28,6 % nulos; hay que medir las filas con los cuatro a la vez. Hay que confirmar la fuente exacta de los rangos antes de publicar.
