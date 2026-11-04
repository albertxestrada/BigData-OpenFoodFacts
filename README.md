# BigData-OpenFoodFacts

Análisis de la información nutricional de Open Food Facts con **Spark sobre Hadoop
(HDFS + YARN) en AWS Academy**. El clúster tiene un master, tres workers y un cliente
desde el que se lanzan los trabajos.

Este README reúne las instrucciones de uso diario y desarrollo del proyecto.
La creación y preparación inicial se documentan en [la guía de despliegue](DEPLOYMENT.md).
El trabajo original de [Semana 0](docs/Semana0.md) se conserva como registro histórico.
La planificación del equipo está en [issues.md](docs/issues.md).

> Contains data from Open Food Facts, available under the
> [Open Database License](https://opendatacommons.org/licenses/odbl/1.0/).
> Fuente: <https://world.openfoodfacts.org/data>.

## Índice

- [Requisitos](#requisitos)
- [Crear el clúster](#crear-el-clúster)
- [Uso diario del clúster](#uso-diario-del-clúster)
- [Copiar y ejecutar el código](#copiar-y-ejecutar-el-código)
- [Cómo están organizados los scripts](#cómo-están-organizados-los-scripts)
- [Errores habituales](#errores-habituales)
- [Documentación del dataset](#documentación-del-dataset)
- [Preguntas candidatas](#preguntas-candidatas)
- [Organización inicial del equipo](#organización-inicial-del-equipo)

## Requisitos

Los dos scripts se ejecutan **en tu ordenador**, desde Linux, WSL o macOS, con Bash,
AWS CLI v2, OpenSSH y rsync. Los comandos `apt` del despliegue son para Linux/WSL.
Para instalar AWS CLI, seguir las [instrucciones oficiales para tu sistema](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

```bash
aws --version
ssh -V
rsync --version
```

Para crear el clúster hacen falta Git y Python 3.9–3.11 con venv, compatibles con
las dependencias del playbook de clase. El script instala Ansible en `.deployment/`.
Renovar las credenciales de AWS Academy en `~/.aws/credentials` al empezar la sesión.
La clave del laboratorio debe estar en `~/.ssh/vockey.pem` con permisos `600`.

## Crear el clúster

Preparar la configuración una vez:

```bash
cp config/cluster.env.example config/cluster.env
nano config/cluster.env
```

| Variable | Para qué sirve |
|---|---|
| `AWS_REGION` | `us-east-1`, región de la AMI de clase |
| `AWS_PROFILE` | Perfil de `~/.aws/credentials`; por defecto `default` |
| `SSH_KEY` | Clave local del laboratorio; `$HOME/.ssh/vockey.pem` |
| `SSH_USER` | `ec2-user`, usuario del playbook de clase |
| `REMOTE_DIR` | Carpeta del proyecto dentro del home remoto |

Con las credenciales y la clave preparadas:

```bash
./scripts/cluster.sh deploy
```

Crea las cinco EC2, prepara red, Hadoop, YARN y Spark, copia el código a su nodo,
carga Open Food Facts, genera la muestra y ejecuta las comprobaciones.
Se detiene si ya existe `hadoop-master`, incluso apagado. No elimina recursos ni
reanuda automáticamente un despliegue fallido.

[DEPLOYMENT.md](DEPLOYMENT.md) conserva la explicación y los comandos de cada paso.

## Uso diario del clúster

Encender y apagar las instancias desde la consola de AWS. Para copiar cambios:

```bash
./scripts/sync.sh all
```

## Copiar y ejecutar el código

El código se organiza según el nodo donde se ejecutará:

| Carpeta local | Nodo de destino | Carpeta remota |
|---|---|---|
| `src/cleaning/` | Master: limpieza de datos | `~/BigData-OpenFoodFacts/src/cleaning/` |
| `src/analysis/` | Cliente: desarrollo de las preguntas | `~/BigData-OpenFoodFacts/src/analysis/` |

Para copiar cada parte por separado, o las dos a su destino correspondiente:

```bash
./scripts/sync.sh master  # Solo cleaning al master
./scripts/sync.sh client  # Solo analysis al cliente
./scripts/sync.sh all     # Las dos carpetas, cada una a su nodo
```

La carpeta base se puede cambiar con `REMOTE_DIR`. Cada nodo recibe únicamente su
parte del código. Se conserva la estructura de carpetas y se añaden o actualizan
archivos; los borrados locales no se borran
automáticamente del nodo remoto. Se excluyen cachés Python, `.venv`, `.pem`, `.env*`,
CSV y Parquet. Los datos grandes permanecen en HDFS.

El script consulta las IP en cada ejecución. Se conecta directamente al master y
al cliente **a través del master**, por su IP privada. La clave permanece en tu
ordenador; una configuración SSH temporal aplica la misma clave a ambos saltos.
SSH puede pedir confirmar la huella de cada nodo en la primera conexión.

Después de `./scripts/sync.sh all`, probar la copia entrando en cada nodo:

```bash
# Desde tu ordenador, con la clave cargada en el agente
ssh -i ~/.ssh/vockey.pem ec2-user@<MASTER_PUBLIC_IP>
# O bien al cliente, pasando por el master:
ssh -J ec2-user@<MASTER_PUBLIC_IP> -i ~/.ssh/vockey.pem ec2-user@<CLIENT_PRIVATE_IP>
```

Ya en el master, el código de limpieza estará en:

```bash
source ~/.bashrc
cd ~/BigData-OpenFoodFacts
ls src/cleaning/

# Cuando exista el trabajo de limpieza:
# spark-submit --master yarn --deploy-mode cluster src/cleaning/clean_data.py
```

En el cliente, el código de las preguntas estará en:

```bash
source ~/.bashrc
cd ~/BigData-OpenFoodFacts
ls src/analysis/

# Cuando exista un trabajo de análisis:
# spark-submit --master yarn --deploy-mode cluster src/analysis/mi_pregunta.py
```

Consultar los logs desde el master con `yarn logs -applicationId <APPLICATION_ID>`.

Los archivos `.py` se irán añadiendo a estas dos carpetas. Para PySpark, los módulos compartidos que
necesiten los executors se distribuirán también con `--py-files` cuando se incorporen,
como explica la [documentación de spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html).

## Cómo están organizados los scripts

```text
scripts/
  cluster.sh                 # Reconstruir el clúster con deploy
  sync.sh                    # Copiar cleaning al master y analysis al cliente
ansible/
  deploy.yml                 # Preparación, configuración remota y datos
config/
  cluster.env.example        # Ejemplo documentado; copiar a cluster.env
src/
  cleaning/                  # Limpieza de datos: se copia al master
  analysis/                  # Preguntas: se copia al cliente
```

Los scripts tienen comentarios breves y funciones pequeñas.
`cluster.sh` prepara Ansible y ejecuta el despliegue de clase junto con
`ansible/deploy.yml`. La caché queda en `.deployment/`, excluida de Git.
`sync.sh` copia a cada nodo su carpeta.
Los scripts pueden invocarse desde otra carpeta y siempre localizan el proyecto
a partir de su propia ubicación.

Si el cliente ya se creó con `hadoop-master-sg`, también recibirá la regla Anywhere
de ese grupo. Para utilizar el acceso solo por el master, dejar el cliente asociado
a `hadoop-sg`, tal como indica la [guía de despliegue](DEPLOYMENT.md).

## Errores habituales

| Mensaje o situación | Qué revisar |
|---|---|
| Falta `config/cluster.env` | Copiar el `.example` como indica el uso rápido |
| `ExpiredToken` o credenciales inválidas | Renovar la sesión del laboratorio y `~/.aws/credentials` |
| No se encuentran nodos | Región, perfil y etiquetas Name |
| Hay varios nodos con el mismo nombre | Corregir las etiquetas |
| `hadoop-master ya existe` | `deploy` sirve para un clúster nuevo; gestionar el existente desde AWS |
| Falla una etapa del despliegue | Revisar la salida y el paso equivalente en DEPLOYMENT.md; los recursos se conservan |
| SSH no conecta | IP pública del master, regla TCP 22, clave y estado del nodo |
| No se puede sincronizar con el cliente | Deben estar running master y cliente, compartir la red del clúster y permitir tráfico interno |
| Falta rsync en un nodo | Instalarlo con `sudo yum install -y rsync` |

## Documentación del dataset

Responsable inicial: Mauro (persona D).

### Checklist

- [x] [#14](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/14) Plantillas .docx del profesor · 30 oct
- [x] [#24](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/24) Borrador del dataset · 2 nov

### Comandos

La descripción del dataset se ha elaborado utilizando el archivo completo de Open Food Facts y los resultados de exploración documentados en el issue [#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17).

### Características verificadas

- Registros del dataset completo: 4.879.512 productos.
- Número de columnas: 211.
- Muestra aleatoria: 100.000 productos.
- Formato: CSV delimitado por tabuladores (TSV).
- Tamaño aproximado del archivo descomprimido: 13 GB.
- Ubicación en HDFS: /data/raw/openfoodfacts.csv.
- Muestra compartida: muestra.csv.gz.

## Preguntas candidatas

Propuestas iniciales de todo el equipo.

### Checklist

- [ ] [#61](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/61) Proponer 4 preguntas candidatas por persona · domingo 1 nov, 20:00
- [ ] Explorar la muestra y empezar la parte genérica de la limpieza (tipos, duplicados, valores imposibles)

### Comandos

_Pendiente de añadir entre todos._

#### Candidatas de Katherine (4 de 4)

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

### Candidatas de Alberto

**1. Marcas con mejor y peor perfil nutricional dentro de cada grupo de alimentos**
- **Enunciado:** dentro de cada grupo PNNS (`pnns_groups_1`), ¿qué 5 marcas con al menos N productos tienen la media más baja y la más alta de azúcar, grasa saturada y sal?
- **Objetivo:** clasificar las marcas por su perfil nutricional dentro de su propio grupo, para no comparar bebidas con aceites.
- **Utilidad:** ayuda a consumidores y a distribuidores a identificar qué marcas de cada categoría tienen mejores productos nutricionalmente y cuáles destacan por lo contrario.
- **Columnas:** `brands`, `pnns_groups_1`, `sugars_100g`, `saturated-fat_100g`, `salt_100g`.
- **Operaciones de Spark:** `split` + `explode` (un producto puede tener varias marcas), `groupBy`/`agg` (`avg`, `count`), `filter` por mínimo de productos, ventana `row_number()` con `Window.partitionBy(...).orderBy(...)`.
- **Dificultad:** 3.
- **Riesgo de datos:** `brands` 35,9 % nulos; `sugars` 33 %, `saturated-fat` 34,6 %, `salt` 37,5 %. `pnns_groups_1` contiene `unknown` (por limpiar).

**2. Evolución anual de la cantidad y calidad de los datos nutricionales**
- **Enunciado:** por año de alta del producto (`created_t`), ¿cuántos productos se añadieron, qué porcentaje tiene energía y macronutrientes informados, y cómo crece cada año respecto al anterior?
- **Objetivo:** medir cómo ha mejorado la completitud de Open Food Facts con el tiempo.
- **Utilidad:** decide de qué años se pueden fiar las demás preguntas, y justifica filtros de limpieza.
- **Columnas:** `created_t`, `energy-kcal_100g`, `fat_100g`, `carbohydrates_100g`, `proteins_100g`.
- **Operaciones de Spark:** `from_unixtime`/`year`, `groupBy`/`agg` con `avg(when(col.isNotNull(),1).otherwise(0))`, ventana con `lag` para la variación interanual.
- **Dificultad:** 2.
- **Riesgo de datos:** `created_t` 0 % nulos; la energía y los macros tienen 28-29 % de nulos, que es justamente la métrica medida.

**3. Coherencia entre la energía declarada y la calculada, por país**
- **Enunciado:** ¿qué porcentaje de productos tiene una energía declarada que difiere más de un 20 % de la calculada con `4·carbohidratos + 4·proteínas + 9·grasa`, y cómo varía según el país?
- **Objetivo:** detectar etiquetados incoherentes o errores de entrada, y ver en qué países son más frecuentes.
- **Utilidad:** distingue qué mercados tienen datos fiables y alimenta las reglas de limpieza de valores imposibles.
- **Columnas:** `energy-kcal_100g`, `carbohydrates_100g`, `proteins_100g`, `fat_100g`, `countries_tags`.
- **Operaciones de Spark:** `withColumn` con expresión aritmética, `when`/`otherwise`, `explode(split(...))` sobre países, `groupBy`/`agg`, filtro por mínimo de productos por país.
- **Dificultad:** 2.
- **Riesgo de datos:** `energy` 28 %, `carbohydrates` 28,7 %, `proteins` 28,6 %, `fat` 28,7 % nulos; hay que medir cuántas filas tienen las cuatro a la vez. `countries_tags` 0,4 % nulos.

### Candidatas de Jorge

**1. Productos que cumplen las alegaciones "fuente de / alto en proteínas y fibra" (Reglamento UE 1924/2006)**
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

### Propuestas ideadas en clase en grupo


**3. Aditivos más frecuentes y su presencia por país**
- **Enunciado:** ¿cuáles son los 10 aditivos (E-números) más usados en los productos, y en qué países aparecen proporcionalmente más?
- **Objetivo:** pasar del número de aditivos de cada producto a saber cuáles son y cómo cambia su uso entre mercados.
- **Utilidad:** permite ver diferencias de regulación o de hábitos de fabricación entre países (un aditivo frecuente en un país y casi ausente en otro).
- **Columnas:** `additives_tags` (lista tipo `en:e330,en:e322i`), `countries_tags`.
- **Operaciones de Spark:** doble `explode(split(...))` (aditivos × países), `groupBy`/`count`, normalizar por el total de productos con aditivos informados de cada país, `filter` por mínimo de productos por país, ventana `row_number()` con `Window.partitionBy("pais")` para el top N.
- **Dificultad:** 3. El doble `explode` multiplica las filas, así que conviene `repartition` y vigilar la memoria de los executors en YARN.
- **Riesgo de datos:** `additives_tags` no está en la tabla de columnas de la muestra; hay que confirmar su nombre con `grep -w "additives_tags" columnas_relleno.tsv`. Se espera un % de nulos parecido al de `additives_n` (72 %), y hay que distinguir "sin aditivos" de "no informado".

**6. Mejores categorías como fuente de hierro, calcio y vitamina C**
- **Enunciado:** ¿qué categorías aportan más hierro, calcio y vitamina C por 100 g y por 100 kcal, y qué proporción de sus productos informa esos nutrientes?
- **Objetivo:** identificar dónde están las fuentes alimentarias de esos micronutrientes y hasta qué punto se dispone de datos.
- **Utilidad:** ayuda a quien necesita más hierro, calcio o vitamina C (por ejemplo, dietas veganas) a ver qué categorías de producto envasado le aportan más.
- **Columnas:** `iron_100g`, `calcium_100g`, `vitamin-c_100g`, `energy-kcal_100g`, `categories_tags`.
- **Operaciones de Spark:** `explode` de categorías, `withColumn` para el valor por 100 kcal, `groupBy`/`agg` (`avg`, `percentile_approx`, `count`, porcentaje de no nulos), filtro por mínimo de productos con dato y `rank` con ventana por nutriente.
- **Dificultad:** 2.
- **Riesgo de datos:** alto; hierro 84 %, calcio 84,2 % y vitamina C 88,8 % de nulos. Solo se interpreta sobre los productos que informan el nutriente, y el porcentaje de cobertura forma parte de la respuesta.

**7. Reparto de la energía entre grasas, carbohidratos y proteínas frente a los rangos recomendados**
- **Enunciado:** ¿qué porcentaje de los productos de cada categoría reparte su energía dentro de los rangos de referencia (grasa 20-35 % y carbohidratos 45-60 % de la energía, según EFSA/OMS), y cuáles se desvían más?
- **Objetivo:** medir el equilibrio de macronutrientes de cada categoría respecto a recomendaciones oficiales, sin inventar umbrales.
- **Utilidad:** muestra qué categorías, por su composición, empujan la dieta hacia el exceso de grasa o de carbohidratos.
- **Columnas:** `fat_100g`, `carbohydrates_100g`, `proteins_100g`, `energy-kcal_100g`, `categories_tags`.
- **Operaciones de Spark:** `withColumn` para el % de energía de cada macronutriente (`9·grasa`, `4·carbohidratos`, `4·proteínas`, sobre la suma), filtro de filas incoherentes, `when`/`otherwise` para "dentro/fuera de rango", `explode` de categorías, `groupBy`/`agg`.
- **Dificultad:** 2.
- **Riesgo de datos:** `energy` 28 %, `fat` 28,7 %, `carbohydrates` 28,7 %, `proteins` 28,6 % nulos; hay que medir las filas con los cuatro a la vez. Hay que confirmar la fuente exacta de los rangos antes de publicar.

## Organización inicial del equipo

Reparto y fechas de la Semana 0 (27 de octubre a 2 de noviembre):

- **Katherine:** instancias, red y SSH ([#16](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/16), 30 oct) · manual de red (2 nov) · también instala Hadoop en los nodos, porque el playbook de Ansible ya lo hace al crear las instancias
- **Alberto:** muestra y columnas ([#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17), 30 oct)
- **Jorge:** candidatas de herramienta extra ([#15](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/15), 1 nov)
- **Mauro:** plantillas ([#14](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/14), 30 oct) · borrador del dataset ([#24](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/24), 2 nov)
- **Todos:** 4 preguntas candidatas ([#61](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/61), domingo 1 nov, 20:00) · explorar la muestra y empezar la parte genérica de la limpieza (tipos, duplicados, valores imposibles)
