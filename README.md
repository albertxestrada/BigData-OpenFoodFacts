# BigData-OpenFoodFacts

Análisis de la información nutricional de **Open Food Facts** con **Spark sobre un clúster Hadoop (HDFS + YARN) en AWS Academy**. Trabajo en grupo de la asignatura ATBD.

> Contains data from Open Food Facts, available under the [Open Database License](https://opendatacommons.org/licenses/odbl/1.0/). Fuente: <https://world.openfoodfacts.org/data>

## TL;DR

| | |
| --- | --- |
| **Qué** | 10 preguntas analíticas (más 2 opcionales) sobre el CSV de productos de Open Food Facts, con la API DataFrame de Spark |
| **Dónde** | Clúster en AWS Academy: 1 master, 3 workers y 1 client (5 `t2.large` en us-east-1) |
| **Datos** | CSV de unos 0,9 GB comprimido y 9 GB descomprimido, separado por tabuladores. Resultados guardados en HDFS |
| **Entrega y defensa** | **25 de noviembre de 2026, 08:00** (pendiente de confirmar por escrito con el profesor, ver [#13](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/13)) |
| **Código congelado** | 22 de noviembre |
| **Equipo** | 4 personas (A, B, C y D). Cada una toca todas las capas: clúster, limpieza, preguntas y documento |
| **Seguimiento** | [Issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues) con etiqueta de persona y [hitos](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestones) con fecha |

## Hitos de un vistazo

Marcad la casilla cuando se cumpla el hito completo (todos sus issues cerrados).

- [ ] **M1 · Requisitos confirmados** — 29 oct · [3 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/1)
- [ ] **M2 · Muestra y red listas** — 30 oct · [2 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/2)
- [ ] **M3 · Clúster operativo** — 6 nov · [7 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/3)
- [ ] **M4 · Parquet limpio en HDFS** — 11 nov · [6 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/4)
- [ ] **M5 · Reajuste de carga** — 13 nov · [1 issue](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/5)
- [ ] **M6 · Analíticas ejecutadas** — 18 nov · [12 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/6)
- [ ] **M7 · Gráficas y borrador** — 20 nov · [6 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/7)
- [ ] **M8 · Código congelado** — 22 nov · [6 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/8)
- [ ] **M9 · Ensayos y entrega** — 24 nov · [2 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/9)

## Detalle por hito

El responsable de cada tarea va entre paréntesis.

### M1 · Requisitos confirmados (29 oct)
- [ ] [#13](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/13) Confirmar con el profesor fechas, qué se entrega y grupo de 4 (todos)
- [ ] [#14](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/14) Conseguir las plantillas .docx (D)
- [ ] [#15](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/15) Elegir la herramienta extra del Tema 6 (C)

### M2 · Muestra y red listas (30 oct)
- [ ] [#16](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/16) Lanzar las 5 instancias EC2: red, SSH y `/etc/hosts` (A)
- [ ] [#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17) Muestra del CSV y columnas que se usarán (B)

### M3 · Clúster operativo (6 nov)
- [ ] [#18](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/18) Instalar Hadoop y configurar HDFS (B)
- [ ] [#19](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/19) Configurar YARN (C)
- [ ] [#20](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/20) Instalar Spark en el client y ejecutar SparkPi en YARN (D)
- [ ] [#21](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/21) Scripts de arranque y parada del clúster (A)
- [ ] [#22](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/22) CSV completo descomprimido y subido a HDFS (B)
- [ ] [#23](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/23) Mini-manuales y sesión de enseñanza cruzada, 6 nov (todos)
- [ ] [#24](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/24) Borrador de la descripción del dataset y de las preguntas (D)

### M4 · Parquet limpio en HDFS (11 nov)
- [ ] [#25](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/25) Limpieza: energía y macronutrientes (A)
- [ ] [#26](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/26) Limpieza: sal, sodio y tamaño de ración (B)
- [ ] [#27](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/27) Limpieza: tags de categorías, países y etiquetas (C)
- [ ] [#28](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/28) Limpieza: fechas, duplicados y bandera de datos nutricionales (D)
- [ ] [#29](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/29) Limpieza completa y Parquet publicado en HDFS (D)
- [ ] [#30](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/30) Prueba de tolerancia a fallos: apagar un worker (D)

### M5 · Reajuste de carga (13 nov)
- [ ] [#31](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/31) Decidir si se hacen Q11 y Q12 (todos)

### M6 · Analíticas ejecutadas (18 nov)
- [ ] [#32](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/32) Q01 · Coherencia energética (C)
- [ ] [#33](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/33) Q02 · Consistencia entre sal y sodio (A)
- [ ] [#34](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/34) Q03 · Variabilidad dentro de cada categoría (B)
- [ ] [#35](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/35) Q04 · Efecto halo de las etiquetas (A)
- [ ] [#36](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/36) Q05 · Grupo NOVA frente a aditivos, fibra y proteína (B)
- [ ] [#37](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/37) Q06 · Ratio azúcar/fibra por categoría y país (A)
- [ ] [#38](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/38) Q07 · Perfil nutricional por marca (D)
- [ ] [#39](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/39) Q08 · Calidad de datos por país y año (C)
- [ ] [#40](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/40) Q09 · Vitaminas y minerales frente a azúcar (D)
- [ ] [#41](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/41) Q10 · Ración real frente a 100 g (B)
- [ ] [#42](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/42) Q11 (opcional) · Clusters nutricionales con MLlib (C)
- [ ] [#43](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/43) Q12 (opcional) · Aditivos y aceite de palma (D)

### M7 · Gráficas y borrador (20 nov)
- [ ] [#44](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/44) Gráficas y sección del documento de Q2, Q4 y Q6 (A)
- [ ] [#45](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/45) Gráficas y sección del documento de Q3, Q5 y Q10 (B)
- [ ] [#46](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/46) Gráficas y sección del documento de Q1 y Q8 (C)
- [ ] [#47](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/47) Gráficas y sección del documento de Q7 y Q9 (D)
- [ ] [#48](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/48) Integrar la herramienta extra con los resultados (C)
- [ ] [#49](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/49) Integrar las secciones del documento (D)

### M8 · Código congelado (22 nov)
- [ ] [#50](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/50) Revisión cruzada: A revisa el código de B
- [ ] [#51](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/51) Revisión cruzada: B revisa el código de C
- [ ] [#52](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/52) Revisión cruzada: C revisa el código de D
- [ ] [#53](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/53) Revisión cruzada: D revisa el código de A
- [ ] [#54](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/54) Ejecutar trabajos ajenos y completar la matriz de conocimiento (todos)
- [ ] [#55](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/55) Congelar el repositorio con la etiqueta `entrega` (D)

### M9 · Ensayos y entrega (24 nov)
- [ ] [#56](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/56) Ensayos de defensa, 23 y 24 nov (todos)
- [ ] [#57](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/57) Subir la entrega a ALUD antes de las 20:00 (D)

## Reparto

| | A | B | C | D |
| --- | --- | --- | --- | --- |
| Nombre | (completar) | (completar) | (completar) | (completar) |
| Clúster | EC2, red y SSH | Hadoop y HDFS | YARN | Spark y tolerancia a fallos |
| Limpieza | Energía y macronutrientes | Sal, sodio y ración | Tags | Fechas, duplicados y Parquet |
| Preguntas | Q2, Q4, Q6 | Q3, Q5, Q10 | Q1, Q8 (Q11 opcional) | Q7, Q9 (Q12 opcional) |
| Transversal | Scripts de arranque | Descarga a HDFS | Herramienta extra y gráficas | Repositorio y documento |
| Su código lo revisa | D | A | B | C |

## Cómo se trabaja

1. Cada tarea es un issue; se trabaja en una rama propia y se abre un pull request que lo cierra.
2. Lo revisa la persona de la fila "Su código lo revisa". Quien revisa tiene que poder explicar ese código en la defensa.
3. Probar siempre primero con una muestra y después con el dataset completo en el clúster.
4. Nunca se suben claves `.pem`, credenciales de AWS ni datos: el CSV y el Parquet viven en HDFS.
5. Apagar el clúster al terminar cada sesión para no gastar crédito de AWS Academy.
