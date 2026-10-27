# BigData-OpenFoodFacts

Análisis de la información nutricional de **Open Food Facts** con **Spark sobre un clúster Hadoop (HDFS + YARN) en AWS Academy**. Trabajo en grupo de la asignatura ATBD.

> Contains data from Open Food Facts, available under the [Open Database License](https://opendatacommons.org/licenses/odbl/1.0/). Fuente: <https://world.openfoodfacts.org/data>

## TL;DR

| | |
| --- | --- |
| **Qué** | 10 preguntas analíticas (más 2 de reserva) sobre el CSV de productos de Open Food Facts, con la API DataFrame de Spark. **Las preguntas están por definir** (ver [#61](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/61) y [#62](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/62)) |
| **Dónde** | Clúster en AWS Academy: 1 master, 3 workers y 1 client (5 `t2.large` en us-east-1) |
| **Datos** | CSV de unos 0,9 GB comprimido y 9 GB descomprimido, separado por tabuladores. Los resultados se guardan en HDFS |
| **Reuniones** | Todos los **martes de 11:30 a 12:30**, desde el 3 de noviembre: 3, 10, 17 y 24 de noviembre |
| **Entrega y defensa** | **25 de noviembre de 2026, 08:00** (subir a ALUD antes del 24 a las 20:00) |
| **Código congelado** | 22 de noviembre |
| **Equipo** | Katherine (A), Alberto (B), Jorge (C) y Mauro (D). Cada uno toca todas las capas: clúster, limpieza, preguntas y documento |
| **Seguimiento** | [Issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues) con etiqueta de persona y [hitos](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestones) con fecha |

## Qué es cada fila del reparto

| Fila | Qué significa |
| --- | --- |
| **Clúster** | Montar la infraestructura: instancias en AWS, HDFS (donde viven los datos), YARN (reparte CPU y memoria) y Spark (hace los cálculos). Sin esto no hay dónde ejecutar nada |
| **Limpieza de datos** | El CSV lo rellenan usuarios, así que trae valores imposibles (100 g de grasa en 30 g de producto), textos libres (`1 cup (240 ml)`), campos con listas separadas por comas y productos repetidos. La limpieza es un trabajo de Spark que lo arregla y guarda un **Parquet fiable** que leen las 10 preguntas. Se parte en cuatro trozos, uno por persona, y Mauro los une |
| **Preguntas** | Las analíticas del enunciado: cada una es un programa de Spark que lee el Parquet limpio, agrupa o calcula algo y guarda una tabla pequeña en HDFS |
| **Transversal** | Tareas que no pertenecen a una capa, pero de las que depende el resto: scripts para arrancar el clúster, subir el CSV a HDFS, herramienta extra y gráficas, repositorio y documento, y el contrato de columnas que mantiene encajadas las cuatro partes de la limpieza |
| **Revisa el código de** | Cadena de revisión cruzada: todos tienen que poder explicar el código de otra persona en la defensa |

## Equipo

| | **A · Katherine** | **B · Alberto** | **C · Jorge** | **D · Mauro** |
| --- | --- | --- | --- | --- |
| **Clúster** | EC2, red y SSH | Hadoop y HDFS | YARN | Spark y tolerancia a fallos |
| **Limpieza de datos** | Energía y macronutrientes | Sal, sodio y ración | Tags de categorías, países, etiquetas y marca | Fechas, duplicados, NOVA y bandera de datos nutricionales |
| **Preguntas** | Q2, Q4, Q6 | Q3, Q5, Q10 | Q1, Q8 (Q11 opcional) | Q7, Q9 (Q12 opcional) |
| **Transversal** | Scripts de arranque y parada | Muestra y CSV completo a HDFS | Herramienta extra y gráficas | Repositorio, documento, contrato de columnas y Parquet |
| **Revisa el código de** | Alberto | Jorge | Mauro | Katherine |

> El reparto de preguntas es provisional hasta la reunión del 3 de noviembre ([#62](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/62)).

## Arquitectura del clúster

```mermaid
flowchart LR
    subgraph AWS["AWS Academy · us-east-1 · 5 instancias t2.large"]
        direction TB
        CL["client<br/>Spark · spark-submit"]
        MA["master<br/>NameNode · ResourceManager"]
        W1["worker1<br/>DataNode · NodeManager"]
        W2["worker2<br/>DataNode · NodeManager"]
        W3["worker3<br/>DataNode · NodeManager"]
        CL -->|"lanza trabajos en YARN"| MA
        MA --- W1
        MA --- W2
        MA --- W3
    end
    HDFS[("HDFS<br/>replicación 3")]
    W1 --- HDFS
    W2 --- HDFS
    W3 --- HDFS
```

| Pieza | Responsable |
| --- | --- |
| Instancias, red, SSH y `/etc/hosts` | Katherine |
| Hadoop y HDFS | Alberto |
| YARN | Jorge |
| Spark en el client y prueba de tolerancia a fallos | Mauro |

## Flujo de datos

```mermaid
flowchart LR
    OFF[("Open Food Facts<br/>CSV de 9 GB")] --> RAW["HDFS<br/>/data/raw"]
    RAW --> LIMP{{"Limpieza en 4 partes<br/>Katherine · Alberto · Jorge · Mauro"}}
    CON["Contrato de columnas<br/>Mauro"] -.-> LIMP
    LIMP --> PQ[("HDFS /data/clean<br/>Parquet")]
    PQ --> Q["10 preguntas con Spark<br/>+ 2 de reserva"]
    Q --> RES["HDFS /results<br/>CSV agregados"]
    RES --> GR["Gráficas<br/>+ herramienta extra"]
    GR --> DOC["Documento<br/>y defensa"]
```

## Calendario por semanas

Se reparte el trabajo para ir avanzando poco a poco: la semana del 27 de octubre sirve para arrancar en paralelo (red, muestra, preguntas candidatas y diseño de la limpieza con la muestra), y las preguntas se ejecutan en tres tandas en vez de todas al final.

| Semana | Foco | Reunión del martes | Se cierra |
| --- | --- | --- | --- |
| **0 · 27 oct – 2 nov** | Arrancar en paralelo: red, muestra, preguntas candidatas, diseño de la limpieza | (no hay) | M2 (30 oct) |
| **1 · 3 – 9 nov** | Clúster operativo y limpieza con la muestra | **3 nov** · [#63](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/63): definir preguntas, herramienta extra y contrato | M1 (4 nov), M3 (6 nov) |
| **2 · 10 – 16 nov** | Parquet completo y preguntas en tandas | **10 nov** · [#64](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/64): enseñanza cruzada y reajuste de carga | M4 (10 nov), M5 (13 nov) |
| **3 · 17 – 23 nov** | Resultados, gráficas, documento y revisión | **17 nov** · [#65](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/65): resultados y documento | M6 (18 nov), M7 (20 nov), M8 (22 nov) |
| **4 · 24 – 25 nov** | Ensayo final y entrega | **24 nov** · [#66](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/66): ensayo y logística | M9 (24 nov) |

Las preguntas se ejecutan en tres tandas, con sus gráficas empezando en cuanto hay resultados:

| Tanda | Preguntas | Código listo | Ejecución en el clúster |
| --- | --- | --- | --- |
| **1** | Q01 (Jorge) · Q02 (Katherine) · Q03 (Alberto) · Q07 (Mauro) | 11 nov | 12–13 nov |
| **2** | Q04 (Katherine) · Q08 (Jorge) · Q09 (Mauro) · Q10 (Alberto) | 15 nov | 16 nov |
| **3** | Q05 (Alberto) · Q06 (Katherine) | 16 nov | 17 nov |
| **Reserva** | Q11 (Jorge) · Q12 (Mauro), si se aprueban en la reunión del 10 nov | 17 nov | 20 nov |

## Calendario con dependencias

Cada barra es una tarea de una persona. Las que usan `after` empiezan cuando termina la tarea de la que dependen, aunque sea de otra persona (por ejemplo, las preguntas empiezan cuando Mauro publica el Parquet de muestra).

```mermaid
gantt
    title De la red al Parquet y de ahí a las preguntas · 27 oct a 25 nov 2026
    dateFormat YYYY-MM-DD
    axisFormat %d/%m
    section Reuniones (martes 11:30)
    R1 preguntas y contrato :milestone, r1, 2026-11-03, 0d
    R2 enseñanza cruzada y reajuste :milestone, r2, 2026-11-10, 0d
    R3 resultados y documento :milestone, r3, 2026-11-17, 0d
    R4 ensayo final :milestone, r4, 2026-11-24, 0d
    section 1 Definición (M1)
    Mauro · profesor y plantillas :d0, 2026-10-27, 4d
    Todos · preguntas candidatas :q0, 2026-10-27, 6d
    Jorge · herramienta extra :c1, 2026-10-27, 8d
    Mauro · borrador del documento :d1, 2026-10-31, 7d
    Todos · definir las preguntas :q1, 2026-11-03, 1d
    Mauro · contrato de columnas :d3, after q1, 1d
    section 2 Clúster (M2 y M3)
    Katherine · instancias EC2 y red :a1, 2026-10-27, 4d
    Alberto · muestra y columnas :b1, 2026-10-27, 4d
    Katherine · manual de red :a2, 2026-10-31, 3d
    Alberto · instalar Hadoop :b2a, after a1, 3d
    Alberto · configurar y arrancar HDFS :b2b, after b2a, 2d
    Jorge · YARN :c2, after b2a, 3d
    Mauro · Spark en el client :d2a, 2026-11-03, 2d
    Mauro · SparkPi en YARN :d2b, after c2, 1d
    Alberto · CSV completo a HDFS :b3, after b2b, 2d
    Katherine · scripts de arranque y parada :a3, after b2b, 2d
    section 3 Limpieza y Parquet (M4)
    Katherine · energía y macros :a4, 2026-10-31, 8d
    Jorge · tags y marca :c3, 2026-11-02, 6d
    Alberto · sal y ración :b4, 2026-11-03, 5d
    Mauro · fechas y NOVA :d4, 2026-11-03, 5d
    Mauro · Parquet de muestra :d5, after a4 b4 c3 d4, 1d
    Mauro · Parquet completo :d6, after d5 b3, 2d
    Mauro · tolerancia a fallos :d8, after d6, 3d
    section 4 Preguntas, tanda 1 (M5)
    Katherine · Q02 sal y sodio :a5, after d5, 3d
    Alberto · Q03 variabilidad :b5, after d5, 3d
    Jorge · Q01 coherencia energética :c4, after d5, 3d
    Mauro · Q07 perfil por marca :d7, after d5, 3d
    Ejecución tanda 1 :e1, 2026-11-12, 2d
    section 5 Preguntas, tandas 2 y 3 (M6)
    Katherine · Q04 efecto halo :a6, after a5, 4d
    Alberto · Q10 ración frente a 100 g :b6, after b5, 4d
    Jorge · Q08 calidad de datos :c5, after c4, 4d
    Mauro · Q09 vitaminas y azúcar :d9, after d7, 4d
    Ejecución tanda 2 :e2, 2026-11-16, 1d
    Katherine · Q06 azúcar y fibra :a7, after a6, 2d
    Alberto · Q05 NOVA y aditivos :b7, after b6, 2d
    Ejecución tanda 3 :e3, 2026-11-17, 1d
    Jorge · Q11 clusters (reserva) :c6, after c5, 5d
    Mauro · Q12 aditivos y palma (reserva) :d10, after d9, 5d
    section 6 Gráficas y documento (M7)
    Katherine · gráficas y sección :a8, 2026-11-13, 8d
    Alberto · gráficas y sección :b8, 2026-11-13, 8d
    Jorge · gráficas y sección :c8, 2026-11-13, 8d
    Mauro · gráficas y sección :d12, 2026-11-13, 8d
    Jorge · integrar herramienta extra :c7, 2026-11-16, 5d
    Mauro · integrar el documento :d11, 2026-11-16, 5d
    section 7 Revisión y cierre (M8 y M9)
    Katherine revisa a Alberto :a9, 2026-11-11, 11d
    Alberto revisa a Jorge :b9, 2026-11-11, 11d
    Jorge revisa a Mauro :c9, 2026-11-11, 11d
    Mauro revisa a Katherine :d13, 2026-11-11, 11d
    Todos · matriz de conocimiento :f1, 2026-11-20, 3d
    Mauro · congelar el repositorio :d14, 2026-11-22, 1d
    Todos · primer ensayo :f2, 2026-11-23, 1d
    Todos · segundo ensayo y subida a ALUD :f3, 2026-11-24, 1d
    Entrega y defensa :milestone, 2026-11-25, 0d
```

## Ruta crítica: quién desbloquea a quién

Cada color es una persona. Una flecha significa «esto tiene que estar hecho para que empiece lo siguiente». La ruta más larga va de las instancias de Katherine al Parquet de Mauro y de ahí a todas las preguntas.

```mermaid
flowchart LR
    subgraph CLU["Clúster"]
        T1["#16 Instancias, red y SSH<br/>Katherine · 30 oct"]:::ka
        T2["#18 Hadoop y HDFS<br/>Alberto · 4 nov"]:::al
        T3["#19 YARN<br/>Jorge · 5 nov"]:::jo
        T4["#20 Spark y SparkPi<br/>Mauro · 6 nov"]:::ma
        T5["#22 CSV completo a HDFS<br/>Alberto · 6 nov"]:::al
        T6["#21 Scripts de arranque<br/>Katherine · 6 nov"]:::ka
    end
    subgraph DEF["Definición"]
        D1["#61 Candidatas<br/>Todos · 1 nov"]:::td
        D2["#62 Preguntas finales<br/>Todos · 3 nov"]:::td
        D3["#67 Contrato de columnas<br/>Mauro · 4 nov"]:::ma
    end
    subgraph LIM["Limpieza (en paralelo, 7 nov)"]
        L1["#25 Energía y macros<br/>Katherine"]:::ka
        L2["#26 Sal, sodio y ración<br/>Alberto"]:::al
        L3["#27 Tags y marca<br/>Jorge"]:::jo
        L4["#28 Fechas y NOVA<br/>Mauro"]:::ma
    end
    P1["#68 Parquet de muestra<br/>Mauro · 8 nov"]:::ma
    P2["#29 Parquet completo<br/>Mauro · 10 nov"]:::ma
    subgraph QS["Preguntas"]
        Q1["Tanda 1 · 13 nov<br/>Q01 Jorge · Q02 Katherine<br/>Q03 Alberto · Q07 Mauro"]:::td
        Q2["Tanda 2 · 16 nov<br/>Q04 Katherine · Q08 Jorge<br/>Q09 Mauro · Q10 Alberto"]:::td
        Q3["Tanda 3 · 17 nov<br/>Q05 Alberto · Q06 Katherine"]:::td
    end
    G["Gráficas y secciones<br/>20 nov"]:::td
    DOC["#49 Documento integrado<br/>Mauro · 20 nov"]:::ma
    FIN["#55 Código congelado<br/>22 nov"]:::ma
    END(["Entrega y defensa<br/>25 nov · 08:00"]):::td

    T1 --> T2
    T2 --> T3
    T3 --> T4
    T2 --> T5
    T2 --> T6
    T3 --> T6
    D1 --> D2
    D2 --> D3
    D3 --> L1
    D3 --> L2
    D3 --> L3
    D3 --> L4
    L1 --> P1
    L2 --> P1
    L3 --> P1
    L4 --> P1
    P1 --> Q1
    P1 --> P2
    T4 --> P2
    T5 --> P2
    P2 --> Q1
    Q1 --> Q2
    Q2 --> Q3
    Q1 --> G
    Q2 --> G
    Q3 --> G
    G --> DOC
    DOC --> FIN
    FIN --> END

    classDef ka fill:#dbeafe,stroke:#1d76db,color:#1f2328
    classDef al fill:#dcfce7,stroke:#0e8a16,color:#1f2328
    classDef jo fill:#ffedd5,stroke:#d93f0b,color:#1f2328
    classDef ma fill:#ede9fe,stroke:#8250df,color:#1f2328
    classDef td fill:#f6f8fa,stroke:#8c959f,color:#1f2328
```

Leyenda: azul = Katherine, verde = Alberto, naranja = Jorge, morado = Mauro, gris = todo el grupo.

## Quién necesita a quién

La flecha sale de quien entrega y llega a quien espera. Cada etiqueta dice qué entrega, en qué issue y cuándo.

```mermaid
flowchart LR
    KA(["Katherine<br/>EC2 · energía y macros"]):::ka
    AL(["Alberto<br/>HDFS · sal y ración"]):::al
    JO(["Jorge<br/>YARN · tags"]):::jo
    MA(["Mauro<br/>Spark · Parquet"]):::ma

    KA -->|"#16 instancias y SSH · 30 oct<br/>#25 macros limpios · 7 nov"| AL
    KA -->|"#16 instancias · 30 oct<br/>#25 macros limpios · 7 nov (Q01)"| JO
    KA -->|"#16 instancias · 30 oct<br/>#25 macros limpios · 7 nov (Q07, Q09)"| MA
    AL -->|"#26 sal y sodio · 7 nov (Q02)"| KA
    AL -->|"#17 muestra · 30 oct<br/>#18 Hadoop instalado · 2 nov<br/>#26 sal y ración · 7 nov (Q08)"| JO
    AL -->|"#17 muestra · 30 oct<br/>#22 CSV en HDFS · 6 nov"| MA
    JO -->|"#27 categoría, país y etiquetas · 7 nov<br/>(Q02, Q04, Q06)"| KA
    JO -->|"#27 categoría y marca · 7 nov<br/>(Q03, Q05, Q10)"| AL
    JO -->|"#19 YARN · 5 nov<br/>#27 marca y categoría · 7 nov (Q07, Q09)"| MA
    MA -->|"#67 contrato · 4 nov<br/>#68 Parquet de muestra · 8 nov<br/>#29 Parquet completo · 10 nov<br/>#28 año y NOVA (Q04, Q06)"| KA
    MA -->|"#67, #68 y #29<br/>#28 NOVA y aditivos (Q05)"| AL
    MA -->|"#67, #68 y #29<br/>#28 año y NOVA (Q01, Q08)"| JO

    classDef ka fill:#dbeafe,stroke:#1d76db,color:#1f2328
    classDef al fill:#dcfce7,stroke:#0e8a16,color:#1f2328
    classDef jo fill:#ffedd5,stroke:#d93f0b,color:#1f2328
    classDef ma fill:#ede9fe,stroke:#8250df,color:#1f2328
```

Si algo se retrasa, esta tabla dice a quién avisar:

| Persona | Necesita de otros | Otros necesitan de ella o él |
| --- | --- | --- |
| **Katherine** | De Mauro: contrato ([#67](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/67), 4 nov), Parquet de muestra ([#68](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/68), 8 nov) y completo ([#29](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/29), 10 nov). De Alberto y Jorge: limpieza de sal y sodio ([#26](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/26)) y de tags ([#27](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/27)) el 7 nov, para Q02, Q04 y Q06. De Alberto y Jorge: HDFS y YARN para los scripts ([#21](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/21)) | Instancias y red ([#16](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/16), 30 oct) para todos. Energía y macronutrientes limpios ([#25](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/25), 7 nov) para Q01, Q03, Q05, Q07, Q09 y Q10 |
| **Alberto** | De Katherine: instancias ([#16](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/16), 30 oct). De Mauro: contrato, Parquet de muestra y completo, y NOVA ([#28](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/28)). De Katherine y Jorge: macros ([#25](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/25)) y tags ([#27](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/27)) para Q03, Q05 y Q10 | Muestra ([#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17), 30 oct) para todos. HDFS instalado ([#18](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/18), 4 nov) para YARN y Spark. CSV en HDFS ([#22](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/22), 6 nov) para el Parquet completo. Sal, sodio y ración ([#26](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/26), 7 nov) para Q02, Q08 y Q10 |
| **Jorge** | De Alberto: Hadoop instalado en los nodos (paso de [#18](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/18), 2 nov) y muestra ([#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17)). De Mauro: contrato, Parquet y NOVA ([#28](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/28)). De Katherine y Alberto: macros ([#25](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/25)) y sal y ración ([#26](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/26)) para Q01 y Q08 | YARN ([#19](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/19), 5 nov) para SparkPi y los scripts. Tags, países, etiquetas y marca ([#27](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/27), 7 nov) para casi todas las preguntas |
| **Mauro** | De Katherine, Alberto y Jorge: las tres piezas de limpieza (7 nov) para el Parquet de muestra. De Alberto: CSV en HDFS (6 nov). De Jorge: YARN (5 nov) para SparkPi. De todos: preguntas definidas ([#62](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/62)) para el contrato, y gráficas y secciones (20 nov) para el documento | Contrato ([#67](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/67), 4 nov), Parquet de muestra ([#68](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/68), 8 nov), Parquet completo ([#29](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/29), 10 nov), plantillas ([#14](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/14), 30 oct) y comando de `spark-submit` ([#20](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/20), 6 nov) para todos |

## Qué limpieza necesita cada pregunta

Una cruz significa que la pregunta lee columnas que produce esa pieza de la limpieza. Si una pieza se retrasa, esta tabla dice qué preguntas se quedan sin poder empezar.

| Pregunta | Responsable | Tanda | Energía y macros ([#25](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/25)) | Sal y ración ([#26](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/26)) | Tags y marca ([#27](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/27)) | Fechas, NOVA y vitaminas ([#28](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/28)) |
| --- | --- | --- | :-: | :-: | :-: | :-: |
| Q01 | Jorge | 1 | ✔ | | ✔ | ✔ |
| Q02 | Katherine | 1 | | ✔ | ✔ | |
| Q03 | Alberto | 1 | ✔ | | ✔ | ✔ |
| Q04 | Katherine | 2 | ✔ | | ✔ | ✔ |
| Q05 | Alberto | 3 | ✔ | | ✔ | ✔ |
| Q06 | Katherine | 3 | ✔ | | ✔ | ✔ |
| Q07 | Mauro | 1 | ✔ | | ✔ | ✔ |
| Q08 | Jorge | 2 | | ✔ | ✔ | ✔ |
| Q09 | Mauro | 2 | ✔ | | ✔ | ✔ |
| Q10 | Alberto | 2 | ✔ | ✔ | ✔ | ✔ |
| Q11 (reserva) | Jorge | 4 | ✔ | ✔ | | ✔ |
| Q12 (reserva) | Mauro | 4 | | | ✔ | ✔ |

## Cómo se va avanzando

Sin clúster no hay datos limpios, y sin datos limpios no hay preguntas. M1 y M2 empiezan a la vez el 27 de octubre; M1 cierra el 4 de noviembre y M2 antes, el 30 de octubre. Para marcar un hito como cumplido, cambiad su línea `class` de `pendiente` a `hecho` al final del diagrama.

```mermaid
flowchart LR
    S(["27 oct · arranque"]) --> M1["M1 · Requisitos, preguntas<br/>y contrato · 4 nov"]
    S --> M2["M2 · Muestra y red<br/>30 oct"]
    M2 --> M3["M3 · Clúster operativo<br/>6 nov"]
    M3 --> M4["M4 · Parquet limpio<br/>10 nov"]
    M1 --> M4
    M4 --> M5["M5 · Primera tanda<br/>13 nov"]
    M5 --> M6["M6 · Analíticas ejecutadas<br/>18 nov"]
    M6 --> M7["M7 · Gráficas y borrador<br/>20 nov"]
    M7 --> M8["M8 · Código congelado<br/>22 nov"]
    M8 --> M9["M9 · Ensayos y entrega<br/>24 nov"]
    M9 --> FIN(["Entrega y defensa<br/>25 nov · 08:00"])
    R1(["R1 · 3 nov"]) -.-> M1
    R2(["R2 · 10 nov"]) -.-> M4
    R3(["R3 · 17 nov"]) -.-> M6
    R4(["R4 · 24 nov"]) -.-> M9

    classDef pendiente fill:#f6f8fa,stroke:#8c959f,color:#1f2328
    classDef hecho fill:#dafbe1,stroke:#1a7f37,color:#1f2328
    classDef reunion fill:#ddf4ff,stroke:#0969da,color:#1f2328
    class S pendiente
    class M1 pendiente
    class M2 pendiente
    class M3 pendiente
    class M4 pendiente
    class M5 pendiente
    class M6 pendiente
    class M7 pendiente
    class M8 pendiente
    class M9 pendiente
    class FIN pendiente
    class R1 reunion
    class R2 reunion
    class R3 reunion
    class R4 reunion
```

Casillas de los hitos (se marcan cuando todos sus issues están cerrados):


- [ ] **M1 · Requisitos, preguntas y contrato definidos** · 4 nov · [7 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/1)
- [ ] **M2 · Muestra y red listas** · 30 oct · [2 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/2)
- [ ] **M3 · Clúster operativo** · 6 nov · [6 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/3)
- [ ] **M4 · Parquet limpio en HDFS** · 10 nov · [9 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/4)
- [ ] **M5 · Primera tanda de preguntas ejecutada** · 13 nov · [5 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/5)
- [ ] **M6 · Analíticas ejecutadas** · 18 nov · [7 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/6)
- [ ] **M7 · Gráficas y borrador** · 20 nov · [8 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/7)
- [ ] **M8 · Código congelado** · 22 nov · [6 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/8)
- [ ] **M9 · Ensayos y entrega** · 24 nov · [3 issues](https://github.com/albertxestrada/BigData-OpenFoodFacts/milestone/9)

## Detalle por hito

El responsable de cada tarea va entre paréntesis y el número lleva al issue.

### M1 · Requisitos, preguntas y contrato definidos (4 nov)
- [ ] [#13](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/13) Confirmar con el profesor fechas, qué se entrega y grupo de 4 (Mauro redacta · Todos) · 30 oct
- [ ] [#14](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/14) Conseguir las plantillas .docx del profesor (Mauro) · 30 oct
- [ ] [#15](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/15) Elegir la herramienta extra del Tema 6 (Jorge) · 3 nov
- [ ] [#61](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/61) Proponer 4 preguntas candidatas por persona (Todos) · 1 nov
- [ ] [#62](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/62) Definir las 10 preguntas finales y su reparto (Todos) · 3 nov
- [ ] [#63](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/63) Reunión 1 · martes 3 nov, 11:30–12:30 (Todos) · 3 nov
- [ ] [#67](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/67) Acordar el contrato de columnas del Parquet (Mauro) · 4 nov

### M2 · Muestra y red listas (30 oct)
- [ ] [#16](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/16) Lanzar las 5 instancias EC2: red, SSH y `/etc/hosts` (Katherine) · 30 oct (manual de red 2 nov)
- [ ] [#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17) Muestra del CSV y columnas que se usarán (Alberto) · 30 oct

### M3 · Clúster operativo (6 nov)
- [ ] [#18](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/18) Instalar Hadoop y configurar HDFS (Alberto) · 4 nov
- [ ] [#19](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/19) Configurar YARN (Jorge) · 5 nov
- [ ] [#20](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/20) Instalar Spark en el client y ejecutar SparkPi en YARN (Mauro) · 6 nov
- [ ] [#21](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/21) Scripts de arranque y parada del clúster (Katherine) · 6 nov
- [ ] [#22](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/22) CSV completo descomprimido y subido a HDFS (Alberto) · 6 nov
- [ ] [#24](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/24) Borrador de la descripción del dataset y de las preguntas (Mauro) · 2 nov (dataset) · 5 nov (preguntas) · 6 nov (revisado)

### M4 · Parquet limpio en HDFS (10 nov)
- [ ] [#25](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/25) Limpieza: energía y macronutrientes (Katherine) · 7 nov
- [ ] [#26](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/26) Limpieza: sal, sodio y tamaño de ración (Alberto) · 7 nov
- [ ] [#27](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/27) Limpieza: tags de categorías, países y etiquetas (Jorge) · 7 nov
- [ ] [#28](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/28) Limpieza: fechas, duplicados, NOVA y bandera de datos nutricionales (Mauro) · 7 nov
- [ ] [#68](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/68) Parquet de muestra para desarrollar las preguntas (Mauro) · 8 nov
- [ ] [#29](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/29) Limpieza completa y Parquet publicado en HDFS (Mauro) · 10 nov
- [ ] [#23](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/23) Mini-manuales y enseñanza cruzada (Todos) · 10 nov (manuales 6 nov)
- [ ] [#31](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/31) Reajuste de carga: decidir Q11 y Q12 (Todos) · 10 nov
- [ ] [#64](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/64) Reunión 2 · martes 10 nov, 11:30–12:30 (Todos) · 10 nov

### M5 · Primera tanda de preguntas ejecutada (13 nov)
- [ ] [#33](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/33) Q02 · Consistencia entre sal y sodio (Katherine) · código 11 nov · ejecución 13 nov
- [ ] [#34](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/34) Q03 · Variabilidad dentro de cada categoría (Alberto) · código 11 nov · ejecución 13 nov
- [ ] [#32](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/32) Q01 · Coherencia energética (Jorge) · código 11 nov · ejecución 13 nov
- [ ] [#38](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/38) Q07 · Perfil nutricional por marca (Mauro) · código 11 nov · ejecución 13 nov
- [ ] [#30](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/30) Prueba de tolerancia a fallos: apagar un worker (Mauro) · 13 nov

### M6 · Analíticas ejecutadas (18 nov)
- [ ] [#35](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/35) Q04 · Efecto halo de las etiquetas (Katherine) · código 15 nov · ejecución 16 nov
- [ ] [#41](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/41) Q10 · Ración real frente a 100 g (Alberto) · código 15 nov · ejecución 16 nov
- [ ] [#39](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/39) Q08 · Calidad de datos por país y año (Jorge) · código 15 nov · ejecución 16 nov
- [ ] [#40](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/40) Q09 · Vitaminas y minerales frente a azúcar (Mauro) · código 15 nov · ejecución 16 nov
- [ ] [#37](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/37) Q06 · Ratio azúcar/fibra por categoría y país (Katherine) · código 16 nov · ejecución 17 nov
- [ ] [#36](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/36) Q05 · Grupo NOVA frente a aditivos, fibra y proteína (Alberto) · código 16 nov · ejecución 17 nov
- [ ] [#65](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/65) Reunión 3 · martes 17 nov, 11:30–12:30 (Todos) · 17 nov

### M7 · Gráficas y borrador (20 nov)
- [ ] [#44](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/44) Gráficas y sección del documento de Q2, Q4 y Q6 (Katherine) · empezar el 13 nov · entregar el 20 nov
- [ ] [#45](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/45) Gráficas y sección del documento de Q3, Q5 y Q10 (Alberto) · empezar el 13 nov · entregar el 20 nov
- [ ] [#46](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/46) Gráficas y sección del documento de Q1 y Q8 (Jorge) · empezar el 13 nov · entregar el 20 nov
- [ ] [#47](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/47) Gráficas y sección del documento de Q7 y Q9 (Mauro) · empezar el 13 nov · entregar el 20 nov
- [ ] [#48](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/48) Integrar la herramienta extra con los resultados (Jorge) · 20 nov
- [ ] [#49](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/49) Integrar las secciones del documento (Mauro) · 20 nov
- [ ] [#42](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/42) Q11 (opcional) · Clusters nutricionales con MLlib (Jorge) · código 17 nov · ejecución 20 nov
- [ ] [#43](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/43) Q12 (opcional) · Aditivos y aceite de palma (Mauro) · código 17 nov · ejecución 20 nov

### M8 · Código congelado (22 nov)
- [ ] [#50](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/50) Revisión cruzada: Katherine revisa el código de Alberto (Katherine) · 21 nov (cada pull request, en menos de 48 h)
- [ ] [#51](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/51) Revisión cruzada: Alberto revisa el código de Jorge (Alberto) · 21 nov (cada pull request, en menos de 48 h)
- [ ] [#52](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/52) Revisión cruzada: Jorge revisa el código de Mauro (Jorge) · 21 nov (cada pull request, en menos de 48 h)
- [ ] [#53](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/53) Revisión cruzada: Mauro revisa el código de Katherine (Mauro) · 21 nov (cada pull request, en menos de 48 h)
- [ ] [#54](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/54) Ejecutar trabajos ajenos y completar la matriz de conocimiento (Todos) · 22 nov
- [ ] [#55](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/55) Congelar el repositorio con la etiqueta `entrega` (Mauro) · 22 nov

### M9 · Ensayos y entrega (24 nov)
- [ ] [#56](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/56) Ensayos de defensa (Todos) · lun 23 y mar 24 nov
- [ ] [#57](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/57) Subir la entrega a ALUD antes de las 20:00 (Mauro) · 24 nov
- [ ] [#66](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/66) Reunión 4 · martes 24 nov, 11:30–12:30 (Todos) · 24 nov

## Las preguntas analíticas

> **Estado: por definir.** Las doce de la tabla son una propuesta inicial. Cada persona aporta cuatro candidatas ([#61](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/61)) y el grupo decide las definitivas el 3 de noviembre ([#62](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/62)). Las filas que cambien se reescriben, y los issues de las preguntas se mantienen, se reescriben o se cierran.

| Pregunta | Tema (propuesta) | Responsable | Tanda | Código listo | Ejecución | Issue |
| --- | --- | --- | :-: | --- | --- | --- |
| Q1 | Coherencia energética: kcal declaradas frente a las calculadas | Jorge | 1 | 11 nov | 13 nov | [#32](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/32) |
| Q2 | Consistencia entre sal y sodio | Katherine | 1 | 11 nov | 13 nov | [#33](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/33) |
| Q3 | Variabilidad de azúcar y grasas saturadas dentro de cada categoría | Alberto | 1 | 11 nov | 13 nov | [#34](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/34) |
| Q4 | Efecto halo de las etiquetas (bio, vegano, sin gluten) | Katherine | 2 | 15 nov | 16 nov | [#35](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/35) |
| Q5 | Grupo NOVA frente a aditivos, fibra y proteína | Alberto | 3 | 16 nov | 17 nov | [#36](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/36) |
| Q6 | Ratio azúcar/fibra por categoría y país | Katherine | 3 | 16 nov | 17 nov | [#37](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/37) |
| Q7 | Perfil nutricional por marca | Mauro | 1 | 11 nov | 13 nov | [#38](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/38) |
| Q8 | Calidad de datos por país y año | Jorge | 2 | 15 nov | 16 nov | [#39](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/39) |
| Q9 | Vitaminas y minerales declarados frente a azúcar | Mauro | 2 | 15 nov | 16 nov | [#40](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/40) |
| Q10 | Aporte por ración real frente a 100 g | Alberto | 2 | 15 nov | 16 nov | [#41](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/41) |
| Q11 (reserva) | Clusters nutricionales con MLlib | Jorge | 4 | 17 nov | 20 nov | [#42](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/42) |
| Q12 (reserva) | Aditivos y aceite de palma por NOVA y país | Mauro | 4 | 17 nov | 20 nov | [#43](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/43) |

## Qué hace cada persona cada semana

| | **Katherine (A)** | **Alberto (B)** | **Jorge (C)** | **Mauro (D)** |
| --- | --- | --- | --- | --- |
| **Semana 0**<br/>27 oct – 2 nov | Instancias, red y SSH ([#16](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/16)) · manual de red · 4 preguntas candidatas ([#61](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/61)) · empezar a diseñar la limpieza de energía y macros | Muestra y columnas ([#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17)) · instalar Hadoop en los nodos ([#18](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/18)) · candidatas | Candidatas de herramienta extra ([#15](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/15)) · candidatas de preguntas · empezar a diseñar la limpieza de tags | Profesor y plantillas ([#13](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/13), [#14](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/14)) · repositorio · borrador del dataset ([#24](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/24)) · candidatas |
| **Semana 1**<br/>3 – 9 nov<br/>**R1: martes 3** | Scripts de arranque ([#21](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/21)) · limpieza de energía y macros lista ([#25](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/25), 7 nov) · empezar Q02 con el Parquet de muestra | HDFS operativo ([#18](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/18), 4 nov) · CSV a HDFS ([#22](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/22)) · limpieza de sal y ración ([#26](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/26)) · empezar Q03 | YARN ([#19](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/19), 5 nov) · limpieza de tags ([#27](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/27)) · empezar Q01 | Contrato ([#67](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/67), 4 nov) · Spark y SparkPi ([#20](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/20)) · limpieza de fechas y NOVA ([#28](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/28)) · Parquet de muestra ([#68](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/68), 8 nov) · lanzar Parquet completo |
| **Semana 2**<br/>10 – 16 nov<br/>**R2: martes 10** | Q02 código 11 y ejecución 13 · Q04 código 15 y ejecución 16 · gráficas de Q02 · revisar los pull requests de Alberto | Q03 código 11 y ejecución 13 · Q10 código 15 y ejecución 16 · gráficas de Q03 · revisar los de Jorge | Q01 código 11 y ejecución 13 · Q08 código 15 y ejecución 16 · gráficas de Q01 · revisar los de Mauro | Parquet completo ([#29](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/29), 10 nov) · Q07 código 11 y ejecución 13 · tolerancia a fallos ([#30](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/30), 13 nov) · Q09 código 15 y ejecución 16 · revisar los de Katherine |
| **Semana 3**<br/>17 – 23 nov<br/>**R3: martes 17** | Q06 ejecución 17 · gráficas y sección ([#44](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/44), 20 nov) · revisión final (21 nov) | Q05 ejecución 17 · gráficas y sección ([#45](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/45)) · revisión final | Herramienta extra ([#48](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/48), 20 nov) · Q11 si se aprueba · gráficas ([#46](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/46)) · revisión final | Documento integrado ([#49](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/49), 20 nov) · Q12 si se aprueba · congelar repositorio ([#55](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/55), 22 nov) |
| **Semana 4**<br/>24 – 25 nov<br/>**R4: martes 24** | Ensayo final ([#66](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/66)) | Ensayo final | Ensayo final | Subida a ALUD antes de las 20:00 ([#57](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/57)) |

## Reuniones y sesiones

| Fecha | Duración | Qué se hace | Issue |
| --- | --- | --- | --- |
| **Mar 3 nov**, 11:30 | 1 h | Definir las preguntas, elegir la herramienta extra y revisar el contrato de columnas | [#63](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/63) |
| **Mar 10 nov**, 11:30 | 1 h | Estado del Parquet, enseñanza cruzada (10 minutos por persona) y reajuste de carga | [#64](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/64) |
| **Mar 17 nov**, 11:30 | 1 h | Revisar resultados y gráficas, reparto del documento y confirmar Q11 y Q12 | [#65](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/65) |
| Sáb 21 nov | 1 h 30 min | Redactar juntos las conclusiones y cerrar el documento (sesión extra) | [#49](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/49) |
| Lun 23 nov | 1 h 30 min | Primer ensayo de defensa con preguntas cruzadas (sesión extra) | [#56](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/56) |
| **Mar 24 nov**, 11:30 | 1 h | Segundo ensayo, logística del día 25 y subida a ALUD | [#66](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/66) |

El resto del trabajo es asíncrono: cada uno avanza su parte y avisa en el chat si se bloquea.

## Cómo se trabaja

1. Cada tarea es un issue; se trabaja en una rama propia y se abre un pull request que lo cierra.
2. El código lo revisa la persona de la cadena (diagrama de abajo) en menos de 48 horas desde que se abre el pull request. Quien revisa tiene que poder explicarlo en la defensa.
3. Probar siempre primero con la muestra y después con el dataset completo en el clúster.
4. Nunca se suben claves `.pem`, credenciales de AWS ni datos: el CSV y el Parquet viven en HDFS.
5. Apagar el clúster al terminar cada sesión para no gastar crédito de AWS Academy.
6. Si se retrasa algo más de un día, se avisa al grupo ese mismo día, diciendo qué tareas de otras personas bloquea (tabla «Quién necesita a quién»).

```mermaid
flowchart LR
    K(["Katherine"]) -->|"revisa el código de"| A(["Alberto"])
    A -->|"revisa el código de"| J(["Jorge"])
    J -->|"revisa el código de"| M(["Mauro"])
    M -->|"revisa el código de"| K
```
