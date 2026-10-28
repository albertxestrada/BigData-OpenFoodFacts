# Semana 0 · 27 oct – 2 nov · arrancar en paralelo

- **Katherine:** instancias, red y SSH ([#16](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/16), 30 oct) · manual de red (2 nov) · también instala Hadoop en los nodos, porque el playbook de Ansible ya lo hace al crear las instancias
- **Alberto:** muestra y columnas ([#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17), 30 oct)
- **Jorge:** candidatas de herramienta extra ([#15](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/15), 1 nov)
- **Mauro:** plantillas ([#14](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/14), 30 oct) · borrador del dataset ([#24](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/24), 2 nov)
- **Todos:** 4 preguntas candidatas ([#61](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/61), domingo 1 nov, 20:00) · explorar la muestra y empezar la parte genérica de la limpieza (tipos, duplicados, valores imposibles)

## Katherine (persona A) — instancias EC2, red y SSH

### Checklist

- [x] [#16](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/16) Instancias EC2, red, SSH y `/etc/hosts` · 30 oct · cierra M2
- [x] Manual de red · 2 nov (documento a escribir una vez el clúster esté arriba, con topología e IPs)

### Diagrama de Client, Master, Workes creados en esta sección

```mermaid
flowchart TB
    client["hadoop-client"]
    master["hadoop-master<br/>NameNode · ResourceManager"]

    subgraph workers["Workers"]
        direction LR
        w1["hadoop-worker-1"]
        w2["hadoop-worker-2"]
        w3["hadoop-worker-3"]
    end

    client --> master
    master --> w1
    master --> w2
    master --> w3

    style client fill:#243447,stroke:#3b82f6,color:#fff
    style master fill:#243447,stroke:#3b82f6,color:#fff
```

### Comandos

Despliegue con el script de Ansible de clase (`hadoop-ansible-ec2`): crea 1 master + N workers en
EC2 y deja red, SSH y `/etc/hosts` configurados entre todos los nodos. El mismo playbook instala
Hadoop y Spark en todos los nodos; esa instalación la asumo yo porque el comando ya la incluye al
crear las instancias, aunque en el reparto original fuera tarea de Alberto/Jorge/Mauro.

#### 1. Credenciales de AWS Academy

Learner Lab → **AWS Details → AWS CLI → Show** → copiar el contenido en `~/.aws/credentials`
(`nano ~/.aws/credentials`)

#### 2. Dependencias y clave SSH (Linux/WSL)

```bash
sudo apt update
sudo apt install git python3-venv libffi-dev
# Learner Lab entrega la clave como labsuser.pem; el playbook espera vockey.pem
cp labsuser.pem ~/.ssh/vockey.pem
chmod og-rwx ~/.ssh/vockey.pem
```

#### 3. Entorno de Ansible

```bash
python3 -m venv ~/ansible
source ~/ansible/bin/activate
git clone https://github.com/memaldi/hadoop-ansible-ec2 && cd hadoop-ansible-ec2
pip install -r requirements.txt
```

#### 4. Crear las instancias EC2 (red, SSH y `/etc/hosts`)

```bash
ansible-playbook -i inventory.aws_ec2.yml --key-file=~/.ssh/vockey.pem --user ec2-user create-instances.yml
```

#### 5. Verificar red y SSH entre nodos

```bash
# desde el equipo local al master
ssh -i ~/.ssh/vockey.pem ec2-user@<master-public-dns>

# dentro del master: solo confirma que /etc/hosts tiene las entradas correctas
cat /etc/hosts
```

#### 6. Acceso a las UIs (opcional, solo para comprobar que el clúster responde)

```bash
# correr en tu local: para ver los namenodes (HDFS)
ssh -i ~/.ssh/vockey.pem -N -L 50070:<master-public-dns>:50070 ec2-user@<master-public-dns>  
```
![image](assets/semana0/namenodes.png)

```bash
# correr en tu local: Resource Manager (YARN)
ssh -i ~/.ssh/vockey.pem -N -L 8088:<master-public-dns>:8088 ec2-user@<master-public-dns> 
```
![image](assets/semana0/resourcemanager.png)

#### 7. Creación de una nueva EC2 -> Nodo client

Misma AMI, clave y red que el resto; sin nada adicional, solo para lanzar `spark-submit`.

```bash
# Lanzar la instancia client
aws ec2 run-instances \
  --image-id ami-066784287e358dad1 \
  --instance-type t2.large \
  --key-name vockey \
  --security-groups hadoop-sg hadoop-master-sg \
  --count 1 \
  --region us-east-1 \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=hadoop-client},{Key=Group,Value=hadoop-client}]' \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":50,"DeleteOnTermination":true}}]'

# Obtener su IP privada/pública para corroborar su creación y detalles
aws ec2 describe-instances --filters "Name=tag:Group,Values=hadoop-client" \
  --query "Reservations[].Instances[].[InstanceId,PrivateIpAddress,PublicDnsName]" --output table
```

IMPORTANT: Wait for initialization to finish before continuing.
![image](assets/semana0/initializing-client.png)

```bash
# Desde tu equipo local (usa /tmp/hosts como archivo intermedio entre master y client)
# Copiar el /etc/hosts completo del master al client: es para que las herramientas de Hadoop/Spark que corran en el client puedan resolver "hadoop-master" y los workers por nombre.
scp -i ~/.ssh/vockey.pem ec2-user@<master-public-dns>:/etc/hosts /tmp/hosts
scp -i ~/.ssh/vockey.pem /tmp/hosts ec2-user@<client-public-dns>:/tmp/hosts
ssh -i ~/.ssh/vockey.pem ec2-user@<client-public-dns> "sudo mv /tmp/hosts /etc/hosts"
```

#### 8. Apagar al terminar la sesión

En la UI de EC2 en AWS Console, podemos darle **stop** a todas las instances.

## Alberto (persona B) — muestra del CSV y columnas

### Checklist

- [x] [#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17) Muestra del CSV y columnas que se usarán · 30 oct · cierra M2 
- [x] CSV completo en HDFS (`/data/raw`) · parte de [#22](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/22)
- [x] HDFS verificado (3 DataNodes) y carpetas creadas · parte de [#18](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/18), porque el playbook de Ansible ya instala Hadoop

### Comandos

Cada integrante tiene su propio clúster, así que estos pasos se repiten en cada uno. Todo se
ejecuta **en el master**, salvo el paso 1. Los DNS públicos cambian al parar y arrancar las instancias.

#### 1. Entrar al master (desde el equipo local)

```bash
ssh-add ~/.ssh/vockey.pem
ssh -A -i ~/.ssh/vockey.pem ec2-user@<DNS-DEL-MASTER>
```

El DNS está en la consola de EC2 (instancia `hadoop-master`, "DNS IPv4 público"). En Windows, usar WSL
con la clave en `~/.ssh` (no en `/mnt/c`) y `chmod 600`. Errores: `Permission denied (publickey)` →
clave equivocada o comando lanzado dentro del master; `UNPROTECTED PRIVATE KEY FILE` → falta el `chmod 600`.

#### 2. Verificar HDFS y crear carpetas

```bash
source ~/.bashrc
hdfs dfsadmin -report | grep -E "Live datanodes|Configured Capacity"   # Live datanodes (3)
hdfs dfs -mkdir -p /data/raw /data/clean /results
```

`hdfs: command not found` → falta el `source ~/.bashrc` (hace falta en cada sesión nueva).

#### 3. Subir el CSV completo a HDFS

Unos 1,4 GB comprimido y 13 GB descomprimido; se baja, descomprime y sube en una tubería, sin pasar
por el disco del master.

```bash
nohup bash -c 'curl -L https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz | gunzip | hdfs dfs -put - /data/raw/openfoodfacts.csv' > put.log 2>&1 &
hdfs dfs -ls -h /data/raw      # durante la subida: openfoodfacts.csv._COPYING_ creciendo
hdfs dfs -du -h /data/raw      # al terminar: 13.0 G  38.9 G (tamaño y réplicas)
```

Tarda unos 10 minutos. Si `put.log` muestra `unexpected end of file` o `curl: (18)`, se cortó:
`hdfs dfs -rm -f /data/raw/openfoodfacts.csv*` y relanzar.

#### 4. Muestra aleatoria de 100.000 filas

El CSV está **ordenado por código de barras**: las primeras 100.000 filas (`head`) eran un 89 % de
EE. UU. y sesgaban todos los porcentajes. Se usa una muestra aleatoria (reservorio) de todo el archivo:

```bash
hdfs dfs -cat /data/raw/openfoodfacts.csv | awk -v K=100000 'NR==1{h=$0;next} NR-1<=K{a[NR-1]=$0;next} {j=int(rand()*(NR-1))+1; if(j<=K)a[j]=$0} END{print h; for(i=1;i<=K;i++)print a[i]; print "TOTAL_FILAS=" NR-1 > "/dev/stderr"}' > muestra.csv
wc -l muestra.csv                                       # 100001
awk -F'\t' '{print NF}' muestra.csv | sort | uniq -c    # 100001 211 (sin filas partidas)
```

Salida de `TOTAL_FILAS`: 4.879.512 productos. Para compartirla: `gzip -k muestra.csv` y `scp` al equipo local.

#### 5. Porcentaje de datos por columna

```bash
awk -F'\t' 'NR==1{h=NF; for(i=1;i<=NF;i++) n[i]=$i; next} {for(i=1;i<=NF;i++) if($i!="") c[i]++} END{for(i=1;i<=h;i++) printf "%d\t%s\t%.1f\n", i, n[i], 100*c[i]/(NR-1)}' muestra.csv > columnas_relleno.tsv
grep -w "iron_100g" columnas_relleno.tsv     # comprobar el nombre real de cada columna
```

El % de vacíos es 100 menos el último campo. Los nombres se copian de la cabecera real, no de la documentación.

#### 6. Leer la muestra con Spark (separador, cabecera, tipos)

```bash
cat > leer_muestra.py <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("file:///home/ec2-user/muestra.csv", sep="\t", header=True, inferSchema=True)
print("FILAS:", df.count(), "COLUMNAS:", len(df.columns))
print(df.select("code", "created_t", "nova_group", "energy-kcal_100g", "nutriscore_grade").dtypes)
EOF
spark-submit --master local[2] --driver-memory 3g leer_muestra.py 2>/dev/null   # FILAS: 100000 COLUMNAS: 211
```

Con tabulador y cabecera basta: las comillas no rompen filas. Si no sale nada, repetir sin `2>/dev/null` para ver el error.

### Resultado: columnas (% de vacíos en la muestra de 100.000 filas)

La tabla completa está en el comentario de [#17](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/17).

| Columna | Tipo | % vacíos |
|---|---|---|
| `code` | string (forzar) | 0 |
| `created_t`, `last_modified_t` | long | 0 |
| `product_name`, `countries_tags` | string | 7 · 0,4 |
| `brands`, `categories_tags`, `labels_tags` | string | 36 · 60 · 74 |
| `nova_group`, `additives_n` | int | 76 · 72 |
| `energy-kcal`, `fat`, `carbohydrates`, `proteins` (`_100g`) | double | 28-29 |
| `sugars`, `saturated-fat`, `salt`/`sodium` (`_100g`) | double | 33 · 35 · 37,5 |
| `energy-kj_100g`, `fiber_100g` | double | 47 · 61 |
| `iron`, `calcium`, `vitamin-c`, `vitamin-a` (`_100g`) | double | 84-89 |
| `nutriscore_grade` | string | 71 (`unknown` 67 %, `not-applicable` 2,4 %) |
| `ingredients_from_palm_oil_n` | — | no existe en este CSV |

- `code` hay que leerlo como texto: Spark lo infiere como número y pierde ceros y dígitos.
- Una columna rellena no implica dato real: `unknown` y `not-applicable` son nulos para la limpieza.
- Sodio y sal aportan casi lo mismo; el resto de vitaminas y minerales está casi vacío.

_ La muestra (`muestra.csv.gz`) en drive compartido del grupo._

## Jorge (persona C) — herramienta extra

### Checklist

- [ ] [#15](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/15) Elegir la herramienta extra del Tema 6 · 1 nov

### Comandos

_Pendiente de añadir por Jorge._

## Mauro (persona D) — plantillas y borrador del dataset

### Checklist

- [ ] [#14](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/14) Plantillas .docx del profesor · 30 oct
- [ ] [#24](https://github.com/albertxestrada/BigData-OpenFoodFacts/issues/24) Borrador del dataset · 2 nov

### Comandos

_Pendiente de añadir por Mauro._

## Todos — preguntas candidatas y exploración de la muestra

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
