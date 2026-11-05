# Open Food Facts analytics

Preguntas analíticas con Spark sobre el CSV de Open Food Facts, en un clúster Hadoop en AWS Academy (Ansible de clase + nuestros scripts).

```
up.sh                        tu equipo: levanta todo (crea, enciende, configura y sube los datos)
down.sh                      tu equipo: borra todo (o --stop para solo apagar)
run.sh                       client: lanza un job de Spark en YARN
cluster/master.sh            master: configurar el clúster, subir los datos, comprobarlo (lo lanza up.sh)
cluster/client.sh            client: instalar Spark (lo lanza master.sh setup)
cluster/worker-yarn-site.xml YARN de los workers: 6 GB por nodo
jobs/                        00-etl.py (CSV → Parquet) y una pregunta por archivo (qNN-*.py)
```

## Requisitos

- Windows: **WSL** (Ubuntu). Mac: **Terminal**. Con `git`, `python3` y [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html#getting-started-install-instructions). En WSL: `sudo apt install -y git python3-venv libffi-dev unzip`.
- Learner Lab arrancado y `labsuser.pem` descargado (AWS Details, Download PEM).
- Si un script da `Permission denied`: `chmod +x *.sh cluster/*.sh`.

---

## Uso

### Preparar tu equipo (una vez)

```bash
mkdir -p ~/.ssh ~/.aws
cp ~/Downloads/labsuser.pem ~/.ssh/vockey.pem && chmod 600 ~/.ssh/vockey.pem
```

En cada sesión del Learner Lab las credenciales cambian: pegar AWS Details, AWS CLI, Show en `~/.aws/credentials` (`nano ~/.aws/credentials`).

### Levantar y destruir

```bash
./up.sh              # la primera vez ~1 h; en las siguientes sesiones, unos minutos
./down.sh --stop     # al acabar la sesión: apaga, HDFS conserva los datos
./down.sh            # al acabar el trabajo: borra las 5 instancias y HDFS
```

`up.sh` se puede relanzar siempre: crea lo que falta, enciende lo apagado y se salta lo ya hecho. Al final imprime el comando ssh del master y los túneles de las webs de HDFS y YARN (cada túnel en su propia terminal).

Lo que hace, en orden:

1. **Ansible**: entorno `~/ansible` y playbook de clase (`memaldi/hadoop-ansible-ec2`). Solo la primera vez.
2. **Master y 3 workers** con HDFS, YARN y Spark (~15-40 min). Solo si no hay master: el playbook formatea HDFS. Si ya existen y están apagados, los enciende.
3. **SSH y client**: abre el puerto 22 del master a cualquier IP (Ansible solo deja la tuya) y crea `hadoop-client`.
4. **Configuración** (`cluster/master.sh setup`, en el master):
   - `~/.ssh/config` para entrar a cualquier nodo con `ssh hadoop-worker-1`.
   - `hadoop-client` en el `/etc/hosts` del master. El client recibe ese `/etc/hosts` sin su propia línea. Los workers no se tocan.
   - `worker-yarn-site.xml` en los workers: YARN da 6 de sus 8 GB (por defecto anuncia 8 GB, más de lo que puede dar una t3.large).
   - En el client: los jobs, la dirección de HDFS y YARN (`core-site.xml` y `yarn-site.xml` del master), Java 8, Spark 3.5.3 y una prueba con SparkPi.
   - Comprobación (`master.sh check`): `Live datanodes (3)`, `Total Nodes:3` y 3 veces `<memory:6144, vCores:2>`.
5. **Datos**: descarga el CSV (~13 G) a `/data/raw/openfoodfacts.csv` en HDFS (39 G con las 3 réplicas por defecto) y crea `muestra.csv` con el 2 % de las filas (~15 min).

> **Por qué el client no lleva `hadoop-client` en su `/etc/hosts`.** En modo client, el ApplicationMaster y los executors (en los workers) se conectan al driver (en el client) con el nombre que este anuncia. Sin esa línea, Spark se anuncia con el nombre interno de EC2 (`ip-172-31-x-x.ec2.internal`), que los workers resuelven con el DNS de la VPC. Con ella se anunciaría como `hadoop-client`, que los workers no conocen, y fallaría con `UnknownHostException: hadoop-client`.

> [!WARNING]
> Si Ansible falla o se queda parado en `Download Spark` (10-35 min sin mostrar progreso), Ctrl+C. Si el master ya tiene `~/spark-3.5.5-bin-hadoop3.tgz` (383 MB), seguir desde la tarea siguiente y relanzar `up.sh`:
>
> ```bash
> source ~/ansible/bin/activate && cd ~/hadoop-ansible-ec2
> ansible-playbook -i inventory.aws_ec2.yml --key-file ~/.ssh/vockey.pem --user ec2-user \
>   create-instances.yml --start-at-task "Extract Spark"
> cd - && ./up.sh
> ```
>
> ![Descarga de Spark en curso](docs/assets/step-01-download-spark.png)

> [!NOTE]
> Si se corta la conexión durante la subida del CSV, la subida se para: relanzar `./up.sh` la empieza de nuevo.

Capturas de cada parte:

![Instancias en EC2](docs/assets/step-01-ec2-instances.png)
![Grupos de seguridad](docs/assets/step-01-security-groups.png)
![Regla SSH abierta](docs/assets/step-02-ssh-rule.png)
![Client](docs/assets/step-04-client-instance.png)
![DataNodes en la web de HDFS](docs/assets/step-03-hdfs-datanodes.png)
![Nodos de YARN antes de la configuración](docs/assets/step-07-yarn-nodes-before.png)
![Nodos de YARN con 6 GB](docs/assets/step-07-yarn-nodes-after.png)
![SparkPi en YARN](docs/assets/step-08-sparkpi.png)
![CSV en HDFS](docs/assets/step-09-hdfs-raw.png)

### ETL y preguntas

Entrar al master con el comando que imprime `up.sh`, y desde ahí al client:

```bash
ssh hadoop-client
cd open-food-facts-analytics
./run.sh jobs/00-etl.py hdfs:///data/raw/muestra.csv   # primero con la muestra (~99.000 filas)
./run.sh jobs/00-etl.py                                # después el CSV completo (~4,88 M filas)
for f in jobs/q*.py; do ./run.sh "$f"; done            # las preguntas
```

El ETL guarda el Parquet en `/data/clean/products` e imprime cuántos productos tienen cada dato. Cada pregunta deja su CSV en `/results/qNN-...`. Para comprobar el clúster en cualquier momento, en el master: `./cluster/master.sh check`.

![ETL con la muestra](docs/assets/step-10-etl-sample.png)
![ETL con el CSV completo](docs/assets/step-10-etl-full.png)
![Resultados en HDFS](docs/assets/step-11-results.png)
