# Desarrollo

Pasos para arrancar el clúster y trabajar en el notebook de consultas. Cada paso dice **dónde** se ejecuta.
Detalle del despliegue en [DEPLOYMENT.md](DEPLOYMENT.md), de la limpieza en [LIMPIEZA.md](LIMPIEZA.md) y de las preguntas en [PREGUNTAS.md](PREGUNTAS.md).

## Índice

- [¿Dónde estoy?](#dónde-estoy)
- [A. Una sola vez: preparar WSL](#a-una-sola-vez-preparar-wsl)
- [B. Cada sesión: arrancar](#b-cada-sesión-arrancar)
- [C. Solo si no hay clúster: crearlo y limpiar los datos](#c-solo-si-no-hay-clúster-crearlo-y-limpiar-los-datos)
- [D. Trabajar en el notebook](#d-trabajar-en-el-notebook)
- [E. Ver YARN y HDFS en el navegador](#e-ver-yarn-y-hdfs-en-el-navegador)
- [F. Cerrar la sesión](#f-cerrar-la-sesión)
- [Problemas frecuentes](#problemas-frecuentes)

## ¿Dónde estoy?

| Lugar | Cómo se reconoce | `echo $HADOOP_USER_NAME` |
|---|---|---|
| **WSL** (equipo local) | `usuario@PC:~/BigData-OpenFoodFacts$` | — |
| **master** | `[ec2-user@ip-… ~]$` | vacío |
| **ingesta** | `[ec2-user@ip-… ~]$` | `ingesta` |
| **consulta** | `[ec2-user@ip-… ~]$` | `consulta` |

Los tres nodos remotos tienen el mismo aspecto: antes de ejecutar algo en un cliente, comprobar `echo $HADOOP_USER_NAME`.
`exit` vuelve al lugar anterior.

## A. Una sola vez: preparar WSL

**A1 · WSL** · Dependencias (Python 3.9–3.11 para el despliegue) y [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html):

```bash
sudo apt update && sudo apt install -y git python3-venv libffi-dev rsync unzip
aws --version
```

**A2 · WSL** · Clave del laboratorio (Learner Lab → **AWS Details → Download PEM**), en el home de WSL:

```bash
mkdir -p ~/.ssh && cp /mnt/c/Users/<usuario>/Downloads/labsuser.pem ~/.ssh/vockey.pem && chmod 600 ~/.ssh/vockey.pem
```

**A3 · WSL** · Clonar el repo **dentro de WSL**, no en `C:\` (en Windows los scripts quedan con CRLF y fallan con `bash\r`):

```bash
cd ~ && git clone https://github.com/albertxestrada/BigData-OpenFoodFacts.git && cd BigData-OpenFoodFacts
cp config/cluster.env.example config/cluster.env     # región us-east-1, usuario ec2-user, clave ~/.ssh/vockey.pem
```

## B. Cada sesión: arrancar

**B1 · AWS Academy** · **Start Lab**. Las instancias arrancan solas. El contador dura 4 h: volver a darle a **Start Lab** antes de una tarea larga.

**B2 · WSL** · **AWS Details → AWS CLI → Show** → pegar en `~/.aws/credentials` (cambian en cada sesión).

**B3 · WSL** · Cargar las direcciones. **Repetir en cada terminal nueva de WSL** (la IP del master cambia en cada arranque):

```bash
cd ~/BigData-OpenFoodFacts
MASTER=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=hadoop-master" "Name=instance-state-name,Values=running" \
  --query "Reservations[].Instances[].PublicDnsName" --output text)
INGESTA=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=hadoop-client-ingesta" "Name=instance-state-name,Values=running" \
  --query "Reservations[].Instances[].PrivateIpAddress" --output text)
CONSULTA=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=hadoop-client-consulta" "Name=instance-state-name,Values=running" \
  --query "Reservations[].Instances[].PrivateIpAddress" --output text)
eval "$(ssh-agent -s)" && ssh-add ~/.ssh/vockey.pem
echo "$MASTER | $INGESTA | $CONSULTA"
```

Si sale vacío, ver las instancias:

```bash
aws ec2 describe-instances --filters "Name=tag:Name,Values=hadoop-*" \
  --query "Reservations[].Instances[].[Tags[?Key=='Name']|[0].Value,State.Name,PublicDnsName,PrivateIpAddress]" --output table
```

| La tabla muestra | Siguiente paso |
|---|---|
| Las 6 instancias en `running` | [D. Trabajar en el notebook](#d-trabajar-en-el-notebook) |
| Alguna en `pending` | Esperar un minuto y repetir B3 |
| Nada, o un solo `hadoop-client` | [C. Crear el clúster](#c-solo-si-no-hay-clúster-crearlo-y-limpiar-los-datos) |

## C. Solo si no hay clúster: crearlo y limpiar los datos

**C1 · WSL** · Desplegar (tarda bastante; en otra terminal, `./scripts/cluster.sh progress`):

```bash
./scripts/cluster.sh deploy
```

Termina con `failed=0` en todos los nodos. Después, repetir **B3**.

**C2 · WSL** · Subir el código al cliente de ingesta y entrar:

```bash
./scripts/sync.sh ingesta
ssh -J ec2-user@$MASTER ec2-user@$INGESTA
```

**C3 · ingesta** · Limpiar. **Primero `cd` a la carpeta del proyecto**: desde `~` falla con `Exit 2`.

```bash
cd ~/BigData-OpenFoodFacts && echo $HADOOP_USER_NAME        # ingesta
nohup bash -c 'set -e; spark-submit src/client_ingesta/limpieza.py; spark-submit src/client_ingesta/verificar.py' > ejecucion.log 2>&1 &
```

**C4 · ingesta** · Esperar y comprobar (varios minutos; repetir hasta ver `RESUMEN`):

```bash
grep -E "ESCRITO|^FALLO|RESUMEN" ejecucion.log      # ESCRITO …/off_limpio  y  RESUMEN: TODO OK
exit                                                 # volver a WSL
```

## D. Trabajar en el notebook

**D1 · WSL** · Subir el código al cliente de consulta y entrar **con el túnel de Jupyter**:

```bash
./scripts/sync.sh consulta
ssh -J ec2-user@$MASTER -L 8888:localhost:8888 ec2-user@$CONSULTA
```

**D2 · consulta** · Solo la primera vez: crear el entorno de Jupyter. Necesita Python 3.8 o superior:

```bash
echo $HADOOP_USER_NAME && python3 --version        # consulta  ·  3.8+
python3 -m venv ~/venv-consulta                    # con 3.7: sudo amazon-linux-extras install -y python3.8 y usar python3.8
~/venv-consulta/bin/pip install --upgrade pip
~/venv-consulta/bin/pip install -r ~/BigData-OpenFoodFacts/src/client_consulta/requirements.txt
```

**D3 · consulta** · Abrir Jupyter (la terminal queda ocupada; `Ctrl+C` lo cierra):

```bash
yarn application -list                              # vacío: nadie más usa YARN
cd ~/BigData-OpenFoodFacts/src/client_consulta && ~/venv-consulta/bin/jupyter lab --no-browser --port 8888
```

**D4 · navegador** · Abrir la URL `http://localhost:8888/lab?token=…` que imprime Jupyter → `consultas.ipynb` → ejecutar la celda de preparación. Debe imprimir:

```
Usuario de HDFS: consulta
Modo de Spark: yarn
Filas en el Parquet limpio: ≈ 4,9 millones (depende del día en que se descargó el CSV)
```

Los avisos `NativeCodeLoader` y `spark.yarn.jars` son normales; el primer arranque tarda ≈ 1 minuto.

**D5 · navegador** · Pruebas en un notebook propio que no se sube (`scratch_<nombre>.ipynb`), con la celda de preparación y una muestra:

```python
dev = df.sample(fraction=0.02, seed=42).cache()
```

**D6 · Guardar una pregunta terminada** (un commit por pregunta):

| Dónde | Qué |
|---|---|
| navegador | Pasar las celdas a `consultas.ipynb`, ejecutar con `df`, escribir la conclusión |
| navegador | **Edit → Clear Outputs of All Cells** y guardar (el notebook se sube sin salidas) |
| WSL (otra terminal, con **B3**) | Traer el notebook y hacer commit |

```bash
scp -J ec2-user@$MASTER ec2-user@$CONSULTA:~/BigData-OpenFoodFacts/src/client_consulta/consultas.ipynb src/client_consulta/
git add src/client_consulta/consultas.ipynb docs/PREGUNTAS.md && git commit
```

> [!WARNING]
> `sync.sh` solo sube: sobrescribe el notebook del cliente con la versión local. Traer el notebook con `scp` **antes** de volver a sincronizar.

## E. Ver YARN y HDFS en el navegador

**E1 · WSL** (otra terminal, con **B3**) · Abrir el túnel; queda abierto hasta `Ctrl+C`:

```bash
ssh -i ~/.ssh/vockey.pem -N -L 8088:$MASTER:8088 -L 50070:$MASTER:50070 ec2-user@$MASTER
```

**E2 · navegador**

| Web | URL | Qué se ve |
|---|---|---|
| YARN | <http://localhost:8088/cluster> | **Nodes**: workers y memoria · **Applications**: trabajos (en uno en marcha, *ApplicationMaster* abre la interfaz de Spark) |
| HDFS | <http://localhost:50070> | **Datanodes**: workers · **Utilities → Browse the file system**: `/data` y `/results` |

Para entrar al master: `ssh -i ~/.ssh/vockey.pem ec2-user@$MASTER`.

## F. Cerrar la sesión

| Dónde | Qué |
|---|---|
| navegador | Ejecutar la última celda del notebook (`spark.stop()`, libera YARN) |
| consulta | `Ctrl+C` a Jupyter y `exit` |
| WSL | Traer el notebook con `scp` (D6) si cambió |
| AWS Academy | **End Lab**: apaga las instancias. **Nunca terminarlas**: HDFS vive en sus discos |

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `/usr/bin/env: 'bash\r'` | Repo clonado en Windows (CRLF) | Clonar dentro de WSL (A3) |
| `No such file … requirements.txt` o `limpieza.py` | Comando ejecutado en el cliente equivocado | `echo $HADOOP_USER_NAME` y entrar al cliente correcto |
| `nohup …` termina enseguida con `Exit 2` | Lanzado desde `~` y no desde `~/BigData-OpenFoodFacts` | `cd ~/BigData-OpenFoodFacts` y relanzar |
| `No se encuentra hadoop-client-ingesta` | Clúster con el diseño anterior (un solo cliente) | [Migrarlo](DEPLOYMENT.md#si-el-clúster-ya-tiene-un-único-hadoop-client) o desplegar uno nuevo |
| `cluster.sh deploy` se cancela al empezar | Existe un `hadoop-master`, aunque esté apagado | Esperar a que las instancias terminadas queden en `terminated` |
| Un trabajo con `nohup` desaparece | Se acabó el contador del lab y las instancias se apagaron | `uptime` en el cliente; relanzar |
| La celda de preparación no avanza | YARN ocupado (`ACCEPTED`) | `yarn application -list` y `yarn application -kill <ID>` |
| `jupyterlab` o `pyspark` no instalan | Python del sistema anterior a 3.8 | Crear el venv con `python3.8` (D2) |
| SSH pide confirmar la huella | La IP del master cambió | Responder `yes` |
