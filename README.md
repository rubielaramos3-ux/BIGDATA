Procesamiento Batch con PySpark - Puestos de Votación

Este repositorio contiene el script de procesamiento por lotes (Batch) en PySpark para el análisis de un conjunto de datos público de puestos de votación.

Descripción de la Solución

El script "proceso_batch.py" realiza las siguientes operaciones secuenciales:
1. Inicialización: Crea la sesión de Spark (SparkSession) configurando los logs(registros) en nivel "WARN".
Es decir importa las librerías necesarias de PySpark, inicializa la sesión principal de trabajo (SparkSession) bajo el nombre de aplicación "ProyectoBatchPuestosVotacion" y configura el nivel de advertencias en WARN para evitar saturar la consola con mensajes informativos innecesarios.
2. Carga de Datos:
  * Lee el archivo en formato CSV (PUESTO_DE_VOTACIÓN_20261027.csv).
  * option("header", "true") le indica a Spark que la primera fila contiene los nombres de las columnas.
  * option("inferSchema", "true") detecta automáticamente el tipo de dato de cada columna (enteros, textos, etc.).
  * printSchema() imprime la estructura de las columnas
  * df.show(5) muestra en forma de tabla las primeras 5 filas del archivo.
3. **Limpieza y Transformación: Elimina filas con datos nulos o vacíos con "df.na.drop()".
4. Análisis Exploratorio (EDA):** Genera el resumen estadístico descriptivo de la base de datos mediante "describe().show()".
5. Resultados: Imprime en consola el conteo total de registros procesados y cierra la sesión de Spark.

---

REQUISITOS PARA LA EJECUCION 
PLATAFORMAS, HERRAMIENTAS TECNOLOGICAS ....
* Virtual box
* putty 
* Python 3
* Apache Spark / PySpark instalado y configurado en el sistema (o en máquina virtual Ubuntu).
* La base de datos PUESTO_DE_VOTACIÓN_20261027.csv ubicado en la misma carpeta del script.


Instrucciones de ejecuciòn

1. Clona o descarga este repositorio en tu máquina virtual:
   Pasos para clonar repositorio:
   git clone <URL_DEL_REPOSITORIO>
   cd <NOMBRE_DE_DEL_REPOSITORIO>

- Inicio de sesión en VirtualBox
 - Inicio de sesión en Putty
asignamos ip de la maquina virtual e inicimos sesion con las mismas credenciales de virtual box
- realizamos la instalacion de de librería python en kafka en donde se descarga  y despues se 
    descomprimir el archivo

Ejecuciòn del ejercicio
 
1. iniciamos el servidor de zookeeper
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
2.  iniciar el implementar el ejercicio se creo un archivo llamado kafka_producer
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties

3. Se utilizo para el procesamiento de batch datos publicos de los puesto de votacion 
en la cual se cargo directamente del link de la pagina 
con el comando "ls -l"  podemos verificar los archivos de trabajo:

* kafkaproducer- para simular y transmitir eventos en tiempo real hacia Kafka.
* prueba batch-análisis estático sobre el archivo CSV de puestos de votación.
* prueba straming- para consumir y procesar el flujo continuo de datos de Kafka.
* archivo de puestos de votacion cvs

4. Propocionamos el comando "python3 prueba_batch.py"
Al ejecutarlo, observamos los siguientes pasos en pantalla:

  Carga de Datos: Spark lee el archivo CSV DE LOS PUESTOS DE VOTACION
  Limpieza y Transformación: Se eliminan registros nulos o inconsistentes 
  Análisis Exploratorio (EDA): Se calculan métricas descriptivas


5. Abrimos una segunda ventana de putty:
iniciamos el productor que simula la llegada continua de datos de sensores
"python3 kafka_producer.py"

6. Ejecutamos una Tercera ventana de putty:
Ejecutamos la aplicación de Spark Streaming incluyendo el conector oficial de Spark-Kafka
"spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 spark_streaming_consumer.py"

observando el resultado  en la consola de Spark empieza a actualizarse la tabla por micro-lotes en tiempo real. 
Spark toma los mensajes JSON de Kafka, los parsea bajo el esquema estructurado y 
calcula al vuelo estadísticas como la temperatura promedio y máxima por sensor.

