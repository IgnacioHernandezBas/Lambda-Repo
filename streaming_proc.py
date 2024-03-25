import os
import pandas as pd
import numpy as np
import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.sql.functions import from_json, col, explode, lit, from_unixtime, date_format, split
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

############################ OBTENER DATOS DE COLLECTD ############################
# Crear una sesión de Spark
conf = (
    SparkConf()
    .setAppName(u"[ICAI] Lambda Architecture")
    .set("spark.executor.memory","4g")
    .set("spark.executor.cores","2")
)

spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.cassandra.connection.host", "192.168.80.34") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()


def unix_to_hour(unix_time):
    import datetime
    return datetime.datetime.fromtimestamp(int(unix_time)).strftime('%H:%M:%S')

# Registrar la función como un UDF en Spark
spark.udf.register("unix_to_hour", unix_to_hour, StringType())

# Leer datos en formato de streaming desde Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "192.168.80.34:9092,192.168.80.35:9092,192.168.80.37:9092") \
  .option("subscribe", "collectd") \
  .load()

# Seleccionar las columnas 'key' y 'value' como STRING
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Dividir la columna 'value' en dos columnas: 'nombre', 'detalle', y 'fecha_unix', manteniendo 'value'
df = df.withColumn("nodo", F.split(F.col("value"), "\s+")[0]) \
       .withColumn("detalle", F.split(F.col("value"), "\s+")[1]) \
       .withColumn("fecha_unix", F.split(F.col("value"), "\s+")[2])

df = df.withColumn("nodo", F.split(F.col("nodo"), "\.")[0])

# Aplicar la función UDF para convertir la fecha de Unix a solo la hora
df = df.withColumn("hora_legible", F.udf(lambda x: unix_to_hour(x), StringType())(F.col("fecha_unix").cast("long")))

# Añadir una columna con el día de la semana en español
df = df.withColumn("day_of_week", F.date_format(F.col("hora_legible"), "EEEE"))

# Traducir el nombre del día de la semana al español y poner en minúsculas
dias_semana = {
    "Monday": "lunes",
    "Tuesday": "martes",
    "Wednesday": "miercoles",
    "Thursday": "jueves",
    "Friday": "viernes",
    "Saturday": "sabado",
    "Sunday": "domingo"
}

# Crear expresión de traducción para el día de la semana
translate_expr = F.create_map([F.lit(x) for x in sum(dias_semana.items(), ())])

# Aplicar la traducción y convertir a minúsculas
df = df.withColumn("day_of_week", F.lower(F.when(translate_expr[F.col("day_of_week")].isNull(), F.col("day_of_week")).otherwise(translate_expr[F.col("day_of_week")])))

# Cambiar el nombre de la columna "day_of_week" a "dia_de_la_semana"
df = (df.withColumnRenamed("day_of_week", "dia_de_la_semana")
        .withColumnRenamed("value", "collectd")
        .withColumn("despues_del_punto", split(col("collectd"), "\.", 2).getItem(1))
        .withColumn("indicador", split(col("despues_del_punto"), " ", 2).getItem(0))
        .withColumn("valor", split(col("collectd"), " ", 4).getItem(1))
        .withColumn("fecha", date_format(from_unixtime(col("fecha_unix")), "yyyy-MM-dd'T'HH:mm:ss")))


# Seleccionar las columnas necesarias incluyendo 'value'
df = df.select("fecha", "nodo", "indicador", "valor", "hora_legible", "dia_de_la_semana")

############################ OBTENER DATOS DE ESPECIFICACIONES ############################
# Configuración de Kafka
kafka_bootstrap_servers = "192.168.80.34:9092,192.168.80.35:9092,192.168.80.37:9092"
kafka_topic = "especificaciones"

# Esquemas
schema_nodos = StructType([
    StructField("servidor_nombre", StringType(), True),
    StructField("servidor_grupo", StringType(), True)
])

schema_turnos = StructType([
    StructField("nombre", StringType(), True),
    StructField("dia_inicio", StringType(), True),
    StructField("dia_fin", StringType(), True),
    StructField("hora_inicio", StringType(), True),
    StructField("hora_fin", StringType(), True)
])

# Leer datos desde Kafka
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Convertir los datos binarios en una cadena
kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING) as json")

# Aplicar from_json para parsear el string JSON a una columna de estructura de datos según el esquema apropiado
df_nodos = kafka_df_string.withColumn("data", from_json("json", schema_nodos)).select("data.*")
df_turnos = kafka_df_string.withColumn("data", from_json("json", schema_turnos)).select("data.*")

# Filtrar para mantener solo las filas que tienen datos (no nulas según el esquema)
final_df_nodos = df_nodos.filter("servidor_nombre IS NOT NULL")
final_df_turnos = df_turnos.filter("nombre IS NOT NULL")


############################ CRUZAR DATOS ############################

def assign_node_group(df_collectd, final_df_nodos):
    # Asegúrate de usar 'col' para referenciar las columnas correctamente en el join
    df_with_node = df_collectd.join(final_df_nodos, df_collectd['nodo'] == final_df_nodos['servidor_nombre'], "left").drop("servidor_nombre")
    return df_with_node


df_with_node = assign_node_group(df, final_df_nodos)

def convert_to_week_hour_number(dia, hora):
    dias = ['lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo']
    dia_index = dias.index(dia.lower())
    hora_index = int(hora.split(':')[0])
    return dia_index * 24 + hora_index

# Registra la función como UDF
convert_to_week_hour_number_udf = F.udf(convert_to_week_hour_number, IntegerType())

# Aplica la UDF para obtener el número de hora correspondiente a cada hora de inicio y fin
final_df_turnos = final_df_turnos.withColumn(
    "hora_inicio_num", convert_to_week_hour_number_udf(F.col("dia_inicio"), F.col("hora_inicio"))
).withColumn(
    "hora_fin_num", convert_to_week_hour_number_udf(F.col("dia_fin"), F.col("hora_fin"))
)

# Si la hora de fin es menor que la hora de inicio, entonces el turno termina la siguiente semana
# Por lo tanto, ajusta la hora de fin sumando las horas totales en una semana
final_df_turnos = final_df_turnos.withColumn(
    "hora_fin_num_ajustada",
    F.when(
        final_df_turnos.hora_fin_num < final_df_turnos.hora_inicio_num,
        final_df_turnos.hora_fin_num + 24 * 7  # Añade el total de horas en una semana
    ).otherwise(final_df_turnos.hora_fin_num)
)

df_with_nodes = df_with_node.withColumn(
    "hora_actual_num", convert_to_week_hour_number_udf(F.col("dia_de_la_semana"), F.col("hora_legible"))
)

df_with_turns = df_with_nodes.alias("nodes").join(
    final_df_turnos.alias("turnos"),
    (col("nodes.hora_actual_num") >= col("turnos.hora_inicio_num")) &
    (col("nodes.hora_actual_num") < col("turnos.hora_fin_num_ajustada")),
    "left"
)


# Selecciona y renombra las columnas como se requiere utilizando los alias correctos
df_with_turns_selected = df_with_turns.select(
    col("fecha"),
    col("turnos.nombre").alias("turno"),
    col("nodes.servidor_grupo").alias("grupo"),
    col("nodes.nodo").alias("nodo"),
    col("indicador"),
    col("valor"),
)


query = df_with_turns_selected \
    .writeStream \
    .outputMode("append") \
    .option("truncate", False) \
    .format("console") \
    .start()

############################ GUARDAR EN CASSANDRA ############################
def save_to_cassandra(df, epoch_id):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode('append') \
      .option("keyspace", "grupo2") \
      .option("table", "mediciones") \
      .save()

query = df_with_turns_selected \
    .writeStream \
    .foreachBatch(save_to_cassandra) \
    .outputMode("append") \
    .start()


query.awaitTermination()