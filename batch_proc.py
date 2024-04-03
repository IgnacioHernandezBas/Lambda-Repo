# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,split, from_unixtime,date_format,from_json, col,explode,concat,lit
from pyspark.sql.functions import explode, split, from_json, concat, lit, col,unix_timestamp
from pyspark.sql.types import StringType,IntegerType,StructType, StructField

import pyspark.sql.functions as F
import datetime
import sys

#-------------------OBTENER DATOS COLLECTD---------------------------------------------------

# Funciones para la lectura
def unix_to_readable(unix_time):
    import datetime
    return datetime.datetime.fromtimestamp(int(unix_time)).strftime("%Y-%m-%dT%H:00")

def unix_to_hour(unix_time):
    import datetime
    return datetime.datetime.fromtimestamp(int(unix_time)).strftime('%H:00:00')


def get_collectd(spark):

    # Registrar la función como un UDF en Spark
    spark.udf.register("unix_to_readable", unix_to_readable, StringType())
    
    # Registrar la función como un UDF en Spark
    spark.udf.register("unix_to_hour", unix_to_readable, StringType())
    
    # Define Kafka parameters
    kafka_params = {
        "kafka.bootstrap.servers": "worker01:9092,worker02:9092,worker04:9092",
        "subscribe": "collectd",
        "startingOffsets": "earliest"
    }
    
    # Create a DataFrame representing the stream of input lines from Kafka
    #Important to put read instead of readStream!
    df = spark \
        .read \
        .format("kafka") \
        .options(**kafka_params) \
        .load()
    
    # Seleccionar las columnas 'key' y 'value' como STRING
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    
    # Dividir la columna 'value' en dos columnas: 'nombre', 'detalle', y 'fecha_unix', manteniendo 'value'
    df = df.withColumn("nombre", F.split(F.col("value"), "\s+")[0]) \
           .withColumn("metrica", F.split(F.col("value"), "\s+")[1]) \
           .withColumn("fecha_unix", F.split(F.col("value"), "\s+")[2])
    #df.show(1,False)
    # Extraer solo 'worker01' del campo 'nombre' y renombrar columna a nodo
    df = df.withColumn("nodo", F.split(F.col("nombre"), "\.")[0])
    #Obtener el indicador
    
    df=df.withColumn("indicador", F.expr("substring(value, instr(value, '.') + 1)"))  # extrae todo después del primer punto
    df=df.withColumn("indicador", F.split(F.col("indicador"), " ")[0]) 
    # Aplicar la función UDF para convertir la fecha de Unix a la fecha legible
    df = df.withColumn("fecha", F.udf(lambda x: unix_to_readable(x), StringType())(F.col("fecha_unix").cast("long")))
    #df = df.withColumn("fecha", from_unixtime(unix_timestamp("fecha_unix"), "yyyy-MM-dd'T'HH:00"))

    # Aplicar la función UDF para obtener la hora solamente
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
    df = df.withColumnRenamed("day_of_week", "dia_de_la_semana")
    
    # Seleccionar las columnas necesarias incluyendo 'value'
    df_collectd = df.select("fecha","dia_de_la_semana","hora_legible","nodo","indicador","metrica")

    return df_collectd

#--------------OBTENER DFS DE LOS TURNOS Y NODOS------------------------------------------------------

def get_especificaciones(spark):
    # Define the Kafka parameters
    kafka_params = {
        "kafka.bootstrap.servers": "192.168.80.34:9092",
        "subscribe": "especificaciones",
        "startingOffsets": "earliest"
    }
    
    # Create a DataFrame representing the stream of input lines from Kafka
    df1 = spark.read.format("kafka").options(**kafka_params).load()
    
    # Define the schema for the JSON data
    enriquecimiento_schema = StructType() \
        .add("servidor_nombre", StringType()) \
        .add("servidor_grupo", StringType()) \
        .add("nombre", StringType()) \
        .add("dia_inicio", StringType()) \
        .add("dia_fin", StringType()) \
        .add("hora_inicio", StringType()) \
        .add("hora_fin", StringType())
    
    # Convert the binary data into a string
    kafka_df = df1.selectExpr("CAST(value AS STRING)")
    
    # Split the messages into different records
    split_df = kafka_df.withColumn("value", explode(split("value", "\\} \\{")))
    
    # Parse the JSON data
    parsed_df = split_df.withColumn("parsed_json", from_json(col("value"), enriquecimiento_schema))
    
    # Select the desired columns for nodos and turnos
    final_df_nodos = parsed_df.select(
        col("parsed_json.servidor_nombre").alias("servidor_nombre"),
        col("parsed_json.servidor_grupo").alias("servidor_grupo")
    ).filter(col("servidor_nombre").isNotNull())
    
    final_df_turnos = parsed_df.select(
        col("parsed_json.nombre").alias("nombre"),
        col("parsed_json.dia_inicio").alias("dia_inicio"),
        col("parsed_json.dia_fin").alias("dia_fin"),
        col("parsed_json.hora_inicio").alias("hora_inicio"),
        col("parsed_json.hora_fin").alias("hora_fin")
    ).filter(col("nombre").isNotNull())

    return final_df_nodos,final_df_turnos

#---------------ASIGNACIÓN NODOS Y TURNOS--------------------------------------------------------------------

def assign_node_group(df_collectd, final_df_nodos):
    # Asegúrate de usar 'col' para referenciar las columnas correctamente en el join
    df_with_node = df_collectd.join(final_df_nodos, df_collectd['nodo'] == final_df_nodos['servidor_nombre'], "left").drop("servidor_nombre")
    return df_with_node

def convert_to_week_hour_number(dia, hora):
    dias = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]
    dia_index = dias.index(dia)
    hora_index = int(hora.split(':')[0])
    return dia_index * 24 + hora_index


#Asignar los turnos al df con el nodo asignado

def assign_shift(df_with_node,final_df_turnos):
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
    #final_df_turnos.show(1,False)

    # Convierte la hora actual a su número correspondiente
    df_with_nodes = df_with_node.withColumn(
        "hora_actual_num", convert_to_week_hour_number_udf(F.col("dia_de_la_semana"), F.col("hora_legible"))
    )
    #print("With Nodes")
    #df_with_nodes.show(20,False)
    #final_df_turnos.show(20,False)
    
    #Dataframe con las columnas resultantes
    df_with_shifts = df_with_nodes.alias("nodes").join(
        final_df_turnos.alias("turnos"),
        (col("nodes.hora_actual_num") >= col("turnos.hora_inicio_num")) &
        (col("nodes.hora_actual_num") < col("turnos.hora_fin_num_ajustada")),  # Adjusted condition
        "left"
    )
    #En el caso de que haya horas en las que no haya turnos asignados
    df_with_shifts = df_with_shifts.fillna("Sin Asignar")
    #print("With Shifts")
    #df_with_shifts.show(1,False)
    
    # Selecciona y renombra las columnas como se requiere utilizando los alias correctos
    df_with_nodes_shifts= df_with_shifts.select("fecha","nombre","servidor_grupo","nodo", "indicador","metrica")

    df_with_nodes_shifts = df_with_nodes_shifts.withColumnRenamed("nombre", "turno") \
                             .withColumnRenamed("servidor_grupo", "grupo")

    
    return df_with_nodes_shifts

#---------------OBTENCIÓN DE LAS MÉTRICAS--------------------------------------------------------------

import pyspark.sql.functions as F

def get_metrics(df_with_nodes_shifts):
    # Group by relevant columns
    df_agrupado_indicador = df_with_nodes_shifts.groupBy("fecha", "turno", "grupo", "nodo", "indicador")

    # Calculate min, max, avg, and distinct count of the metrica column
    df_metricas = df_agrupado_indicador.agg(
        F.min("metrica").alias("min"),
        F.max("metrica").alias("max"),
        F.avg("metrica").alias("avg"),
        F.count("metrica").alias("count")
    )

    # Rename columns for clarity
    df_metricas = df_metricas.select(
        F.col("fecha").alias("fecha"),
        F.col("turno").alias("turno"),
        F.col("grupo").alias("grupo"),
        F.col("nodo").alias("nodo"),
        F.col("indicador").alias("indicador"),
        "min",
        "avg",
        "max",
        "count"
    )

    return df_metricas


if __name__=="__main__":


    
        
    # Create a Spark session configured with the necessary Kafka package
    spark = SparkSession.builder.appName("KafkaSparkSQLBatch").config("spark.jars.packages", 
                                                                  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1").getOrCreate()
    df_collectd=get_collectd(spark)
    
    final_df_nodos,final_df_turnos=get_especificaciones(spark)

    final_df_with_nodes=assign_node_group(df_collectd,final_df_nodos)

    final_df_with_nodes_shifts=assign_shift(final_df_with_nodes,final_df_turnos)

     # Obtener el timestamp actual
    current_time = F.current_timestamp()
    
    # Calcular las 02:00 AM del día actual
    start_of_day = F.date_trunc('day', current_time)
    two_am_today = start_of_day + F.expr('INTERVAL 2 HOURS')
    
    # Si es después de las 02:00 AM, usar el timestamp de las 02:00 AM de hoy
    # Si es antes, usar el timestamp de las 02:00 AM del día anterior
    cutoff_time = F.when(current_time >= two_am_today, two_am_today).otherwise(start_of_day - F.expr('INTERVAL 22 HOURS'))
    
    # Filtrar por las últimas 24 horas hasta las 02:00 AM
    filtered_df = final_df_with_nodes_shifts.filter((final_df_with_nodes_shifts['fecha'] < cutoff_time) & (final_df_with_nodes_shifts['fecha'] >= (cutoff_time - F.expr('INTERVAL 24 HOURS'))))
    
    metrics_df=get_metrics(filtered_df)

    """
    # Comprobación de que carga la fecha y nodo respectivas
    fecha="2024-03-30T11:00"
    nodo="edge01"
    
    example_df = metrics_df.filter(
        (metrics_df.fecha == fecha) & (metrics_df.nodo == nodo)
    )

    example_df.show(10,False)

    """
    
    #Conexion HDFS
    hdfs_path = "/datos/arq-lambda/datos-g2/agg_metrics.parquet"

    # Write the DataFrame to HDFS in Parquet format
    metrics_df.write.mode("append").parquet(hdfs_path)
    
    # close spark session
    spark.stop()

