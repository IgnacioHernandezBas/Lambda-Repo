"""
Usage: 
source /opt/share/anaconda/start_env.sh
python3 /home/alumnos/mbd45/arq-lambda/run_agg_tests.py node date
"""
import argparse
from datetime import datetime
from pyspark.sql import SparkSession

def main(args):
    nodo = args.nodo
    fecha = args.fecha

    spark = SparkSession.builder \
        .appName("Read Parquet to PySpark DF") \
        .getOrCreate()

    hdfs_path = "/datos/arq-lambda/datos-g2/agg_metrics.parquet"
    df = spark.read.parquet(hdfs_path)
    filtered_df = df.filter(
        (df.fecha == fecha) & (df.nodo == nodo)
    )
    filtered_df.show()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calcula las estadísticas agregadas para un determinado nodo en una determinada fecha"
    )
    parser.add_argument("nodo", help="Nodo, e.g. edge01")
    parser.add_argument("fecha", help="Fecha en formato YYYY-MM-DDTHH::MM")
    args = parser.parse_args()
    main(args)
