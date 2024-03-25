import sys
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py nodo fecha [yyyy-mm-ddThh:mi]")
        sys.exit(1)

    nodo = sys.argv[1]
    cadena_fecha = sys.argv[2]

    fecha_objeto = datetime.strptime(cadena_fecha, '%Y-%m-%dT%H:%M')
    fecha_objeto = fecha_objeto.replace(tzinfo=timezone.utc)  

    ts_desde = int(fecha_objeto.timestamp()*1000)
    ts_hasta = int(fecha_objeto.timestamp()*1000 + 60000)

    print(f"ts_desde (ms desde epoch): {ts_desde}")
    print(f"ts_hasta (ms desde epoch): {ts_hasta}")

    cluster = Cluster(['192.168.80.34'], port=9042)
    session = cluster.connect('grupo2')

    session.row_factory = dict_factory

    query = f"SELECT * FROM mediciones WHERE nodo='{nodo}' AND fecha >= {ts_desde} AND fecha < {ts_hasta} ALLOW FILTERING"
    print(f"Query: {query}")

    print("Ejecutando consulta...")
    rows = session.execute(query)

    results = list(rows)

    for row in results:
        print(row)

    row_count = len(results)
    print(f"Consulta ejecutada. Número de filas devueltas: {row_count}")

    session.shutdown()
    cluster.shutdown()
