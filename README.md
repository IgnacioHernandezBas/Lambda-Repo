# Lambda-Repo
Design and Implementation of a Lambda Architecture using Big Data Tools
## Introduction
First of all, a Lambda architecture is a data processing architecture designed for batch and real-time processing created to surpass the CAP theorem. It is divided in three different layers:
<ul>
  <li><b>Batch Layer</b>: Layer oriented for the processing of large volumes of data. High latency and usually used with historical data</li>
  <li><b>Speed Layer</b>: Real-Time processing of data streams. Low latency that provides immediate responses and up-to-date results </li>
  <li><b>Serving Layer</b>: Layer that displays the results of the Batch and RT processing </li>
</ul>

![image](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/cedfb0dd-ad0f-4ecb-89a2-45ffacee2c00)

## Design 
In order to deploy this architecture it is important to know which tools are the most suited for the desired task. It is needed to work in a cluster because the architecture is going to process large volumes of data so topics like scalability, fault tolerance, resource utilización, and parallel processing are key to recreating a realistic data processing architecture that could be used in real applications.

![image](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/edd2b6dc-ce65-4c25-b7bc-90c50db4c99b)

+ Collectd is an Unix daemon that will transfer the data related to the performance of the cluster nodes, using Kafka the data will be ingested in both layers </li> 

+ Spark and Spark Streaming have been chosen for the batch and real-time processing respectively, other options could have been chosen, but using these technologies reduces the complexity and time cost of having to set up and configure two different processing tools.

+ For data storage, Cassandra has been chosen for storing the real-time processed data and HDFS for the batch views, HIVE will be needed in order to consult the data stored in HDFS.

## Implementation

### Ingestion
The data form collectd will be available to read by the processing layers using a topic from kafka. Here is an example:

![image](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/82800bee-abe4-4216-ae09-cfe1d3abcebf)

This information represents the output collectd regarding one node of the cluster (worker01) and additional data like the CPU usage (metric) and timestamp. The processing layers will be in charge of using this data to assign the corresponding group of the cluster node efficiently and its shift depending on its timestamp. This assignment is arbitrary and defined in another topic called "especificaciones" with the node groups and shifts:

![image](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/dd6270a9-7f2e-4aa4-bb28-a4b4e86434de)

### Processing

+ Script for real-time processing: real_time_proc.py
  
+ Script for batch processing: batch_proc.py (this script is an example that only records only one day of the data for small latency purposes)

Example of the resulting assignments for one node:

![image](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/212be37d-f0e6-4762-9bcf-018db4b580da)

*** Bear in mind that the processing stage could be more complex, but the main objective of this project is to create a functioning architecture that interconnects the multiple Big Data tools correctly 


### Reading and testing from the serving layer

Retrieves the data previously stored from the respective databases and shows the assignments done for a specific node and date

+ Execution command for real-time testing: python3 run_rt_test.py worker01 2024-03-20T22:00
  
+ Script for batch processing:  python3 run_batch_test.py worker01 2024-03-20T22:00


# Repositorio Lambda
Diseño e Implementación de una Arquitectura Lambda utilizando Herramientas de Big Data
## Introducción
En primer lugar, una arquitectura Lambda es una arquitectura de procesamiento de datos diseñada para el procesamiento por lotes y en tiempo real creada para superar el teorema CAP. Se divide en tres capas diferentes:
<ul>
  <li><b>Capa de Lotes</b>: Capa orientada al procesamiento de grandes volúmenes de datos. Alta latencia y generalmente utilizada con datos históricos</li>
  <li><b>Capa de Velocidad</b>: Procesamiento en tiempo real de flujos de datos. Baja latencia que proporciona respuestas inmediatas y resultados actualizados </li>
  <li><b>Capa de Servicio</b>: Capa que muestra los resultados del procesamiento por lotes y en tiempo real </li>
</ul>

![imagen](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/cedfb0dd-ad0f-4ecb-89a2-45ffacee2c00)

## Diseño 
Para implementar esta arquitectura es importante conocer qué herramientas son las más adecuadas para la tarea deseada. Es necesario trabajar en un clúster porque la arquitectura va a procesar grandes volúmenes de datos, por lo que temas como escalabilidad, tolerancia a fallos, utilización de recursos y procesamiento paralelo son clave para recrear una arquitectura de procesamiento de datos realista que podría ser utilizada en aplicaciones reales.

![imagen](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/edd2b6dc-ce65-4c25-b7bc-90c50db4c99b)

+ Collectd es un demonio de Unix que transferirá los datos relacionados con el rendimiento de los nodos del clúster, utilizando Kafka los datos serán ingestados en ambas capas </li> 

+ Se ha elegido Spark y Spark Streaming para el procesamiento por lotes y en tiempo real respectivamente, podrían haberse elegido otras opciones, pero el uso de estas tecnologías reduce la complejidad y el costo temporal de tener que configurar y configurar dos herramientas de procesamiento diferentes.

+ Para el almacenamiento de datos, se ha elegido Cassandra para almacenar los datos procesados en tiempo real y HDFS para las vistas por lotes, HIVE será necesario para consultar los datos almacenados en HDFS.

## Implementación

### Ingestión
Los datos de collectd estarán disponibles para ser leídos por las capas de procesamiento utilizando un tema de kafka. Aquí hay un ejemplo:

![imagen](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/82800bee-abe4-4216-ae09-cfe1d3abcebf)

Esta información representa la salida de collectd con respecto a un nodo del clúster (worker01) y datos adicionales como el uso de la CPU (métrica) y la marca de tiempo. Las capas de procesamiento se encargarán de utilizar estos datos para asignar el grupo correspondiente del nodo del clúster de manera eficiente y su turno dependiendo de su marca de tiempo. Esta asignación es arbitraria y está definida en otro tema llamado "especificaciones" con los grupos de nodos y turnos:

![imagen](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/dd6270a9-7f2e-4aa4-bb28-a4b4e86434de)

### Procesamiento

+ Script para procesamiento en tiempo real: real_time_proc.py
  
+ Script para procesamiento por lotes: batch_proc.py (este script es un ejemplo que registra solo un día de datos por propósitos de baja latencia)

Ejemplo de las asignaciones resultantes para un nodo:

![imagen](https://github.com/IgnacioHernandezBas/Lambda-Repo/assets/91118338/212be37d-f0e6-4762-9bcf-018db4b580da)

*** Ten en cuenta que la etapa de procesamiento podría ser más compleja, pero el objetivo principal de este proyecto es crear una arquitectura funcional que interconecte las múltiples herramientas de Big Data correctamente 


### Lectura y pruebas desde la capa de servicio

Recupera los datos almacenados previamente de las bases de datos respectivas y muestra las asignaciones realizadas para un nodo específico y fecha

+ Comando de ejecución para pruebas en tiempo real: python3 run_rt_test.py worker01 2024-03-20T22:00
  
+ Script para procesamiento por lotes:  python3 run_batch_test.py worker01 2024-03-20T22:00











