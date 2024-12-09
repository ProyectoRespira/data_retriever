# Proyecto Respira pipelines
El proyecto consta de un total de 17 pipelines de datos, los cuales se agrupan de la siguiente manera:

## Inicialización del proyecto

- `initialize_database`: es responsable de crear e inicializar las tablas esenciales para el correcto funcionamiento del sistema de monitoreo y predicción de calidad del aire. Este proceso genera un total de 16 tablas en la base de datos PostgreSQL, las cuales son descritas a continuación:

  - `regions`: Almacena información sobre las regiones monitoreadas, como su nombre, código, y datos relacionados.
    - Se almacenan por defecto los datos de la región de Gran Asunción. Para editar o agregar otras regiones, puedes editar el bloque [`create_table_regions.sql`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_regions.sql)
  - `stations`: Contiene los datos de las estaciones de monitoreo y estaciones de calibración, incluyendo su ubicación y estado.
    - Se almacenan por defecto los datos de la Red de Monitoreo de FIUNA y de la estación de monitoreo de la Embajada de Estados Unidos en Paraguay. Para editar o agregar otras estaciones, puedes editar el bloque [`create_table_stations.sql`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_stations.sql)
  - `weather_stations`: Guarda información de las estaciones meteorológicas asociadas.
    - Se almacenan por defecto los datos de la estación meteorológica del Aeropuerto Silvio Petirossi en Asunción. Para editar o agregar otras estaciones, puedes editar el bloque [`create_table_weather_stations`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_weather_stations.sql)
  - `station_readings_bronze`: Registra los datos iniciales no procesados provenientes de las estaciones de monitoreo calidad del aire.
  - `station_readings_silver`: Contiene datos limpios e interpolados de las estaciones de monitoreo para análisis.
  - `station_readings_gold`: Incluye datos calibrados y agregados para reportes avanzados y análisis de calidad del aire.
  - `airnow_readings_bronze`: Almacena lecturas iniciales no procesadas provenientes de estaciones patrón.
  - `airnow_readings_silver`: Guarda lecturas limpias e interpoladas de las estaciones patrón.
  - `weather_readings_bronze`: Contiene lecturas iniciales no procesadas de las estaciones meteorológicas.
  - `weather_readings_silver`: Guarda lecturas limpias e interpoladas de las estaciones meteorológicas.
  - `weather_readings_gold`: Agrega y transforma los datos meteorológicos para análisis avanzados.
  - `region_readings`: Calcula métricas agregadas a nivel regional, como promedios y desviaciones de calidad del aire.
  - `calibration_factors`: Registra los factores de calibración utilizados para ajustar las lecturas de las estaciones.
  - `inference_runs`: Almacena la metadata de las ejecuciones de modelos de predicción.
  - `inference_results`: Contiene los resultados de las predicciones, incluyendo pronósticos y métricas derivadas.
  - `health_checks`: Realiza seguimiento al estado de las estaciones, determinando si están activas según las lecturas recientes.

Cada tabla cumple una función específica dentro del sistema, asegurando la correcta gestión y procesamiento de los datos necesarios para el monitoreo, análisis y predicción de la calidad del aire.

## Pipelines de datos de FIUNA
- `etl_fiuna_bronze`: Extrae datos en crudo de la base de datos de la [Red de Monitoreo de Material Particulado MP2,5 y MP10 de la Facultad de Ingeniería de la Universidad Nacional de Asunción](https://www.ing.una.py/?page_id=45577) y los inserta en la tabla `station_readings_bronze`.

- `etl_fiuna_silver`:
  - Valida e interpola los datos en crudo de los sensores.
  - Marca las lecturas ya procesadas en la tabla `station_readings_bronze`
  
- `etl_fiuna_gold_measurements`:
  - Realiza un cambio de frecuencia de las mediciones de las estaciones de 5 minutos a 1 hora, promediando los datos en intervalos horarios.
  - Calibra las mediciones de material particulado mediante factores de calibración y ajustes basados en la humedad relativa para mejorar la precisión de las lecturas.
  - Marca las lecturas ya procesadas en la tabla `station_readings_silver`

- `etl_fiuna_gold_aqi_stats`:
  - Calcula dentro de la tabla `station_readings_gold` el Índice de Calidad del Aire para cada estación de monitoreo, junto a diversas variables estadísticas necesarias para realizar predicciones.
  
- `etl_fiuna_regional_stats`:
  - Transforma los datos de la tabla `station_readings_gold` para obtener promedios regionales y otros valores estadísticos de cada región de monitoreo necesarias para realizar predicciones.

## Pipelines de Inferencia
