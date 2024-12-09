# Proyecto Respira pipelines
El proyecto consta de un total de 17 pipelines de datos, los cuales se agrupan de la siguiente manera:
- **@once**: Estos procesos se corren una sola vez al iniciar el proyecto
- **@hourly**: Estos procesos deben correr una vez por hora.
- **@monthly**: Estos procesos deben correr una vez al mes.
- **@daily**: Estos procesos deben correr una vez al día.

## @once: Inicialización del proyecto

- `initialize_database`: es responsable de crear e inicializar las tablas esenciales para el correcto funcionamiento del sistema de monitoreo y predicción de calidad del aire. Este proceso genera un total de 16 tablas en la base de datos PostgreSQL, las cuales son descritas a continuación:

  - Tabla `regions`: Almacena información sobre las regiones monitoreadas, como su nombre, código, y datos relacionados.
    - Se almacenan por defecto los datos de la región de Gran Asunción. Para editar o agregar otras regiones, puedes editar el bloque [`create_table_regions.sql`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_regions.sql)
  - Tabla `stations`: Contiene los datos de las estaciones de monitoreo y estaciones de calibración, incluyendo su ubicación y estado.
    - Se almacenan por defecto los datos de la Red de Monitoreo de FIUNA y de la estación de monitoreo de la Embajada de Estados Unidos en Paraguay. Para editar o agregar otras estaciones, puedes editar el bloque [`create_table_stations.sql`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_stations.sql)
  - Tabla `weather_stations`: Guarda información de las estaciones meteorológicas asociadas.
    - Se almacenan por defecto los datos de la estación meteorológica del Aeropuerto Silvio Petirossi en Asunción. Para editar o agregar otras estaciones, puedes editar el bloque [`create_table_weather_stations`](https://github.com/ProyectoRespira/data_retriever/blob/documentation/etl-pipeline/custom/create_table_weather_stations.sql)
  - Tabla `station_readings_bronze`: Registra los datos iniciales no procesados provenientes de las estaciones de monitoreo calidad del aire.
  - Tabla `station_readings_silver`: Contiene datos limpios e interpolados de las estaciones de monitoreo para análisis.
  - Tabla `station_readings_gold`: Incluye datos calibrados y agregados para reportes avanzados y análisis de calidad del aire.
  - Tabla `airnow_readings_bronze`: Almacena lecturas iniciales no procesadas provenientes de estaciones patrón.
  - Tabla `airnow_readings_silver`: Guarda lecturas limpias e interpoladas de las estaciones patrón.
  - Tabla `weather_readings_bronze`: Contiene lecturas iniciales no procesadas de las estaciones meteorológicas.
  - Tabla `weather_readings_silver`: Guarda lecturas limpias e interpoladas de las estaciones meteorológicas.
  - Tabla `weather_readings_gold`: Agrega y transforma los datos meteorológicos para análisis avanzados.
  - Tabla `region_readings`: Calcula métricas agregadas a nivel regional, como promedios y desviaciones de calidad del aire.
  - Tabla `calibration_factors`: Registra los factores de calibración utilizados para ajustar las lecturas de las estaciones.
  - Tabla `inference_runs`: Almacena la metadata de las ejecuciones de modelos de predicción.
  - Tabla `inference_results`: Contiene los resultados de las predicciones, incluyendo pronósticos y métricas derivadas.
  - Tabla `health_checks`: Realiza seguimiento al estado de las estaciones, determinando si están activas según las lecturas recientes.

Cada tabla cumple una función específica dentro del sistema, asegurando la correcta gestión y procesamiento de los datos necesarios para el monitoreo, análisis y predicción de la calidad del aire.

![data_retriever_v4(3)](https://github.com/user-attachments/assets/e1203340-85c2-4a4b-b12d-3348f5e00e16)

## @hourly
### Datos de Estaciones Meteorológicas (@hourly)
- `etl_meteostat_bronze`: Extrae datos meteorológicos en crudo de las estaciones de [Meteostat](https://dev.meteostat.net/python/#installation) registradas en la tabla `weather_stations` y las inserta en la tabla `weather_readings_bronze`

- `etl_meteostat_silver`:
  - Valida e interpola los datos en crudo de meteostat y las inserta en la tabla `weather_readings_silver`.
  - Marca las lecturas ya procesadas en la tabla `weather_readings_bronze`.

- `etl_meteostat_gold`:
  - Transforma los datos de la tabla `weather_readings_silver` para su uso en el proceso de inferencia.
  - Marca las lecturas ya procesadas en la tabla `weather_readings_silver`

![data_retriever_weather](https://github.com/user-attachments/assets/ab02437d-cc7e-47ee-9bd3-0105412f2b15)
 
### Datos de Estaciones Patrón (@hourly)
- `etl_airnow_bronze`: Extrae datos de polución en crudo de las estaciones patrón del servicio [Airnow](https://www.airnow.gov/) registradas en la tabla `stations` y las inserta en la tabla `airnow_readings_bronze`.

- `etl_airnow_silver`:
  - Valida e interpola los datos en crudo de airnow y las inserta en la tabla `airnow_readings_silver`.
  - Marca las lecturas ya procesadas en la tabla `airnow_readings_bronze`.

- `etl_airnow_gold`:
  - Transforma los datos de la tabla `airnow_readings_silver` para su uso en el proceso de inferencia y las inserta en la tabla `station_readings_gold`.
  - Marca las lecturas ya procesadas en la tabla `airnow_readings_silver`.
 
![data_retriever_airnow](https://github.com/user-attachments/assets/18c846ed-25d3-41ec-95b4-aab3066fb7a1)

 
### Datos de Estaciones de Monitoreo (@hourly)
- `etl_fiuna_bronze`:
  - Extrae datos en crudo de la base de datos de la [Red de Monitoreo de Material Particulado MP2,5 y MP10 de la Facultad de Ingeniería de la Universidad Nacional de Asunción](https://www.ing.una.py/?page_id=45577) y los inserta en la tabla `station_readings_bronze`.
  - **Nota:** Para extraer datos en crudo de otras estaciones de monitoreo, es necesario crear un nuevo pipeline adaptado a la nueva fuente de datos.

- `etl_fiuna_silver`:
  - Valida e interpola los datos en crudo de los sensores.
  - Marca las lecturas ya procesadas en la tabla `station_readings_bronze`
  -  **Nota:** Para procesar datos en crudo de otras estaciones de monitoreo, es necesario crear un nuevo pipeline adaptado a la nueva fuente de datos y transformar los datos de acuerdo a la estructura de la tabla `station_readings_silver`.
  
- `etl_fiuna_gold_measurements`:
  - Realiza un cambio de frecuencia de las mediciones de las estaciones de 5 minutos a 1 hora, promediando los datos en intervalos horarios.
  - Calibra las mediciones de material particulado mediante factores de calibración y ajustes basados en la humedad relativa para mejorar la precisión de las lecturas.
  - Marca las lecturas ya procesadas en la tabla `station_readings_silver`.

- `etl_fiuna_gold_aqi_stats`:
  - Calcula dentro de la tabla `station_readings_gold` el Índice de Calidad del Aire para cada estación de monitoreo, junto a diversas variables estadísticas necesarias para realizar predicciones.
  
- `etl_fiuna_regional_stats`:
  - Transforma los datos de la tabla `station_readings_gold` para obtener promedios regionales y otros valores estadísticos de cada región de monitoreo necesarias para realizar predicciones.
 
![data_retriever_fiuna](https://github.com/user-attachments/assets/b0c05ec8-bab4-4713-be39-aeacc4468007)


### Pipeline de Inferencia
- `inference_results`:
  - Registra los metadatos del proceso de inferencia en la tabla `inference_runs`.
  - Extrae datos de las tablas `station_readings_gold`, `region_readings` y `weather_readings_gold`.
  - Carga los modelos de predicción y realiza la inferencia para cada estación de monitoreo.
  - Guarda los resultados en la tabla `inference_results`
  
![data_retriever_inference](https://github.com/user-attachments/assets/e21655ae-9318-434c-9fb8-2e403e06d08d)

## @monthly
### Calibración remota 
El sistema está preparado para calcular factores de calibración de las estaciones de monitoreo de una misma región utilizando datos de una **estación patrón** y datos de humedad relativa provenientes de las **estaciones meteorológicas**.
- `calibration_factors`:
  - Extrae los últimos 3 meses de datos de polución de cada **estación de monitoreo** (`station_readings_silver`), su correspondiente **estación patrón** (`station_readings_gold`) y datos de humedad de su correspondiente **estación meteorológica** (`weather_readings_gold`).
  - Realiza ajustes de humedad a las lecturas de las estaciones de monitoreo utilizando la estrategia propuesta por [Crilley et. al](https://amt.copernicus.org/articles/11/709/2018/).
  - Calcula el factor de amplificación de la señal del sensor de la estación de monitoreo y lo inserta en la tabla `calibration_factors`.
  - Este factor es utilizado en el proceso `etl_fiuna_gold_measurements` para corregir las lecturas de las estaciones de monitoreo.

 ![data_retriever_calibration](https://github.com/user-attachments/assets/4e540bed-578d-40e0-9d9b-6ef4c51e9703)

 ## @daily: Bots
 El diseño de los bots está basado en el proyecto [linkaBot](https://github.com/melizeche/linkaBot/tree/main) para el proyecto [AireLibre](https://github.com/melizeche/AireLibre)

 - `telegram_bot` y `twitter_bot`:
   - Extrae los metadatos de la última inferencia registrada en `inference_results`.
   - Calcula los promedios regionales de las predicciones.
   - Construye y envía mensajes diarios en Telegram y X (antes Twitter)

  
