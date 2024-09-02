# ¿Cómo levantamos el proyecto?:

## First run 

**1. Inicialización de la base de datos:**
- Primero corremos initialize_database para configurar las tablas y estructuras iniciales.

**2. Backfills de datos para Airnow y Meteostat Bronze:**
- A continuación, realizamos los backfills de datos para las tablas Bronze de Airnow y Meteostat. Los bloques de extracción en los pipelines de Bronze contienen condiciones específicas según el tipo de ejecución definido:
``` python
execution_type = kwargs['execution_type']

if execution_type == 'incremental':
    end_date_utc = kwargs['execution_date']  # Fecha actual
    start_date_utc = end_date_utc

elif execution_type == 'backfill_year':  # Caso backfill anual
    start_date_utc = kwargs['execution_date']
    end_date_utc = start_date_utc + relativedelta(years=1)
```
**3. Ejecución de los pipelines Silver y Gold para Meteostat y Airnow:**
- Los pipelines Silver se activan automáticamente al finalizar los pipelines Bronze, y los Gold una vez que finalizan los pipelines Silver. Estos pipelines están configurados para extraer todos los datos que se encuentran en la tabla anterior y no en la actual, basándose en la columna de fecha.
- Aquí utilizo COALESCE para cubrir el caso en el que Silver está vacío (es decir, no hay un MAX(silver.date_utc)). Esto asegura que en ese primer procesamiento masivo, se traigan todos los registros disponibles en Bronze, facilitando la integración completa de datos desde el inicio.

**4. FIUNA Bronze y Silver:**
- Una vez completados los backfills de Airnow y Meteostat hasta Gold (donde también se usa COALESCE para asegurar que todos los datos en Silver se pasen a Gold si están vacíos), traemos los datos de FIUNA y ejecutamos los pipelines de FIUNA Bronze y Silver. 
- Los pipeline Bronze y Silver de FIUNA buscan traer datos nuevos mirando la columna 'id' de la BD remota de FIUNA (para Bronze) y 'measurement_id' de Bronze (para Silver). 

**5. Generación de Calibration Factors:**
- Con los datos de Airnow y Meteostat en Gold y FIUNA en Silver, realizamos el backfill de los Calibration Factors.

**6. Backfill de FIUNA Gold:**
- Usando los Calibration Factors obtenidos, procesamos los datos de FIUNA hasta la etapa Gold.

**7. Ejecución continua con triggers:**
- Una vez completados estos pasos, el sistema debería estar listo para funcionar con los triggers definidos: ejecuciones horarias para ETL, diarias para Health Checks y mensuales para Calibration.
