"""
DAG 3: PROCESAMIENTO PARALELO CON DASK
======================================
PROPÓSITO: Procesar datos NYC Taxi usando Dask (procesamiento paralelo)
ORDEN: TERCERO - después del procesamiento secuencial
"""

from datetime import datetime, timedelta
import json
from pathlib import Path
import time

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'dag_03_dask_simple',
    default_args=default_args,
    description='DAG 3: Procesamiento paralelo con Dask',
    schedule_interval=None,
    catchup=False,
)


def parallel_processing_dask(**context):
    """DAG 3: Procesamiento paralelo con Dask - ANÁLISIS COMPLETO COMO SECUENCIAL"""
    print("DAG 3: PROCESAMIENTO PARALELO CON DASK - ANÁLISIS COMPLETO")
    print("=" * 65)

    data_dir = Path("/workspace/data/nyc_taxi")
    if not data_dir.exists():
        raise Exception("No hay datos! Ejecuta dag_01_download_data primero")

    # Usar Dask para procesamiento paralelo
    import dask.dataframe as dd
    import pandas as pd
    import numpy as np

    start_time = time.time()

    # Leer todos los archivos con Dask
    parquet_files = list(data_dir.glob("*.parquet"))
    print(f"🔍 Archivos encontrados: {len(parquet_files)}")

    if not parquet_files:
        raise Exception("No hay archivos .parquet para procesar")

    # Leer con Dask (procesamiento paralelo automático) - solo columnas necesarias
    columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'fare_amount', 'total_amount',
               'PULocationID', 'DOLocationID', 'payment_type', 'tip_amount', 'passenger_count']

    print("📊 Cargando datos con Dask...")
    ddf = dd.read_parquet(str(data_dir / "*.parquet"), columns=columns)

    print("🔄 Realizando análisis paralelo con Dask...")

    # ===== ANÁLISIS COMPLETO CON DASK =====

    # 1. Estadísticas básicas
    total_rows = len(ddf)
    avg_fare = ddf['fare_amount'].mean()
    avg_distance = ddf['trip_distance'].mean()
    std_distance = ddf['trip_distance'].std()

    # 2. Percentiles (más complejos en Dask)
    fare_percentiles = ddf['fare_amount'].quantile([0.25, 0.5, 0.75, 0.95])
    distance_percentiles = ddf['trip_distance'].quantile(
        [0.25, 0.5, 0.75, 0.95])

    # 3. Análisis temporal con Dask
    ddf['pickup_hour'] = dd.to_datetime(ddf['tpep_pickup_datetime']).dt.hour
    ddf['pickup_day_of_week'] = dd.to_datetime(
        ddf['tpep_pickup_datetime']).dt.dayofweek

    # Calcular duración del viaje
    pickup_dt = dd.to_datetime(ddf['tpep_pickup_datetime'])
    dropoff_dt = dd.to_datetime(ddf['tpep_dropoff_datetime'])
    ddf['trip_duration_minutes'] = (
        dropoff_dt - pickup_dt).dt.total_seconds() / 60

    # Análisis por hora con Dask
    hourly_stats = ddf.groupby('pickup_hour').agg({
        'fare_amount': ['count', 'mean'],
        'trip_distance': 'mean',
        'trip_duration_minutes': 'mean'
    })

    # 4. Análisis geográfico con Dask
    pickup_zones = ddf['PULocationID'].value_counts().nlargest(20)
    dropoff_zones = ddf['DOLocationID'].value_counts().nlargest(20)

    # 5. Análisis de tipos de pago con Dask
    payment_counts = ddf['payment_type'].value_counts()
    payment_tips = ddf.groupby('payment_type')[
        'tip_amount'].agg(['count', 'mean'])

    print("⚡ Ejecutando computación paralela...")

    # EJECUTAR TODAS LAS COMPUTACIONES EN PARALELO
    results_computed = dd.compute(
        total_rows, avg_fare, avg_distance, std_distance,
        fare_percentiles, distance_percentiles,
        hourly_stats, pickup_zones, dropoff_zones,
        payment_counts, payment_tips
    )

    # Extraer resultados computados
    (total_rows, avg_fare, avg_distance, std_distance,
     fare_perc, dist_perc, hourly, pickup_top, dropoff_top,
     pay_counts, pay_tips) = results_computed

    end_time = time.time()
    processing_time = end_time - start_time

    # ===== ESTRUCTURAR RESULTADOS COMO EL SECUENCIAL =====

    # Análisis por hora estructurado
    hourly_analysis = {}
    for hour in range(24):
        if hour in hourly.index:
            hourly_analysis[f"hour_{hour}"] = {
                'trips': int(hourly.loc[hour, ('fare_amount', 'count')]),
                'avg_fare': float(hourly.loc[hour, ('fare_amount', 'mean')]),
                'avg_distance': float(hourly.loc[hour, ('trip_distance', 'mean')]),
                'avg_duration': float(hourly.loc[hour, ('trip_duration_minutes', 'mean')])
            }

    # Análisis geográfico estructurado
    geographic_analysis = {
        'top_pickup_zones': {f"zone_{zone_id}": int(count) for zone_id, count in pickup_top.items()},
        'top_dropoff_zones': {f"zone_{zone_id}": int(count) for zone_id, count in dropoff_top.items()}
    }

    # Análisis de tipos de pago estructurado
    payment_analysis = {
        'payment_distribution': {f"type_{pay_type}": int(count) for pay_type, count in pay_counts.items()},
        'tip_by_payment': {
            f"type_{pay_type}": {
                'count': int(pay_tips.loc[pay_type, 'count']),
                'avg_tip': float(pay_tips.loc[pay_type, 'mean'])
            } for pay_type in pay_tips.index
        }
    }

    # Preparar resultados completos (formato similar al secuencial)
    results = {
        'timestamp': datetime.now().isoformat(),
        'dag': 'dag_03_dask_simple',
        'method': 'dask_parallel',
        'execution_type': 'paralelo_dask_completo',
        'total_time': processing_time,
        'total_rows': int(total_rows),
        'files_processed': len(parquet_files),
        'throughput': total_rows / processing_time,
        'avg_time_per_file': processing_time / len(parquet_files),

        # Análisis detallado como el secuencial
        'analysis': {
            'advanced_stats': {
                'rows': int(total_rows),
                'avg_distance': float(avg_distance),
                'std_distance': float(std_distance),
                'avg_fare': float(avg_fare),
                'percentiles_distance': [float(x) for x in dist_perc.values],
                'percentiles_fare': [float(x) for x in fare_perc.values]
            },
            'hourly_analysis': hourly_analysis,
            'geographic_analysis': geographic_analysis,
            'payment_analysis': payment_analysis
        },

        # Métricas básicas para comparación
        'metrics': {
            'avg_fare': float(avg_fare),
            'avg_distance': float(avg_distance)
        }
    }

    # Guardar resultados
    results_dir = Path("/workspace/results")
    results_dir.mkdir(exist_ok=True)

    results_file = results_dir / "dask_simple_results.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)

    # Mostrar resumen completo
    print(f"\n🎯 DAG 3: COMPLETADO - Procesamiento paralelo COMPLETO")
    print(f"⚡ Tiempo total: {processing_time:.2f}s")
    print(f"📊 Filas procesadas: {total_rows:,}")
    print(f"🚀 Throughput: {results['throughput']:,.0f} filas/s")
    print(f"📁 Archivos procesados: {len(parquet_files)}")
    print(
        f"⏱️ Tiempo promedio por archivo: {processing_time/len(parquet_files):.2f}s")

    # Mostrar algunos análisis clave
    print(f"\n📈 ANÁLISIS COMPLETADOS:")
    print(f"   • Estadísticas avanzadas (percentiles, std dev)")
    print(f"   • Análisis temporal por hora (24 horas)")
    print(f"   • Top 20 zonas de pickup y dropoff")
    print(f"   • Análisis de tipos de pago y propinas")
    print(
        f"   • Métricas: Tarifa ${avg_fare:.2f}, Distancia {avg_distance:.2f} mi")

    print(f"\n📄 Reporte completo: {results_file}")
    print(f"🔄 Listo para comparación con secuencial!")

    return results


# Tarea
dask_task = PythonOperator(
    task_id='parallel_processing',
    python_callable=parallel_processing_dask,
    dag=dag,
)

dag.doc_md = """
# DAG 3: Procesamiento Paralelo (Dask) - ANÁLISIS COMPLETO

**ORDEN: EJECUTAR TERCERO**

## Prerrequisitos
- ✅ dag_01_download_data completado
- ✅ dag_02_sequential completado

## Método
- **Dask**: Procesamiento distribuido/paralelo automático
- **Paralelización**: Múltiples workers procesan datos simultáneamente
- **Análisis Completo**: Mismos análisis que el secuencial para comparación justa

## Análisis Incluidos
- ✅ Estadísticas avanzadas (percentiles, desviación estándar)
- ✅ Análisis temporal por hora (24 horas)
- ✅ Análisis geográfico (top 20 zonas pickup/dropoff)
- ✅ Análisis de tipos de pago y propinas

## Archivo Generado
- `dask_simple_results.json` (con análisis completo)

## Siguiente
- ▶️ DAG 4: Comparación y gráficos técnicos
"""
