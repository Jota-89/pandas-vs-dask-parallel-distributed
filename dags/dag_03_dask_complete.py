"""
DAG 3: PROCESAMIENTO PARALELO CON DASK - ANÁLISIS 100% IGUAL AL SECUENCIAL
==========================================================================
PROPÓSITO: Procesar datos NYC Taxi usando Dask con EXACTAMENTE los mismos análisis por archivo
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
    'dag_03_dask_complete',
    default_args=default_args,
    description='DAG 3: Procesamiento paralelo completo (100% igual al secuencial)',
    schedule_interval=None,
    catchup=False,
)


def parallel_processing_dask_complete(**context):
    """DAG 3: Procesamiento paralelo con Dask - ANÁLISIS 100% IGUAL AL SECUENCIAL"""
    print("DAG 3: PROCESAMIENTO PARALELO CON DASK - ANÁLISIS 100% COMPLETO")
    print("=" * 70)

    data_dir = Path("/workspace/data/nyc_taxi")
    if not data_dir.exists():
        raise Exception("No hay datos! Ejecuta dag_01_download_data primero")

    import dask.dataframe as dd
    import pandas as pd
    import numpy as np
    from dask.distributed import Client

    # CONFIGURACIÓN AGRESIVA PARA MÁXIMO RENDIMIENTO
    import dask
    dask.config.set({
        'array.chunk-size': '512MiB',
        'dataframe.shuffle.method': 'p2p',
        'distributed.worker.memory.target': 0.8,
        'distributed.worker.memory.spill': 0.9,
        'distributed.worker.memory.pause': 0.95,
        'distributed.nanny.pre-spawn-environ.MALLOC_TRIM_THRESHOLD_': '0'
    })

    # Conectar al cluster Dask optimizado
    try:
        client = Client('tcp://localhost:8786')
        print(
            f"🚀 Cluster Dask conectado: {len(client.scheduler_info()['workers'])} workers")
        print(
            f"🧠 Memoria total cluster: {sum(w['memory_limit'] for w in client.scheduler_info()['workers'].values()) / (1024**3):.1f} GB")
    except:
        print("⚠️ Usando configuración local fallback")
        client = None

    start_time = time.time()

    # Obtener archivos (en el mismo orden que secuencial)
    parquet_files = sorted(list(data_dir.glob("*.parquet")))
    print(f"🔍 Archivos encontrados: {len(parquet_files)}")

    if not parquet_files:
        raise Exception("No hay archivos .parquet para procesar")

    # Columnas necesarias (igual que secuencial)
    columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'fare_amount', 'total_amount',
               'PULocationID', 'DOLocationID', 'payment_type', 'tip_amount', 'passenger_count']

    print("📊 Procesando cada archivo individualmente con Dask (igual que secuencial)...")

    # ===== ANÁLISIS POR ARCHIVO INDIVIDUAL =====
    file_results = []
    total_rows = 0

    for i, file_path in enumerate(parquet_files, 1):
        file_start = time.time()
        print(f"   📄 Archivo {i}/10: {file_path.name}")

        # Leer archivo individual con Dask
        ddf_file = dd.read_parquet(str(file_path), columns=columns)

        # ===== ANÁLISIS INTENSIVO POR ARCHIVO (IGUAL QUE SECUENCIAL) =====

        # 1. Estadísticas avanzadas
        file_rows = len(ddf_file)
        file_avg_distance = ddf_file['trip_distance'].mean()
        file_std_distance = ddf_file['trip_distance'].std()
        file_avg_fare = ddf_file['fare_amount'].mean()
        file_distance_percentiles = ddf_file['trip_distance'].quantile(
            [0.25, 0.5, 0.75, 0.95])
        file_fare_percentiles = ddf_file['fare_amount'].quantile(
            [0.25, 0.5, 0.75, 0.95])

        # 2. Análisis temporal intensivo
        pickup_times = dd.to_datetime(ddf_file['tpep_pickup_datetime'])
        dropoff_times = dd.to_datetime(ddf_file['tpep_dropoff_datetime'])
        trip_duration = (dropoff_times - pickup_times).dt.total_seconds() / 60

        ddf_file['trip_duration_minutes'] = trip_duration
        ddf_file['pickup_hour'] = pickup_times.dt.hour
        ddf_file['pickup_day_of_week'] = pickup_times.dt.dayofweek

        hourly_analysis_file = ddf_file.groupby('pickup_hour').agg({
            'fare_amount': ['count', 'mean', 'std'],
            'trip_distance': ['mean', 'std'],
            'trip_duration_minutes': ['mean', 'std']
        })

        # 3. Análisis geográfico intensivo
        pickup_analysis_file = ddf_file['PULocationID'].value_counts().nlargest(
            20)
        dropoff_analysis_file = ddf_file['DOLocationID'].value_counts().nlargest(
            20)

        # 4. Análisis de tipos de pago intensivo
        payment_analysis_file = ddf_file['payment_type'].value_counts()
        tip_analysis_file = ddf_file.groupby(
            'payment_type')['tip_amount'].agg(['count', 'mean', 'std'])

        print(f"      ⚡ Ejecutando computación paralela...")

        # COMPUTAR TODO EN PARALELO
        computed_results = dd.compute(
            file_rows, file_avg_distance, file_std_distance, file_avg_fare,
            file_distance_percentiles, file_fare_percentiles,
            hourly_analysis_file, pickup_analysis_file, dropoff_analysis_file,
            payment_analysis_file, tip_analysis_file
        )

        # Extraer resultados
        (f_rows, f_avg_distance, f_std_distance, f_avg_fare,
         f_dist_perc, f_fare_perc, f_hourly, f_pickup, f_dropoff,
         f_payment, f_tips) = computed_results

        file_end = time.time()
        file_processing_time = file_end - file_start

        # ===== ESTRUCTURAR RESULTADOS EXACTAMENTE COMO SECUENCIAL =====

        # Análisis por hora estructurado
        hourly_structured = {}
        for hour in f_hourly.index:
            hourly_structured[f"hour_{hour}"] = {
                'trips': int(f_hourly.loc[hour, ('fare_amount', 'count')]),
                'avg_fare': float(f_hourly.loc[hour, ('fare_amount', 'mean')]),
                'avg_distance': float(f_hourly.loc[hour, ('trip_distance', 'mean')]),
                'avg_duration': float(f_hourly.loc[hour, ('trip_duration_minutes', 'mean')])
            }

        # Análisis geográfico estructurado
        geographic_structured = {
            'top_pickup_zones': {f"zone_{zone_id}": int(count) for zone_id, count in f_pickup.items()},
            'top_dropoff_zones': {f"zone_{zone_id}": int(count) for zone_id, count in f_dropoff.items()}
        }

        # Análisis de tipos de pago estructurado
        payment_structured = {
            'payment_distribution': {f"type_{pay_type}": int(count) for pay_type, count in f_payment.items()},
            'tip_by_payment': {
                f"type_{pay_type}": {
                    'count': int(f_tips.loc[pay_type, 'count']) if pay_type in f_tips.index else 0,
                    'avg_tip': float(f_tips.loc[pay_type, 'mean']) if pay_type in f_tips.index else 0.0
                } for pay_type in f_payment.index
            }
        }

        # Resultado del archivo (EXACTO como secuencial)
        file_result = {
            "file": file_path.name,
            "processing_time": file_processing_time,
            "rows": int(f_rows),
            "analysis": {
                "advanced_stats": {
                    "rows": int(f_rows),
                    "avg_distance": float(f_avg_distance),
                    "std_distance": float(f_std_distance),
                    "avg_fare": float(f_avg_fare),
                    "percentiles_distance": [float(x) for x in f_dist_perc.values],
                    "percentiles_fare": [float(x) for x in f_fare_perc.values]
                },
                "hourly_analysis": hourly_structured,
                "geographic_analysis": geographic_structured,
                "payment_analysis": payment_structured
            }
        }

        file_results.append(file_result)
        total_rows += int(f_rows)
        print(
            f"      ✅ {file_path.name}: {f_rows:,} filas en {file_processing_time:.2f}s")

    end_time = time.time()
    processing_time = end_time - start_time

    # ===== ESTRUCTURAR RESULTADO FINAL EXACTO COMO SECUENCIAL =====
    results = {
        'timestamp': datetime.now().isoformat(),
        'method': 'dask_parallel_complete',
        'dag': 'dag_03_dask_complete',
        'execution_type': 'paralelo_dask_completo_100_igual',
        'total_time': processing_time,
        'files_processed': len(parquet_files),
        'total_rows': total_rows,
        'throughput': total_rows / processing_time,
        'avg_time_per_file': processing_time / len(parquet_files),

        # CLAVE: file_results igual que secuencial
        'file_results': file_results,

        # Métricas básicas para comparación
        'metrics': {
            'avg_fare': sum(fr['analysis']['advanced_stats']['avg_fare'] * fr['rows'] for fr in file_results) / total_rows,
            'avg_distance': sum(fr['analysis']['advanced_stats']['avg_distance'] * fr['rows'] for fr in file_results) / total_rows
        }
    }

    # Guardar resultados
    results_dir = Path("/workspace/results")
    results_dir.mkdir(exist_ok=True)

    results_file = results_dir / "dask_complete_results.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)

    # Mostrar resumen completo
    print(f"\n🎯 DAG 3: COMPLETADO - Procesamiento paralelo 100% IGUAL al secuencial")
    print(f"⚡ Tiempo total: {processing_time:.2f}s")
    print(f"📊 Filas procesadas: {total_rows:,}")
    print(f"🚀 Throughput: {results['throughput']:,.0f} filas/s")
    print(
        f"📁 Archivos procesados: {len(parquet_files)} (análisis individual c/u)")
    print(
        f"⏱️ Tiempo promedio por archivo: {processing_time/len(parquet_files):.2f}s")

    # Mostrar análisis completados
    print(f"\n📈 ANÁLISIS COMPLETADOS (IGUALES AL SECUENCIAL):")
    print(f"   • ✅ Análisis por archivo individual (10 archivos)")
    print(f"   • ✅ Estadísticas avanzadas por archivo")
    print(f"   • ✅ Análisis temporal por hora por archivo")
    print(f"   • ✅ Top 20 zonas pickup/dropoff por archivo")
    print(f"   • ✅ Análisis de tipos de pago por archivo")
    print(f"   • ✅ Estructura JSON idéntica al secuencial")

    print(f"\n📄 Reporte completo: {results_file}")
    print(f"🔄 COMPARACIÓN 100% JUSTA vs secuencial!")

    return results


# Tarea
dask_complete_task = PythonOperator(
    task_id='parallel_processing_complete',
    python_callable=parallel_processing_dask_complete,
    dag=dag,
)

dag.doc_md = """
# DAG 3: Procesamiento Paralelo (Dask) - ANÁLISIS 100% COMPLETO

**ORDEN: EJECUTAR TERCERO**

## Prerrequisitos
- ✅ dag_01_download_data completado
- ✅ dag_02_sequential completado

## Método
- **Dask**: Procesamiento distribuido/paralelo automático
- **Análisis 100% Igual**: Mismos análisis por archivo que secuencial
- **Comparación Justa**: Estructura JSON idéntica

## Análisis Incluidos (Por Archivo Individual)
- ✅ Estadísticas avanzadas (percentiles, desviación estándar)
- ✅ Análisis temporal por hora (24 horas)
- ✅ Análisis geográfico (top 20 zonas pickup/dropoff)
- ✅ Análisis de tipos de pago y propinas
- ✅ 10 archivos procesados individualmente

## Archivo Generado
- `dask_complete_results.json` (estructura 100% igual a sequential_results.json)

## Siguiente
- ▶️ DAG 4: Comparación y gráficos técnicos
"""
