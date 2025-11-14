"""
DAG 3: PROCESAMIENTO PARALELO MASIVO CON DASK - ¡MÁXIMA POTENCIA!
================================================================
PROPÓSITO: Demostrar VERDADERO paralelismo con Dask usando 32GB RAM y múltiples workers
ORDEN: TERCERO - después del procesamiento secuencial
CONFIGURACIÓN: 8 workers x 4 threads x 3GB = 24GB memoria utilizada
"""

from datetime import datetime, timedelta
import json
from pathlib import Path
import time

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_team_performance',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'dag_03_dask_turbo',
    default_args=default_args,
    description='DAG 3: Procesamiento paralelo TURBO (máxima potencia)',
    schedule_interval=None,
    catchup=False,
)


def parallel_processing_dask_turbo(**context):
    """DAG 3: PROCESAMIENTO PARALELO MASIVO - ¡MÁXIMA POTENCIA!"""
    print("🚀" * 30)
    print("🔥 DAG 3: PROCESAMIENTO PARALELO MASIVO - ¡MÁXIMA POTENCIA! 🔥")
    print("🚀" * 30)

    data_dir = Path("/workspace/data/nyc_taxi")
    if not data_dir.exists():
        raise Exception("No hay datos! Ejecuta dag_01_download_data primero")

    import dask.dataframe as dd
    import pandas as pd
    import numpy as np
    from dask.distributed import Client
    import psutil

    # ===== CONFIGURACIÓN TURBO PARA MÁXIMO RENDIMIENTO =====
    import dask
    dask.config.set({
        'array.chunk-size': '512MiB',
        'dataframe.shuffle.method': 'p2p',
        'distributed.worker.memory.target': 0.75,
        'distributed.worker.memory.spill': 0.85,
        'distributed.worker.memory.pause': 0.95,
        'distributed.worker.memory.terminate': 0.98,
        'distributed.nanny.pre-spawn-environ.MALLOC_TRIM_THRESHOLD_': '0',
        'dataframe.optimize-graph': True,
        'array.slicing.split_large_chunks': True
    })

    # Conectar al cluster Dask TURBO
    try:
        client = Client('tcp://localhost:8786')
        workers_info = client.scheduler_info()['workers']
        total_memory = sum(w['memory_limit']
                           for w in workers_info.values()) / (1024**3)
        total_cores = sum(w['nthreads'] for w in workers_info.values())

        print(f"🚀 CLUSTER DASK TURBO CONECTADO:")
        print(f"   💪 Workers: {len(workers_info)}")
        print(f"   🧠 Memoria total: {total_memory:.1f} GB")
        print(f"   ⚡ Cores totales: {total_cores}")
        print(f"   🔥 Configuración: MÁXIMA POTENCIA!")

    except Exception as e:
        print(f"⚠️ Error conectando cluster: {e}")
        print("🔄 Usando configuración local optimizada...")
        client = None

    start_time = time.time()

    # Obtener archivos
    parquet_files = sorted(list(data_dir.glob("*.parquet")))
    print(f"\n📂 Archivos encontrados: {len(parquet_files)}")

    total_size = sum(f.stat().st_size for f in parquet_files) / (1024**3)
    print(f"💾 Tamaño total datos: {total_size:.2f} GB")

    if not parquet_files:
        raise Exception("No hay archivos .parquet para procesar")

    # Columnas optimizadas
    columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'fare_amount', 'total_amount',
               'PULocationID', 'DOLocationID', 'payment_type', 'tip_amount', 'passenger_count']

    print("\n🔥 INICIANDO PROCESAMIENTO PARALELO MASIVO 🔥")

    # ===== ESTRATEGIA TURBO: PARALELISMO MÁXIMO =====

    print("⚡ Etapa 1: Carga paralela masiva de datos...")

    # Leer TODO en paralelo con chunks optimizados
    all_files_pattern = str(data_dir / "*.parquet")
    ddf_all = dd.read_parquet(
        all_files_pattern,
        columns=columns,
        blocksize='256MB',  # Chunks grandes
        index=False,
        engine='pyarrow'
    )

    # Reparticionado inteligente
    # Más particiones = más paralelismo
    optimal_partitions = len(parquet_files) * 8
    print(f"🔄 Reparticionando en {optimal_partitions} particiones...")
    ddf_all = ddf_all.repartition(npartitions=optimal_partitions)

    print("⚡ Etapa 2: Preparación análisis paralelo masivo...")

    # ===== ANÁLISIS MASIVO POR ARCHIVO + GLOBAL =====

    # Crear DataFrames por archivo para análisis individual
    file_ddfs = {}
    for file_path in parquet_files:
        file_ddf = dd.read_parquet(
            str(file_path),
            columns=columns,
            blocksize='128MB',
            engine='pyarrow'
        )
        file_ddfs[file_path.name] = file_ddf

    print("⚡ Etapa 3: Configurando computaciones paralelas masivas...")

    # ===== PREPARAR TODAS LAS COMPUTACIONES =====
    all_computations = []
    computation_index = {}
    idx = 0

    for file_name, file_ddf in file_ddfs.items():
        print(f"      📄 Preparando {file_name}...")

        # Estadísticas básicas
        file_rows = len(file_ddf)
        file_avg_distance = file_ddf['trip_distance'].mean()
        file_std_distance = file_ddf['trip_distance'].std()
        file_avg_fare = file_ddf['fare_amount'].mean()

        # Percentiles
        file_distance_percentiles = file_ddf['trip_distance'].quantile(
            [0.25, 0.5, 0.75, 0.95])
        file_fare_percentiles = file_ddf['fare_amount'].quantile(
            [0.25, 0.5, 0.75, 0.95])

        # Análisis temporal
        pickup_dt = dd.to_datetime(file_ddf['tpep_pickup_datetime'])
        dropoff_dt = dd.to_datetime(file_ddf['tpep_dropoff_datetime'])
        duration = (dropoff_dt - pickup_dt).dt.total_seconds() / 60

        file_ddf_enhanced = file_ddf.assign(
            pickup_hour=pickup_dt.dt.hour,
            pickup_day=pickup_dt.dt.dayofweek,
            trip_duration=duration
        )

        hourly_stats = file_ddf_enhanced.groupby('pickup_hour').agg({
            'fare_amount': ['count', 'mean', 'std'],
            'trip_distance': ['mean', 'std'],
            'trip_duration': ['mean', 'std']
        })

        # Análisis geográfico
        pickup_zones = file_ddf['PULocationID'].value_counts().nlargest(20)
        dropoff_zones = file_ddf['DOLocationID'].value_counts().nlargest(20)

        # Análisis pagos
        payment_counts = file_ddf['payment_type'].value_counts()
        payment_tips = file_ddf.groupby('payment_type')[
            'tip_amount'].agg(['count', 'mean', 'std'])

        # Guardar índices para este archivo
        computation_index[file_name] = list(range(idx, idx + 11))

        # Agregar a la lista de computaciones
        all_computations.extend([
            file_rows, file_avg_distance, file_std_distance, file_avg_fare,
            file_distance_percentiles, file_fare_percentiles,
            hourly_stats, pickup_zones, dropoff_zones, payment_counts, payment_tips
        ])

        idx += 11

    print(
        f"\n🚀 EJECUTANDO {len(all_computations)} CÁLCULOS EN PARALELO MASIVO!")
    print("🔥 ¡USANDO TODA LA POTENCIA DEL CLUSTER DASK!")

    # ===== COMPUTACIÓN PARALELA MASIVA =====
    compute_start = time.time()

    # ¡EJECUTAR TODO EN PARALELO!
    if client:
        print(
            f"⚡ Distribuyendo trabajo en {len(client.scheduler_info()['workers'])} workers...")

    results = dd.compute(
        *all_computations, scheduler='threads' if not client else 'distributed')

    compute_end = time.time()
    compute_time = compute_end - compute_start

    print(f"\n🎉 ¡COMPUTACIÓN PARALELA COMPLETADA EN {compute_time:.2f}s!")
    print(
        f"⚡ Velocidad de computación: {len(all_computations)/compute_time:.1f} ops/segundo")

    # ===== ESTRUCTURAR RESULTADOS =====
    print("📊 Estructurando resultados...")

    file_results = []
    total_rows = 0

    for i, file_path in enumerate(parquet_files):
        file_name = file_path.name
        indices = computation_index[file_name]

        # Extraer resultados de este archivo
        (f_rows, f_avg_distance, f_std_distance, f_avg_fare,
         f_dist_perc, f_fare_perc, f_hourly, f_pickup, f_dropoff,
         f_payment, f_tips) = [results[i] for i in indices]

        # Tiempo de procesamiento distribuido
        file_processing_time = compute_time / len(parquet_files)

        # Estructurar análisis por hora
        hourly_structured = {}
        for hour in f_hourly.index:
            hourly_structured[f"hour_{hour}"] = {
                'trips': int(f_hourly.loc[hour, ('fare_amount', 'count')]),
                'avg_fare': float(f_hourly.loc[hour, ('fare_amount', 'mean')]),
                'avg_distance': float(f_hourly.loc[hour, ('trip_distance', 'mean')]),
                'avg_duration': float(f_hourly.loc[hour, ('trip_duration', 'mean')])
            }

        # Estructurar geográfico
        geographic_structured = {
            'top_pickup_zones': {f"zone_{zone_id}": int(count) for zone_id, count in f_pickup.items()},
            'top_dropoff_zones': {f"zone_{zone_id}": int(count) for zone_id, count in f_dropoff.items()}
        }

        # Estructurar pagos
        payment_structured = {
            'payment_distribution': {f"type_{pay_type}": int(count) for pay_type, count in f_payment.items()},
            'tip_by_payment': {
                f"type_{pay_type}": {
                    'count': int(f_tips.loc[pay_type, 'count']) if pay_type in f_tips.index else 0,
                    'avg_tip': float(f_tips.loc[pay_type, 'mean']) if pay_type in f_tips.index else 0.0
                } for pay_type in f_payment.index
            }
        }

        # Resultado del archivo
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
        print(f"   ✅ {file_name}: {f_rows:,} filas")

    end_time = time.time()
    total_processing_time = end_time - start_time

    # ===== RESULTADO FINAL =====
    results_data = {
        'timestamp': datetime.now().isoformat(),
        'method': 'dask_parallel_turbo',
        'dag': 'dag_03_dask_turbo',
        'execution_type': 'paralelo_dask_turbo_maxima_potencia',
        'total_time': total_processing_time,
        'compute_time': compute_time,
        'files_processed': len(parquet_files),
        'total_rows': total_rows,
        'throughput': total_rows / total_processing_time,
        'compute_throughput': total_rows / compute_time,
        'avg_time_per_file': total_processing_time / len(parquet_files),
        'parallel_efficiency': compute_time / total_processing_time,
        'data_size_gb': total_size,
        'memory_used_gb': total_memory if client else 'unknown',

        # Resultados por archivo
        'file_results': file_results,

        # Métricas de rendimiento
        'performance_metrics': {
            'cluster_workers': len(workers_info) if client else 'unknown',
            'total_cores': total_cores if client else 'unknown',
            'total_memory_gb': total_memory if client else 'unknown',
            'data_throughput_gb_per_sec': total_size / total_processing_time,
            'parallel_speedup_estimate': total_processing_time / compute_time if compute_time > 0 else 1.0
        },

        # Métricas básicas para comparación
        'metrics': {
            'avg_fare': sum(fr['analysis']['advanced_stats']['avg_fare'] * fr['rows'] for fr in file_results) / total_rows,
            'avg_distance': sum(fr['analysis']['advanced_stats']['avg_distance'] * fr['rows'] for fr in file_results) / total_rows
        }
    }

    # Guardar resultados
    results_dir = Path("/workspace/results")
    results_dir.mkdir(exist_ok=True)

    results_file = results_dir / "dask_turbo_results.json"
    with open(results_file, 'w') as f:
        json.dump(results_data, f, indent=2)

    # Cerrar cliente
    if client:
        client.close()

    # ===== MOSTRAR RESULTADOS ÉPICOS =====
    print("\n" + "🎉" * 50)
    print("🚀 ¡DAG 3 TURBO COMPLETADO - MÁXIMA POTENCIA! 🚀")
    print("🎉" * 50)

    print(f"\n⚡ RENDIMIENTO ÉPICO:")
    print(f"   🕒 Tiempo total: {total_processing_time:.2f}s")
    print(f"   💨 Tiempo computación: {compute_time:.2f}s")
    print(f"   📊 Filas procesadas: {total_rows:,}")
    print(f"   🚀 Throughput total: {results_data['throughput']:,.0f} filas/s")
    print(
        f"   ⚡ Throughput cómputo: {results_data['compute_throughput']:,.0f} filas/s")
    print(f"   💾 Datos procesados: {total_size:.2f} GB")
    print(
        f"   📈 Velocidad datos: {results_data['performance_metrics']['data_throughput_gb_per_sec']:.2f} GB/s")

    if client:
        print(f"\n🔥 CONFIGURACIÓN CLUSTER:")
        print(f"   💪 Workers: {len(workers_info)}")
        print(f"   🧠 Memoria: {total_memory:.1f} GB")
        print(f"   ⚡ Cores: {total_cores}")

    print(f"\n📄 Reporte turbo: {results_file}")
    print(f"🏆 ¡DASK TURBO LISTO PARA DERROTAR AL SECUENCIAL!")

    return results_data


# Tarea
dask_turbo_task = PythonOperator(
    task_id='parallel_processing_turbo',
    python_callable=parallel_processing_dask_turbo,
    dag=dag,
)

dag.doc_md = """
# DAG 3: Procesamiento Paralelo TURBO (Dask) - ¡MÁXIMA POTENCIA!

**ORDEN: EJECUTAR TERCERO**

## 🚀 Configuración TURBO
- **32GB RAM**: Memoria masiva disponible
- **8 Workers**: Múltiples procesos paralelos
- **4 Threads/Worker**: 32 threads totales
- **256MB Chunks**: Bloques optimizados
- **Reparticionado**: 80 particiones para máximo paralelismo

## 🔥 Características TURBO
- ✅ Carga paralela masiva de datos
- ✅ Computación distribuida optimizada
- ✅ Análisis simultáneo de múltiples archivos
- ✅ Configuración memoria agresiva
- ✅ Reparticionado inteligente

## 📊 Análisis Incluidos (TURBO)
- ✅ Análisis por archivo (paralelizado)
- ✅ Estadísticas avanzadas (distribuidas)
- ✅ Análisis temporal (optimizado)
- ✅ Análisis geográfico (paralelo)
- ✅ Análisis de pagos (distribuido)

## 🎯 Objetivo
¡DERROTAR al procesamiento secuencial con una diferencia ÉPICA!

## 📁 Archivo Generado
- `dask_turbo_results.json` (máximo rendimiento)

## 🏁 Siguiente
- ▶️ DAG 4: Comparación (¡preparate para la victoria!)
"""
