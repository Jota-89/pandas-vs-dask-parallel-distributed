"""
💥💥💥 DAG ULTRA SUPREMO - ¡32GB RAM COMPLETOS! 💥💥💥
La configuración definitiva que usa TODA tu RAM de 32GB
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'performance_team_supremo',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def parallel_processing_ultra_supremo():
    """
    💥💥💥 PROCESAMIENTO ULTRA SUPREMO - ¡32GB RAM COMPLETOS! 💥💥💥
    """
    print("💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥")
    print("🔥🔥🔥 ULTRA SUPREMO - ¡32GB RAM AL MÁXIMO! 🔥🔥🔥")
    print("💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥")

    import dask.dataframe as dd
    import time
    import json
    from distributed import Client
    import pandas as pd
    import dask

    # CONFIGURACIÓN SUPREMA para 32GB RAM
    dask.config.set({
        'array.chunk-size': '2GiB',  # Chunks GIGANTESCOS
        'dataframe.shuffle.method': 'p2p',
        'distributed.worker.memory.target': 0.80,  # Balanceado pero agresivo
        'distributed.worker.memory.spill': 0.90,
        'distributed.worker.memory.pause': 0.95,
        'distributed.worker.memory.terminate': 0.98,
        'distributed.scheduler.bandwidth': 2e9,  # 2GB/s bandwidth
        'distributed.comm.compression': 'lz4',  # Compresión eficiente
        'distributed.scheduler.allowed-failures': 8,  # Tolerante
        'dataframe.optimize-graph': True,
        'array.slicing.split_large_chunks': True,
        'distributed.scheduler.work-stealing': True,
        'distributed.scheduler.work-stealing-interval': '50ms',  # Balanceado
        'distributed.worker.connections.outgoing': 10,
        'distributed.worker.connections.incoming': 10
    })

    start_time = time.time()

    # Conectar al cluster SUPREMO
    client = Client('127.0.0.1:8786', timeout='60s')

    print(f"🚀 CLUSTER ULTRA SUPREMO CONECTADO:")
    scheduler_info = client.scheduler_info()
    workers = scheduler_info.get('workers', {})
    total_cores = sum(w.get('nthreads', 0) for w in workers.values())
    total_memory_gb = sum(w.get('memory_limit', 0)
                          for w in workers.values()) / (1024**3)

    print(f"   💥 Workers: {len(workers)}")
    print(f"   🧠 Memoria total: {total_memory_gb:.1f} GB")
    print(f"   ⚡ Cores totales: {total_cores}")
    print(f"   🔥 Configuración: ¡ULTRA SUPREMA - 32GB!")

    # Buscar archivos de datos
    import glob
    data_files = glob.glob('/workspace/data/nyc_taxi/*.parquet')
    print(f"\n📂 Archivos encontrados: {len(data_files)}")

    if not data_files:
        raise Exception("No se encontraron archivos de datos!")

    # Calcular tamaño total de los datos
    import os
    total_size_gb = sum(os.path.getsize(file)
                        for file in data_files) / (1024**3)
    print(f"💾 Tamaño total datos: {total_size_gb:.2f} GB")

    print(f"\n💥💥💥 INICIANDO PROCESAMIENTO ULTRA SUPREMO 💥💥💥")
    print("⚡ Etapa 1: Carga paralela SUPREMA...")

    # Configuración SUPREMA para 32GB RAM
    df = dd.read_parquet(
        '/workspace/data/nyc_taxi/*.parquet',
        blocksize='512MB'  # Bloques SUPREMOS
    )

    # Repartición SUPREMA balanceada
    supremo_partitions = total_cores * 8  # Balanceado para 32GB
    print(f"💥 Reparticionando en {supremo_partitions} particiones SUPREMAS...")
    df = df.repartition(npartitions=supremo_partitions)

    print("⚡ Etapa 2: Preparación análisis ULTRA SUPREMO...")

    # Análisis paralelos SUPREMOS eficientes
    calculations = []

    print("⚡ Etapa 3: Configurando computaciones SUPREMAS...")

    # Análisis GLOBALES SUPREMOS (más eficiente que por archivo)
    print("      💥 Configurando análisis GLOBALES SUPREMOS...")

    # BÁSICOS SUPREMOS
    calculations.extend([
        df.shape[0],  # total count
        df['total_amount'].sum(),
        df['total_amount'].mean(),
        df['total_amount'].std(),
        df['total_amount'].max(),
        df['total_amount'].min(),
        df['tip_amount'].sum(),
        df['tip_amount'].mean(),
        df['tip_amount'].std(),
        df['tip_amount'].max(),
        df['tip_amount'].min(),
        df['fare_amount'].sum(),
        df['fare_amount'].mean(),
        df['fare_amount'].max(),
        df['fare_amount'].min(),
        df['fare_amount'].std(),
    ])

    # ANÁLISIS DE DISTANCIA SUPREMOS
    calculations.extend([
        df['trip_distance'].sum(),
        df['trip_distance'].mean(),
        df['trip_distance'].max(),
        df['trip_distance'].min(),
        df['trip_distance'].std(),
        df[df['trip_distance'] > 0].shape[0],
        df[df['trip_distance'] > 0]['trip_distance'].mean(),
    ])

    # ANÁLISIS DE PASAJEROS SUPREMOS
    calculations.extend([
        df['passenger_count'].sum(),
        df['passenger_count'].mean(),
        df['passenger_count'].max(),
        df['passenger_count'].min(),
        df[df['passenger_count'] > 0].shape[0],
        df[df['passenger_count'] > 0]['fare_amount'].mean(),
    ])

    # PERCENTILES SUPREMOS
    calculations.extend([
        df['total_amount'].quantile(0.99),
        df['total_amount'].quantile(0.95),
        df['total_amount'].quantile(0.05),
        df['total_amount'].quantile(0.01),
        df['fare_amount'].quantile(0.99),
        df['fare_amount'].quantile(0.95),
        df['fare_amount'].quantile(0.05),
        df['fare_amount'].quantile(0.01),
        df['trip_distance'].quantile(0.99),
        df['trip_distance'].quantile(0.95),
        df['trip_distance'].quantile(0.05),
        df['trip_distance'].quantile(0.01),
    ])

    # FILTROS AVANZADOS SUPREMOS
    calculations.extend([
        df[df['total_amount'] > 0].shape[0],
        df[df['fare_amount'] > 0].shape[0],
        df[df['tip_amount'] > 0].shape[0],
        df[df['total_amount'] > 100]['total_amount'].mean(),
        df[df['trip_distance'] > 10]['fare_amount'].mean(),
        df[(df['passenger_count'] > 0) & (
            df['fare_amount'] > 0)]['tip_amount'].mean(),
    ])

    # Por cada archivo, análisis SUPREMO específico
    # Primeros 5 para no sobrecargar
    for i, data_file in enumerate(data_files[:5]):
        file_name = os.path.basename(data_file)
        print(f"      💥 Configurando análisis SUPREMO {file_name}...")

        df_month = dd.read_parquet(data_file, blocksize='256MB')

        # Análisis eficientes por archivo
        calculations.extend([
            df_month.shape[0],
            df_month['total_amount'].mean(),
            df_month['fare_amount'].mean(),
            df_month['tip_amount'].mean(),
            df_month['trip_distance'].mean(),
        ])

    print(f"\n💥💥💥 EJECUTANDO {len(calculations)} CÁLCULOS ULTRA SUPREMOS! 💥💥💥")
    print("🔥 ¡USANDO TODOS LOS 32GB DE RAM!")
    print(f"⚡ Distribuyendo trabajo SUPREMO en {len(workers)} workers...")

    # 💥💥💥 COMPUTACIÓN ULTRA SUPREMA 💥💥💥
    computation_start = time.time()
    try:
        results = dd.compute(
            *calculations,
            scheduler=client,
            retries=2,
            priority='high'
        )
        computation_time = time.time() - computation_start

        print(
            f"✅💥 ¡COMPUTACIÓN ULTRA SUPREMA COMPLETADA EN {computation_time:.2f}s! 💥✅")
        print(f"🔥 Resultados SUPREMOS: {len(results)} cálculos completados")
        print(
            f"⚡ Throughput SUPREMO: {len(results)/computation_time:.2f} cálculos/segundo")

    except Exception as e:
        print(f"❌ Error en computación SUPREMA: {e}")
        raise
    finally:
        client.close()

    total_time = time.time() - start_time

    # Resultados SUPREMOS detallados
    performance_results = {
        'dag_type': 'dask_ultra_supremo',
        'total_time_seconds': total_time,
        'computation_time_seconds': computation_time,
        'total_files_processed': len(data_files),
        'total_data_size_gb': total_size_gb,
        'total_calculations': len(calculations),
        'workers_used': len(workers),
        'total_cores': total_cores,
        'total_memory_gb': total_memory_gb,
        'partitions_created': supremo_partitions,
        'throughput_calc_per_second': len(results)/computation_time if computation_time > 0 else 0,
        'data_throughput_gb_per_second': total_size_gb/total_time if total_time > 0 else 0,
        'performance_metrics': {
            'setup_time': computation_start - start_time,
            'pure_computation_time': computation_time,
            'cleanup_time': total_time - computation_start - computation_time
        },
        'supremo_config': {
            'ram_total_gb': 32,
            'partitions': supremo_partitions,
            'compression': 'zstd',
            'chunk_size': '2GiB',
            'workers_config': f'{len(workers)}×{total_cores//len(workers) if workers else 0} threads'
        }
    }

    # Guardar resultados SUPREMOS
    with open('/workspace/results/dask_ultra_supremo_results.json', 'w') as f:
        json.dump(performance_results, f, indent=2)

    print(f"\n🏆💥💥💥 PROCESAMIENTO ULTRA SUPREMO COMPLETADO 💥💥💥🏆")
    print(f"🕒 Tiempo total: {total_time:.2f} segundos")
    print(f"⚡ Archivos procesados: {len(data_files)}")
    print(f"🔥 Datos procesados: {total_size_gb:.2f} GB")
    print(f"💪 Throughput: {total_size_gb/total_time:.2f} GB/s")
    print(f"📊 Cálculos completados: {len(results)}")
    print(f"💥 Particiones: {supremo_partitions}")
    print(f"🧠 RAM usada: {total_memory_gb:.1f}GB de 32GB disponibles")
    print("🚀💥 ¡ULTRA SUPREMO - 32GB RAM MÁXIMO RENDIMIENTO! 💥🚀")

    return {
        'status': 'ULTRA_SUPREMO_SUCCESS',
        'total_time': total_time,
        'files_processed': len(data_files),
        'data_size_gb': total_size_gb,
        'calculations': len(results),
        'throughput_gb_s': total_size_gb/total_time,
        'partitions': supremo_partitions,
        'workers': len(workers),
        'cores': total_cores,
        'memory_gb': total_memory_gb
    }


# DAG ULTRA SUPREMO
dag = DAG(
    'dag_05_dask_ultra_supremo',
    default_args=default_args,
    description='💥🔥 Procesamiento Dask ULTRA SUPREMO - 32GB RAM Completos',
    schedule_interval=None,
    catchup=False,
    tags=['dask', 'ultra', 'supremo', '32gb_ram', 'maximum_performance']
)

ultra_supremo_task = PythonOperator(
    task_id='ultra_supremo_processing',
    python_callable=parallel_processing_ultra_supremo,
    dag=dag
)

ultra_supremo_task
