"""
🚀🚀🚀 DAG ULTRA EXTREMO - MÁXIMA POTENCIA ABSOLUTA 🚀🚀🚀
Configuración que exprime hasta la última gota de rendimiento del sistema
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'performance_team_ultra',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def parallel_processing_ultra_extreme():
    """
    🔥🔥🔥 PROCESAMIENTO ULTRA EXTREMO - MÁXIMA POTENCIA ABSOLUTA 🔥🔥🔥
    """
    print("🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥")
    print("💥💥💥 ULTRA EXTREMO - ¡EXPLOTANDO TODOS LOS LÍMITES! 💥💥💥")
    print("🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥")

    import dask.dataframe as dd
    import time
    import json
    from distributed import Client
    import pandas as pd
    import dask

    # CONFIGURACIÓN ULTRA AGRESIVA
    dask.config.set({
        'array.chunk-size': '1GiB',  # Chunks gigantes
        'dataframe.shuffle.method': 'p2p',
        'distributed.worker.memory.target': 0.85,  # Más agresivo
        'distributed.worker.memory.spill': 0.90,
        'distributed.worker.memory.pause': 0.95,
        'distributed.worker.memory.terminate': 0.98,
        'distributed.scheduler.bandwidth': 1e9,  # 1GB/s bandwidth
        'distributed.comm.compression': 'lz4',  # Compresión rápida
        'distributed.scheduler.allowed-failures': 10,  # Más tolerante a fallos
        'dataframe.optimize-graph': True,
        'array.slicing.split_large_chunks': True,
        'distributed.scheduler.work-stealing': True,
        'distributed.scheduler.work-stealing-interval': '10ms'  # Ultra agresivo
    })

    start_time = time.time()

    # Conectar al cluster ULTRA
    client = Client('127.0.0.1:8786', timeout='45s')

    print(f"🚀 CLUSTER ULTRA EXTREMO CONECTADO:")
    scheduler_info = client.scheduler_info()
    workers = scheduler_info.get('workers', {})
    total_cores = sum(w.get('nthreads', 0) for w in workers.values())
    total_memory_gb = sum(w.get('memory_limit', 0)
                          for w in workers.values()) / (1024**3)

    print(f"   💥 Workers: {len(workers)}")
    print(f"   🧠 Memoria total: {total_memory_gb:.1f} GB")
    print(f"   ⚡ Cores totales: {total_cores}")
    print(f"   🔥 Configuración: ¡ULTRA EXTREMA!")

    # Buscar archivos de datos
    import glob
    data_files = glob.glob('/workspace/data/nyc_taxi/*.parquet')
    print(f"\n📂 Archivos encontrados: {len(data_files)}")

    # Calcular tamaño total de los datos
    import os
    total_size_gb = sum(os.path.getsize(file)
                        for file in data_files) / (1024**3)
    print(f"💾 Tamaño total datos: {total_size_gb:.2f} GB")

    print(f"\n💥💥💥 INICIANDO PROCESAMIENTO ULTRA EXTREMO 💥💥💥")
    print("⚡ Etapa 1: Carga paralela ULTRA AGRESIVA...")

    # Configuración EXTREMA para datos masivos
    df = dd.read_parquet(
        '/workspace/data/nyc_taxi/*.parquet',
        blocksize='256MB'  # Bloques ULTRA grandes
    )

    # Repartición EXTREMA basada en workers disponibles
    extreme_partitions = total_cores * 12  # ULTRA AGRESIVO
    print(f"💥 Reparticionando en {extreme_partitions} particiones EXTREMAS...")
    df = df.repartition(npartitions=extreme_partitions)

    print("⚡ Etapa 2: Preparación análisis ULTRA EXTREMO...")

    # Análisis paralelos ULTRA MASIVOS
    calculations = []

    print("⚡ Etapa 3: Configurando computaciones ULTRA EXTREMAS...")

    # Para cada archivo, crear MÚLTIPLES análisis paralelos EXTREMOS
    for i, data_file in enumerate(data_files):
        file_name = os.path.basename(data_file)
        print(f"      💥 Preparando EXTREMO {file_name}...")

        # Cargar archivo específico para análisis ULTRA detallado
        df_month = dd.read_parquet(data_file, blocksize='128MB')

        # Análisis MÚLTIPLE EXTREMO por archivo
        calculations.extend([
            # Básicos EXTREMOS
            df_month['total_amount'].sum(),
            df_month['tip_amount'].mean(),
            df_month['passenger_count'].sum(),
            df_month.shape[0],  # count
            df_month['trip_distance'].mean(),
            df_month['fare_amount'].max(),
            df_month['fare_amount'].min(),
            df_month.dropna()['total_amount'].std(),
            df_month[df_month['passenger_count'] > 0]['fare_amount'].mean(),

            # Análisis ADICIONALES EXTREMOS
            df_month['tip_amount'].max(),
            df_month['trip_distance'].std(),
            df_month['passenger_count'].max(),
            df_month[df_month['trip_distance'] > 0]['trip_distance'].mean(),
            df_month['total_amount'].quantile(0.95),
            df_month['fare_amount'].quantile(0.05),
        ])

    # Análisis globales ULTRA ADICIONALES
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
        df['fare_amount'].sum(),
        df['fare_amount'].mean(),
        df['fare_amount'].max(),
        df['fare_amount'].min(),
        df['fare_amount'].std(),
        df['trip_distance'].sum(),
        df['trip_distance'].mean(),
        df['trip_distance'].max(),
        df['trip_distance'].std(),
        df['passenger_count'].sum(),
        df['passenger_count'].mean(),
        df['passenger_count'].max(),
        # PERCENTILES EXTREMOS
        df['total_amount'].quantile(0.99),
        df['total_amount'].quantile(0.01),
        df['fare_amount'].quantile(0.99),
        df['fare_amount'].quantile(0.01),
        df['trip_distance'].quantile(0.99),
        df['trip_distance'].quantile(0.01),
        # FILTROS EXTREMOS
        df[df['total_amount'] > 0].shape[0],
        df[df['trip_distance'] > 0].shape[0],
        df[df['passenger_count'] > 0].shape[0],
        df[df['fare_amount'] > 0]['total_amount'].mean(),
    ])

    print(f"\n💥💥💥 EJECUTANDO {len(calculations)} CÁLCULOS ULTRA EXTREMOS! 💥💥💥")
    print("🔥 ¡USANDO ABSOLUTAMENTE TODA LA POTENCIA DEL SISTEMA!")
    print(f"⚡ Distribuyendo trabajo EXTREMO en {len(workers)} workers...")

    # 💥💥💥 COMPUTACIÓN ULTRA EXTREMA 💥💥💥
    computation_start = time.time()
    try:
        results = dd.compute(
            *calculations,
            scheduler=client,
            resources={'memory': '800MB'},  # Límite ajustado para más workers
            retries=3,
            priority='high'
        )
        computation_time = time.time() - computation_start

        print(
            f"✅💥 ¡COMPUTACIÓN ULTRA EXTREMA COMPLETADA EN {computation_time:.2f}s! 💥✅")
        print(f"🔥 Resultados EXTREMOS: {len(results)} cálculos completados")
        print(
            f"⚡ Throughput ULTRA: {len(results)/computation_time:.2f} cálculos/segundo")

    except Exception as e:
        print(f"❌ Error en computación EXTREMA: {e}")
        raise
    finally:
        client.close()

    total_time = time.time() - start_time

    # Resultados ULTRA detallados
    performance_results = {
        'dag_type': 'dask_ultra_extreme',
        'total_time_seconds': total_time,
        'computation_time_seconds': computation_time,
        'total_files_processed': len(data_files),
        'total_data_size_gb': total_size_gb,
        'total_calculations': len(calculations),
        'workers_used': len(workers),
        'total_cores': total_cores,
        'total_memory_gb': total_memory_gb,
        'partitions_created': extreme_partitions,
        'throughput_calc_per_second': len(results)/computation_time if computation_time > 0 else 0,
        'data_throughput_gb_per_second': total_size_gb/total_time if total_time > 0 else 0,
        'performance_metrics': {
            'setup_time': computation_start - start_time,
            'pure_computation_time': computation_time,
            'cleanup_time': total_time - computation_start - computation_time
        },
        'extreme_config': {
            'partitions': extreme_partitions,
            'compression': 'lz4',
            'work_stealing': True,
            'memory_target': 0.85,
            'chunk_size': '1GiB'
        }
    }

    # Guardar resultados EXTREMOS
    with open('/workspace/results/dask_ultra_extreme_results.json', 'w') as f:
        json.dump(performance_results, f, indent=2)

    print(f"\n🏆💥💥💥 PROCESAMIENTO ULTRA EXTREMO COMPLETADO 💥💥💥🏆")
    print(f"🕒 Tiempo total: {total_time:.2f} segundos")
    print(f"⚡ Archivos procesados: {len(data_files)}")
    print(f"🔥 Datos procesados: {total_size_gb:.2f} GB")
    print(f"💪 Throughput: {total_size_gb/total_time:.2f} GB/s")
    print(f"📊 Cálculos completados: {len(results)}")
    print(f"💥 Particiones: {extreme_partitions}")
    print("🚀💥 ¡ULTRA EXTREMO SUPREMACÍA ABSOLUTA! 💥🚀")

    return {
        'status': 'ULTRA_EXTREME_SUCCESS',
        'total_time': total_time,
        'files_processed': len(data_files),
        'data_size_gb': total_size_gb,
        'calculations': len(results),
        'throughput_gb_s': total_size_gb/total_time,
        'partitions': extreme_partitions,
        'workers': len(workers),
        'cores': total_cores
    }


# DAG ULTRA EXTREMO
dag = DAG(
    'dag_04_dask_ultra_extreme',
    default_args=default_args,
    description='🚀💥 Procesamiento Dask ULTRA EXTREMO - Máxima potencia absoluta',
    schedule_interval=None,
    catchup=False,
    tags=['dask', 'ultra', 'extreme', 'maximum_performance']
)

ultra_extreme_task = PythonOperator(
    task_id='ultra_extreme_processing',
    python_callable=parallel_processing_ultra_extreme,
    dag=dag
)

ultra_extreme_task
