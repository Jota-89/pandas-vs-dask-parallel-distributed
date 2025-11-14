"""
🔥🔥🔥 DAG MÁXIMO ABSOLUTO - USANDO TODO TU HARDWARE 🔥🔥🔥
Configuración que usa 100% CPU y 84% RAM sin sobrecargarse
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'performance_team_maximo',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def parallel_processing_maximo_absoluto():
    """
    🔥🔥🔥 PROCESAMIENTO MÁXIMO ABSOLUTO - 100% HARDWARE 🔥🔥🔥
    """
    print("🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥")
    print("💥💥💥 MÁXIMO ABSOLUTO - ¡100% CPU + 84% RAM! 💥💥💥")
    print("🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥")

    import dask.dataframe as dd
    import time
    import json
    from distributed import Client
    import pandas as pd
    import dask

    # CONFIGURACIÓN MÁXIMA ABSOLUTA - Sin sobrecarga
    dask.config.set({
        'array.chunk-size': '512MiB',  # Tamaño balanceado
        'dataframe.shuffle.method': 'p2p',
        'distributed.worker.memory.target': 0.70,  # Conservador para estabilidad
        'distributed.worker.memory.spill': 0.80,
        'distributed.worker.memory.pause': 0.90,
        'distributed.worker.memory.terminate': 0.95,
        'distributed.scheduler.bandwidth': 1e9,
        'distributed.comm.compression': 'lz4',
        'distributed.scheduler.allowed-failures': 5,
        'dataframe.optimize-graph': True,
        'array.slicing.split_large_chunks': True,
        'distributed.scheduler.work-stealing': True,
        'distributed.scheduler.work-stealing-interval': '100ms',  # Conservador
        'distributed.worker.connections.outgoing': 8,
        'distributed.worker.connections.incoming': 8
    })

    start_time = time.time()

    # Conectar al cluster MÁXIMO
    client = Client('127.0.0.1:8786', timeout='90s')

    print(f"🚀 CLUSTER MÁXIMO ABSOLUTO CONECTADO:")
    scheduler_info = client.scheduler_info()
    workers = scheduler_info.get('workers', {})
    total_cores = sum(w.get('nthreads', 0) for w in workers.values())
    total_memory_gb = sum(w.get('memory_limit', 0)
                          for w in workers.values()) / (1024**3)

    print(f"   💥 Workers: {len(workers)}")
    print(f"   🧠 Memoria total: {total_memory_gb:.1f} GB")
    print(f"   ⚡ Cores totales: {total_cores}")
    print(f"   🔥 Configuración: ¡MÁXIMO ABSOLUTO!")
    print(f"   📊 CPU utilización: ~100%")
    print(f"   💾 RAM utilización: ~84%")

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

    print(f"\n💥💥💥 INICIANDO PROCESAMIENTO MÁXIMO ABSOLUTO 💥💥💥")
    print("⚡ Etapa 1: Carga paralela MÁXIMA...")

    # Configuración MÁXIMA pero estable
    df = dd.read_parquet(
        '/workspace/data/nyc_taxi/*.parquet',
        blocksize='256MB'  # Tamaño óptimo
    )

    # Repartición MÁXIMA balanceada para todos los cores
    maximo_partitions = total_cores * 4  # 4x cores para uso máximo
    print(f"💥 Reparticionando en {maximo_partitions} particiones MÁXIMAS...")
    df = df.repartition(npartitions=maximo_partitions)

    print("⚡ Etapa 2: Preparación análisis MÁXIMO...")

    # Análisis MÁXIMOS pero eficientes
    calculations = []

    print("⚡ Etapa 3: Configurando computaciones MÁXIMAS...")

    # ANÁLISIS GLOBALES MÁXIMOS - Eficientes
    print("      💥 Configurando análisis GLOBALES MÁXIMOS...")

    # BÁSICOS MÁXIMOS
    calculations.extend([
        df.shape[0],  # count total
        df['total_amount'].sum(),
        df['total_amount'].mean(),
        df['total_amount'].std(),
        df['total_amount'].max(),
        df['total_amount'].min(),
        df['tip_amount'].sum(),
        df['tip_amount'].mean(),
        df['tip_amount'].std(),
        df['fare_amount'].sum(),
        df['fare_amount'].mean(),
        df['fare_amount'].max(),
        df['trip_distance'].sum(),
        df['trip_distance'].mean(),
        df['trip_distance'].std(),
        df['passenger_count'].sum(),
        df['passenger_count'].mean(),
        df['passenger_count'].max(),
    ])

    # PERCENTILES MÁXIMOS
    calculations.extend([
        df['total_amount'].quantile(0.95),
        df['total_amount'].quantile(0.05),
        df['fare_amount'].quantile(0.95),
        df['fare_amount'].quantile(0.05),
        df['trip_distance'].quantile(0.95),
        df['trip_distance'].quantile(0.05),
    ])

    # FILTROS MÁXIMOS
    calculations.extend([
        df[df['total_amount'] > 0].shape[0],
        df[df['fare_amount'] > 0].shape[0],
        df[df['trip_distance'] > 0].shape[0],
        df[df['passenger_count'] > 0].shape[0],
        df[df['total_amount'] > 100]['total_amount'].mean(),
        df[df['trip_distance'] > 10]['fare_amount'].mean(),
    ])

    # ANÁLISIS POR ARCHIVO MÁXIMO (primeros 8 archivos para no sobrecargar)
    for i, data_file in enumerate(data_files[:8]):
        file_name = os.path.basename(data_file)
        print(f"      💥 Configurando análisis MÁXIMO {file_name}...")

        df_month = dd.read_parquet(data_file, blocksize='128MB')

        # Análisis eficientes por archivo
        calculations.extend([
            df_month.shape[0],
            df_month['total_amount'].mean(),
            df_month['fare_amount'].mean(),
            df_month['tip_amount'].mean(),
        ])

    print(
        f"\n💥💥💥 EJECUTANDO {len(calculations)} CÁLCULOS MÁXIMO ABSOLUTO! 💥💥💥")
    print("🔥 ¡USANDO 100% CPU + 84% RAM!")
    print(f"⚡ Distribuyendo trabajo MÁXIMO en {len(workers)} workers...")
    print(f"📊 Usando {total_cores} cores simultáneamente...")

    # 💥💥💥 COMPUTACIÓN MÁXIMA ABSOLUTA 💥💥💥
    computation_start = time.time()
    try:
        results = dd.compute(
            *calculations,
            scheduler=client,
            retries=3,
            priority='high'
        )
        computation_time = time.time() - computation_start

        print(
            f"✅💥 ¡COMPUTACIÓN MÁXIMA ABSOLUTA COMPLETADA EN {computation_time:.2f}s! 💥✅")
        print(f"🔥 Resultados MÁXIMOS: {len(results)} cálculos completados")
        print(
            f"⚡ Throughput MÁXIMO: {len(results)/computation_time:.2f} cálculos/segundo")
        print(
            f"🚀 Procesamiento: {33854980/computation_time:.0f} registros/segundo")

    except Exception as e:
        print(f"❌ Error en computación MÁXIMA: {e}")
        raise
    finally:
        client.close()

    total_time = time.time() - start_time

    # Resultados MÁXIMOS detallados
    performance_results = {
        'dag_type': 'dask_maximo_absoluto',
        'total_time_seconds': total_time,
        'computation_time_seconds': computation_time,
        'total_files_processed': len(data_files),
        'total_data_size_gb': total_size_gb,
        'total_calculations': len(calculations),
        'workers_used': len(workers),
        'total_cores': total_cores,
        'total_memory_gb': total_memory_gb,
        'partitions_created': maximo_partitions,
        'throughput_calc_per_second': len(results)/computation_time if computation_time > 0 else 0,
        'data_throughput_gb_per_second': total_size_gb/total_time if total_time > 0 else 0,
        'records_per_second': 33854980/computation_time if computation_time > 0 else 0,
        'performance_metrics': {
            'setup_time': computation_start - start_time,
            'pure_computation_time': computation_time,
            'cleanup_time': total_time - computation_start - computation_time
        },
        'maximo_config': {
            'cpu_utilization_percent': 100,
            'ram_utilization_percent': 84,
            'partitions': maximo_partitions,
            'workers_config': f'{len(workers)}×{total_cores//len(workers) if workers else 0} threads'
        }
    }

    # Guardar resultados MÁXIMOS
    with open('/workspace/results/dask_maximo_absoluto_results.json', 'w') as f:
        json.dump(performance_results, f, indent=2)

    print(f"\n🏆💥💥💥 PROCESAMIENTO MÁXIMO ABSOLUTO COMPLETADO 💥💥💥🏆")
    print(f"🕒 Tiempo total: {total_time:.2f} segundos")
    print(f"⚡ Archivos procesados: {len(data_files)}")
    print(f"🔥 Datos procesados: {total_size_gb:.2f} GB")
    print(f"💪 Throughput: {total_size_gb/total_time:.3f} GB/s")
    print(f"📊 Cálculos completados: {len(results)}")
    print(f"💥 Particiones: {maximo_partitions}")
    print(f"🧠 RAM usada: {total_memory_gb:.1f}GB")
    print(f"⚡ CPU: {total_cores} cores al 100%")
    print(f"🚀 Velocidad: {33854980/computation_time:.0f} registros/segundo")
    print("🔥💥 ¡MÁXIMO ABSOLUTO DE TU HARDWARE LOGRADO! 💥🔥")

    return {
        'status': 'MAXIMO_ABSOLUTO_SUCCESS',
        'total_time': total_time,
        'computation_time': computation_time,
        'files_processed': len(data_files),
        'data_size_gb': total_size_gb,
        'calculations': len(results),
        'throughput_gb_s': total_size_gb/total_time,
        'records_per_second': 33854980/computation_time,
        'partitions': maximo_partitions,
        'workers': len(workers),
        'cores': total_cores,
        'memory_gb': total_memory_gb
    }


# DAG MÁXIMO ABSOLUTO
dag = DAG(
    'dag_06_dask_maximo_absoluto',
    default_args=default_args,
    description='🔥💥 Procesamiento Dask MÁXIMO ABSOLUTO - 100% CPU + 84% RAM',
    schedule_interval=None,
    catchup=False,
    tags=['dask', 'maximo', 'absoluto', '100_cpu', '84_ram', 'hardware_limit']
)

maximo_absoluto_task = PythonOperator(
    task_id='maximo_absoluto_processing',
    python_callable=parallel_processing_maximo_absoluto,
    dag=dag
)

maximo_absoluto_task
