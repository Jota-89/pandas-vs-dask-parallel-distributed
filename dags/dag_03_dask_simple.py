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
    """DAG 3: Procesamiento paralelo con Dask"""
    print("DAG 3: PROCESAMIENTO PARALELO CON DASK")
    print("=" * 50)

    data_dir = Path("/workspace/data/nyc_taxi")
    if not data_dir.exists():
        raise Exception("No hay datos! Ejecuta dag_01_download_data primero")

    # Usar Dask para procesamiento paralelo
    import dask.dataframe as dd
    import pandas as pd
    
    start_time = time.time()
    
    # Leer todos los archivos con Dask
    parquet_files = list(data_dir.glob("*.parquet"))
    print(f"Archivos encontrados: {len(parquet_files)}")
    
    if not parquet_files:
        raise Exception("No hay archivos .parquet para procesar")
    
    # Leer con Dask (procesamiento paralelo automático)
    ddf = dd.read_parquet(str(data_dir / "*.parquet"))
    
    # Realizar análisis básico con Dask
    total_rows = len(ddf)
    avg_fare = ddf['fare_amount'].mean().compute()
    avg_distance = ddf['trip_distance'].mean().compute()
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    # Preparar resultados
    results = {
        'timestamp': datetime.now().isoformat(),
        'dag': 'dag_03_dask_simple',
        'method': 'dask_parallel',
        'total_time': processing_time,
        'total_rows': int(total_rows),
        'files_processed': len(parquet_files),
        'throughput': total_rows / processing_time,
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
    
    print(f"DAG 3: COMPLETADO - Procesamiento paralelo")
    print(f"Tiempo: {processing_time:.2f}s")
    print(f"Filas: {total_rows:,}")
    print(f"Throughput: {results['throughput']:,.0f} filas/s")
    print(f"Resultados: {results_file}")
    
    return results


# Tarea
dask_task = PythonOperator(
    task_id='parallel_processing',
    python_callable=parallel_processing_dask,
    dag=dag,
)

dag.doc_md = """
# DAG 3: Procesamiento Paralelo (Dask)

**ORDEN: EJECUTAR TERCERO**

## Prerrequisitos
- ✅ dag_01_download_data completado
- ✅ dag_02_sequential completado

## Método
- **Dask**: Procesamiento distribuido/paralelo automático
- **Paralelización**: Múltiples workers procesan datos simultáneamente

## Archivo Generado
- `dask_simple_results.json`

## Siguiente
- `dag_04_comparison` (comparación final)
"""