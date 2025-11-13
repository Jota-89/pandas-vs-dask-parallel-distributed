"""
DAG 2: PROCESAMIENTO SECUENCIAL BASELINE
=======================================
PROPÓSITO: Pandas tradicional secuencial como baseline para comparar con Dask
ORDEN: SEGUNDO - después de descargar datos
"""

from datetime import datetime, timedelta
from pathlib import Path
import os
import json
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
    'dag_02_sequential',
    default_args=default_args,
    description='DAG 2: Procesamiento secuencial baseline con pandas',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)


def verify_data_task(**context):
    """Verificar que los datos estén disponibles"""
    print("📥 DAG 2: VERIFICANDO DATOS PARA PROCESAMIENTO SECUENCIAL")
    print("=" * 65)

    data_dir = Path('/workspace/data/nyc_taxi')
    print(f"📁 Buscando archivos en: {data_dir}")
    print(f"📁 Directorio existe: {data_dir.exists()}")
    
    if not data_dir.exists():
        raise FileNotFoundError(f"❌ CRÍTICO: Directorio no existe: {data_dir}")
    
    parquet_files = list(data_dir.glob('*.parquet'))
    print(f"🔍 Archivos .parquet encontrados: {len(parquet_files)}")
    
    # MOSTRAR CADA ARCHIVO ENCONTRADO
    total_size = 0
    for i, f in enumerate(parquet_files, 1):
        size_mb = f.stat().st_size / (1024*1024)
        total_size += size_mb
        print(f"   {i}. {f.name} ({size_mb:.1f} MB)")

    if not parquet_files:
        print("❌ CRÍTICO: No se encontraron archivos .parquet")
        print("📂 Contenido del directorio:")
        all_files = list(data_dir.glob('*'))
        for f in all_files:
            print(f"   📄 {f.name}")
        raise FileNotFoundError("❌ Datos NYC Taxi no encontrados - ejecutar DAG 1 primero")

    print(f"\n✅ VERIFICACIÓN EXITOSA:")
    print(f"   📊 Archivos: {len(parquet_files)}")
    print(f"   💾 Tamaño total: {total_size:.1f} MB")
    print(f"   📁 Listos para procesamiento secuencial")

    return {"files": len(parquet_files), "size_mb": total_size}


def intensive_analysis_single_file(file_path):
    """Análisis computacionalmente intensivo de un archivo individual"""
    import pandas as pd
    import numpy as np
    import time

    start_time = time.time()
    
    # OPTIMIZACIÓN: Solo leer columnas necesarias
    df = pd.read_parquet(
        file_path,
        engine='pyarrow',
        columns=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'fare_amount', 'total_amount', 
                'PULocationID', 'DOLocationID', 'payment_type', 'tip_amount', 'passenger_count']
    )

    # TRABAJO INTENSIVO: Múltiples análisis complejos (OPTIMIZADO)
    results = {}

    # 1. Estadísticas avanzadas
    results['advanced_stats'] = {
        'rows': len(df),
        'avg_distance': float(df['trip_distance'].mean()),
        'std_distance': float(df['trip_distance'].std()),
        'avg_fare': float(df['fare_amount'].mean()),
        'percentiles_distance': [float(df['trip_distance'].quantile(q)) for q in [0.25, 0.5, 0.75, 0.95]],
        'percentiles_fare': [float(df['fare_amount'].quantile(q)) for q in [0.25, 0.5, 0.75, 0.95]]
    }

    # 2. Análisis temporal intensivo
    pickup_times = pd.to_datetime(df['tpep_pickup_datetime'])
    dropoff_times = pd.to_datetime(df['tpep_dropoff_datetime'])
    trip_duration = (dropoff_times - pickup_times).dt.total_seconds() / 60

    df['trip_duration_minutes'] = trip_duration
    df['pickup_hour'] = pickup_times.dt.hour
    df['pickup_day_of_week'] = pickup_times.dt.dayofweek

    hourly_analysis = df.groupby('pickup_hour').agg({
        'fare_amount': ['count', 'mean', 'std'],
        'trip_distance': ['mean', 'std'],
        'trip_duration_minutes': ['mean', 'std']
    }).round(2)

    results['hourly_analysis'] = {
        f"hour_{hour}": {
            'trips': int(hourly_analysis.loc[hour, ('fare_amount', 'count')]),
            'avg_fare': float(hourly_analysis.loc[hour, ('fare_amount', 'mean')]),
            'avg_distance': float(hourly_analysis.loc[hour, ('trip_distance', 'mean')]),
            'avg_duration': float(hourly_analysis.loc[hour, ('trip_duration_minutes', 'mean')])
        } for hour in hourly_analysis.index
    }

    # 3. Análisis geográfico intensivo
    pickup_analysis = df['PULocationID'].value_counts().head(20)
    dropoff_analysis = df['DOLocationID'].value_counts().head(20)

    results['geographic_analysis'] = {
        'top_pickup_zones': {f"zone_{zone_id}": int(count) for zone_id, count in pickup_analysis.items()},
        'top_dropoff_zones': {f"zone_{zone_id}": int(count) for zone_id, count in dropoff_analysis.items()}
    }

    # 4. Análisis de tipos de pago intensivo
    payment_analysis = df['payment_type'].value_counts()
    tip_analysis = df.groupby('payment_type')['tip_amount'].agg(['count', 'mean', 'std'])

    results['payment_analysis'] = {
        'payment_distribution': {f"type_{payment_type}": int(count) for payment_type, count in payment_analysis.items()},
        'tip_by_payment': {f"type_{idx}": {'avg_tip': float(row['mean']), 'count': int(row['count'])} 
                          for idx, row in tip_analysis.iterrows()}
    }

    # 5. Cálculos intensivos usando numpy
    fare_amounts = df['fare_amount'].values
    trip_distances = df['trip_distance'].values
    tip_amounts = df['tip_amount'].values
    total_amounts = df['total_amount'].values

    # Correlaciones
    correlations = np.corrcoef([trip_distances, fare_amounts, tip_amounts, total_amounts])
    
    # Histogramas computacionalmente intensivos
    distance_bins = np.histogram(trip_distances, bins=50)
    fare_bins = np.histogram(fare_amounts, bins=50)

    # 6. Outlier analysis
    fare_q95 = np.percentile(fare_amounts, 95)
    distance_q95 = np.percentile(trip_distances, 95)
    
    outliers = {
        'high_fare_trips': len(df[df['fare_amount'] > fare_q95]),
        'long_distance_trips': len(df[df['trip_distance'] > distance_q95])
    }

    processing_time = time.time() - start_time
    
    return {
        'file': Path(file_path).name,
        'processing_time': processing_time,
        'rows': len(df),
        'analysis': results,
        'correlations': correlations.tolist(),
        'outliers': outliers
    }


def pandas_sequential_benchmark(**context):
    """
    BENCHMARK PANDAS TRADICIONAL SECUENCIAL
    ======================================
    Procesa archivos uno por uno - BASELINE para comparar con Dask
    """
    print("DAG 2: INICIANDO PANDAS SECUENCIAL (BASELINE)")
    print("=" * 55)

    # Obtener archivos de datos  
    data_path = Path("/workspace/data/nyc_taxi")
    if not data_path.exists():
        raise Exception("No hay datos! Ejecuta primero dag_01_download_data")

    files = sorted([str(f) for f in data_path.glob("*.parquet")])
    print(f"📁 Archivos a procesar: {len(files)}")

    if not files:
        raise Exception("No hay archivos .parquet para procesar")

    # MOSTRAR CADA ARCHIVO QUE VA A PROCESAR
    for i, file_path in enumerate(files, 1):
        size_mb = Path(file_path).stat().st_size / (1024*1024)
        print(f"   {i}. {Path(file_path).name} ({size_mb:.1f} MB)")

    start_time = time.time()

    # Procesamiento SECUENCIAL (uno por uno - tradicional)
    all_results = []

    for i, file_path in enumerate(files, 1):
        print(f"\n🔄 Procesando {i}/{len(files)}: {Path(file_path).name}")
        
        file_result = intensive_analysis_single_file(file_path)
        all_results.append(file_result)
        
        print(f"✅ Completado {i}/{len(files)}: {file_result['rows']:,} filas en {file_result['processing_time']:.1f}s")

    end_time = time.time()
    total_time = end_time - start_time

    # Calcular métricas totales
    total_rows = sum(r['rows'] for r in all_results)
    
    results = {
        'method': 'pandas_sequential_baseline',
        'dag': 'dag_02_sequential',
        'execution_type': 'secuencial_tradicional',
        'total_time': total_time,
        'files_processed': len(files),
        'total_rows': total_rows,
        'throughput': total_rows / total_time,
        'avg_time_per_file': total_time / len(files),
        'file_results': all_results
    }

    # Guardar resultados
    results_path = Path("/workspace/results/sequential_results.json")
    results_path.parent.mkdir(exist_ok=True)

    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print("DAG 2 SECUENCIAL COMPLETADO (BASELINE):")
    print(f"   ⏱️  Tiempo total: {total_time:.2f}s")
    print(f"   📊 Archivos: {len(files)}")
    print(f"   📈 Filas: {total_rows:,}")
    print(f"   🐌 Throughput: {results['throughput']:,.0f} filas/seg")
    print(f"   📄 Tiempo promedio/archivo: {results['avg_time_per_file']:.2f}s")
    print(f"   💾 Guardado: {results_path}")
    
    return results


# Tareas del DAG
verify_task = PythonOperator(
    task_id='verify_data',
    python_callable=verify_data_task,
    dag=dag
)

sequential_task = PythonOperator(
    task_id='pandas_sequential',
    python_callable=pandas_sequential_benchmark,
    dag=dag
)

# Dependencias simples
verify_task >> sequential_task

dag.doc_md = """
# DAG 2: Procesamiento Secuencial Baseline

**ORDEN: EJECUTAR SEGUNDO**

## Propósito
Procesar archivos NYC Taxi usando pandas tradicional secuencial como baseline.

## Método
- Procesa archivos uno por uno (secuencial)
- Pandas tradicional sin paralelismo
- Análisis computacionalmente intensivo por archivo
- Baseline para comparar con Dask distribuido

## Siguientes DAGs
Después de este, ejecutar: 
- `dag_03_parallel` (Dask distribuido)
- `dag_04_comparison` (comparación final)
"""