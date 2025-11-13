"""
DAG 1: DESCARGA DE DATOS NYC TAXI
=================================
PROPÓSITO: Descargar y verificar datos reales de NYC Taxi
ORDEN: PRIMERO - debe ejecutarse antes que todo
"""

from datetime import datetime, timedelta
import urllib.request
import os
from pathlib import Path

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
    'dag_01_download_data',
    default_args=default_args,
    description='DAG 1: Descargar datos NYC Taxi',
    schedule_interval=None,
    catchup=False,
)


def download_nyc_data(**context):
    """Descargar archivos NYC Taxi reales"""
    print("📥 DAG 1: DESCARGANDO DATOS NYC TAXI")
    print("=" * 50)

    data_dir = Path("/workspace/data/nyc_taxi")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Directorio destino: {data_dir}")
    print(f"📁 Directorio existe: {data_dir.exists()}")

    # URLs de datos reales NYC Taxi 2024
    files_to_download = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-04.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet",
    ]

    print(f"🌐 URLs a descargar: {len(files_to_download)}")

    downloaded_files = []
    total_size = 0

    # Listar archivos existentes ANTES
    print(f"\n📂 CONTENIDO ACTUAL DIRECTORIO:")
    existing_files = list(data_dir.glob("*"))
    for f in existing_files:
        size_mb = f.stat().st_size / (1024 * 1024) if f.is_file() else 0
        print(f"   📄 {f.name} ({size_mb:.1f} MB)")

    for i, url in enumerate(files_to_download, 1):
        filename = url.split("/")[-1]
        file_path = data_dir / filename

        print(f"\n🔍 {i}/5 Verificando: {filename}")
        print(f"   📁 Ruta completa: {file_path}")
        print(f"   📁 Existe: {file_path.exists()}")

        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"✅ {i}/5 Ya existe: {filename} ({size_mb:.1f} MB)")
            downloaded_files.append(str(file_path))
            total_size += size_mb
            continue

        print(f"📥 {i}/5 Descargando: {filename}...")
        print(f"   🌐 Desde URL: {url}")
        
        try:
            urllib.request.urlretrieve(url, file_path)
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"✅ {i}/5 DESCARGADO EXITOSAMENTE: {filename} ({size_mb:.1f} MB)")
            print(f"   📁 Verificación existe: {file_path.exists()}")
            downloaded_files.append(str(file_path))
            total_size += size_mb
        except Exception as e:
            print(f"❌ ERROR descargando {filename}: {e}")

    # Listar archivos existentes DESPUÉS
    print(f"\n📂 CONTENIDO FINAL DIRECTORIO:")
    final_files = list(data_dir.glob("*"))
    for f in final_files:
        size_mb = f.stat().st_size / (1024 * 1024) if f.is_file() else 0
        print(f"   📄 {f.name} ({size_mb:.1f} MB)")

    print(f"\n📊 RESUMEN DESCARGA:")
    print(f"   ✅ Archivos descargados: {len(downloaded_files)}")
    print(f"   💾 Tamaño total: {total_size:.1f} MB")
    print(f"   📁 Ubicación: {data_dir}")
    
    # MOSTRAR LISTA COMPLETA DE ARCHIVOS
    print(f"\n📋 LISTA COMPLETA DE ARCHIVOS:")
    for i, file_path in enumerate(downloaded_files, 1):
        print(f"   {i}. {file_path}")

    # Verificar que tenemos al menos algunos archivos
    if len(downloaded_files) == 0:
        raise Exception("❌ CRÍTICO: No se pudieron descargar archivos")

    print(f"\n🎉 DAG 1 COMPLETADO EXITOSAMENTE!")
    
    return {
        'files_downloaded': len(downloaded_files),
        'total_size_mb': total_size,
        'file_list': downloaded_files
    }


download_task = PythonOperator(
    task_id='download_nyc_data',
    python_callable=download_nyc_data,
    dag=dag,
)

dag.doc_md = """
# DAG 1: Descarga de Datos NYC Taxi

**ORDEN: EJECUTAR PRIMERO**

## Propósito
Descargar datos reales de NYC Taxi para el benchmark.

## Archivos Descargados
- 5 archivos .parquet de 2024 (~250MB total)
- Datos reales de millones de viajes

## Siguiente DAG
Después de este, ejecutar: `dag_02_sequential`
"""
