"""
DAG 5: ANALISIS DETALLADO DE ESTADISTICAS
==========================================
PROPÓSITO: Generar análisis completo de estadísticas NYC Taxi
ORDEN: QUINTO - después de comparación
"""

from datetime import datetime, timedelta
import json
from pathlib import Path
import pandas as pd

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
    'dag_05_analytics',
    default_args=default_args,
    description='DAG 5: Análisis detallado estadísticas',
    schedule_interval=None,
    catchup=False,
)


def generate_detailed_analytics(**context):
    """DAG 5: Generar análisis detallado de todas las métricas"""
    print("DAG 5: GENERANDO ANALISIS DETALLADO")
    print("=" * 60)

    results_dir = Path("/workspace/results")
    sequential_file = results_dir / "sequential_results.json"
    
    if not sequential_file.exists():
        raise Exception("No hay resultados secuenciales para analizar")

    with open(sequential_file) as f:
        data = json.load(f)

    # Mapeo de tipos de pago CORREGIDO
    payment_type_mapping = {
        '1': 'Tarjeta de Crédito',
        '2': 'Efectivo', 
        '3': 'Sin Cargo',
        '4': 'Disputa',
        '5': 'Desconocido',
        '6': 'Viaje Cancelado'
    }

    # Consolidar datos de todos los archivos
    all_hourly = {}
    all_zones_pickup = {}
    all_zones_dropoff = {}
    all_payments = {}
    total_tips_by_payment = {}
    
    advanced_stats = {
        'total_distance': 0,
        'total_fare': 0,
        'total_trips': 0,
        'avg_distance_all': 0,
        'avg_fare_all': 0
    }

    # Consolidar datos de todos los meses
    for file_result in data['file_results']:
        analysis = file_result['analysis']
        
        # Estadísticas avanzadas
        stats = analysis['advanced_stats']
        rows = stats['rows']
        advanced_stats['total_trips'] += rows
        advanced_stats['total_distance'] += stats['avg_distance'] * rows
        advanced_stats['total_fare'] += stats['avg_fare'] * rows
        
        # Análisis por hora
        for hour, hour_data in analysis['hourly_analysis'].items():
            if hour not in all_hourly:
                all_hourly[hour] = {'trips': 0, 'total_fare': 0, 'total_distance': 0, 'total_duration': 0}
            
            trips = hour_data['trips']
            all_hourly[hour]['trips'] += trips
            all_hourly[hour]['total_fare'] += hour_data['avg_fare'] * trips
            all_hourly[hour]['total_distance'] += hour_data['avg_distance'] * trips
            all_hourly[hour]['total_duration'] += hour_data['avg_duration'] * trips

        # Zonas geográficas
        for zone, count in analysis['geographic_analysis']['top_pickup_zones'].items():
            all_zones_pickup[zone] = all_zones_pickup.get(zone, 0) + count
            
        for zone, count in analysis['geographic_analysis']['top_dropoff_zones'].items():
            all_zones_dropoff[zone] = all_zones_dropoff.get(zone, 0) + count

        # Tipos de pago - APLICAR MAPEO CORREGIDO
        for payment_type, count in analysis['payment_analysis']['payment_distribution'].items():
            # Convertir a nombre descriptivo
            payment_name = payment_type_mapping.get(str(payment_type), f'Tipo {payment_type}')
            all_payments[payment_name] = all_payments.get(payment_name, 0) + count
            
        for payment_type, tip_data in analysis['payment_analysis']['tip_by_payment'].items():
            # Convertir a nombre descriptivo
            payment_name = payment_type_mapping.get(str(payment_type), f'Tipo {payment_type}')
            if payment_name not in total_tips_by_payment:
                total_tips_by_payment[payment_name] = {'total_tips': 0, 'total_trips': 0}
            
            total_tips_by_payment[payment_name]['total_tips'] += tip_data['avg_tip'] * tip_data['count']
            total_tips_by_payment[payment_name]['total_trips'] += tip_data['count']

    # Calcular promedios finales
    advanced_stats['avg_distance_all'] = advanced_stats['total_distance'] / advanced_stats['total_trips']
    advanced_stats['avg_fare_all'] = advanced_stats['total_fare'] / advanced_stats['total_trips']

    # Calcular promedios por hora
    hourly_summary = {}
    for hour, hour_data in all_hourly.items():
        trips = hour_data['trips']
        hourly_summary[hour] = {
            'trips': trips,
            'avg_fare': hour_data['total_fare'] / trips,
            'avg_distance': hour_data['total_distance'] / trips,
            'avg_duration': hour_data['total_duration'] / trips,
            'percentage_trips': (trips / advanced_stats['total_trips']) * 100
        }

    # Top zonas
    top_pickup_zones = sorted(all_zones_pickup.items(), key=lambda x: x[1], reverse=True)[:10]
    top_dropoff_zones = sorted(all_zones_dropoff.items(), key=lambda x: x[1], reverse=True)[:10]

    # Promedios de propinas por tipo de pago
    avg_tips = {}
    for payment_type, tip_data in total_tips_by_payment.items():
        avg_tips[payment_type] = tip_data['total_tips'] / tip_data['total_trips']

    # Encontrar horas peak
    peak_hours = sorted(hourly_summary.items(), key=lambda x: x[1]['trips'], reverse=True)[:5]
    low_hours = sorted(hourly_summary.items(), key=lambda x: x[1]['trips'])[:5]

    # Crear reporte detallado
    detailed_report = {
        'timestamp': datetime.now().isoformat(),
        'dataset_overview': {
            'total_files': data['files_processed'],
            'total_trips': advanced_stats['total_trips'],
            'total_rows': data['total_rows'],
            'processing_time': data['total_time'],
            'throughput_rows_per_sec': data['throughput']
        },
        'financial_metrics': {
            'average_fare': round(advanced_stats['avg_fare_all'], 2),
            'total_revenue_estimate': round(advanced_stats['total_fare'], 2),
            'fare_per_mile': round(advanced_stats['avg_fare_all'] / advanced_stats['avg_distance_all'], 2)
        },
        'distance_metrics': {
            'average_distance_miles': round(advanced_stats['avg_distance_all'], 2),
            'total_miles_estimate': round(advanced_stats['total_distance'], 2)
        },
        'temporal_analysis': {
            'peak_hours': [
                {
                    'hour': peak[0],
                    'trips': peak[1]['trips'],
                    'percentage': round(peak[1]['percentage_trips'], 1),
                    'avg_fare': round(peak[1]['avg_fare'], 2),
                    'avg_distance': round(peak[1]['avg_distance'], 2)
                }
                for peak in peak_hours
            ],
            'low_hours': [
                {
                    'hour': low[0],
                    'trips': low[1]['trips'],
                    'percentage': round(low[1]['percentage_trips'], 1)
                }
                for low in low_hours
            ]
        },
        'geographic_analysis': {
            'top_pickup_zones': [
                {'zone': zone, 'trips': count, 'percentage': round((count/advanced_stats['total_trips'])*100, 1)}
                for zone, count in top_pickup_zones
            ],
            'top_dropoff_zones': [
                {'zone': zone, 'trips': count, 'percentage': round((count/advanced_stats['total_trips'])*100, 1)}
                for zone, count in top_dropoff_zones
            ]
        },
        'payment_analysis': {
            'payment_distribution': [
                {
                    'type': payment_type,
                    'trips': count,
                    'percentage': round((count/advanced_stats['total_trips'])*100, 1),
                    'avg_tip': round(avg_tips.get(payment_type, 0), 2)
                }
                for payment_type, count in sorted(all_payments.items(), key=lambda x: x[1], reverse=True)
            ]
        }
    }

    # Guardar reporte
    report_file = results_dir / "detailed_analytics_report.json"
    with open(report_file, 'w') as f:
        json.dump(detailed_report, f, indent=2)

    # Mostrar resumen por consola
    print("ANALISIS DETALLADO COMPLETADO")
    print("=" * 50)
    print(f"DATASET:")
    print(f"  Total viajes: {advanced_stats['total_trips']:,}")
    print(f"  Tarifa promedio: ${advanced_stats['avg_fare_all']:.2f}")
    print(f"  Distancia promedio: {advanced_stats['avg_distance_all']:.2f} millas")
    print(f"")
    print(f"HORAS PEAK (Top 3):")
    for i, (hour, data) in enumerate(peak_hours[:3]):
        print(f"  {i+1}. Hora {hour.replace('hour_', '')}: {data['trips']:,} viajes ({data['percentage_trips']:.1f}%)")
    print(f"")
    print(f"ZONAS TOP PICKUP (Top 3):")
    for i, (zone, count) in enumerate(top_pickup_zones[:3]):
        pct = (count/advanced_stats['total_trips'])*100
        print(f"  {i+1}. Zona {zone}: {count:,} viajes ({pct:.1f}%)")
    print(f"")
    print(f"TIPOS DE PAGO CORREGIDOS:")
    for payment_type, count in sorted(all_payments.items(), key=lambda x: x[1], reverse=True):
        pct = (count/advanced_stats['total_trips'])*100
        tip_avg = avg_tips.get(payment_type, 0)
        print(f"  {payment_type}: {count:,} viajes ({pct:.1f}%) - Propina: ${tip_avg:.2f}")
    
    print(f"")
    print(f"Reporte guardado en: {report_file}")
    
    return detailed_report


# Tarea
analytics_task = PythonOperator(
    task_id='generate_analytics',
    python_callable=generate_detailed_analytics,
    dag=dag,
)

dag.doc_md = """
# DAG 5: Análisis Detallado

**ORDEN: EJECUTAR QUINTO**

## Prerrequisitos
-  dag_02_sequential completado

## Análisis Incluidos
-  Métricas financieras (tarifas, ingresos)
-  Métricas de distancia  
-  Análisis temporal (horas peak)
-  Análisis geográfico (zonas top)
-  Análisis de tipos de pago y propinas

## Archivo Generado
- `detailed_analytics_report.json`
"""