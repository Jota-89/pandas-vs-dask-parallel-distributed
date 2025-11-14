"""
DAG 4: COMPARACIÓN Y RESULTADOS FINALES
=======================================
PROPÓSITO: Comparar resultados secuencial vs paralelo y generar gráficas
ORDEN: CUARTO - último, después de ambos benchmarks
"""

from datetime import datetime, timedelta
import json
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
    'dag_04_comparison',
    default_args=default_args,
    description='DAG 4: Comparación final y gráficas',
    schedule_interval=None,
    catchup=False,
)


def generate_comparison_report(**context):
    """DAG 4: Generar reporte final comparando ambos métodos"""
    print("DAG 4: GENERANDO COMPARACION FINAL")
    print("=" * 50)

    results_dir = Path("/workspace/results")
    if not results_dir.exists():
        raise Exception(
            "No hay resultados! Ejecuta primero dag_02_sequential y dag_03_parallel")

    # Buscar resultados específicos
    sequential_file = results_dir / "sequential_results.json"
    parallel_file = results_dir / "dask_turbo_results.json"  # ARCHIVO DASK TURBO

    if not sequential_file.exists():
        raise Exception(
            "No hay resultados secuenciales! Ejecuta dag_02_sequential primero")

    if not parallel_file.exists():
        raise Exception(
            "No hay resultados paralelos! Ejecuta dag_03_dask_turbo primero")

    # Cargar resultados
    with open(sequential_file) as f:
        sequential_data = json.load(f)

    with open(parallel_file) as f:
        parallel_data = json.load(f)

    # Calcular métricas
    seq_time = sequential_data.get('total_time', 0)
    par_time = parallel_data.get('total_time', 0)

    if seq_time > 0 and par_time > 0:
        speedup = seq_time / par_time
        efficiency = speedup / 6  # 6 workers
        improvement = ((speedup - 1) * 100)

        comparison_report = {
            'benchmark_date': datetime.now().isoformat(),
            'summary': {
                'sequential_time': seq_time,
                'parallel_time': par_time,
                'speedup': speedup,
                'efficiency': efficiency,
                'improvement_percent': improvement
            },
            'detailed_results': {
                'sequential': sequential_data,
                'parallel': parallel_data
            },
            'conclusion': {
                'winner': 'Paralelo' if speedup > 1 else 'Secuencial',
                'performance_gain': f"{improvement:.1f}%" if speedup > 1 else f"{(1-speedup)*100:.1f}% slower"
            }
        }

        # Guardar reporte
        report_file = results_dir / "final_benchmark_report.json"
        with open(report_file, 'w') as f:
            json.dump(comparison_report, f, indent=2)

        print(f"DAG 4: COMPARACION COMPLETADA")
        print(f"Reporte: {report_file}")
        print(f"")
        print(f"RESULTADOS FINALES:")
        print(f"   Secuencial:    {seq_time:.2f}s")
        print(f"   Paralelo:      {par_time:.2f}s")
        print(f"   Speedup:       {speedup:.2f}x")
        print(f"   Eficiencia:    {efficiency:.2f}")
        print(f"   Mejora:        {improvement:.1f}%")
        print(f"")
        print(f"GANADOR: {comparison_report['conclusion']['winner']}")

        return comparison_report

    raise Exception("Error calculando métricas")


def generate_final_charts(**context):
    """DAG 4: Generar gráficas finales simples"""
    import matplotlib.pyplot as plt

    print("DAG 4: GENERANDO GRAFICAS FINALES SIMPLES")

    report_file = Path("/workspace/results/final_benchmark_report.json")
    if not report_file.exists():
        raise Exception("No hay reporte de comparación")

    with open(report_file) as f:
        report = json.load(f)

    summary = report['summary']

    # Crear gráfica
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Tiempos
    methods = ['Pandas\nSecuencial', 'Dask\nParalelo']
    times = [summary['sequential_time'], summary['parallel_time']]
    colors = ['#ff6b6b', '#4ecdc4']

    bars = ax1.bar(methods, times, color=colors, alpha=0.8)
    ax1.set_ylabel('Tiempo (segundos)')
    ax1.set_title('Comparación de Tiempos')
    ax1.grid(axis='y', alpha=0.3)

    for bar, time in zip(bars, times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                 f'{time:.1f}s', ha='center', va='bottom', fontweight='bold')

    # Métricas
    metrics = ['Speedup', 'Eficiencia']
    values = [summary['speedup'], summary['efficiency']]

    bars2 = ax2.bar(metrics, values, color=['#45b7d1', '#96ceb4'], alpha=0.8)
    ax2.set_ylabel('Factor')
    ax2.set_title('Métricas de Rendimiento')
    ax2.grid(axis='y', alpha=0.3)

    for bar, value in zip(bars2, values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                 f'{value:.2f}x', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()

    chart_file = Path("/workspace/results/final_comparison_chart.png")
    plt.savefig(chart_file, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"DAG 4: Grafica guardada en {chart_file}")
    return True


def generate_technical_charts(**context):
    """DAG 4: Generar gráficos técnicos completos"""
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    import numpy as np
    import psutil

    print("DAG 4: GENERANDO GRAFICOS TECNICOS COMPLETOS")
    print("=" * 60)

    results_dir = Path("/workspace/results")

    # Cargar datos
    sequential_file = results_dir / "sequential_results.json"
    parallel_file = results_dir / "dask_turbo_results.json"

    if not sequential_file.exists() or not parallel_file.exists():
        raise Exception("Faltan archivos de resultados para gráficos técnicos")

    with open(sequential_file) as f:
        sequential_data = json.load(f)
    with open(parallel_file) as f:
        parallel_data = json.load(f)

    # Obtener información del sistema
    cpu_count = psutil.cpu_count()
    cpu_count_logical = psutil.cpu_count(logical=True)
    memory = psutil.virtual_memory()
    memory_gb = memory.total / (1024**3)

    seq_time = sequential_data['total_time']
    par_time = parallel_data['total_time']
    speedup = seq_time / par_time
    efficiency = speedup / cpu_count

    print(
        f"Sistema detectado: {cpu_count} cores, {cpu_count_logical} threads, {memory_gb:.2f}GB RAM")

    # GRÁFICO 1: ANÁLISIS DE PERFORMANCE TÉCNICO
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('ANÁLISIS TÉCNICO DE PERFORMANCE - NYC TAXI BENCHMARK',
                 fontsize=16, fontweight='bold')

    # 1A. Comparación de tiempos con detalles técnicos
    methods = ['Pandas\nSecuencial', 'Dask\nParalelo']
    times = [seq_time, par_time]
    colors = ['#e74c3c', '#27ae60']

    bars = ax1.bar(methods, times, color=colors, alpha=0.8,
                   edgecolor='black', linewidth=1)
    ax1.set_ylabel('Tiempo de Procesamiento (segundos)', fontweight='bold')
    ax1.set_title('Tiempo de Ejecución por Método', fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)

    for bar, time in zip(bars, times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                 f'{time:.2f}s\n({int(33800000/time):,} reg/s)',
                 ha='center', va='bottom', fontweight='bold')

    # 1B. Speedup y Eficiencia
    metrics = ['Speedup', 'Eficiencia', 'Mejora %']
    values = [speedup, efficiency, (speedup - 1) * 100]
    colors_metrics = ['#3498db', '#f39c12', '#9b59b6']

    bars2 = ax2.bar(metrics, values, color=colors_metrics,
                    alpha=0.8, edgecolor='black', linewidth=1)
    ax2.set_ylabel('Factor / Porcentaje', fontweight='bold')
    ax2.set_title('Métricas de Performance', fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)

    for bar, value, metric in zip(bars2, values, metrics):
        height = bar.get_height()
        if metric == 'Mejora %':
            text = f'{value:.1f}%'
        else:
            text = f'{value:.2f}x'
        ax2.text(bar.get_x() + bar.get_width()/2., height + max(values)*0.02,
                 text, ha='center', va='bottom', fontweight='bold')

    # 1C. Utilización de recursos
    resource_labels = ['CPU Cores', 'CPU Threads', 'RAM (GB)']
    resource_values = [cpu_count, cpu_count_logical, memory_gb]
    colors_resources = ['#e67e22', '#34495e', '#2ecc71']

    bars3 = ax3.bar(resource_labels, resource_values,
                    color=colors_resources, alpha=0.8, edgecolor='black', linewidth=1)
    ax3.set_ylabel('Cantidad', fontweight='bold')
    ax3.set_title('Recursos del Sistema', fontweight='bold')
    ax3.grid(axis='y', alpha=0.3)

    for bar, value in zip(bars3, resource_values):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + max(resource_values)*0.02,
                 f'{value:.1f}', ha='center', va='bottom', fontweight='bold')

    # 1D. Throughput comparativo
    throughput_seq = 33800000 / seq_time
    throughput_par = 33800000 / par_time

    throughput_methods = ['Secuencial', 'Paralelo']
    throughput_values = [throughput_seq, throughput_par]

    bars4 = ax4.bar(throughput_methods, throughput_values, color=[
                    '#e74c3c', '#27ae60'], alpha=0.8, edgecolor='black', linewidth=1)
    ax4.set_ylabel('Registros por Segundo', fontweight='bold')
    ax4.set_title('Throughput de Procesamiento', fontweight='bold')
    ax4.grid(axis='y', alpha=0.3)

    for bar, value in zip(bars4, throughput_values):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height + max(throughput_values)*0.02,
                 f'{value:,.0f}', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    chart1_file = results_dir / "technical_performance_analysis.png"
    plt.savefig(chart1_file, dpi=300, bbox_inches='tight')
    plt.close()

    # GRÁFICO 2: ANÁLISIS DE ESCALABILIDAD
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('ANÁLISIS DE ESCALABILIDAD Y RECURSOS',
                 fontsize=16, fontweight='bold')

    # 2A. Escalabilidad teórica vs real
    workers = np.arange(1, cpu_count + 1)
    theoretical_speedup = workers
    real_speedup_projected = [
        1 + (speedup - 1) * (w / cpu_count) * (1 - 0.1 * (w - 1) / cpu_count) for w in workers]

    ax1.plot(workers, theoretical_speedup, 'b--',
             linewidth=2, label='Speedup Teórico', marker='o')
    ax1.plot(workers, real_speedup_projected, 'r-', linewidth=2,
             label='Speedup Real Proyectado', marker='s')
    ax1.axhline(y=speedup, color='green', linestyle='-',
                linewidth=3, label=f'Speedup Actual ({speedup:.2f}x)')
    ax1.axvline(x=cpu_count, color='orange', linestyle=':',
                linewidth=2, label=f'CPU Cores ({cpu_count})')

    ax1.set_xlabel('Número de Workers', fontweight='bold')
    ax1.set_ylabel('Speedup Factor', fontweight='bold')
    ax1.set_title('Escalabilidad: Teórico vs Real', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 2B. Eficiencia por número de workers
    efficiency_values = [s / w for s,
                         w in zip(real_speedup_projected, workers)]

    ax2.bar(workers, efficiency_values, color='orange',
            alpha=0.7, edgecolor='black')
    ax2.axhline(y=efficiency, color='red', linestyle='-',
                linewidth=3, label=f'Eficiencia Actual ({efficiency:.2f})')
    ax2.set_xlabel('Número de Workers', fontweight='bold')
    ax2.set_ylabel('Eficiencia (0-1)', fontweight='bold')
    ax2.set_title('Eficiencia por Workers', fontweight='bold')
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)

    # 2C. Overhead de paralelización
    overhead_percentage = [(theoretical_speedup[i] - real_speedup_projected[i]) /
                           theoretical_speedup[i] * 100 for i in range(len(workers))]

    ax3.plot(workers, overhead_percentage, 'purple', linewidth=3, marker='D')
    ax3.set_xlabel('Número de Workers', fontweight='bold')
    ax3.set_ylabel('Overhead (%)', fontweight='bold')
    ax3.set_title('Overhead de Paralelización', fontweight='bold')
    ax3.grid(True, alpha=0.3)

    # 2D. Tiempo proyectado por workers
    time_projected = [seq_time / s for s in real_speedup_projected]

    ax4.plot(workers, time_projected, 'teal', linewidth=3, marker='o')
    ax4.axhline(y=par_time, color='red', linestyle='-',
                linewidth=3, label=f'Tiempo Real ({par_time:.2f}s)')
    ax4.set_xlabel('Número de Workers', fontweight='bold')
    ax4.set_ylabel('Tiempo (segundos)', fontweight='bold')
    ax4.set_title('Tiempo Proyectado por Workers', fontweight='bold')
    ax4.legend()
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    chart2_file = results_dir / "scalability_analysis.png"
    plt.savefig(chart2_file, dpi=300, bbox_inches='tight')
    plt.close()

    # GRÁFICO 3: ANÁLISIS DE MEMORIA Y CPU
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('ANÁLISIS DE MEMORIA Y CPU - SISTEMA',
                 fontsize=16, fontweight='bold')

    # 3A. Comparación de uso de memoria estimado
    memory_seq_mb = 570  # Archivo original
    memory_par_mb = memory_seq_mb * 1.3  # Overhead de Dask

    memory_methods = ['Secuencial\n(Pandas)', 'Paralelo\n(Dask)']
    memory_usage = [memory_seq_mb, memory_par_mb]

    bars = ax1.bar(memory_methods, memory_usage, color=[
                   '#e74c3c', '#27ae60'], alpha=0.8, edgecolor='black')
    ax1.set_ylabel('Uso de Memoria (MB)', fontweight='bold')
    ax1.set_title('Uso Estimado de Memoria', fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)

    for bar, usage in zip(bars, memory_usage):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 10,
                 f'{usage:.0f} MB', ha='center', va='bottom', fontweight='bold')

    # 3B. CPU utilization timeline simulado
    time_points = np.linspace(0, max(seq_time, par_time), 100)
    cpu_seq = np.where(time_points <= seq_time, 100, 0)  # 1 core al 100%
    cpu_par = np.where(time_points <= par_time, 100 *
                       cpu_count * 0.8, 0)  # Múltiples cores

    ax2.fill_between(time_points, 0, cpu_seq, alpha=0.7,
                     color='red', label='CPU Secuencial')
    ax2.fill_between(time_points, 0, cpu_par, alpha=0.7,
                     color='green', label='CPU Paralelo')
    ax2.set_xlabel('Tiempo (segundos)', fontweight='bold')
    ax2.set_ylabel('CPU Total (%)', fontweight='bold')
    ax2.set_title('Utilización de CPU (Simulado)', fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # 3C. Recursos del sistema - gráfico circular
    sizes = [cpu_count, cpu_count_logical - cpu_count, 24 - cpu_count_logical]
    labels = [f'Cores Físicos\n({cpu_count})', f'Hyperthreading\n({cpu_count_logical - cpu_count})',
              f'No Disponible\n({24 - cpu_count_logical})']
    colors = ['#e74c3c', '#f39c12', '#bdc3c7']

    ax3.pie(sizes, labels=labels, colors=colors,
            autopct='%1.1f%%', startangle=90)
    ax3.set_title('Distribución de CPU del Sistema', fontweight='bold')

    # 3D. Métricas de eficiencia energética estimada
    power_seq_watts = 50  # CPU básico
    power_par_watts = cpu_count * 15  # Multi-core

    energy_seq = power_seq_watts * seq_time / 3600  # Wh
    energy_par = power_par_watts * par_time / 3600  # Wh

    energy_methods = ['Secuencial', 'Paralelo']
    energy_usage = [energy_seq, energy_par]

    bars = ax4.bar(energy_methods, energy_usage, color=[
                   '#e74c3c', '#27ae60'], alpha=0.8, edgecolor='black')
    ax4.set_ylabel('Energía Estimada (Wh)', fontweight='bold')
    ax4.set_title('Eficiencia Energética Estimada', fontweight='bold')
    ax4.grid(axis='y', alpha=0.3)

    for bar, usage in zip(bars, energy_usage):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height + max(energy_usage)*0.02,
                 f'{usage:.3f} Wh', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    chart3_file = results_dir / "memory_cpu_analysis.png"
    plt.savefig(chart3_file, dpi=300, bbox_inches='tight')
    plt.close()

    # Guardar resumen técnico
    technical_summary = {
        'timestamp': datetime.now().isoformat(),
        'system_info': {
            'cpu_count': cpu_count,
            'cpu_count_logical': cpu_count_logical,
            'memory_total_gb': memory_gb,
            'memory_available_gb': memory.available / (1024**3),
            'memory_percent': memory.percent,
            'cpu_percent': psutil.cpu_percent(interval=1)
        },
        'performance_metrics': {
            'sequential_time': seq_time,
            'parallel_time': par_time,
            'speedup': speedup,
            'efficiency': efficiency,
            'throughput_sequential': 33800000 / seq_time,
            'throughput_parallel': 33800000 / par_time
        },
        'charts_generated': [
            str(chart1_file),
            str(chart2_file),
            str(chart3_file)
        ]
    }

    summary_file = results_dir / "technical_benchmark_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(technical_summary, f, indent=2)

    print(f"DAG 4: GRÁFICOS TÉCNICOS COMPLETADOS")
    print(f"  1. {chart1_file.name}")
    print(f"  2. {chart2_file.name}")
    print(f"  3. {chart3_file.name}")
    print(f"  Resumen: {summary_file.name}")

    return technical_summary


# Tareas
comparison_task = PythonOperator(
    task_id='generate_comparison',
    python_callable=generate_comparison_report,
    dag=dag,
)

charts_task = PythonOperator(
    task_id='generate_charts',
    python_callable=generate_final_charts,
    dag=dag,
)

technical_charts_task = PythonOperator(
    task_id='generate_technical_charts',
    python_callable=generate_technical_charts,
    dag=dag,
)

comparison_task >> charts_task >> technical_charts_task

dag.doc_md = """
# DAG 4: Comparación Final

**ORDEN: EJECUTAR CUARTO (ÚLTIMO)**

## Prerrequisitos
- ✅ dag_02_sequential completado
- ✅ dag_03_parallel completado

## Archivos Generados
- `final_benchmark_report.json`
- `final_comparison_chart.png`
"""
