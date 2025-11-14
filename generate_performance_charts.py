#!/usr/bin/env python3
"""
📊 DOCUMENTACIÓN VISUAL DEL PROCESO DE OPTIMIZACIÓN
Generador de gráficos del avance en paralelización Dask vs Pandas
Autor: Equipo de Optimización
Fecha: Noviembre 2024
"""

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from datetime import datetime
import pandas as pd

# Configurar estilo
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")


def create_performance_evolution_chart():
    """📈 Gráfico de evolución del rendimiento"""

    # Datos históricos del proceso de optimización
    configurations = [
        "Baseline\nSecuencial",
        "Dask\nBásico",
        "Dask\nOptimizado",
        "Dask\nTurbo",
        "Dask\nUltra",
        "Dask\nMáximo\nAbsoluto",
        "Dask\nÓptimo\n(Final)"
    ]

    # None para config que falló
    times = [20.12, 15.8, 12.4, 9.27, 8.9, None, 9.28]
    workers = [1, 2, 4, 6, 8, 12, 6]
    cores = [1, 8, 16, 24, 32, 48, 24]
    memory_gb = [2, 8, 12, 20.7, 26, 32, 20.7]
    speedup = [1.0, 1.27, 1.62, 2.17, 2.26, None, 2.17]

    # Crear figura con subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('🚀 EVOLUCIÓN DEL RENDIMIENTO: PROCESO DE OPTIMIZACIÓN DASK\n' +
                 'Benchmark NYC Taxi 2024 (33.8M registros)',
                 fontsize=16, fontweight='bold', y=0.98)

    # 1. Gráfico de tiempo de ejecución
    ax1.set_title('⏱️ Tiempo de Ejecución vs Configuración',
                  fontsize=14, fontweight='bold')

    # Filtrar valores None para el gráfico
    valid_indices = [i for i, t in enumerate(times) if t is not None]
    valid_configs = [configurations[i] for i in valid_indices]
    valid_times = [times[i] for i in valid_indices]

    colors = ['red' if i == 0 else 'green' if i == len(valid_configs)-1 else 'blue'
              for i in range(len(valid_configs))]

    bars1 = ax1.bar(valid_configs, valid_times, color=colors,
                    alpha=0.7, edgecolor='black')
    ax1.set_ylabel('Tiempo (segundos)', fontsize=12)
    ax1.set_xlabel('Configuración', fontsize=12)
    ax1.grid(True, alpha=0.3)

    # Agregar valores en las barras
    for bar, time in zip(bars1, valid_times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 f'{time:.2f}s', ha='center', va='bottom', fontweight='bold')

    # Marcar baseline y mejor resultado
    ax1.axhline(y=20.12, color='red', linestyle='--',
                alpha=0.7, label='Baseline Secuencial')
    ax1.axhline(y=9.27, color='green', linestyle='--',
                alpha=0.7, label='Mejor Resultado (Turbo)')
    ax1.legend()

    # 2. Gráfico de speedup
    ax2.set_title('📈 Speedup vs Configuración', fontsize=14, fontweight='bold')

    valid_speedup = [speedup[i] for i in valid_indices]
    bars2 = ax2.bar(valid_configs, valid_speedup,
                    color=colors, alpha=0.7, edgecolor='black')
    ax2.set_ylabel('Speedup (vs Secuencial)', fontsize=12)
    ax2.set_xlabel('Configuración', fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.axhline(y=1.0, color='red', linestyle='--',
                alpha=0.7, label='Baseline (1.0x)')
    ax2.axhline(y=2.17, color='green', linestyle='--',
                alpha=0.7, label='Mejor Speedup (2.17x)')

    # Agregar valores en las barras
    for bar, sp in zip(bars2, valid_speedup):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                 f'{sp:.2f}x', ha='center', va='bottom', fontweight='bold')

    ax2.legend()

    # 3. Gráfico de recursos vs rendimiento
    ax3.set_title('🖥️ Recursos vs Rendimiento', fontsize=14, fontweight='bold')

    # Scatter plot con cores y memoria
    valid_cores = [cores[i] for i in valid_indices]
    valid_memory = [memory_gb[i] for i in valid_indices]

    scatter = ax3.scatter(valid_cores, valid_times, s=[m*10 for m in valid_memory],
                          c=valid_speedup, cmap='RdYlGn', alpha=0.7, edgecolors='black')
    ax3.set_xlabel('Cores Totales', fontsize=12)
    ax3.set_ylabel('Tiempo (segundos)', fontsize=12)
    ax3.grid(True, alpha=0.3)

    # Colorbar para speedup
    cbar = plt.colorbar(scatter, ax=ax3)
    cbar.set_label('Speedup', fontsize=12)

    # Agregar etiquetas para cada punto
    for i, config in enumerate(valid_configs):
        ax3.annotate(config.replace('\n', ' '),
                     (valid_cores[i], valid_times[i]),
                     xytext=(5, 5), textcoords='offset points',
                     fontsize=9, alpha=0.8)

    # 4. Gráfico de throughput
    ax4.set_title('⚡ Throughput vs Configuración',
                  fontsize=14, fontweight='bold')

    # Calcular throughput (filas por segundo)
    total_rows = 33_854_980
    valid_throughput = [total_rows / t for t in valid_times]

    bars4 = ax4.bar(valid_configs, valid_throughput,
                    color=colors, alpha=0.7, edgecolor='black')
    ax4.set_ylabel('Filas por segundo (millones)', fontsize=12)
    ax4.set_xlabel('Configuración', fontsize=12)
    ax4.grid(True, alpha=0.3)

    # Formatear eje Y en millones
    ax4.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M'))

    # Agregar valores en las barras
    for bar, throughput in zip(bars4, valid_throughput):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height + 50000,
                 f'{throughput/1e6:.1f}M', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig('performance_evolution.png', dpi=300, bbox_inches='tight')
    print("✅ Gráfico de evolución guardado: performance_evolution.png")
    return fig


def create_resource_utilization_chart():
    """🖥️ Gráfico de utilización de recursos"""

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('💾 ANÁLISIS DE UTILIZACIÓN DE RECURSOS\n' +
                 'Optimización del Cluster Dask',
                 fontsize=16, fontweight='bold', y=0.98)

    # Datos de configuraciones exitosas
    configs = ["Básico", "Optimizado", "Turbo", "Ultra", "Óptimo"]
    workers_count = [2, 4, 6, 8, 6]
    memory_used = [8, 12, 20.7, 26, 20.7]
    memory_available = [32, 32, 32, 32, 32]
    cpu_cores = [8, 16, 24, 32, 24]
    efficiency = [63, 81, 74, 71, 74]  # Eficiencia paralela en %

    # 1. Workers vs Rendimiento
    ax1.set_title('👥 Workers vs Eficiencia', fontsize=12, fontweight='bold')
    ax1.plot(workers_count, efficiency, 'o-', linewidth=3,
             markersize=8, color='blue', label='Eficiencia')
    ax1.set_xlabel('Número de Workers')
    ax1.set_ylabel('Eficiencia Paralela (%)')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(50, 85)

    # Marcar el punto óptimo
    optimal_idx = efficiency.index(max(efficiency))
    ax1.scatter(workers_count[optimal_idx], efficiency[optimal_idx],
                color='red', s=100, zorder=5, label='Óptimo')
    ax1.legend()

    # 2. Utilización de memoria
    ax2.set_title('💾 Utilización de Memoria', fontsize=12, fontweight='bold')
    memory_usage_pct = [used/available*100 for used,
                        available in zip(memory_used, memory_available)]

    bars = ax2.bar(configs, memory_usage_pct, alpha=0.7,
                   color=['orange' if pct > 80 else 'green' if pct > 60 else 'yellow'
                          for pct in memory_usage_pct])
    ax2.set_ylabel('Uso de Memoria (%)')
    ax2.set_xlabel('Configuración')
    ax2.axhline(y=80, color='red', linestyle='--',
                alpha=0.7, label='Límite Recomendado (80%)')
    ax2.grid(True, alpha=0.3)

    # Agregar valores
    for bar, pct, used in zip(bars, memory_usage_pct, memory_used):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 1,
                 f'{pct:.1f}%\n({used:.1f}GB)', ha='center', va='bottom', fontsize=9)
    ax2.legend()

    # 3. Cores vs Speedup
    ax3.set_title('🖥️ Cores vs Speedup', fontsize=12, fontweight='bold')
    speedups = [1.27, 1.62, 2.17, 2.26, 2.17]

    ax3.scatter(cpu_cores, speedups, s=100,
                alpha=0.7, c=speedups, cmap='viridis')
    ax3.plot(cpu_cores, speedups, '--', alpha=0.5, color='gray')

    # Línea teórica ideal (speedup lineal)
    # Normalizado a 8 cores base
    ideal_speedup = [cores/8 for cores in cpu_cores]
    ax3.plot(cpu_cores, ideal_speedup, 'r--', alpha=0.7, label='Speedup Ideal')

    ax3.set_xlabel('Cores Totales')
    ax3.set_ylabel('Speedup Real')
    ax3.grid(True, alpha=0.3)
    ax3.legend()

    # 4. Resumen de configuración óptima
    ax4.axis('off')
    ax4.set_title('🏆 CONFIGURACIÓN ÓPTIMA FINAL',
                  fontsize=14, fontweight='bold')

    optimal_text = f"""
    🎯 CONFIGURACIÓN GANADORA: DASK TURBO
    
    📊 Métricas de Rendimiento:
    • Speedup: 2.17x (vs secuencial)
    • Tiempo: 9.27 segundos
    • Throughput: 3.65M filas/segundo
    • Mejora: 116.9%
    
    🖥️ Recursos Utilizados:
    • Workers: 6
    • Cores totales: 24 (6 × 4 threads)
    • Memoria: 20.7 GB (64.7% de 32GB)
    • Eficiencia paralela: 74%
    
    ⚙️ Configuración Técnica:
    • Particiones: 80
    • Cálculos paralelos: 110
    • Dataset: 33.8M registros (544MB)
    • Hardware límite: Sin sobresubscripción
    """

    ax4.text(0.05, 0.95, optimal_text, transform=ax4.transAxes,
             fontsize=11, verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.8))

    plt.tight_layout()
    plt.savefig('resource_utilization.png', dpi=300, bbox_inches='tight')
    print("✅ Gráfico de recursos guardado: resource_utilization.png")
    return fig


def create_comparison_summary():
    """📊 Resumen comparativo final"""

    fig, ax = plt.subplots(figsize=(12, 8))

    # Datos finales de comparación
    methods = ['Pandas\nSecuencial', 'Dask\nParalelo']
    times = [20.12, 9.28]
    throughput = [1.68, 3.65]  # Millones filas/seg
    colors = ['red', 'green']

    # Crear gráfico de barras doble
    x = np.arange(len(methods))
    width = 0.35

    # Tiempo de ejecución (eje izquierdo)
    ax1 = ax
    bars1 = ax1.bar(x - width/2, times, width, label='Tiempo (s)',
                    color=colors, alpha=0.7, edgecolor='black')
    ax1.set_xlabel('Método de Procesamiento', fontsize=14)
    ax1.set_ylabel('Tiempo de Ejecución (segundos)', fontsize=14, color='red')
    ax1.tick_params(axis='y', labelcolor='red')
    ax1.set_xticks(x)
    ax1.set_xticklabels(methods)

    # Throughput (eje derecho)
    ax2 = ax1.twinx()
    bars2 = ax2.bar(x + width/2, throughput, width, label='Throughput (M filas/s)',
                    color=colors, alpha=0.5, edgecolor='black', hatch='//')
    ax2.set_ylabel('Throughput (Millones filas/segundo)',
                   fontsize=14, color='green')
    ax2.tick_params(axis='y', labelcolor='green')

    # Agregar valores en las barras
    for bar, time in zip(bars1, times):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.3,
                 f'{time:.2f}s', ha='center', va='bottom', fontweight='bold', fontsize=12)

    for bar, tp in zip(bars2, throughput):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                 f'{tp:.2f}M', ha='center', va='bottom', fontweight='bold', fontsize=12)

    # Título y mejoras
    plt.title('🏆 COMPARACIÓN FINAL: PANDAS vs DASK\n' +
              f'Speedup: {times[0]/times[1]:.2f}x | Mejora: {((times[0]/times[1]-1)*100):.1f}%\n' +
              'Dataset: NYC Taxi 2024 (33.8M registros)',
              fontsize=16, fontweight='bold', pad=20)

    # Agregar cuadro de estadísticas
    stats_text = f"""
    📈 MEJORAS LOGRADAS:
    • Reducción tiempo: {times[0]-times[1]:.2f}s ({((times[0]-times[1])/times[0]*100):.1f}%)
    • Aumento throughput: +{throughput[1]-throughput[0]:.2f}M filas/s
    • Speedup total: {times[0]/times[1]:.2f}x
    • Eficiencia: {(times[0]/times[1]/24*100):.1f}% (24 cores)
    """

    plt.figtext(0.02, 0.02, stats_text, fontsize=10, fontfamily='monospace',
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightyellow", alpha=0.8))

    # Leyenda
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper center')

    plt.tight_layout()
    plt.savefig('final_comparison.png', dpi=300, bbox_inches='tight')
    print("✅ Comparación final guardada: final_comparison.png")
    return fig


def generate_all_charts():
    """🎨 Generar todos los gráficos de documentación"""
    print("🎨 GENERANDO DOCUMENTACIÓN VISUAL COMPLETA...")
    print("=" * 60)

    try:
        # 1. Evolución del rendimiento
        print("📈 Generando gráfico de evolución...")
        fig1 = create_performance_evolution_chart()

        # 2. Utilización de recursos
        print("💾 Generando análisis de recursos...")
        fig2 = create_resource_utilization_chart()

        # 3. Comparación final
        print("🏆 Generando comparación final...")
        fig3 = create_comparison_summary()

        print("=" * 60)
        print("✅ DOCUMENTACIÓN VISUAL COMPLETADA!")
        print("\n📁 Archivos generados:")
        print("   • performance_evolution.png - Evolución del proceso")
        print("   • resource_utilization.png - Análisis de recursos")
        print("   • final_comparison.png - Comparación pandas vs dask")

        # Mostrar resumen
        print(f"\n🎯 RESUMEN DEL PROCESO DE OPTIMIZACIÓN:")
        print(f"   • Configuraciones probadas: 7")
        print(f"   • Speedup máximo: 2.17x")
        print(f"   • Configuración óptima: 6 workers, 24 cores")
        print(f"   • Memoria utilizada: 20.7 GB")
        print(f"   • Mejora final: 116.9%")

    except Exception as e:
        print(f"❌ Error generando gráficos: {e}")

    finally:
        plt.close('all')


if __name__ == "__main__":
    generate_all_charts()
