#!/bin/bash
# =============================================================================
#  NYC TAXI BENCHMARK - SCRIPT AUTOMATIZADO COMPLETO (LINUX/MAC)
# =============================================================================
# Ejecuta todo el pipeline de Airflow de forma automatizada con DAGs mejorados
# Autor: Benchmark Team
# =============================================================================

echo "====================================================================="
echo "NYC TAXI BENCHMARK - PROCESAMIENTO PARALELO vs SECUENCIAL"
echo "====================================================================="

# 1. LIMPIAR ENTORNO ANTERIOR
echo "Paso 1: Limpiando entorno anterior..."
docker-compose down --remove-orphans > /dev/null 2>&1
sleep 2

# 2. LEVANTAR SERVICIOS
echo "Paso 2: Levantando servicios Docker..."
docker-compose up -d
echo "Esperando que los servicios estén listos..."
sleep 30

# 3. VERIFICAR SERVICIOS
echo "Paso 3: Verificando servicios..."
docker-compose ps

# 4. ESPERAR A QUE AIRFLOW ESTÉ LISTO
echo "Paso 4: Esperando a que Airflow esté completamente listo..."
sleep 60

# 5. ACTIVAR TODOS LOS DAGS
echo "Paso 5: Activando todos los DAGs..."
docker exec comp_paralel-app-1 airflow dags unpause dag_01_download_data
docker exec comp_paralel-app-1 airflow dags unpause dag_02_sequential  
docker exec comp_paralel-app-1 airflow dags unpause dag_03_dask_simple
docker exec comp_paralel-app-1 airflow dags unpause dag_04_comparison
docker exec comp_paralel-app-1 airflow dags unpause dag_05_analytics

# 6. VERIFICAR CLUSTER DASK
echo "Paso 6: Verificando cluster Dask..."
docker exec comp_paralel-app-1 python -c "from dask.distributed import Client; client = Client('tcp://localhost:8786', timeout=10); print('Dask cluster conectado'); client.close()" || echo "Advertencia: Dask no respondió, pero continuamos..."

# 7. EJECUTAR PIPELINE COMPLETO AUTOMATIZADO
echo "Paso 7: Ejecutando pipeline completo automatizado..."
echo "================================================================="

echo "7.1 - Descargando datos NYC Taxi..."
docker exec comp_paralel-app-1 airflow dags trigger dag_01_download_data
echo "^^ Esperando descarga de datos (45 segundos)..."
sleep 45

echo "7.2 - Ejecutando procesamiento SECUENCIAL..."
docker exec comp_paralel-app-1 airflow dags trigger dag_02_sequential
echo "^^ Esperando procesamiento secuencial (60 segundos)..."
sleep 60

echo "7.3 - Ejecutando procesamiento PARALELO (Dask)..."
docker exec comp_paralel-app-1 airflow dags trigger dag_03_dask_simple  
echo "^^ Esperando procesamiento paralelo (40 segundos)..."
sleep 40

echo "7.4 - Generando comparación y gráficos técnicos..."
docker exec comp_paralel-app-1 airflow dags trigger dag_04_comparison
echo "^^ Esperando generación de gráficos técnicos (30 segundos)..."
sleep 30

echo "7.5 - Generando análisis detallado con tipos de pago corregidos..."
docker exec comp_paralel-app-1 airflow dags trigger dag_05_analytics
echo "^^ Esperando análisis detallado (25 segundos)..."
sleep 25

# 8. VERIFICAR TODOS LOS RESULTADOS
echo "Paso 8: Verificando todos los archivos generados..."
echo "================================================================="
docker exec comp_paralel-app-1 ls -la /workspace/results/

# 9. MOSTRAR MÉTRICAS FINALES COMPLETAS
echo "Paso 9: RESULTADOS FINALES DEL BENCHMARK COMPLETO"
echo "================================================================="

# Mostrar resultados del benchmark
echo ""
echo "=== BENCHMARK COMPLETADO ==="
docker exec comp_paralel-app-1 python -c "import json; report=json.load(open('/workspace/results/final_benchmark_report.json')); summary=report['summary']; print('Secuencial: ' + str(round(summary['sequential_time'], 2)) + ' s'); print('Paralelo:   ' + str(round(summary['parallel_time'], 2)) + ' s'); print('Speedup:    ' + str(round(summary['speedup'], 1)) + ' x'); print('Mejora:     ' + str(round(summary['improvement_percent'], 1)) + ' porciento'); print('Ganador:    ' + report['conclusion']['winner'])"

echo ""
echo "=== SISTEMA Y GRAFICOS ==="
docker exec comp_paralel-app-1 python -c "import json; tech=json.load(open('/workspace/results/technical_benchmark_summary.json')); print('Sistema:    ' + str(tech['system_info']['cpu_count']) + ' cores, ' + str(round(tech['system_info']['memory_total_gb'], 1)) + ' GB RAM'); print('Eficiencia: ' + str(round(tech['performance_metrics']['efficiency'], 3))); print('Throughput: ' + str('{:,}'.format(int(tech['performance_metrics']['throughput_parallel']))) + ' registros/segundo')"

echo ""
echo "=== DATOS PROCESADOS ==="
docker exec comp_paralel-app-1 python -c "import json; analytics=json.load(open('/workspace/results/detailed_analytics_report.json')); print('Total viajes procesados: ' + str('{:,}'.format(analytics['dataset_overview']['total_trips']))); print('Archivos de datos: ' + str(analytics['dataset_overview']['total_files'])); print('Tiempo total procesamiento: ' + str(round(analytics['dataset_overview']['processing_time'], 2)) + ' segundos')"

echo ""
echo "=== TIPOS DE PAGO PRINCIPALES ==="
docker exec comp_paralel-app-1 python -c "import json; analytics=json.load(open('/workspace/results/detailed_analytics_report.json')); payments=analytics['payment_analysis']['payment_distribution']; top4 = payments[:4]; [print('  ' + p.get('type_name', p.get('type', 'Tipo')) + ': ' + str('{:,}'.format(p['trips'])) + ' viajes (' + str(p['percentage']) + ' porciento)') for p in top4]"

echo ""
echo "=== ARCHIVOS GENERADOS ==="
docker exec comp_paralel-app-1 python -c "import os; files = [f for f in os.listdir('/workspace/results/') if f.endswith(('.png', '.json'))]; pngs = len([f for f in files if f.endswith('.png')]); jsons = len([f for f in files if f.endswith('.json')]); print('Graficos PNG: ' + str(pngs) + ' archivos'); print('Reportes JSON: ' + str(jsons) + ' archivos'); print('Total: ' + str(len(files)) + ' archivos generados')"

echo "================================================================="

# 10. LISTA DE ARCHIVOS GENERADOS
echo "Paso 10: ARCHIVOS GENERADOS AUTOMÁTICAMENTE:"
echo "=========================================="
echo "> final_benchmark_report.json        - Reporte completo del benchmark"
echo "> technical_performance_analysis.png - Análisis técnico de performance"  
echo "> scalability_analysis.png           - Análisis de escalabilidad"
echo "> memory_cpu_analysis.png            - Análisis de memoria y CPU"
echo "> final_comparison_chart.png         - Comparación básica"
echo "> detailed_analytics_report.json     - Análisis con tipos de pago corregidos"
echo "> technical_benchmark_summary.json   - Resumen técnico completo"
echo "=========================================="

# 11. INFO DE ACCESO
echo "Paso 11: Información de acceso:"
echo "Airflow UI:     http://localhost:8081 (demo/demo)"
echo "Dask Dashboard: http://localhost:8788"
echo "Resultados:     /workspace/results/"
echo ""
echo "^^ TODOS LOS DAGs SE EJECUTARON AUTOMÁTICAMENTE ^^"
echo "¡Proceso Completado!"
echo "====================================================================="

read -p "Presiona Enter para continuar..."