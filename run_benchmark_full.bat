@echo off
REM =============================================================================
REM  NYC TAXI BENCHMARK - SCRIPT AUTOMATIZADO COMPLETO (WINDOWS)
REM =============================================================================
REM Ejecuta todo el pipeline de Airflow de forma automatizada con DAGs mejorados
REM Autor: Benchmark Team
REM =============================================================================

echo  =====================================================================
echo  NYC TAXI BENCHMARK - PROCESAMIENTO PARALELO vs SECUENCIAL
echo  =====================================================================

REM 1. LIMPIAR ENTORNO ANTERIOR
echo Paso 1: Limpiando entorno anterior...
docker-compose down --remove-orphans >nul 2>&1
timeout /t 2 /nobreak >nul

REM 2. LEVANTAR SERVICIOS
echo Paso 2: Levantando servicios Docker...
docker-compose up -d
echo Esperando que los servicios esten listos...
timeout /t 30 /nobreak >nul

REM 3. VERIFICAR SERVICIOS
echo  Paso 3: Verificando servicios...
docker-compose ps

REM 4. ESPERAR A QUE AIRFLOW ESTÉ LISTO
echo  Paso 4: Esperando a que Airflow este completamente listo...
timeout /t 60 /nobreak >nul

REM 5. ACTIVAR TODOS LOS DAGS
echo  Paso 5: Activando todos los DAGs...
call docker exec comp_paralel-app-1 airflow dags unpause dag_01_download_data
call docker exec comp_paralel-app-1 airflow dags unpause dag_02_sequential  
call docker exec comp_paralel-app-1 airflow dags unpause dag_03_dask_simple
call docker exec comp_paralel-app-1 airflow dags unpause dag_04_comparison
call docker exec comp_paralel-app-1 airflow dags unpause dag_05_analytics

REM 6. VERIFICAR CLUSTER DASK
echo  Paso 6: Verificando cluster Dask...
call docker exec comp_paralel-app-1 python -c "from dask.distributed import Client; client = Client('tcp://localhost:8786', timeout=10); print('Dask cluster conectado'); client.close()" || echo "Advertencia: Dask no respondio, pero continuamos..."

REM 7. EJECUTAR PIPELINE COMPLETO AUTOMATIZADO
echo  Paso 7: Ejecutando pipeline completo automatizado...
echo =================================================================

echo  7.1 - Descargando datos NYC Taxi...
call docker exec comp_paralel-app-1 airflow dags trigger dag_01_download_data
echo  ^^ Esperando descarga de datos (45 segundos)...
timeout /t 45 /nobreak >nul

echo  7.2 - Ejecutando procesamiento SECUENCIAL...
call docker exec comp_paralel-app-1 airflow dags trigger dag_02_sequential
echo  ^^ Esperando procesamiento secuencial (60 segundos)...
timeout /t 60 /nobreak >nul

echo  7.3 - Ejecutando procesamiento PARALELO (Dask)...
call docker exec comp_paralel-app-1 airflow dags trigger dag_03_dask_simple  
echo  ^^ Esperando procesamiento paralelo (40 segundos)...
timeout /t 40 /nobreak >nul

echo  7.4 - Generando comparacion y graficos tecnicos...
call docker exec comp_paralel-app-1 airflow dags trigger dag_04_comparison
echo  ^^ Esperando generacion de graficos tecnicos (30 segundos)...
timeout /t 30 /nobreak >nul

echo  7.5 - Generando analisis detallado con tipos de pago corregidos...
call docker exec comp_paralel-app-1 airflow dags trigger dag_05_analytics
echo  ^^ Esperando analisis detallado (25 segundos)...
timeout /t 25 /nobreak >nul

REM 8. VERIFICAR TODOS LOS RESULTADOS
echo  Paso 8: Verificando todos los archivos generados...
echo =================================================================
call docker exec comp_paralel-app-1 ls -la /workspace/results/

REM 9. MOSTRAR METRICAS FINALES COMPLETAS
echo  Paso 9: RESULTADOS FINALES DEL BENCHMARK COMPLETO
echo =================================================================

REM Mostrar resultados del benchmark
echo.
echo === BENCHMARK COMPLETADO ===
call docker exec comp_paralel-app-1 python -c "import json; report=json.load(open('/workspace/results/final_benchmark_report.json')); summary=report['summary']; print('Secuencial: ' + str(round(summary['sequential_time'], 2)) + ' s'); print('Paralelo:   ' + str(round(summary['parallel_time'], 2)) + ' s'); print('Speedup:    ' + str(round(summary['speedup'], 1)) + ' x'); print('Mejora:     ' + str(round(summary['improvement_percent'], 1)) + ' porciento'); print('Ganador:    ' + report['conclusion']['winner'])"

echo.
echo === SISTEMA Y GRAFICOS ===
call docker exec comp_paralel-app-1 python -c "import json; tech=json.load(open('/workspace/results/technical_benchmark_summary.json')); print('Sistema:    ' + str(tech['system_info']['cpu_count']) + ' cores, ' + str(round(tech['system_info']['memory_total_gb'], 1)) + ' GB RAM'); print('Eficiencia: ' + str(round(tech['performance_metrics']['efficiency'], 3))); print('Throughput: ' + str('{:,}'.format(int(tech['performance_metrics']['throughput_parallel']))) + ' registros/segundo')"

echo.
echo === DATOS PROCESADOS ===
call docker exec comp_paralel-app-1 python -c "import json; analytics=json.load(open('/workspace/results/detailed_analytics_report.json')); print('Total viajes procesados: ' + str('{:,}'.format(analytics['dataset_overview']['total_trips']))); print('Archivos de datos: ' + str(analytics['dataset_overview']['total_files'])); print('Tiempo total procesamiento: ' + str(round(analytics['dataset_overview']['processing_time'], 2)) + ' segundos')"

echo.
echo === TIPOS DE PAGO PRINCIPALES ===
call docker exec comp_paralel-app-1 python -c "import json; analytics=json.load(open('/workspace/results/detailed_analytics_report.json')); payments=analytics['payment_analysis']['payment_distribution']; top4 = payments[:4]; [print('  ' + p.get('type_name', p.get('type', 'Tipo')) + ': ' + str('{:,}'.format(p['trips'])) + ' viajes (' + str(p['percentage']) + ' porciento)') for p in top4]"

echo.
echo === ARCHIVOS GENERADOS ===
call docker exec comp_paralel-app-1 python -c "import os; files = [f for f in os.listdir('/workspace/results/') if f.endswith(('.png', '.json'))]; pngs = len([f for f in files if f.endswith('.png')]); jsons = len([f for f in files if f.endswith('.json')]); print('Graficos PNG: ' + str(pngs) + ' archivos'); print('Reportes JSON: ' + str(jsons) + ' archivos'); print('Total: ' + str(len(files)) + ' archivos generados')"

echo =================================================================

REM 10. LISTA DE ARCHIVOS GENERADOS
echo  Paso 10: ARCHIVOS GENERADOS AUTOMATICAMENTE:
echo  ==========================================
echo  ^> final_benchmark_report.json        - Reporte completo del benchmark
echo  ^> technical_performance_analysis.png - Analisis tecnico de performance  
echo  ^> scalability_analysis.png           - Analisis de escalabilidad
echo  ^> memory_cpu_analysis.png            - Analisis de memoria y CPU
echo  ^> final_comparison_chart.png         - Comparacion basica
echo  ^> detailed_analytics_report.json     - Analisis con tipos de pago corregidos
echo  ^> technical_benchmark_summary.json   - Resumen tecnico completo
echo  ==========================================

REM 11. INFO DE ACCESO
echo  Paso 11: Informacion de acceso:
echo  Airflow UI:     http://localhost:8081 (demo/demo)
echo  Dask Dashboard: http://localhost:8788
echo  Resultados:     /workspace/results/
echo.
echo  ^^ TODOS LOS DAGS SE EJECUTARON AUTOMATICAMENTE ^^
echo  ¡Proceso Completado! - Benchmark con graficos tecnicos listo
echo =====================================================================
pause