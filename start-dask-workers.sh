#!/bin/bash

echo "🚀 Iniciando scheduler Dask..."
dask-scheduler --host 0.0.0.0 --port 8786 --dashboard-address 0.0.0.0:8787 &

echo "⏳ Esperando que el scheduler esté listo..."
sleep 8

echo "��💥 CONFIGURACIÓN ULTRA SUPREMA - ¡32GB RAM COMPLETOS! ��💥"
echo "� Iniciando 8 workers Dask con 3 threads cada uno (24 cores totales)..."
for i in {1..8}; do
    echo "   Iniciando worker SUPREMO $i/8..."
    dask-worker tcp://localhost:8786 --nthreads 3 --memory-limit 3GB --death-timeout 120s --nanny-port 0 --worker-port 0 &
    sleep 2
done

echo "✅ ¡TODOS LOS WORKERS SUPREMOS INICIADOS!"
echo "📊 Configuración ULTRA SUPREMA:"
echo "   - 8 workers × 3 threads = 24 cores totales"
echo "   - 8 workers × 3GB = 24GB memoria workers"
echo "   - Scheduler + overhead = ~28GB total"
echo "   - ⚡ ¡USANDO TODA TU RAM DE 32GB! ⚡"