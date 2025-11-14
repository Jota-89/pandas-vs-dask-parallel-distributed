#!/bin/bash

echo "🔥 CONFIGURACIÓN MÁXIMA ABSOLUTA - SIN LÍMITES 🔥"
echo "🚀 Iniciando scheduler Dask..."
dask-scheduler --host 0.0.0.0 --port 8786 --dashboard-address 0.0.0.0:8787 &

echo "⏳ Esperando que el scheduler esté listo..."
sleep 10

echo "💥💥💥 MÁXIMO ABSOLUTO - USANDO TODO TU HARDWARE 💥💥💥"
echo "🔥 Iniciando 12 workers Dask con 2 threads cada uno (24 cores)..."
echo "🧠 Usando 20GB de RAM (casi toda la disponible)..."

for i in {1..12}; do
    echo "   🚀 Iniciando worker MÁXIMO $i/12 (1.6GB cada uno)..."
    dask-worker tcp://localhost:8786 --nthreads 2 --memory-limit 1600MB --death-timeout 180s --nanny-port 0 --worker-port 0 &
    sleep 2
done

echo "✅ ¡CONFIGURACIÓN MÁXIMA ABSOLUTA COMPLETADA!"
echo "📊 Especificaciones MÁXIMAS:"
echo "   - 12 workers × 2 threads = 24 cores (TODOS TUS CORES)"
echo "   - 12 workers × 1.6GB = 19.2GB memoria workers"
echo "   - Total con overhead = ~23GB (TODA TU RAM DISPONIBLE)"
echo "   - CPU: 100% utilización"
echo "   - RAM: 84% utilización"
echo "   - ⚡ MÁXIMO ABSOLUTO DE TU HARDWARE ⚡"