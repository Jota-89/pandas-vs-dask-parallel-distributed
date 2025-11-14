#!/bin/bash

# 🎯🔥 CONFIGURACIÓN ÓPTIMA ABSOLUTA 🔥🎯
# 6 workers × 2 threads = 12 threads totales (100% CPU)
# 4GB por worker = 24GB RAM total (87.5% RAM)
# ¡SIN OVERSUBSCRIPTION!

echo "🎯🔥🔥🔥 INICIANDO CLUSTER ÓPTIMO ABSOLUTO 🔥🔥🔥🎯"
echo "💥 6 Workers × 2 Threads = 12 Cores (100% CPU)"
echo "🧠 4GB × 6 Workers = 24GB RAM (87.5% RAM)" 
echo "⚡ ¡MÁXIMO RENDIMIENTO SIN SOBRECARGA!"

# 1. Iniciar scheduler PRIMERO
echo "🚀 Iniciando Dask Scheduler..."
dask-scheduler --port 8786 --dashboard-address 0.0.0.0:8787 &
SCHEDULER_PID=$!

echo "⏱️ Esperando 15s para que scheduler se estabilice..."
sleep 15

# Función para iniciar un worker ÓPTIMO
start_worker() {
    local worker_id=$1
    local port=$((8790 + worker_id))
    local worker_dir="/tmp/dask-worker-${worker_id}"
    
    echo "🚀 Iniciando Worker ÓPTIMO ${worker_id} (Puerto: ${port})"
    
    mkdir -p "${worker_dir}"
    
    dask-worker \
        127.0.0.1:8786 \
        --nthreads 2 \
        --memory-limit 4GB \
        --worker-port ${port} \
        --nanny-port $((port + 100)) \
        --dashboard-address :$((port + 200)) \
        --local-directory "${worker_dir}" \
        --death-timeout 240s \
        --no-bokeh \
        --pid-file "${worker_dir}/worker-${worker_id}.pid" \
        --resources "worker_id=${worker_id}" \
        --name "worker_optimo_${worker_id}" &
        
    echo "✅ Worker ÓPTIMO ${worker_id} iniciado en background"
}

echo ""
echo "💥💥💥 INICIANDO 6 WORKERS ÓPTIMOS 💥💥💥"

# Iniciar 6 workers óptimos
for i in {1..6}; do
    start_worker $i
    echo "⏱️ Pausa de 5s para estabilización..."
    sleep 5
done

echo ""
echo "⚡ Esperando 30s para que todos los workers se estabilicen..."
sleep 30

echo ""
echo "🎯💥💥💥 CLUSTER ÓPTIMO ABSOLUTO LISTO! 💥💥💥🎯"
echo "📊 Configuración final:"
echo "   🔥 Workers: 6"
echo "   ⚡ Threads totales: 12 (100% CPU)"
echo "   🧠 RAM total: 24GB (87.5% disponible)"
echo "   💪 Sin oversubscription - Máximo rendimiento sostenible"
echo "🚀 ¡Listo para MÁXIMA PERFORMANCE!"

# Mantener el script activo
wait