# NYC Taxi Benchmark - Procesamiento Paralelo vs Secuencial

## **EJECUCIÓN AUTOMÁTICA - UNA SOLA LÍNEA**

### Windows:

```powershell
.\run_benchmark_full.bat
```

### Linux/Mac:

```bash
chmod +x run_benchmark_full.sh
./run_benchmark_full.sh
```

**¡ESO ES TODO!** El script ejecuta automáticamente **todos los DAGs** en orden sin intervención manual.

---

## **¿Qué hace el script automático?**

### 1. **Preparación del Entorno** (30 segundos)

- Limpia contenedores previos
- Levanta servicios Docker (Airflow + Dask + PostgreSQL)
- Verifica conectividad del cluster Dask

### 2. **Pipeline Completo Automatizado** (~4 minutos)

- **DAG 1**: Descarga datos NYC Taxi 2024 (570MB)
- **DAG 2**: Procesamiento secuencial con Pandas
- **DAG 3**: Procesamiento paralelo con Dask
- **DAG 4**: Comparación + **gráficos técnicos avanzados**
- **DAG 5**: Análisis detallado con **tipos de pago corregidos**

### 3. **Resultados Automáticos**

- **7 archivos generados automáticamente**
- Métricas de performance completas
- **Gráficos técnicos profesionales**
- Análisis de escalabilidad y recursos

---

## **Archivos Generados Automáticamente**

| Archivo                                  | Descripción                               |
| ---------------------------------------- | ----------------------------------------- |
| `final_benchmark_report.json`            | Reporte completo del benchmark            |
| **`technical_performance_analysis.png`** | **Análisis técnico de performance**       |
| **`scalability_analysis.png`**           | **Análisis de escalabilidad**             |
| **`memory_cpu_analysis.png`**            | **Análisis de memoria y CPU**             |
| `final_comparison_chart.png`             | Comparación visual básica                 |
| **`detailed_analytics_report.json`**     | **Análisis con tipos de pago corregidos** |
| `technical_benchmark_summary.json`       | Resumen técnico del sistema               |

---

## **Gráficos Técnicos Incluidos**

### 1. **Technical Performance Analysis**

- Comparación de tiempos de ejecución
- Métricas de speedup y eficiencia
- Recursos del sistema (CPU/RAM)
- Throughput de procesamiento

### 2. **Scalability Analysis**

- Escalabilidad teórica vs real
- Eficiencia por número de workers
- Overhead de paralelización
- Proyecciones de tiempo

### 3. **Memory & CPU Analysis**

- Uso estimado de memoria
- Utilización de CPU simulada
- Distribución de recursos del sistema
- Eficiencia energética estimada

---

## **Resultados Típicos Esperados**

```
=== BENCHMARK COMPLETADO ===
Secuencial: 18.74 s
Paralelo:   3.14 s
Speedup:    6.0 x
Mejora:     496.7 porciento
Ganador:    Paralelo

=== SISTEMA Y GRAFICOS ===
Sistema:    12 cores, 15.5 GB RAM
Eficiencia: 0.531
Throughput: 10,758,437 registros/segundo

=== TIPOS DE PAGO PRINCIPALES ===
  Tarjeta de Crédito: 24,999,870 viajes (73.8 porciento)
  Efectivo: 4,599,611 viajes (13.6 porciento)
  Desconocido/Disputado: 3,391,267 viajes (10.0 porciento)
  Descuento/Cortesía: 628,268 viajes (1.9 porciento)
```

---

## **Acceso a Interfaces**

- **Airflow UI**: http://localhost:8081 (usuario: `demo`, password: `demo`)
- **Dask Dashboard**: http://localhost:8788
- **Resultados**: Carpeta `sample_results/` (ejemplos incluidos)

---

## **Estructura Limpia del Proyecto**

```
comp_paralel/
├── dags/                           # ← DAGs de Airflow (5 archivos)
│   ├── dag_01_download_data.py     # Descarga datos
│   ├── dag_02_sequential.py        # Procesamiento secuencial
│   ├── dag_03_dask_simple.py       # Procesamiento paralelo
│   ├── dag_04_comparison.py        # Comparación + gráficos técnicos
│   └── dag_05_analytics.py         # Análisis con tipos de pago corregidos
├── sample_results/                 # ← Ejemplos de resultados incluidos
│   ├── technical_performance_analysis.png
│   ├── scalability_analysis.png
│   ├── memory_cpu_analysis.png
│   ├── final_comparison_chart.png
│   ├── final_benchmark_report.json
│   └── detailed_analytics_report.json
├── run_benchmark_full.bat          # ← SCRIPT PRINCIPAL WINDOWS
├── run_benchmark_full.sh           # ← SCRIPT PRINCIPAL LINUX/MAC
├── docker-compose.yml              # ← Configuración Docker
├── Dockerfile                      # ← Imagen personalizada
├── requirements.txt                # ← Dependencias Python
└── README.md                       # ← Este archivo
```

---

## ⚙️ **Requisitos Mínimos**

- **Docker** y **Docker Compose**
- **8GB RAM** (recomendado 16GB)
- **4 CPU cores** (mejor con más cores)
- **10GB espacio libre**

---

## 🎯 **Objetivo del Proyecto**

Demostrar **cuantitativamente** la diferencia entre:

1. **Procesamiento secuencial** tradicional (pandas clásico)
2. **Paralelización distribuida** (DaskExecutor)

Con **datos reales** NYC Taxi y **métricas precisas** de rendimiento.
