# Juego de la Vida de Conway - Análisis de Rendimiento y Visualización

## Descripción

Implementación del Juego de la Vida de Conway con interfaz gráfica interactiva, medición de rendimiento y visualizaciones.

## Instalación y Configuración

### Prerrequisitos

- Python 3.8 o superior
- Git (opcional, para clonar el repositorio)

### Instalación Paso a Paso

1. **Navegar al directorio del proyecto:**

   ```bash
   cd comp_paralel
   ```

2. **Activar el entorno virtual:**

   ```bash
   # En Windows
   venv\\Scripts\\activate

   # En macOS/Linux
   source venv/bin/activate
   ```

3. **Verificar instalación de dependencias:**

   ```bash
   pip list
   ```

   Deberías ver: pygame, matplotlib, numpy, scipy, pandas, seaborn

## Ejecución de la Aplicación

### Script Principal

Para ejecutar el menú principal con todas las opciones, recomiendo usar la opcion 1 porque se ve todo bonito con pygame:

```bash
python main.py
```

### 1. Interfaz Gráfica Interactiva (Pygame)

Ejecuta la interfaz principal con botón de inicio y controles:

```bash
python src/pygame_gui.py
```

**Controles disponibles:**

- **START**: Comenzar simulación
- **PAUSE**: Pausar/reanudar simulación
- **RESET**: Generar nuevo estado aleatorio
- **PATTERN**: Cargar patrones clásicos predefinidos
- **SPEED +/-**: Ajustar velocidad de simulación
- **SPACE**: Pausar/reanudar (teclado)
- **R**: Reiniciar (teclado)
- **P**: Cambiar patrón (teclado)
- **↑/↓**: Cambiar velocidad (teclado)
- **ESC**: Salir

### 2. Análisis de Rendimiento

Ejecuta el análisis completo de rendimiento:

```bash
python src/performance_analysis.py
```

Este script:

- Mide tiempos de ejecución para diferentes tamaños de grilla
- Genera gráficas de escalabilidad y complejidad
- Crea análisis log-log para determinar la complejidad empírica
- Guarda resultados en CSV para análisis posterior

### 3. Visualizaciones y Animaciones

Genera visualizaciones avanzadas con matplotlib:

```bash
python src/matplotlib_visualizations.py
```

Este script crea:

- Animaciones GIF de patrones clásicos
- Grillas de evolución temporal
- Análisis de dinámicas poblacionales
- Mapas de calor de densidad de actividad

### 4. Prueba Individual de Componentes

```bash
# Probar solo la clase GameOfLife
python src/game_of_life.py

# Probar patrones clásicos
python src/patterns.py
```

## Uso de la Interfaz Pygame

### Características Principales

1. **Visualización en Tiempo Real**: Observa la evolución del autómata celular
2. **Patrones Predefinidos**: Carga patrones como Glider, Blinker, Toad, Pulsar, etc.
3. **Control de Velocidad**: Ajusta la velocidad de 1 a 30 FPS
4. **Estadísticas en Vivo**: Monitorea generación, población y estado
5. **Interfaz Intuitiva**: Botones claros y controles de teclado

### Patrones Disponibles

- **Glider**: Nave espacial que se mueve diagonalmente
- **Blinker**: Oscilador simple con período 2
- **Toad**: Oscilador con período 2
- **Block**: Estructura estática permanente
- **Beacon**: Oscilador con período 2
- **Pulsar**: Oscilador complejo con período 3
- **LWSS**: Nave espacial ligera

## Análisis de Rendimiento

### Métricas Evaluadas

1. **Tiempo por Iteración**: Tiempo promedio para actualizar el tablero
2. **Throughput**: Celdas procesadas por segundo
3. **Escalabilidad**: Cómo varía el rendimiento con el tamaño
4. **Complejidad Empírica**: Análisis log-log para determinar O(n^x)

### Tamaños de Grilla Probados

- 32x32 (1,024 celdas)
- 64x64 (4,096 celdas)
- 128x128 (16,384 celdas)
- 256x256 (65,536 celdas)
- 512x512 (262,144 celdas)

### Resultados Esperados

La implementación vectorizada con NumPy debería mostrar:

- **Complejidad**: Aproximadamente O(n) donde n es el número de celdas
- **Escalabilidad**: Buena para grillas hasta 512x512
- **Throughput**: Decenas de miles de celdas por segundo

## Visualizaciones Generadas

### 1. Gráficas de Rendimiento

- **scaling_analysis.png**: Análisis completo de escalabilidad
- **step_analysis.png**: Análisis temporal por pasos

### 2. Animaciones de Patrones

- **glider_animation.gif**: Evolución del Glider
- **blinker_animation.gif**: Oscilación del Blinker
- **toad_animation.gif**: Evolución del Toad
- **pulsar_animation.gif**: Oscilación del Pulsar

### 3. Análisis Poblacionales

- **pattern_evolution_grid.png**: Grilla comparativa de evolución
- **population_dynamics.png**: Dinámicas poblacionales por patrón
- **density_heatmap.png**: Mapa de calor de actividad

## Implementación Técnica

### Clase GameOfLife

La clase principal implementa:

```python
class GameOfLife:
    def __init__(self, rows, cols, initial_state=None)
    def step(self)                    # Actualiza una generación
    def run(self, steps)              # Ejecuta múltiples pasos
    def get_state(self)               # Obtiene estado actual
    def set_state(self, new_state)    # Establece nuevo estado
    def get_population(self)          # Cuenta celdas vivas
```

### Optimizaciones Implementadas

1. **Vectorización con NumPy**: Operaciones en paralelo sobre matrices
2. **Topología Toroidal**: Bordes conectados para evitar efectos de borde
3. **Conteo Eficiente de Vecinos**: Usando `np.roll()` para desplazamientos
4. **Aplicación Simultánea de Reglas**: Todas las celdas se actualizan a la vez

### Reglas de Conway Implementadas

1. **Superpoblación**: Celda viva con >3 vecinos muere
2. **Soledad**: Celda viva con <2 vecinos muere
3. **Supervivencia**: Celda viva con 2-3 vecinos permanece viva
4. **Reproducción**: Celda muerta con exactamente 3 vecinos nace

## Resultados del Análisis

### Complejidad Computacional

El análisis empírico revela:

- **Complejidad temporal**: O(n) donde n = número de celdas
- **Complejidad espacial**: O(n) para almacenar el tablero
- **Eficiencia**: Excelente escalabilidad hasta 512x512

### Comportamientos Observados

1. **Osciladores**: Patrones con período fijo (Blinker: 2, Pulsar: 3)
2. **Naves Espaciales**: Patrones que se desplazan (Glider, LWSS)
3. **Estructuras Estáticas**: Patrones inmutables (Block)
4. **Emergencia**: Comportamientos complejos desde reglas simples

## Personalización y Extensiones

### Agregar Nuevos Patrones

```python
# En patterns.py
@staticmethod
def mi_patron():
    return np.array([
        [0, 1, 0],
        [1, 1, 1],
        [0, 1, 0]
    ])
```

### Modificar Tamaños de Grilla

```python
# En pygame_gui.py
grid_size = (200, 200)  # Cambiar tamaño
window_size = (1200, 900)  # Ajustar ventana
```

### Personalizar Análisis

```python
# En performance_analysis.py
grid_sizes = [64, 128, 256, 512, 1024]  # Tamaños a probar
iterations = 20  # Repeticiones para promedio
```

## Solución de Problemas

### Error: "No module named 'pygame'"

```bash
pip install pygame matplotlib numpy scipy pandas seaborn
```

### Rendimiento Lento

- Reducir tamaño de grilla en pygame_gui.py
- Ajustar FPS para simulaciones más fluidas
- Cerrar otras aplicaciones pesadas

### Problemas con Animaciones

- Verificar que PIL esté instalado: `pip install pillow`
- Reducir número de frames para GIFs más pequeños

### Error de Memoria

- Usar grillas más pequeñas (<512x512)
- Reducir número de pasos en análisis
