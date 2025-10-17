"""
Script principal para ejecutar el proyecto completo.
"""

import sys
import os
import time

# Agregar el directorio src al path para importar módulos
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))


def print_header():
    print("="*70)
    print("JUEGO DE LA VIDA DE CONWAY - COMPUTACIÓN PARALELA")
    print("="*70)
    print("Implementación con interfaz gráfica y medición de rendimiento")
    print("="*70)


def print_menu():
    print("\n OPCIONES:")
    print("1. Ejecutar interfaz gráfica interactiva (Pygame)")
    print("2. Realizar análisis de rendimiento completo")
    print("3. Generar visualizaciones y animaciones (Matplotlib)")
    print("4. Probar clase GameOfLife básica")
    print("5. Probar patrones clásicos")
    print("6. Ejecutar demostración completa (TODO)")
    print("0. Salir")


def run_pygame_gui():
    """Ejecuta la interfaz gráfica con Pygame."""
    print("\n Iniciando interfaz gráfica interactiva...")
    print("Controles: ESPACIO=pausar, R=reiniciar, P=patrón, ↑↓=velocidad, ESC=salir")

    try:
        from pygame_gui import main
        main()
    except ImportError as e:
        print(f" Error al importar pygame: {e}")
        print("Asegúrate de que pygame esté instalado: pip install pygame")
    except Exception as e:
        print(f" Error ejecutando interfaz: {e}")


def run_performance_analysis():
    """Ejecuta el análisis de rendimiento."""
    print("\n Iniciando análisis de rendimiento...")
    print("Esto puede tomar varios minutos dependiendo de tu sistema...")

    try:
        from performance_analysis import main
        main()
    except ImportError as e:
        print(f" Error al importar módulo de análisis: {e}")
    except Exception as e:
        print(f" Error ejecutando análisis: {e}")


def run_visualizations():
    """Ejecuta las visualizaciones con matplotlib."""
    print("\n Generando visualizaciones y animaciones...")
    print("Esto creará archivos en la carpeta 'visualizations'...")

    try:
        from matplotlib_visualizations import main
        main()
    except ImportError as e:
        print(f" Error al importar matplotlib: {e}")
        print("Asegúrate de que matplotlib esté instalado: pip install matplotlib")
    except Exception as e:
        print(f" Error generando visualizaciones: {e}")


def test_game_of_life():
    """Prueba la clase GameOfLife básica."""
    print("\n Probando clase GameOfLife...")

    try:
        from game_of_life import GameOfLife

        # Crear una instancia pequeña
        game = GameOfLife(10, 10)
        print(f"Juego creado: {game.rows}x{game.cols}")
        print(f"Población inicial: {game.get_population()} celdas vivas")

        # Ejecutar algunas generaciones
        print("\nEjecutando 5 generaciones...")
        for i in range(5):
            game.step()
            print(
                f"Generación {game.get_generation()}: {game.get_population()} celdas vivas")

        print(" Prueba de GameOfLife completada exitosamente!")

    except ImportError as e:
        print(f" Error al importar GameOfLife: {e}")
    except Exception as e:
        print(f" Error en la prueba: {e}")


def test_patterns():
    """Prueba los patrones clásicos."""
    print("\n Probando patrones clásicos...")

    try:
        from patterns import ConwayPatterns

        patterns = ConwayPatterns.get_all_patterns()
        print(f" Patrones disponibles: {len(patterns)}")

        for name, pattern in patterns.items():
            print(f"  - {name.upper()}: {pattern.shape}")

        # Mostrar un patrón de ejemplo
        print("\n Patrón Glider:")
        glider = ConwayPatterns.glider()
        for row in glider:
            print(''.join(['█' if cell else '·' for cell in row]))

        print(" Prueba de patrones completada exitosamente!")

    except ImportError as e:
        print(f" Error al importar patrones: {e}")
    except Exception as e:
        print(f" Error en la prueba: {e}")


def run_complete_demo():
    print("\n Ejecutando demo completa...")
    print("Esto ejecutará todas las funcionalidades principales...")

    test_game_of_life()
    time.sleep(2)

    test_patterns()
    time.sleep(2)

    # Análisis
    print("\n Ejecutando análisis de rendimiento reducido...")
    try:
        from performance_analysis import PerformanceAnalyzer
        analyzer = PerformanceAnalyzer()
        # Usar tamaños más pequeños para demo rápida
        small_sizes = [32, 64, 128]
        results = analyzer.measure_single_step_performance(
            small_sizes, iterations=3)
        analyzer.plot_scaling_analysis(results)
        print(" Análisis de demo completado!")
    except Exception as e:
        print(f" Error en análisis de demo: {e}")

    # Visualizaciones
    print("\n Generando visualizaciones de demo...")
    try:
        from matplotlib_visualizations import ConwayVisualizer
        viz = ConwayVisualizer()
        viz.create_pattern_evolution_grid(
            ['glider', 'blinker'], grid_size=20, steps=5)
        print(" Visualizaciones de demo completadas!")
    except Exception as e:
        print(f" Error en visualizaciones de demo: {e}")

    print("\n Demostración completada!")


def main():
    """Función principal del script."""
    print_header()

    while True:
        print_menu()

        try:
            choice = input("\n Selecciona una opción (0-6): ").strip()

            if choice == '0':
                print("\n ¡Gracias!")
                break

            elif choice == '1':
                run_pygame_gui()

            elif choice == '2':
                run_performance_analysis()

            elif choice == '3':
                run_visualizations()

            elif choice == '4':
                test_game_of_life()

            elif choice == '5':
                test_patterns()

            elif choice == '6':
                run_complete_demo()

            else:
                print(" Opción no válida. Selecciona un número del 0 al 6.")

            # Pausa antes de mostrar el menú nuevamente
            if choice != '0':
                input("\n Presiona Enter para continuar...")

        except KeyboardInterrupt:
            print("\n\n Saliendo del programa...")
            break
        except Exception as e:
            print(f" Error inesperado: {e}")
            input("\n Presiona Enter para continuar...")


if __name__ == "__main__":
    main()
