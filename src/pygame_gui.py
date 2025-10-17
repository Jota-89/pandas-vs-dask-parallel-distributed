"""
Interfaz gráfica moderna con Pygame para el Juego de la Vida de Conway.
"""

import pygame
import numpy as np
import sys
import time
from typing import Optional, Tuple

from game_of_life import GameOfLife
from patterns import ConwayPatterns


class ConwayGUI:
    """Interfaz gráfica moderna para el Juego de la Vida usando Pygame."""

    # Paleta de colores moderna - Modo oscuro
    BACKGROUND = (15, 15, 20)          # Casi negro
    SURFACE = (25, 25, 35)             # Superficie oscura
    PRIMARY = (100, 120, 255)          # Azul moderno
    SECONDARY = (60, 80, 200)          # Azul oscuro
    ACCENT = (255, 100, 150)           # Rosa/coral
    SUCCESS = (50, 200, 120)           # Verde moderno
    WARNING = (255, 180, 50)           # Naranja
    DANGER = (255, 80, 80)             # Rojo moderno
    TEXT_PRIMARY = (240, 240, 245)     # Texto principal
    TEXT_SECONDARY = (180, 180, 190)   # Texto secundario
    GRID_LINE = (40, 40, 50)           # Líneas de grilla
    CELL_ALIVE = (255, 255, 255)       # Celda viva
    CELL_DEAD = (30, 30, 40)           # Celda muerta
    BUTTON_HOVER = (120, 140, 255)     # Hover de botones

    def __init__(self, grid_size: Tuple[int, int] = (100, 100),
                 window_size: Tuple[int, int] = (1200, 800)):
        """Inicializa la interfaz gráfica."""
        self.grid_rows, self.grid_cols = grid_size
        self.window_width, self.window_height = window_size

        pygame.init()
        pygame.font.init()
        self.screen = pygame.display.set_mode(
            (self.window_width, self.window_height))
        pygame.display.set_caption("Conway's Game of Life")

        # Fuentes modernas
        self.font_title = pygame.font.Font(None, 48)
        self.font_large = pygame.font.Font(None, 28)
        self.font_medium = pygame.font.Font(None, 22)
        self.font_small = pygame.font.Font(None, 18)

        # Configurar área de simulación
        self.sim_area_width = 750
        self.sim_area_height = 650
        self.sim_area_x = 30
        self.sim_area_y = 80

        # Calcular tamaño de celda
        self.cell_width = self.sim_area_width // self.grid_cols
        self.cell_height = self.sim_area_height // self.grid_rows

        # Ajustar área real de simulación
        self.actual_sim_width = self.cell_width * self.grid_cols
        self.actual_sim_height = self.cell_height * self.grid_rows

        # Inicializar juego
        self.game = GameOfLife(self.grid_rows, self.grid_cols)

        # Variables de control
        self.running = True
        self.paused = True
        self.fps = 10
        self.clock = pygame.time.Clock()
        self.last_update = time.time()

        # Estadísticas
        self.total_generations = 0
        self.start_time = time.time()

        # Estado de hover para botones
        self.mouse_pos = (0, 0)

        # Botones
        self.buttons = self._create_buttons()

        # Patrones disponibles
        self.patterns = ConwayPatterns.get_all_patterns()
        self.current_pattern_index = 0
        self.pattern_names = list(self.patterns.keys())

    def _create_buttons(self) -> list:
        """Crea botones con diseño moderno."""
        button_width = 140
        button_height = 45
        button_x = self.sim_area_x + self.actual_sim_width + 40
        spacing = 15

        buttons = [
            {
                'rect': pygame.Rect(button_x, 120, button_width, button_height),
                'text': 'START',
                'action': 'start',
                'color': self.SUCCESS,
                'icon': '▶'
            },
            {
                'rect': pygame.Rect(button_x, 120 + (button_height + spacing), button_width, button_height),
                'text': 'PAUSE',
                'action': 'pause',
                'color': self.WARNING,
                'icon': '⏸'
            },
            {
                'rect': pygame.Rect(button_x, 120 + 2*(button_height + spacing), button_width, button_height),
                'text': 'RESET',
                'action': 'reset',
                'color': self.DANGER,
                'icon': '↻'
            },
            {
                'rect': pygame.Rect(button_x, 120 + 3*(button_height + spacing), button_width, button_height),
                'text': 'PATTERN',
                'action': 'pattern',
                'color': self.PRIMARY,
                'icon': '◈'
            },
            {
                'rect': pygame.Rect(button_x, 120 + 4*(button_height + spacing), button_width, button_height),
                'text': 'SPEED +',
                'action': 'speed_up',
                'color': self.SECONDARY,
                'icon': '⇧'
            },
            {
                'rect': pygame.Rect(button_x, 120 + 5*(button_height + spacing), button_width, button_height),
                'text': 'SPEED -',
                'action': 'speed_down',
                'color': self.SECONDARY,
                'icon': '⇩'
            }
        ]

        return buttons

    def _draw_button(self, button: dict) -> None:
        """Dibuja botón con diseño moderno y efectos hover."""
        is_hovered = button['rect'].collidepoint(self.mouse_pos)

        # Color base y hover
        color = self.BUTTON_HOVER if is_hovered else button['color']

        # Sombra
        shadow_rect = button['rect'].copy()
        shadow_rect.x += 3
        shadow_rect.y += 3
        pygame.draw.rect(self.screen, (10, 10, 15),
                         shadow_rect, border_radius=8)

        # Botón principal
        pygame.draw.rect(self.screen, color, button['rect'], border_radius=8)

        # Borde sutil
        border_color = self.TEXT_PRIMARY if is_hovered else self.SURFACE
        pygame.draw.rect(self.screen, border_color,
                         button['rect'], 2, border_radius=8)

        # Icono
        icon_font = pygame.font.Font(None, 28)
        icon_surface = icon_font.render(
            button['icon'], True, self.TEXT_PRIMARY)
        icon_rect = icon_surface.get_rect()
        icon_rect.centerx = button['rect'].x + 25
        icon_rect.centery = button['rect'].centery
        self.screen.blit(icon_surface, icon_rect)

        # Texto
        text_surface = self.font_medium.render(
            button['text'], True, self.TEXT_PRIMARY)
        text_rect = text_surface.get_rect()
        text_rect.centerx = button['rect'].x + 85
        text_rect.centery = button['rect'].centery
        self.screen.blit(text_surface, text_rect)

    def _handle_button_click(self, pos: Tuple[int, int]) -> None:
        """Maneja los clics en los botones."""
        for button in self.buttons:
            if button['rect'].collidepoint(pos):
                action = button['action']

                if action == 'start':
                    self.paused = False
                elif action == 'pause':
                    self.paused = True
                elif action == 'reset':
                    self.game.reset()
                    self.total_generations = 0
                    self.start_time = time.time()
                elif action == 'pattern':
                    self._load_next_pattern()
                elif action == 'speed_up':
                    self.fps = min(self.fps + 2, 30)
                elif action == 'speed_down':
                    self.fps = max(self.fps - 2, 1)

    def _load_next_pattern(self) -> None:
        """Carga el siguiente patrón predefinido."""
        pattern_name = self.pattern_names[self.current_pattern_index]
        pattern = self.patterns[pattern_name]

        # Crear un tablero vacío y colocar el patrón en el centro
        new_grid = np.zeros((self.grid_rows, self.grid_cols))

        # Calcular posición central
        center_row = (self.grid_rows - pattern.shape[0]) // 2
        center_col = (self.grid_cols - pattern.shape[1]) // 2

        new_grid = ConwayPatterns.place_pattern_in_grid(
            new_grid, pattern, (center_row, center_col)
        )

        self.game.set_state(new_grid)
        self.total_generations = 0
        self.start_time = time.time()

        # Avanzar al siguiente patrón
        self.current_pattern_index = (
            self.current_pattern_index + 1) % len(self.pattern_names)

    def _draw_grid(self) -> None:
        """Dibuja el tablero con diseño moderno."""
        # Fondo del área de simulación con borde redondeado
        sim_rect = pygame.Rect(self.sim_area_x, self.sim_area_y,
                               self.actual_sim_width, self.actual_sim_height)
        pygame.draw.rect(self.screen, self.SURFACE, sim_rect, border_radius=12)
        pygame.draw.rect(self.screen, self.GRID_LINE,
                         sim_rect, 2, border_radius=12)

        # Obtener estado actual
        grid = self.game.get_state()

        # Dibujar celdas
        for row in range(self.grid_rows):
            for col in range(self.grid_cols):
                x = self.sim_area_x + col * self.cell_width
                y = self.sim_area_y + row * self.cell_height

                if grid[row, col] == 1:  # Celda viva
                    cell_rect = pygame.Rect(x + 1, y + 1,
                                            self.cell_width - 2, self.cell_height - 2)
                    pygame.draw.rect(self.screen, self.CELL_ALIVE, cell_rect)
                else:  # Celda muerta
                    if self.grid_rows <= 60 and self.grid_cols <= 60:  # Solo para grillas pequeñas
                        cell_rect = pygame.Rect(x + 1, y + 1,
                                                self.cell_width - 2, self.cell_height - 2)
                        pygame.draw.rect(
                            self.screen, self.CELL_DEAD, cell_rect)

    def _draw_info(self) -> None:
        """Dibuja información con diseño moderno."""
        info_x = self.sim_area_x + self.actual_sim_width + 40

        # Título principal
        title = self.font_title.render("Conway's", True, self.TEXT_PRIMARY)
        self.screen.blit(title, (info_x, 20))

        subtitle = self.font_large.render("Game of Life", True, self.ACCENT)
        self.screen.blit(subtitle, (info_x, 60))

        # Panel de estadísticas
        stats_y = 480
        panel_rect = pygame.Rect(info_x - 10, stats_y - 10, 180, 280)
        pygame.draw.rect(self.screen, self.SURFACE,
                         panel_rect, border_radius=12)
        pygame.draw.rect(self.screen, self.GRID_LINE,
                         panel_rect, 1, border_radius=12)

        # Estadísticas
        stats = [
            ("Generation", f"{self.game.get_generation()}"),
            ("Population", f"{self.game.get_population()}"),
            ("Grid Size", f"{self.grid_rows}×{self.grid_cols}"),
            ("FPS", f"{self.fps}"),
            ("Status", "Running" if not self.paused else "Paused"),
            ("Pattern", f"{self.pattern_names[self.current_pattern_index-1]}")
        ]

        for i, (label, value) in enumerate(stats):
            # Etiqueta
            label_text = self.font_small.render(
                label, True, self.TEXT_SECONDARY)
            self.screen.blit(label_text, (info_x, stats_y + i * 25))

            # Valor
            value_color = self.PRIMARY if label == "Status" and not self.paused else self.TEXT_PRIMARY
            value_text = self.font_small.render(value, True, value_color)
            self.screen.blit(value_text, (info_x, stats_y + i * 25 + 12))

        # Controles
        controls_y = stats_y + 180
        controls_text = self.font_small.render(
            "CONTROLS", True, self.TEXT_SECONDARY)
        self.screen.blit(controls_text, (info_x, controls_y))

        controls = [
            "SPACE - Pause/Resume",
            "R - Reset",
            "P - Next Pattern",
            "↑/↓ - Speed",
            "ESC - Exit"
        ]

        for i, control in enumerate(controls):
            text = self.font_small.render(control, True, self.TEXT_SECONDARY)
            self.screen.blit(text, (info_x, controls_y + 20 + i * 15))

    def _handle_events(self) -> None:
        """Maneja eventos de pygame."""
        self.mouse_pos = pygame.mouse.get_pos()

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                self.running = False

            elif event.type == pygame.MOUSEBUTTONDOWN:
                if event.button == 1:
                    self._handle_button_click(event.pos)

            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_SPACE:
                    self.paused = not self.paused
                elif event.key == pygame.K_r:
                    self.game.reset()
                    self.total_generations = 0
                    self.start_time = time.time()
                elif event.key == pygame.K_p:
                    self._load_next_pattern()
                elif event.key == pygame.K_UP:
                    self.fps = min(self.fps + 2, 30)
                elif event.key == pygame.K_DOWN:
                    self.fps = max(self.fps - 2, 1)
                elif event.key == pygame.K_ESCAPE:
                    self.running = False

    def run(self) -> None:
        """Ejecuta el bucle principal de la interfaz."""
        print("Conway's Game of Life - Modern Interface")
        print("Controls:")
        print("- SPACE: Pause/Resume")
        print("- R: Reset")
        print("- P: Next Pattern")
        print("- ↑/↓: Speed Control")
        print("- ESC: Exit")

        while self.running:
            self._handle_events()

            if not self.paused:
                current_time = time.time()
                if current_time - self.last_update >= 1.0 / self.fps:
                    self.game.step()
                    self.total_generations += 1
                    self.last_update = current_time

            # Dibujar todo
            self.screen.fill(self.BACKGROUND)
            self._draw_grid()
            self._draw_info()

            for button in self.buttons:
                self._draw_button(button)

            pygame.display.flip()
            self.clock.tick(60)

        pygame.quit()


def main():
    """Función principal para ejecutar la interfaz."""
    grid_size = (80, 80)
    window_size = (1200, 800)

    gui = ConwayGUI(grid_size, window_size)
    gui.run()


if __name__ == "__main__":
    main()
