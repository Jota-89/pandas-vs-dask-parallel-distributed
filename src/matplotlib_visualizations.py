"""
Matplotlib visualizations for Conway's Game of Life.
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.colors import ListedColormap
import os
from typing import List, Dict

from game_of_life import GameOfLife
from patterns import ConwayPatterns


class GameOfLifeVisualizer:
    """Create visualizations and animations for Game of Life."""

    def __init__(self, output_dir: str = "visualizations"):
        """Initialize visualizer."""
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Modern dark color scheme
        self.cmap = ListedColormap(['#1a1a2e', '#ffffff'])  # Dark blue, white
        plt.style.use('dark_background')

    def create_pattern_animation(self, pattern_name: str, pattern: np.ndarray, 
                                grid_size: int = 50, steps: int = 30) -> None:
        """Create GIF animation of pattern evolution."""
        game = GameOfLife(grid_size, grid_size)
        
        # Place pattern in center
        center_row = (grid_size - pattern.shape[0]) // 2
        center_col = (grid_size - pattern.shape[1]) // 2
        initial_grid = ConwayPatterns.place_pattern_in_grid(
            np.zeros((grid_size, grid_size)), pattern, (center_row, center_col)
        )
        game.set_state(initial_grid)

        # Collect frames
        frames = []
        for _ in range(steps):
            frames.append(game.get_state().copy())
            game.step()

        # Create animation
        fig, ax = plt.subplots(figsize=(10, 10), facecolor='black')
        ax.set_facecolor('black')
        
        im = ax.imshow(frames[0], cmap=self.cmap, animated=True)
        ax.set_title(f'{pattern_name} Evolution', color='white', fontsize=16)
        ax.axis('off')

        def animate(frame):
            im.set_array(frames[frame])
            return [im]

        anim = animation.FuncAnimation(fig, animate, frames=len(frames), 
                                     interval=200, blit=True, repeat=True)
        
        # Save as GIF
        gif_path = os.path.join(self.output_dir, f'{pattern_name.lower()}_animation.gif')
        anim.save(gif_path, writer='pillow', fps=5)
        plt.close()
        
        print(f"Saved {pattern_name} animation: {gif_path}")

    def create_pattern_evolution_grid(self) -> None:
        """Create grid showing evolution of multiple patterns."""
        patterns = ConwayPatterns.get_all_patterns()
        grid_size = 30
        steps = 10
        
        fig, axes = plt.subplots(len(patterns), steps, figsize=(20, 14), facecolor='black')
        fig.suptitle('Pattern Evolution Comparison', color='white', fontsize=20)
        
        for i, (name, pattern) in enumerate(patterns.items()):
            game = GameOfLife(grid_size, grid_size)
            
            # Place pattern in center
            center_row = (grid_size - pattern.shape[0]) // 2
            center_col = (grid_size - pattern.shape[1]) // 2
            initial_grid = ConwayPatterns.place_pattern_in_grid(
                np.zeros((grid_size, grid_size)), pattern, (center_row, center_col)
            )
            game.set_state(initial_grid)
            
            for j in range(steps):
                ax = axes[i, j]
                ax.imshow(game.get_state(), cmap=self.cmap)
                ax.set_xticks([])
                ax.set_yticks([])
                
                if j == 0:
                    ax.set_ylabel(name, color='white', fontsize=12)
                if i == 0:
                    ax.set_title(f'Step {j}', color='white', fontsize=10)
                
                game.step()
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'pattern_evolution_grid.png'), 
                   dpi=300, bbox_inches='tight', facecolor='black')
        plt.close()
        
        print(f"Saved pattern evolution grid")

    def analyze_population_dynamics(self) -> None:
        """Analyze and plot population dynamics for different patterns."""
        patterns = ConwayPatterns.get_all_patterns()
        grid_size = 60
        steps = 50
        
        plt.figure(figsize=(12, 8), facecolor='black')
        
        colors = ['cyan', 'magenta', 'yellow', 'green', 'orange', 'red', 'lightblue']
        
        for i, (name, pattern) in enumerate(patterns.items()):
            game = GameOfLife(grid_size, grid_size)
            
            # Place pattern in center
            center_row = (grid_size - pattern.shape[0]) // 2
            center_col = (grid_size - pattern.shape[1]) // 2
            initial_grid = ConwayPatterns.place_pattern_in_grid(
                np.zeros((grid_size, grid_size)), pattern, (center_row, center_col)
            )
            game.set_state(initial_grid)
            
            populations = []
            for _ in range(steps):
                populations.append(game.get_population())
                game.step()
            
            plt.plot(populations, label=name, color=colors[i % len(colors)], 
                    linewidth=2, marker='o', markersize=4, alpha=0.8)
        
        plt.xlabel('Generation', color='white')
        plt.ylabel('Population', color='white')
        plt.title('Population Dynamics by Pattern', color='white', fontsize=16)
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'population_dynamics.png'), 
                   dpi=300, bbox_inches='tight', facecolor='black')
        plt.close()
        
        print("Saved population dynamics analysis")

    def create_density_heatmap(self) -> None:
        """Create activity density heatmap."""
        game = GameOfLife(100, 100)
        steps = 100
        
        # Accumulate activity
        activity_map = np.zeros((100, 100))
        
        for _ in range(steps):
            activity_map += game.get_state()
            game.step()
        
        plt.figure(figsize=(10, 8), facecolor='black')
        plt.imshow(activity_map, cmap='hot', interpolation='bilinear')
        plt.colorbar(label='Activity Level')
        plt.title('Cell Activity Density Heatmap', color='white', fontsize=16)
        plt.xlabel('Column', color='white')
        plt.ylabel('Row', color='white')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'density_heatmap.png'), 
                   dpi=300, bbox_inches='tight', facecolor='black')
        plt.close()
        
        print("Saved density heatmap")

    def generate_all_visualizations(self) -> None:
        """Generate all visualizations."""
        print("Conway's Game of Life - Visualization Generator")
        print("=" * 55)
        print(f"Output directory: {self.output_dir}")
        print()
        
        # Generate pattern animations
        patterns = ConwayPatterns.get_all_patterns()
        for name, pattern in patterns.items():
            self.create_pattern_animation(name, pattern)
        
        # Generate comparison plots
        self.create_pattern_evolution_grid()
        self.analyze_population_dynamics()
        self.create_density_heatmap()
        
        print("\nAll visualizations generated successfully!")


def main():
    """Generate all visualizations."""
    visualizer = GameOfLifeVisualizer()
    visualizer.generate_all_visualizations()


if __name__ == "__main__":
    main()