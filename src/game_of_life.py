"""
Conway's Game of Life Implementation
"""

import numpy as np
import time
from typing import Optional, Tuple


class GameOfLife:
    """Conway's Game of Life cellular automaton."""

    def __init__(self, rows: int, cols: int, initial_state: Optional[np.ndarray] = None):
        """Initialize the Game of Life board."""
        self.rows = rows
        self.cols = cols
        self.generation = 0

        if initial_state is not None:
            if initial_state.shape != (rows, cols):
                raise ValueError(f"Initial state must have shape ({rows}, {cols})")
            self.grid = initial_state.copy()
        else:
            self.grid = np.random.choice([0, 1], size=(rows, cols), p=[0.7, 0.3])

    def _count_neighbors_vectorized(self) -> np.ndarray:
        """Count neighbors using vectorized operations for efficiency."""
        neighbors = np.zeros_like(self.grid)
        
        # Use np.roll for toroidal boundary conditions
        for di in [-1, 0, 1]:
            for dj in [-1, 0, 1]:
                if di == 0 and dj == 0:
                    continue
                neighbors += np.roll(np.roll(self.grid, di, axis=0), dj, axis=1)
        
        return neighbors

    def step(self) -> None:
        """Update the grid by one generation following Conway's rules."""
        neighbors = self._count_neighbors_vectorized()
        
        # Apply Conway's rules using vectorized operations
        # Any live cell with 2-3 neighbors survives
        # Any dead cell with exactly 3 neighbors becomes alive
        new_grid = np.where(
            (self.grid == 1) & ((neighbors == 2) | (neighbors == 3)), 1,
            np.where((self.grid == 0) & (neighbors == 3), 1, 0)
        )
        
        self.grid = new_grid
        self.generation += 1

    def run(self, steps: int) -> None:
        """Run the simulation for specified number of steps."""
        for _ in range(steps):
            self.step()

    def get_state(self) -> np.ndarray:
        """Return current grid state."""
        return self.grid.copy()

    def set_state(self, new_state: np.ndarray) -> None:
        """Set new grid state."""
        if new_state.shape != (self.rows, self.cols):
            raise ValueError(f"State must have shape ({self.rows}, {self.cols})")
        self.grid = new_state.copy()
        self.generation = 0

    def get_population(self) -> int:
        """Return number of living cells."""
        return int(np.sum(self.grid))

    def get_generation(self) -> int:
        """Return current generation number."""
        return self.generation

    def reset(self) -> None:
        """Reset to random initial state."""
        self.grid = np.random.choice([0, 1], size=(self.rows, self.cols), p=[0.7, 0.3])
        self.generation = 0

    def is_stable(self) -> bool:
        """Check if the grid has reached a stable state."""
        old_grid = self.grid.copy()
        self.step()
        stable = np.array_equal(old_grid, self.grid)
        if stable:
            self.grid = old_grid
            self.generation -= 1
        return stable

    def clear(self) -> None:
        """Clear the grid (all cells dead)."""
        self.grid = np.zeros((self.rows, self.cols), dtype=int)
        self.generation = 0

    def get_grid_size(self) -> Tuple[int, int]:
        """Return grid dimensions."""
        return self.rows, self.cols

    def print_grid(self) -> None:
        """Print current grid to console."""
        print(f"Generation {self.generation}:")
        for row in self.grid:
            print(''.join('█' if cell else '·' for cell in row))
        print(f"Population: {self.get_population()}")
        print()


def main():
    """Demonstration of GameOfLife class."""
    print("Conway's Game of Life Demo")
    
    # Create a small game instance
    game = GameOfLife(20, 20)
    
    print("Initial state:")
    game.print_grid()
    
    # Run for a few generations
    for i in range(5):
        game.step()
        print(f"After step {i+1}:")
        game.print_grid()
        time.sleep(0.5)


if __name__ == "__main__":
    main()