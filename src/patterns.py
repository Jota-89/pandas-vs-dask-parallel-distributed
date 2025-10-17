"""
Classic Conway's Game of Life patterns.
"""

import numpy as np
from typing import Dict, Tuple


class ConwayPatterns:
    """Classic Game of Life patterns."""

    @staticmethod
    def glider() -> np.ndarray:
        """Glider pattern - moves diagonally."""
        return np.array([
            [0, 1, 0],
            [0, 0, 1],
            [1, 1, 1]
        ])

    @staticmethod
    def blinker() -> np.ndarray:
        """Blinker pattern - oscillates with period 2."""
        return np.array([
            [1, 1, 1]
        ])

    @staticmethod
    def toad() -> np.ndarray:
        """Toad pattern - oscillates with period 2."""
        return np.array([
            [0, 1, 1, 1],
            [1, 1, 1, 0]
        ])

    @staticmethod
    def block() -> np.ndarray:
        """Block pattern - static structure."""
        return np.array([
            [1, 1],
            [1, 1]
        ])

    @staticmethod
    def beacon() -> np.ndarray:
        """Beacon pattern - oscillates with period 2."""
        return np.array([
            [1, 1, 0, 0],
            [1, 1, 0, 0],
            [0, 0, 1, 1],
            [0, 0, 1, 1]
        ])

    @staticmethod
    def pulsar() -> np.ndarray:
        """Pulsar pattern - oscillates with period 3."""
        return np.array([
            [0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1],
            [1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1],
            [1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1],
            [0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0],
            [1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1],
            [1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1],
            [1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0]
        ])

    @staticmethod
    def lightweight_spaceship() -> np.ndarray:
        """Lightweight Spaceship (LWSS) - moves horizontally."""
        return np.array([
            [1, 0, 0, 1, 0],
            [0, 0, 0, 0, 1],
            [1, 0, 0, 0, 1],
            [0, 1, 1, 1, 1]
        ])

    @staticmethod
    def get_all_patterns() -> Dict[str, np.ndarray]:
        """Return dictionary of all available patterns."""
        return {
            'Glider': ConwayPatterns.glider(),
            'Blinker': ConwayPatterns.blinker(),
            'Toad': ConwayPatterns.toad(),
            'Block': ConwayPatterns.block(),
            'Beacon': ConwayPatterns.beacon(),
            'Pulsar': ConwayPatterns.pulsar(),
            'LWSS': ConwayPatterns.lightweight_spaceship()
        }

    @staticmethod
    def place_pattern_in_grid(grid: np.ndarray, pattern: np.ndarray, 
                              position: Tuple[int, int]) -> np.ndarray:
        """Place pattern in grid at specified position."""
        rows, cols = grid.shape
        pattern_rows, pattern_cols = pattern.shape
        start_row, start_col = position

        # Ensure pattern fits in grid
        end_row = min(start_row + pattern_rows, rows)
        end_col = min(start_col + pattern_cols, cols)
        
        # Adjust pattern size if needed
        actual_pattern_rows = end_row - start_row
        actual_pattern_cols = end_col - start_col
        
        result = grid.copy()
        result[start_row:end_row, start_col:end_col] = pattern[:actual_pattern_rows, :actual_pattern_cols]
        
        return result


def main():
    """Demonstrate all patterns."""
    patterns = ConwayPatterns.get_all_patterns()
    
    print("Conway's Game of Life - Classic Patterns")
    print("=" * 50)
    
    for name, pattern in patterns.items():
        print(f"\n{name}:")
        print("-" * len(name))
        for row in pattern:
            print(''.join('█' if cell else '·' for cell in row))
        print(f"Size: {pattern.shape}")


if __name__ == "__main__":
    main()