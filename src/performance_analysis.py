"""
Performance analysis for Conway's Game of Life.
"""

import numpy as np
import matplotlib.pyplot as plt
import time
import os
from typing import List, Tuple, Dict
import seaborn as sns

from game_of_life import GameOfLife
from patterns import ConwayPatterns


class PerformanceAnalyzer:
    """Performance analyzer for Game of Life."""

    def __init__(self, output_dir: str = "performance"):
        """Initialize analyzer."""
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def measure_single_step_performance(self, grid_sizes: List[int],
                                        iterations: int = 10) -> Dict[str, List]:
        """Measure performance for different grid sizes."""
        results = {
            'grid_sizes': [],
            'cell_counts': [],
            'avg_times': [],
            'std_times': [],
            'throughput': []
        }

        print("Performance Analysis - Single Step Times")
        print("=" * 50)

        for size in grid_sizes:
            print(f"Testing {size}x{size} grid...")

            # Initialize game
            game = GameOfLife(size, size)
            cell_count = size * size

            # Warm-up
            for _ in range(3):
                game.step()

            # Measure performance
            times = []
            for i in range(iterations):
                start_time = time.perf_counter()
                game.step()
                end_time = time.perf_counter()
                times.append(end_time - start_time)

            avg_time = np.mean(times)
            std_time = np.std(times)
            throughput = cell_count / avg_time

            results['grid_sizes'].append(size)
            results['cell_counts'].append(cell_count)
            results['avg_times'].append(avg_time)
            results['std_times'].append(std_time)
            results['throughput'].append(throughput)

            print(f"  Size: {size}x{size}")
            print(f"  Cells: {cell_count:,}")
            print(f"  Avg time: {avg_time:.6f}s")
            print(f"  Throughput: {throughput:,.0f} cells/s")
            print()

        return results

    def plot_scaling_analysis(self, results: Dict[str, List]) -> None:
        """Create scaling analysis plots."""
        cell_counts = np.array(results['cell_counts'])
        avg_times = np.array(results['avg_times'])
        throughput = np.array(results['throughput'])

        # Set modern style
        plt.style.use('dark_background')
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

        # Time vs Grid Size (Linear)
        ax1.plot(cell_counts, avg_times, 'o-',
                 color='cyan', linewidth=2, markersize=8)
        ax1.set_xlabel('Number of Cells')
        ax1.set_ylabel('Average Time (seconds)')
        ax1.set_title('Execution Time vs Grid Size')
        ax1.grid(True, alpha=0.3)

        # Time vs Grid Size (Log-Log)
        ax2.loglog(cell_counts, avg_times, 'o-',
                   color='magenta', linewidth=2, markersize=8)

        # Add theoretical complexity curves
        x_theory = np.linspace(min(cell_counts), max(cell_counts), 100)

        # O(n) curve
        c1 = avg_times[0] / cell_counts[0]
        y_linear = c1 * x_theory
        ax2.loglog(x_theory, y_linear, '--',
                   color='green', alpha=0.7, label='O(n)')

        # O(n log n) curve
        c2 = avg_times[0] / (cell_counts[0] * np.log(cell_counts[0]))
        y_nlogn = c2 * x_theory * np.log(x_theory)
        ax2.loglog(x_theory, y_nlogn, '--', color='yellow',
                   alpha=0.7, label='O(n log n)')

        # O(n²) curve
        c3 = avg_times[0] / (cell_counts[0] ** 2)
        y_quadratic = c3 * x_theory ** 2
        ax2.loglog(x_theory, y_quadratic, '--',
                   color='red', alpha=0.7, label='O(n²)')

        ax2.set_xlabel('Number of Cells')
        ax2.set_ylabel('Average Time (seconds)')
        ax2.set_title('Log-Log Plot: Time Complexity Analysis')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # Throughput vs Grid Size
        ax3.plot(cell_counts, throughput, 'o-',
                 color='orange', linewidth=2, markersize=8)
        ax3.set_xlabel('Number of Cells')
        ax3.set_ylabel('Throughput (cells/second)')
        ax3.set_title('Throughput vs Grid Size')
        ax3.grid(True, alpha=0.3)

        # Error bars
        ax4.errorbar(cell_counts, avg_times, yerr=results['std_times'],
                     fmt='o-', color='lightblue', linewidth=2, markersize=8, capsize=5)
        ax4.set_xlabel('Number of Cells')
        ax4.set_ylabel('Average Time (seconds)')
        ax4.set_title('Performance with Error Bars')
        ax4.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, 'scaling_analysis.png'),
                    dpi=300, bbox_inches='tight', facecolor='black')
        print(
            f"Saved scaling analysis to {self.output_dir}/scaling_analysis.png")

    def analyze_complexity(self, results: Dict[str, List]) -> None:
        """Analyze computational complexity."""
        cell_counts = np.array(results['cell_counts'])
        avg_times = np.array(results['avg_times'])

        print("Complexity Analysis")
        print("=" * 30)

        # Calculate growth rates
        for i in range(1, len(cell_counts)):
            size_ratio = cell_counts[i] / cell_counts[i-1]
            time_ratio = avg_times[i] / avg_times[i-1]

            print(f"Size {results['grid_sizes'][i-1]} -> {results['grid_sizes'][i]}: "
                  f"Cells x{size_ratio:.1f}, Time x{time_ratio:.2f}")

        # Linear regression on log-log data
        log_cells = np.log(cell_counts)
        log_times = np.log(avg_times)

        coeffs = np.polyfit(log_cells, log_times, 1)
        slope = coeffs[0]

        print(f"\nLog-log regression slope: {slope:.3f}")
        print(f"Empirical complexity: O(n^{slope:.2f})")

        if slope < 1.2:
            print("Result: Close to O(n) - Linear complexity")
        elif slope < 1.8:
            print("Result: Between O(n) and O(n²)")
        else:
            print("Result: Close to O(n²) - Quadratic complexity")

    def save_results_csv(self, results: Dict[str, List]) -> None:
        """Save results to CSV file."""
        import pandas as pd

        df = pd.DataFrame(results)
        csv_path = os.path.join(self.output_dir, 'performance_results.csv')
        df.to_csv(csv_path, index=False)
        print(f"Results saved to {csv_path}")

    def run_full_analysis(self) -> None:
        """Run complete performance analysis."""
        print("Conway's Game of Life - Performance Analysis")
        print("=" * 60)

        # Test different grid sizes
        grid_sizes = [32, 64, 128, 256, 512]
        print(f"Testing grid sizes: {grid_sizes}")
        print(f"Output directory: {self.output_dir}")
        print()

        # Measure performance
        results = self.measure_single_step_performance(
            grid_sizes, iterations=15)

        # Analyze and visualize
        self.analyze_complexity(results)
        self.plot_scaling_analysis(results)
        self.save_results_csv(results)

        print("\nAnalysis complete!")


def main():
    """Run performance analysis."""
    analyzer = PerformanceAnalyzer()
    analyzer.run_full_analysis()


if __name__ == "__main__":
    main()
