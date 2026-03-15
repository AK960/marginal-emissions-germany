"""
Addon for the validation CLI command, containing cross-regional tests.
"""
import json
from pathlib import Path
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from marginal_emissions import logger
from marginal_emissions.vars import RESULTS_DIR

class CrossRegionalValidator:
    def __init__(self, is_test: bool):
        self.is_test = is_test
        self.base_path = RESULTS_DIR / "test" / "msar" if is_test else RESULTS_DIR / "msar"
        self.save_dir = RESULTS_DIR / "test" if is_test else RESULTS_DIR
        self.save_dir.mkdir(exist_ok=True)

    def collect_results(self) -> list[dict] | None:
        """
        Collects all individual validation summaries to extract data for the cross-regional test.
        """
        logger.info(f"Searching for validation summaries in: {self.base_path}")
        summary_files = list(self.base_path.rglob("validation/validation_summary_*.json"))

        if len(summary_files) < 2:
            logger.error(f"Found only {len(summary_files)} validation summary file(s). Need at least 2 for a correlation test.")
            return None

        logger.info(f"Found {len(summary_files)} summary files. Collecting data...")
        
        collected_data = []
        for file in summary_files:
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                
                tso = file.stem.split('_')[-2]
                year = file.stem.split('_')[-1]

                # Extract required data points
                coal_share_str = data['Test 3']['Indicators']['Coal Share']
                avg_mef_str = data['Test 2.2 Data']['Result']['Model Annual Average MEF (g/kWh)']

                collected_data.append({
                    'tso': tso,
                    'year': year,
                    'label': f"{tso.capitalize()}-{year}",
                    'coal_share': float(coal_share_str.strip('%')),
                    'avg_mef': float(avg_mef_str)
                })
            except (KeyError, IndexError, json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Could not process file {file.name}. Error: {e}. Skipping.")
                continue
        
        if len(collected_data) < 2:
            logger.error("Could not extract valid data from at least 2 summary files. Aborting.")
            return None
            
        return collected_data

    def run_correlation_test(self, results_data: list[dict]) -> float:
        """
        Calculates the Pearson correlation between coal share and average MEF.
        """
        df = pd.DataFrame(results_data)
        correlation = df['coal_share'].corr(df['avg_mef'], method='pearson')
        logger.info(f"Pearson correlation between Coal Share and Avg MEF: {correlation:.4f}")
        return correlation

    def plot_correlation(self, results_data: list[dict], correlation: float):
        """
        Creates a scatter plot to visualize the correlation.
        """
        df = pd.DataFrame(results_data)

        plt.style.use('default')
        fig, ax = plt.subplots(figsize=(8, 5))
        
        sns.regplot(data=df, x='coal_share', y='avg_mef', ax=ax, ci=None, line_kws={'color': 'red', 'linestyle': '--'})
        sns.scatterplot(data=df, x='coal_share', y='avg_mef', ax=ax, s=100)

        # Annotate points with labels
        for i, row in df.iterrows():
            ax.text(row['coal_share'] + 0.5, row['avg_mef'], row['label'], fontsize=9)

        ax.set_title(f'Cross-Regional MEF vs. Coal Share\nCorrelation = {correlation:.4f}')
        ax.set_xlabel('Annual Coal Share in Generation Mix (%)')
        ax.set_ylabel('Annual Average MEF (g/kWh)')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        filename = "cross_regional_coal_correlation.png" if not self.is_test else "cross_regional_coal_correlation_test.png"
        plot_path = self.save_dir / filename
        try:
            fig.savefig(plot_path, bbox_inches='tight', facecolor='white')
            logger.info(f"Saved cross-regional correlation plot to {plot_path}")
        except Exception as e:
            logger.error(f"Failed to save cross-regional plot: {e}")
        finally:
            plt.close(fig)

    def save_correlation_summary(self, results_data: list[dict], correlation: float):
        """
        Saves the final summary of the cross-regional test to a JSON file.
        """
        summary = {
            'Test 2.2': {
                'Description': 'Correlation with Coal Share',
                'Result': {
                    'Pearson Correlation': f"{correlation:.4f}"
                },
                'Data': results_data
            }
        }
        
        filename = "validation_summary_cross_regional.json" if not self.is_test else "validation_summary_cross_regional_test.json"
        file_path = self.save_dir / filename
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=4)
            logger.info(f"Cross-regional validation summary saved to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save cross-regional summary: {e}")
