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
from collections import OrderedDict

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
                avg_mef_str = data['Test 2.1']['Result']['Model Annual Average MEF (g/kWh)']

                collected_data.append({
                    'file_path': str(file), # Store the file path for later use
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

    @staticmethod
    def run_correlation_test(results_data: list[dict]) -> float:
        """
        Test 2.2: Across grid regions, the annual average MEF is expected to be positively correlated with the share of coal in the fuel mix.
        The function calculates the Pearson correlation between coal share and average MEF.
        """
        df = pd.DataFrame(results_data)
        correlation = df['coal_share'].corr(df['avg_mef'], method='pearson')
        logger.info(f"Test 2.2 (Cross-Regional Correlation): Pearson correlation between Coal Share and Avg MEF: {correlation:.4f}")
        return correlation

    @staticmethod
    def plot_correlation(results_data: list[dict], correlation: float):
        """
        Creates a scatter plot and saves it to each validation directory.
        """
        df = pd.DataFrame(results_data)

        # noinspection PyTypeChecker
        with plt.style.context('default'):
            fig, ax = plt.subplots(figsize=(8, 5))
            
            sns.regplot(data=df, x='coal_share', y='avg_mef', ax=ax, ci=None, line_kws={'color': 'tab:orange', 'linestyle': '--'})
            sns.scatterplot(data=df, x='coal_share', y='avg_mef', ax=ax, s=100, color='tab:blue')

            # Annotate points with labels
            for i, row in df.iterrows():
                ax.text(row['coal_share'] + 0.5, row['avg_mef'], row['label'], fontsize=9)

            ax.set_title(f'Cross-Regional MEF vs. Coal Share\nCorrelation = {correlation:.4f}')
            ax.set_xlabel('Annual Coal Share in Generation Mix (%)')
            ax.set_ylabel('Annual Average MEF (g/kWh)')
            ax.grid(True, alpha=0.3)
            
            fig.tight_layout()
            
            # Save the same plot to each validation directory
            for item in results_data:
                target_dir = Path(item['file_path']).parent
                plot_path = target_dir / f"2.2_cross_regional_coal_correlation.png"
                try:
                    fig.savefig(plot_path, bbox_inches='tight', facecolor='white')
                    logger.info(f"Saved cross-regional correlation plot to {target_dir}")
                except Exception as e:
                    logger.error(f"Failed to save cross-regional plot to {target_dir}: {e}")
            
            plt.close(fig)

    # ____________________ Validation Summary ____________________#
    @staticmethod
    def update_individual_summaries(results_data: list[dict], correlation: float):
        """
        Reads each summary, inserts the cross-regional test results, and overwrites the file.
        """
        logger.info("Updating individual validation summaries with cross-regional results...")
        
        # Create a clean version of the results data without the file_path for the JSON output
        clean_results_data = [{k: v for k, v in item.items() if k != 'file_path'} for item in results_data]

        cross_regional_result = {
            'Test 2.2': {
                'Description': 'Correlation with Coal Share',
                'Result': {
                    'Pearson Correlation': f"{correlation:.4f}"
                },
                'Data': clean_results_data
            }
        }

        for item in results_data:
            file_path = Path(item['file_path'])
            try:
                with open(file_path, 'r') as f:
                    original_data = json.load(f, object_pairs_hook=OrderedDict)

                # Rebuild the dictionary to ensure chronological order
                new_data = OrderedDict()
                for key, value in original_data.items():
                    new_data[key] = value
                    if key == 'Test 2.1':
                        new_data['Test 2.2'] = cross_regional_result['Test 2.2']
                
                with open(file_path, 'w') as f:
                    json.dump(new_data, f, ensure_ascii=False, indent=4)
                
                logger.info(f"Updated {file_path.name}")

            except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
                logger.warning(f"Could not update file {file_path.name}. Error: {e}. Skipping.")
                continue
