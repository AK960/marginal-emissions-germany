# GEMINI.md - marginal-emissions-germany

## Project Overview
This project is a Python-based pipeline for computing Marginal Emission Factors (MEF) for the German electricity market. It uses high-resolution market data and a Markov-switching autoregression (MSAR) model to assess the environmental impact of electricity consumption (e.g., for heat pumps).

## Technical Stack
- **Language:** Python 3.x
- **Data Processing:** `pandas`
- **CLI Framework:** `click`
- **Statistical Modeling:** `statsmodels` (Markov-switching models)
- **Visualization:** `matplotlib` / `seaborn`
- **Project Management:** `setuptools` (with `pyproject.toml`)

## Project Structure
- `src/marginal_emissions/`: Main package
    - `core/`: Core logic for preprocessing (`preprocess.py`), MSAR modeling (`msar.py`), and validation (`validate.py`).
    - `cli/`: CLI implementation using Click.
    - `clients/`: API clients for data fetching (ENTSO-E, SMARD).
    - `conf/`: Configuration and variable definitions.
    - `utils/`: Helper functions.
- `data/`:
    - `raw/`: Original CSV files from external sources.
    - `interim/`: Intermediate processing steps.
    - `processed/`: Final datasets ready for analysis.
- `results/`: Output directory for plots and CSV results.
- `notebooks/`: Jupyter notebooks for exploratory data analysis (EDA) and testing.

## Development Guidelines
- **Code Style:** Follow PEP 8. Use descriptive variable names.
- **CLI Entry Point:** The main command is `mef-tool`, defined in `src/marginal_emissions/cli/cli.py`.
- **Data Handling:** Use absolute or relative paths consistent with the project root. Always assume data is in `.csv` format unless specified.
- **Testing:** New features should be verified using notebooks or by adding test cases in `tests/` (if established) or via the `mef-tool analysis run --is-test` flag.
- **Docstrings:** Use Google-style or NumPy-style docstrings for complex functions, especially in `core/`.

## Common Commands
- **Install in editable mode:** `pip install -e .`
- **Preprocess data:** `mef-tool prep`
- **Run analysis:** `mef-tool analysis run --operator [TSO] --year [YEAR]`
- **Run test analysis:** `mef-tool analysis run --operator [TSO] --is-test --num-iterations 50`

## Agent Specific Instructions
- **Context:** Always refer to `pyproject.toml` for dependencies and package structure.
- **Surgical Updates:** When modifying `core/msar.py` or `core/preprocess.py`, ensure that existing data pipelines are not broken.
- **Validation:** After modifying preprocessing logic, suggest running `mef-tool prep` to verify changes if possible (keeping in mind execution time).
- **Visualization:** If asked to modify plots, look into the `_plot_*` methods in `MSARAnalyzer` within `msar.py`.
