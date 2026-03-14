# marginal-emissions-germany
This project contains a Python pipeline to compute marginal emission factors for the German electricity market. It processes high-resolution historical open source market data from official sources to model market dynamics beyond the merit-order principle and assess the environmental impact of heatpumps, based on their marginal emissions.

---
## Usage

### 1. Data Preprocessing

The `prep` command runs the entire preprocessing pipeline. It loads the raw data from `data/raw`, prepares the emissions and generation datasets, and saves the final processed files to `data/processed`.

```bash
mef-tool prep [OPTIONS]
```

**Options:**
* `--skip-validation`: If set, the validation step after emission allocation is skipped.

### 2. Data Analysis

The `analysis run` command executes the MSAR (Markov-switching autoregression) analysis on the preprocessed data. You can filter the data by transmission system operator (TSO) and year. The results, including data files and plots, are saved to a structured directory in `results/`.

```bash
mef-tool analysis run [OPTIONS]
```

**Options:**
* `-tso`, `--operator`: Select the TSO to analyze (`50Hertz`, `Amprion`, `TenneT`, `TransnetBW`). Defaults to `All`, processing all TSOs.
* `-y`, `--year`: Select the year to analyze (`2023`, `2024`). Defaults to `All`, processing all years.
* `-t`, `--is-test`: Flag to indicate a test run. This will save results to a separate `results/test/` directory.
* `--num-iterations`: Number of sliding window iterations for the test run. Only used if `--is-test` is set. Defaults to 50.

**Example (Normal Run):**
```bash
# Run analysis for TenneT for 2023
mef-tool analysis run --operator TenneT --year 2023
```

**Example (Test Run):**
```bash
# Run a test with 50 iterations on the Amprion 2024 dataset
mef-tool analysis run --operator Amprion --year 2024 --is-test --num-iterations 50
```

### 3. Workflow Sequence Diagram

The following diagram illustrates the complete workflow, from data preprocessing to analysis, including file interactions.

```mermaid
sequenceDiagram
    actor User
    participant CLI
    participant MEFPreprocessor
    participant MSARAnalyzer
    participant Files

    User->>CLI: mef-tool prep
    CLI->>MEFPreprocessor: __init__()
    
    Files-->>MEFPreprocessor: Read (data/raw/*.csv)
    
    CLI->>MEFPreprocessor: prep_emissions()
    MEFPreprocessor->>Files: Write (data/interim/emissions_germany_utc_*.csv)
    
    CLI->>MEFPreprocessor: prep_generation()
    MEFPreprocessor->>Files: Write (data/interim/generation_*.csv)
    
    CLI->>MEFPreprocessor: alloc_emissions()
    MEFPreprocessor->>Files: Write (data/processed/final_*.csv)
    
    CLI->>MEFPreprocessor: validate_allocation()
    MEFPreprocessor->>Files: Write Plots (results/figures/delta_profile_*.png)
    MEFPreprocessor->>Files: Write Plots (results/figures/allocation_validation_comparison.png)
    MEFPreprocessor-->>CLI: Return results
    CLI-->>User: Log success/failure

    User->>CLI: mef-tool analysis run --operator TenneT --year 2023
    CLI->>CLI: _get_analysis_files('TenneT', '2023')
    
    loop For each found file
        CLI->>CLI: Calculate rows_to_load (if test)
        Files-->>CLI: Read (data/processed/final_*.csv)
        CLI->>MSARAnalyzer: __init__(data, tso, year, is_test, ...)
        CLI->>MSARAnalyzer: prepare()
        CLI->>MSARAnalyzer: fit_compute()
        
        MSARAnalyzer->>MSARAnalyzer: _plot_results()
        MSARAnalyzer->>Files: Write Plot (results/.../estimated_emissions.png)
        
        MSARAnalyzer->>MSARAnalyzer: _plot_sawtooth_debug()
        MSARAnalyzer->>Files: Write Plot (results/.../sawtooth_debug_profile_smoothed.png)

        MSARAnalyzer->>MSARAnalyzer: _plot_avg_daily_profile()
        MSARAnalyzer->>Files: Write Plot (results/.../mef_avg_daily_profile.png)
        
        MSARAnalyzer->>MSARAnalyzer: _diagnose_residuals()
        MSARAnalyzer->>Files: Write Plot (results/.../residual_diagnostics.png)
        MSARAnalyzer->>Files: Write Data (results/.../residual_diagnostics.json)

        MSARAnalyzer->>Files: Write Data (results/.../mef_final.csv)
        MSARAnalyzer->>Files: Write Data (results/.../coefficients.csv)
        MSARAnalyzer->>Files: Write Data (results/.../indicators.json)
    end
    CLI-->>User: Log success/failure
```

---
## Appendix
### [Important links]
#### ENTSOe
- [ENTSO-E API Documentation](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#intro)
- [API Parameter Guide](https://transparencyplatform.zendesk.com/hc/en-us/articles/15692855254548-Sitemap-for-Restful-API-Integration)
- [EIC Manual & Codes](https://www.entsoe.eu/data/energy-identification-codes-eic/)
- [Transparency Platform Guide](https://transparencyplatform.zendesk.com/hc/en-us/categories/13771885458964-Guides) <!-- Data consumers: MoP Ref2 and Ref19 recommended -->
- [Transparency Platform Knowledge Base](https://transparencyplatform.zendesk.com/hc/en-us/categories/12818231533716-Knowledge-base)
- [Manual of Procedures](https://www.entsoe.eu/data/transparency-platform/mop/)
- [Manual of Procedures v3.5 Download with Material](https://eepublicdownloads.blob.core.windows.net/public-cdn-container/clean-documents/mc-documents/transparency-platform/MOP/MoP_v3r5_final.zip)
  - File Detailed Data Description: MoP Ref2 DDD v3r5
  - File Manual of Procedures: MoP v3r5
- [Data Description Actual Generation per Generation Unit](https://transparencyplatform.zendesk.com/hc/en-us/articles/16648326220564-Actual-Generation-per-Generation-Unit-16-1-A)
- [Data Description Actual Generation per Production Type](https://transparencyplatform.zendesk.com/hc/en-us/articles/16648290299284-Actual-Generation-per-Production-Type-16-1-B-C)

### SMARD
- [SMARD API Documentation](https://smard.api.bund.dev/)

### Agora

### MSDR
- [MarkovRegression Model Documentation](https://www.statsmodels.org/stable/generated/statsmodels.tsa.regime_switching.markov_regression.MarkovRegression.html)
- [Heteroskedasticity Explanation](https://www.google.com/search?q=heteroskedasticity&oq=heteroskedasticity&gs_lcrp=EgRlZGdlKgkIABBFGDkYgAQyCQgAEEUYORiABDIHCAEQABiABDIHCAIQABiABDIGCAMQABgeMgYIBBAAGB4yBggFEAAYHjIGCAYQABgeMgYIBxAAGB7SAQczMzFqMGoxqAIAsAIA&sourceid=chrome&ie=UTF-8#fpstate=ive&vld=cid:63ab98e5,vid:ZIOnCoi1ZRw,st:0)
- 