# NTL-GDP Regression Analysis Skill

## Overview
This skill provides a standardized workflow for analyzing the relationship between Annual Nighttime Light (ANTL) and GDP data using multiple regression models. It is designed for tasks involving economic indicator estimation from nighttime light imagery.

## When to Use
- User requests GDP-NTL correlation analysis
- Multiple regression model comparison is needed
- Time-series economic analysis with NTL data (typically 5+ years)
- Model selection and validation for NTL-based economic indicators

## Task Level Classification
This is typically an **L3 (custom_or_algorithm_gap)** task because:
- Requires custom regression modeling beyond built-in tools
- Involves statistical model comparison and selection
- Needs both data retrieval (GDP + NTL) and custom analysis code

## Workflow Steps

### Step 1: Knowledge Grounding (Mandatory)
Query `NTL_Knowledge_Base` for:
- GDP estimation methodologies using NTL
- Recommended regression models for NTL-GDP analysis
- Model validation metrics (R², AIC, BIC, RMSE)

### Step 2: Data Retrieval Strategy

#### 2.1 NTL Data (Data_Searcher)
- **Dataset**: Prefer the annual NTL dataset selected by `/skills/gee-dataset-selection/`; common long-term default is `projects/sat-io/open-datasets/npp-viirs-ntl`.
- **Temporal Coverage**: Validate requested year range with `dataset_latest_availability_tool` or `GEE_dataset_metadata_tool` before retrieval/analysis.
- **Period Semantics**: For annual products, treat `system:time_start` anchor dates as annual period anchors. For example, `latest_available_date = 2024-01-01` means `latest_available_period = 2024`, not a single-day cutoff.
- **Spatial Boundary**: Shanghai administrative boundary (SHP/GeoJSON)
- **Product Type**: Annual composite (NOT daily/monthly aggregation)
- **Output**: ANTL values per year (via `NTL_raster_statistics` tool)

**Retrieval Contract Requirements**:
```json
{
  "schema": "ntl.retrieval.contract.v1",
  "status": "complete",
  "task_level": "L3",
  "files": ["shanghai_ntl_2013.tif", ..., "shanghai_ntl_2022.tif"],
  "coverage_check": {
    "expected_count": 10,
    "actual_count": 10,
    "missing_items": []
  },
  "boundary": {
    "source": "GEE/OSM",
    "crs": "EPSG:4326",
    "bounds": [min_lon, min_lat, max_lon, max_lat]
  }
}
```

#### 2.2 GDP Data (Data_Searcher)
- **Priority 1**: Use user-uploaded files in `inputs/` if available
- **Priority 2**: Call `China_Official_GDP_tool` for Shanghai (上海市)
- **Priority 3**: Search via Tavily for official statistical yearbooks
- **Format**: Annual GDP values (2013-2022) in constant prices or current prices
- **Output**: CSV/JSON with year-GDP pairs

**Critical**: For China GDP requests, explicitly require `China_Official_GDP_tool` as primary source.

### Step 3: ANTL Calculation
Use `NTL_raster_statistics` tool:
```python
# Input: Annual NTL rasters + Shanghai boundary
# Output: ANTL (Average Nighttime Light) per year
# Tool call:
NTL_raster_statistics(
    ntl_tif_paths=["shanghai_ntl_2013.tif", ..., "shanghai_ntl_2022.tif"],
    shapefile_path="shanghai_boundary.shp",
    output_csv_path="shanghai_antl_stats.csv",
    selected_indices=["ANTL", "TNTL"],
    only_global=True
)
```

### Step 4: Regression Modeling (Code_Assistant)

#### 4.1 Draft Script Structure (Save Before Handoff)
Create `shanghai_gdp_ntl_regression_v1.py`:

```python
"""
Shanghai GDP-NTL Regression Analysis
Models: Linear, Log-Linear, Quadratic, Power, Exponential
"""
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score, mean_squared_error, aic, bic
import statsmodels.api as sm
import matplotlib.pyplot as plt

# === INPUTS ===
# ANTL data: shanghai_antl_stats.csv (from NTL_raster_statistics)
# GDP data: shanghai_gdp_2013_2022.csv (from China_Official_GDP_tool)

# === STEP 1: Load Data ===
antl_df = pd.read_csv('inputs/shanghai_antl_stats.csv')
gdp_df = pd.read_csv('inputs/shanghai_gdp_2013_2022.csv')

# Merge on year
merged = pd.merge(antl_df, gdp_df, on='year')
X = merged['ANTL'].values
y = merged['GDP'].values

# === STEP 2: Define Models ===
models = {
    'Linear': lambda x: x.reshape(-1, 1),
    'Log-Linear': lambda x: np.log(x).reshape(-1, 1),
    'Quadratic': lambda x: PolynomialFeatures(degree=2).fit_transform(x.reshape(-1, 1)),
    'Power': lambda x: np.log(x).reshape(-1, 1),  # log(GDP) = a + b*log(ANTL)
    'Exponential': lambda x: x.reshape(-1, 1)  # log(GDP) = a + b*ANTL
}

results = []

# === STEP 3: Fit and Evaluate ===
for name, transform in models.items():
    X_trans = transform(X)
    
    if name in ['Power']:
        y_trans = np.log(y)
    elif name in ['Exponential']:
        y_trans = np.log(y)
    else:
        y_trans = y
    
    model = LinearRegression()
    model.fit(X_trans, y_trans)
    y_pred = model.predict(X_trans)
    
    # Back-transform if needed
    if name in ['Power', 'Exponential']:
        y_pred = np.exp(y_pred)
        y_eval = y
    else:
        y_eval = y_trans
    
    r2 = r2_score(y_eval, y_pred)
    rmse = np.sqrt(mean_squared_error(y_eval, y_pred))
    
    # AIC/BIC (using statsmodels for proper calculation)
    X_with_const = sm.add_constant(X_trans)
    ols_model = sm.OLS(y_trans, X_with_const).fit()
    aic_val = ols_model.aic
    bic_val = ols_model.bic
    
    results.append({
        'model': name,
        'r2': r2,
        'rmse': rmse,
        'aic': aic_val,
        'bic': bic_val,
        'coefficients': model.coef_.tolist(),
        'intercept': model.intercept_
    })

# === STEP 4: Select Best Model ===
results_df = pd.DataFrame(results)
best_model = results_df.loc[results_df['r2'].idxmax()]

# === STEP 5: Visualization ===
plt.figure(figsize=(12, 8))
plt.scatter(X, y, label='Observed', s=100, alpha=0.7)

# Plot best fit
X_sorted = np.sort(X)
X_trans_sorted = transform(X_sorted)
y_pred_sorted = model.predict(X_trans_sorted)
if name in ['Power', 'Exponential']:
    y_pred_sorted = np.exp(y_pred_sorted)
plt.plot(X_sorted, y_pred_sorted, 'r-', linewidth=2, label=f'Best: {best_model["model"]} (R²={best_model["r2"]:.3f})')

plt.xlabel('ANTL')
plt.ylabel('GDP')
plt.title('Shanghai GDP-NTL Relationship (2013-2022)')
plt.legend()
plt.savefig('outputs/shanghai_gdp_ntl_regression.png', dpi=300, bbox_inches='tight')

# === STEP 6: Save Results ===
results_df.to_csv('outputs/shanghai_regression_models_comparison.csv', index=False)
print(f"Best Model: {best_model['model']}")
print(f"R²: {best_model['r2']:.4f}, RMSE: {best_model['rmse']:.4f}")
```

#### 4.2 Handoff Packet to Code_Assistant
```json
{
  "task_level": "L3",
  "draft_script_name": "shanghai_gdp_ntl_regression_v1.py",
  "execution_objective": "Fit 5 regression models (Linear, Log-Linear, Quadratic, Power, Exponential) to Shanghai ANTL-GDP data (2013-2022), evaluate using R²/AIC/BIC/RMSE, select best-fitting model, and generate visualization",
  "inputs": [
    "shanghai_antl_stats.csv (from NTL_raster_statistics)",
    "shanghai_gdp_2013_2022.csv (from China_Official_GDP_tool)"
  ],
  "outputs": [
    "outputs/shanghai_regression_models_comparison.csv",
    "outputs/shanghai_gdp_ntl_regression.png"
  ],
  "validation_criteria": [
    "All 5 models successfully fitted",
    "R² values computed for each model",
    "Best model selected by highest R²",
    "Scatter plot with regression line saved"
  ]
}
```

### Step 5: Model Validation
Code_Assistant must:
1. Read the saved `.py` script before execution
2. Execute using `execute_geospatial_script_tool`
3. Verify all output files are generated
4. Report model comparison metrics in tabular format

### Step 6: Result Synthesis
Final output must include:
- **Model Comparison Table**: R², RMSE, AIC, BIC for all models
- **Best Model Selection**: Name, equation, coefficients, R²
- **Visualization**: Scatter plot with regression line
- **Interpretation**: Economic meaning of the relationship

## Output Files
- `outputs/shanghai_antl_stats.csv` - ANTL statistics per year
- `outputs/shanghai_gdp_2013_2022.csv` - GDP data per year
- `outputs/shanghai_regression_models_comparison.csv` - Model metrics
- `outputs/shanghai_gdp_ntl_regression.png` - Visualization
- `outputs/shanghai_gdp_ntl_analysis_summary.txt` - Text summary (optional)

## Common Pitfalls & Solutions

| Issue | Solution |
|-------|----------|
| NTL date range mismatch | Use `/skills/gee-dataset-selection/` and `dataset_latest_availability_tool`; compare requested years to `latest_available_period`, not a literal annual anchor date |
| GDP data in current vs constant prices | Specify in analysis; use constant prices for real growth |
| Log transformation with zero/negative values | Add small constant (e.g., +1) before log |
| Overfitting with polynomial models | Prefer parsimonious models; check AIC/BIC |
| Spatial boundary mismatch | Ensure NTL and GDP use same Shanghai boundary |

## Example Task JSON
```json
{
  "task": "Shanghai GDP-NTL regression analysis",
  "time_range": "2013-2022",
  "study_area": "上海市",
  "models": ["Linear", "Log-Linear", "Quadratic", "Power", "Exponential"],
  "selection_metric": "R²",
  "outputs": ["model_comparison_csv", "regression_plot", "best_model_summary"]
}
```

## References
- NTL-GDP estimation literature (Henderson et al., 2012; Chen & Nordhaus, 2011)
- `NTL_Estimate_Indicator_Provincial` tool documentation
- `China_Official_GDP_tool` API specification
- `NTL_raster_statistics` tool for ANTL calculation
