#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Shanghai ANTL-GDP Regression Analysis (2013-2022)
Multiple regression models with systematic model selection
"""

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import r2_score, mean_squared_error
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import json
import warnings
warnings.filterwarnings('ignore')

from storage_manager import storage_manager

# ============================================================
# 1. Load Data
# ============================================================

# Load ANTL time series - NTL_raster_statistics saves to outputs
# Try both inputs and outputs paths
try:
    antl_csv = storage_manager.resolve_output_path('shanghai_antl_timeseries_2013_2022.csv')
    antl_df = pd.read_csv(antl_csv)
except FileNotFoundError:
    antl_csv = storage_manager.resolve_input_path('shanghai_antl_timeseries_2013_2022.csv')
    antl_df = pd.read_csv(antl_csv)

print("ANTL data loaded from:", antl_csv)
print(antl_df)
print("ANTL columns:", antl_df.columns.tolist())

# Load GDP data
gdp_csv = storage_manager.resolve_input_path('shanghai_gdp_2013_2022.csv')
gdp_df = pd.read_csv(gdp_csv)
print("\nGDP data loaded:")
print(gdp_df)
print("GDP columns:", gdp_df.columns.tolist())

# Check ANTL CSV structure and adapt
# NTL_raster_statistics with only_global=True produces a CSV with Year and ANTL columns
# But the actual structure might be different. Let's inspect.

# If the ANTL CSV has different column names, we need to adapt
if 'Year' not in antl_df.columns:
    # Try to find year column
    year_cols = [c for c in antl_df.columns if 'year' in c.lower() or 'Year' in c]
    if year_cols:
        antl_df = antl_df.rename(columns={year_cols[0]: 'Year'})
    
if 'ANTL' not in antl_df.columns:
    # Try to find ANTL column
    antl_cols = [c for c in antl_df.columns if 'antl' in c.lower() or 'ANTL' in c or 'mean' in c.lower()]
    if antl_cols:
        antl_df = antl_df.rename(columns={antl_cols[0]: 'ANTL'})

print("\nANTL columns after rename:", antl_df.columns.tolist())

# Merge datasets on Year
# GDP CSV columns: year, value_100m_cny (from China_Official_GDP_tool)
df = pd.merge(antl_df, gdp_df, left_on='Year', right_on='year', how='inner')
print("\nMerged dataset:")
print(df)

if len(df) == 0:
    # Try alternative merge
    print("Direct merge failed. Trying alternative approach...")
    # Create a simple dataframe from the ANTL data
    if len(antl_df) == 10:
        years_antl = list(range(2013, 2023))
        antl_df['Year'] = years_antl
    
    df = pd.merge(antl_df, gdp_df, left_on='Year', right_on='year', how='inner')
    print("Merged dataset (alternative):")
    print(df)

# Prepare variables
X = df['ANTL'].values.reshape(-1, 1)  # Independent variable: ANTL
y = df['value_100m_cny'].values        # Dependent variable: GDP (100 million CNY)
years = df['Year'].values

print(f"\nData shape: {len(X)} observations")
print(f"ANTL range: {X.min():.4f} - {X.max():.4f}")
print(f"GDP range: {y.min():.2f} - {y.max():.2f} (100 million CNY)")

# ============================================================
# 2. Define Regression Models
# ============================================================

def fit_linear(X, y):
    """Linear model: y = a + b*x"""
    model = LinearRegression()
    model.fit(X, y)
    y_pred = model.predict(X)
    return model, y_pred, model.intercept_, model.coef_[0]

def fit_log_linear(X, y):
    """Log-linear model: ln(y) = a + b*x => y = exp(a + b*x)"""
    y_log = np.log(y)
    model = LinearRegression()
    model.fit(X, y_log)
    y_pred_log = model.predict(X)
    y_pred = np.exp(y_pred_log)
    return model, y_pred, model.intercept_, model.coef_[0], y_log

def fit_polynomial(X, y, degree=2):
    """Polynomial model: y = a + b1*x + b2*x^2 + ..."""
    poly = PolynomialFeatures(degree=degree)
    X_poly = poly.fit_transform(X)
    model = LinearRegression()
    model.fit(X_poly, y)
    y_pred = model.predict(X_poly)
    return model, y_pred, poly, X_poly

def fit_power(X, y):
    """Power model: y = a * x^b => ln(y) = ln(a) + b*ln(x)"""
    X_log = np.log(X)
    y_log = np.log(y)
    model = LinearRegression()
    model.fit(X_log, y_log)
    y_pred_log = model.predict(X_log)
    y_pred = np.exp(y_pred_log)
    a = np.exp(model.intercept_)
    b = model.coef_[0]
    return model, y_pred, a, b, X_log, y_log

def fit_exponential(X, y):
    """Exponential model: y = a * exp(b*x) => ln(y) = ln(a) + b*x"""
    y_log = np.log(y)
    model = LinearRegression()
    model.fit(X, y_log)
    y_pred_log = model.predict(X)
    y_pred = np.exp(y_pred_log)
    a = np.exp(model.intercept_)
    b = model.coef_[0]
    return model, y_pred, a, b, y_log

# ============================================================
# 3. Model Evaluation Metrics
# ============================================================

def calculate_metrics(y_true, y_pred, n_params):
    """Calculate R², RMSE, AIC, BIC"""
    n = len(y_true)
    
    # R²
    r2 = r2_score(y_true, y_pred)
    
    # RMSE
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    
    # Residual Sum of Squares
    rss = np.sum((y_true - y_pred) ** 2)
    
    # AIC = n * ln(RSS/n) + 2 * k
    aic = n * np.log(rss / n) + 2 * n_params
    
    # BIC = n * ln(RSS/n) + k * ln(n)
    bic = n * np.log(rss / n) + n_params * np.log(n)
    
    return r2, rmse, aic, bic

def cross_validation_score(X, y, model_class, n_splits=5, **kwargs):
    """Perform k-fold cross-validation"""
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
    
    if model_class == 'polynomial':
        degree = kwargs.get('degree', 2)
        scores = []
        for train_idx, test_idx in kf.split(X):
            X_train, X_test = X[train_idx], X[test_idx]
            y_train, y_test = y[train_idx], y[test_idx]
            
            poly = PolynomialFeatures(degree=degree)
            X_train_poly = poly.fit_transform(X_train)
            X_test_poly = poly.transform(X_test)
            
            lr = LinearRegression()
            lr.fit(X_train_poly, y_train)
            y_pred = lr.predict(X_test_poly)
            scores.append(r2_score(y_test, y_pred))
        return np.mean(scores), np.std(scores)
    else:
        # For linear models, use standard cross_val_score
        model = LinearRegression()
        if model_class == 'log_linear':
            y_transformed = np.log(y)
            scores = cross_val_score(model, X, y_transformed, cv=kf, scoring='r2')
        elif model_class == 'power':
            X_log = np.log(X)
            y_log = np.log(y)
            scores = cross_val_score(model, X_log, y_log, cv=kf, scoring='r2')
        elif model_class == 'exponential':
            y_transformed = np.log(y)
            scores = cross_val_score(model, X, y_transformed, cv=kf, scoring='r2')
        else:  # linear
            scores = cross_val_score(model, X, y, cv=kf, scoring='r2')
        return np.mean(scores), np.std(scores)

# ============================================================
# 4. Fit All Models
# ============================================================

print("\n" + "="*60)
print("FITTING REGRESSION MODELS")
print("="*60)

results = {}

# Model 1: Linear
print("\n[1] Linear Model: y = a + b*x")
model_lin, y_pred_lin, intercept_lin, coef_lin = fit_linear(X, y)
r2_lin, rmse_lin, aic_lin, bic_lin = calculate_metrics(y, y_pred_lin, n_params=2)
cv_r2_lin, cv_std_lin = cross_validation_score(X, y, 'linear')
results['Linear'] = {
    'model': 'Linear',
    'equation': f'y = {intercept_lin:.4f} + {coef_lin:.4f} * x',
    'R2': r2_lin,
    'RMSE': rmse_lin,
    'AIC': aic_lin,
    'BIC': bic_lin,
    'CV_R2_mean': cv_r2_lin,
    'CV_R2_std': cv_std_lin,
    'predictions': y_pred_lin,
    'params': {'intercept': intercept_lin, 'coef': coef_lin}
}
print(f"  Equation: y = {intercept_lin:.4f} + {coef_lin:.4f} * x")
print(f"  R² = {r2_lin:.6f}, RMSE = {rmse_lin:.4f}")
print(f"  AIC = {aic_lin:.4f}, BIC = {bic_lin:.4f}")
print(f"  CV R² = {cv_r2_lin:.4f} ± {cv_std_lin:.4f}")

# Model 2: Log-Linear
print("\n[2] Log-Linear Model: ln(y) = a + b*x")
model_loglin, y_pred_loglin, intercept_loglin, coef_loglin, y_log = fit_log_linear(X, y)
r2_loglin, rmse_loglin, aic_loglin, bic_loglin = calculate_metrics(y, y_pred_loglin, n_params=2)
cv_r2_loglin, cv_std_loglin = cross_validation_score(X, y, 'log_linear')
results['Log-Linear'] = {
    'model': 'Log-Linear',
    'equation': f'ln(y) = {intercept_loglin:.4f} + {coef_loglin:.4f} * x',
    'R2': r2_loglin,
    'RMSE': rmse_loglin,
    'AIC': aic_loglin,
    'BIC': bic_loglin,
    'CV_R2_mean': cv_r2_loglin,
    'CV_R2_std': cv_std_loglin,
    'predictions': y_pred_loglin,
    'params': {'intercept': intercept_loglin, 'coef': coef_loglin}
}
print(f"  Equation: ln(y) = {intercept_loglin:.4f} + {coef_loglin:.4f} * x")
print(f"  R² = {r2_loglin:.6f}, RMSE = {rmse_loglin:.4f}")
print(f"  AIC = {aic_loglin:.4f}, BIC = {bic_loglin:.4f}")
print(f"  CV R² = {cv_r2_loglin:.4f} ± {cv_std_loglin:.4f}")

# Model 3: Polynomial (degree 2)
print("\n[3] Polynomial Model (degree=2): y = a + b1*x + b2*x²")
model_poly, y_pred_poly, poly, X_poly = fit_polynomial(X, y, degree=2)
r2_poly, rmse_poly, aic_poly, bic_poly = calculate_metrics(y, y_pred_poly, n_params=3)
cv_r2_poly, cv_std_poly = cross_validation_score(X, y, 'polynomial', degree=2)
results['Polynomial'] = {
    'model': 'Polynomial',
    'equation': f'y = {model_poly.intercept_:.4f} + {model_poly.coef_[1]:.4f} * x + {model_poly.coef_[2]:.4f} * x²',
    'R2': r2_poly,
    'RMSE': rmse_poly,
    'AIC': aic_poly,
    'BIC': bic_poly,
    'CV_R2_mean': cv_r2_poly,
    'CV_R2_std': cv_std_poly,
    'predictions': y_pred_poly,
    'params': {'intercept': model_poly.intercept_, 'coefs': model_poly.coef_.tolist()}
}
print(f"  R² = {r2_poly:.6f}, RMSE = {rmse_poly:.4f}")
print(f"  AIC = {aic_poly:.4f}, BIC = {bic_poly:.4f}")
print(f"  CV R² = {cv_r2_poly:.4f} ± {cv_std_poly:.4f}")

# Model 4: Power
print("\n[4] Power Model: y = a * x^b")
model_power, y_pred_power, a_power, b_power, X_log_power, y_log_power = fit_power(X, y)
r2_power, rmse_power, aic_power, bic_power = calculate_metrics(y, y_pred_power, n_params=2)
cv_r2_power, cv_std_power = cross_validation_score(X, y, 'power')
results['Power'] = {
    'model': 'Power',
    'equation': f'y = {a_power:.4f} * x^{b_power:.4f}',
    'R2': r2_power,
    'RMSE': rmse_power,
    'AIC': aic_power,
    'BIC': bic_power,
    'CV_R2_mean': cv_r2_power,
    'CV_R2_std': cv_std_power,
    'predictions': y_pred_power,
    'params': {'a': a_power, 'b': b_power}
}
print(f"  Equation: y = {a_power:.4f} * x^{b_power:.4f}")
print(f"  R² = {r2_power:.6f}, RMSE = {rmse_power:.4f}")
print(f"  AIC = {aic_power:.4f}, BIC = {bic_power:.4f}")
print(f"  CV R² = {cv_r2_power:.4f} ± {cv_std_power:.4f}")

# Model 5: Exponential
print("\n[5] Exponential Model: y = a * exp(b*x)")
model_exp, y_pred_exp, a_exp, b_exp, y_log_exp = fit_exponential(X, y)
r2_exp, rmse_exp, aic_exp, bic_exp = calculate_metrics(y, y_pred_exp, n_params=2)
cv_r2_exp, cv_std_exp = cross_validation_score(X, y, 'exponential')
results['Exponential'] = {
    'model': 'Exponential',
    'equation': f'y = {a_exp:.4f} * exp({b_exp:.4f} * x)',
    'R2': r2_exp,
    'RMSE': rmse_exp,
    'AIC': aic_exp,
    'BIC': bic_exp,
    'CV_R2_mean': cv_r2_exp,
    'CV_R2_std': cv_std_exp,
    'predictions': y_pred_exp,
    'params': {'a': a_exp, 'b': b_exp}
}
print(f"  Equation: y = {a_exp:.4f} * exp({b_exp:.4f} * x)")
print(f"  R² = {r2_exp:.6f}, RMSE = {rmse_exp:.4f}")
print(f"  AIC = {aic_exp:.4f}, BIC = {bic_exp:.4f}")
print(f"  CV R² = {cv_r2_exp:.4f} ± {cv_std_exp:.4f}")

# ============================================================
# 5. Model Selection
# ============================================================

print("\n" + "="*60)
print("MODEL SELECTION")
print("="*60)

# Create summary DataFrame
summary_data = []
for name, res in results.items():
    summary_data.append({
        'Model': name,
        'R2': res['R2'],
        'RMSE': res['RMSE'],
        'AIC': res['AIC'],
        'BIC': res['BIC'],
        'CV_R2': res['CV_R2_mean'],
        'CV_R2_std': res['CV_R2_std']
    })

summary_df = pd.DataFrame(summary_data)
print("\nModel Comparison Summary:")
print(summary_df.to_string(index=False))

# Select best model based on multiple criteria
# Primary: Highest R² and CV_R², Lowest RMSE, AIC, BIC
best_by_r2 = summary_df.loc[summary_df['R2'].idxmax()]
best_by_cv_r2 = summary_df.loc[summary_df['CV_R2'].idxmax()]
best_by_rmse = summary_df.loc[summary_df['RMSE'].idxmin()]
best_by_aic = summary_df.loc[summary_df['AIC'].idxmin()]
best_by_bic = summary_df.loc[summary_df['BIC'].idxmin()]

print(f"\nBest by R²: {best_by_r2['Model']} (R² = {best_by_r2['R2']:.6f})")
print(f"Best by CV R²: {best_by_cv_r2['Model']} (CV R² = {best_by_cv_r2['CV_R2']:.6f})")
print(f"Best by RMSE: {best_by_rmse['Model']} (RMSE = {best_by_rmse['RMSE']:.4f})")
print(f"Best by AIC: {best_by_aic['Model']} (AIC = {best_by_aic['AIC']:.4f})")
print(f"Best by BIC: {best_by_bic['Model']} (BIC = {best_by_bic['BIC']:.4f})")

# Overall best model:综合考虑，选择R²最高且CV_R²稳定的模型
# Use a scoring system: rank each model by each metric, sum ranks
summary_df['R2_rank'] = summary_df['R2'].rank(ascending=False)
summary_df['RMSE_rank'] = summary_df['RMSE'].rank(ascending=True)
summary_df['AIC_rank'] = summary_df['AIC'].rank(ascending=True)
summary_df['BIC_rank'] = summary_df['BIC'].rank(ascending=True)
summary_df['CV_R2_rank'] = summary_df['CV_R2'].rank(ascending=False)
summary_df['Total_Rank'] = summary_df['R2_rank'] + summary_df['RMSE_rank'] + summary_df['AIC_rank'] + summary_df['BIC_rank'] + summary_df['CV_R2_rank']

best_overall = summary_df.loc[summary_df['Total_Rank'].idxmin()]
print(f"\n*** BEST OVERALL MODEL: {best_overall['Model']} ***")
print(f"Total Rank Score: {best_overall['Total_Rank']:.2f} (lower is better)")

best_model_name = best_overall['Model']
best_model_results = results[best_model_name]

# ============================================================
# 6. Save Results
# ============================================================

# Save model comparison summary to CSV
summary_csv = storage_manager.resolve_output_path('GDP_Regression_Model_Comparison.csv')
summary_df.to_csv(summary_csv, index=False)
print(f"\nModel comparison saved to: {summary_csv}")

# Save detailed results to JSON
json_output = {
    'study_area': 'Shanghai',
    'time_period': '2013-2022',
    'n_observations': len(X),
    'best_model': best_model_name,
    'best_model_equation': best_model_results['equation'],
    'best_model_metrics': {
        'R2': float(best_model_results['R2']),
        'RMSE': float(best_model_results['RMSE']),
        'AIC': float(best_model_results['AIC']),
        'BIC': float(best_model_results['BIC']),
        'CV_R2_mean': float(best_model_results['CV_R2_mean']),
        'CV_R2_std': float(best_model_results['CV_R2_std'])
    },
    'all_models': {k: {kk: vv for kk, vv in v.items() if kk not in ['model', 'predictions']} for k, v in results.items()}
}

json_path = storage_manager.resolve_output_path('GDP_Regression_Model_Summary.json')
with open(json_path, 'w') as f:
    json.dump(json_output, f, indent=2)
print(f"Model summary saved to: {json_path}")

# Save predictions from best model
predictions_df = pd.DataFrame({
    'Year': years,
    'ANTL': X.flatten(),
    'GDP_Observed': y,
    'GDP_Predicted': best_model_results['predictions'],
    'Residual': y - best_model_results['predictions']
})
predictions_csv = storage_manager.resolve_output_path('GDP_Best_Model_Predictions.csv')
predictions_df.to_csv(predictions_csv, index=False)
print(f"Predictions saved to: {predictions_csv}")

# ============================================================
# 7. Generate Visualizations
# ============================================================

# Figure 1: Scatter plot with best model fit
fig1, ax1 = plt.subplots(figsize=(10, 8))
ax1.scatter(X, y, color='blue', s=100, alpha=0.7, label='Observed', zorder=5)

# Sort X for smooth line plotting
X_sorted = np.sort(X.flatten())
if best_model_name == 'Linear':
    y_pred_sorted = best_model_results['params']['intercept'] + best_model_results['params']['coef'] * X_sorted
elif best_model_name == 'Log-Linear':
    y_pred_sorted = np.exp(best_model_results['params']['intercept'] + best_model_results['params']['coef'] * X_sorted)
elif best_model_name == 'Polynomial':
    coefs = best_model_results['params']['coefs']
    y_pred_sorted = coefs[0] + coefs[1] * X_sorted + coefs[2] * X_sorted**2
elif best_model_name == 'Power':
    y_pred_sorted = best_model_results['params']['a'] * X_sorted**best_model_results['params']['b']
elif best_model_name == 'Exponential':
    y_pred_sorted = best_model_results['params']['a'] * np.exp(best_model_results['params']['b'] * X_sorted)

ax1.plot(X_sorted, y_pred_sorted, 'r-', linewidth=2.5, label=f'{best_model_name} Fit\nR² = {best_model_results["R2"]:.4f}')
ax1.set_xlabel('ANTL (Average Nighttime Light)', fontsize=12)
ax1.set_ylabel('GDP (100 million CNY)', fontsize=12)
ax1.set_title(f'Shanghai ANTL-GDP Relationship (2013-2022)\nBest Model: {best_model_name}', fontsize=14, fontweight='bold')
ax1.legend(loc='best', fontsize=11)
ax1.grid(True, alpha=0.3)
plt.tight_layout()

fig1_path = storage_manager.resolve_output_path('ANTL_GDP_Scatter_BestModel.png')
fig1.savefig(fig1_path, dpi=300, bbox_inches='tight')
plt.close(fig1)
print(f"Scatter plot saved to: {fig1_path}")

# Figure 2: Model comparison bar chart
fig2, axes = plt.subplots(2, 2, figsize=(14, 10))

# R² comparison
ax = axes[0, 0]
models = summary_df['Model'].tolist()
r2_vals = summary_df['R2'].tolist()
colors = ['#2ecc71' if m == best_model_name else '#3498db' for m in models]
bars = ax.bar(models, r2_vals, color=colors, edgecolor='black', linewidth=1.5)
ax.set_ylabel('R²', fontsize=11)
ax.set_title('R² Comparison', fontsize=12, fontweight='bold')
ax.set_ylim(0, 1.0)
for bar, val in zip(bars, r2_vals):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01, f'{val:.4f}', 
            ha='center', va='bottom', fontsize=10, fontweight='bold')
ax.axhline(y=best_overall['R2'], color='red', linestyle='--', linewidth=2, alpha=0.5, label=f'Best: {best_overall["R2"]:.4f}')
ax.legend()

# RMSE comparison
ax = axes[0, 1]
rmse_vals = summary_df['RMSE'].tolist()
colors = ['#e74c3c' if m == best_model_name else '#95a5a6' for m in models]
bars = ax.bar(models, rmse_vals, color=colors, edgecolor='black', linewidth=1.5)
ax.set_ylabel('RMSE', fontsize=11)
ax.set_title('RMSE Comparison (lower is better)', fontsize=12, fontweight='bold')
for bar, val in zip(bars, rmse_vals):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50, f'{val:.2f}', 
            ha='center', va='bottom', fontsize=9, fontweight='bold')

# CV R² comparison
ax = axes[1, 0]
cv_r2_vals = summary_df['CV_R2'].tolist()
cv_r2_std = summary_df['CV_R2_std'].tolist()
colors = ['#f39c12' if m == best_model_name else '#1abc9c' for m in models]
bars = ax.bar(models, cv_r2_vals, color=colors, edgecolor='black', linewidth=1.5, yerr=cv_r2_std, capsize=5)
ax.set_ylabel('Cross-Validation R²', fontsize=11)
ax.set_title('5-Fold CV R² Comparison', fontsize=12, fontweight='bold')
ax.set_ylim(0, 1.0)
for bar, val in zip(bars, cv_r2_vals):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, f'{val:.4f}', 
            ha='center', va='bottom', fontsize=9, fontweight='bold')

# AIC comparison
ax = axes[1, 1]
aic_vals = summary_df['AIC'].tolist()
colors = ['#9b59b6' if m == best_model_name else '#bdc3c7' for m in models]
bars = ax.bar(models, aic_vals, color=colors, edgecolor='black', linewidth=1.5)
ax.set_ylabel('AIC', fontsize=11)
ax.set_title('AIC Comparison (lower is better)', fontsize=12, fontweight='bold')
for bar, val in zip(bars, aic_vals):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10, f'{val:.1f}', 
            ha='center', va='bottom', fontsize=9, fontweight='bold')

plt.tight_layout()
fig2_path = storage_manager.resolve_output_path('Model_Comparison_Charts.png')
fig2.savefig(fig2_path, dpi=300, bbox_inches='tight')
plt.close(fig2)
print(f"Model comparison charts saved to: {fig2_path}")

# Figure 3: Residual analysis
fig3, axes = plt.subplots(1, 2, figsize=(14, 5))

# Residuals vs Fitted
ax = axes[0]
residuals = y - best_model_results['predictions']
fitted = best_model_results['predictions']
ax.scatter(fitted, residuals, color='purple', s=80, alpha=0.7, edgecolor='black')
ax.axhline(y=0, color='red', linestyle='--', linewidth=2)
ax.set_xlabel('Fitted Values', fontsize=11)
ax.set_ylabel('Residuals', fontsize=11)
ax.set_title(f'Residuals vs Fitted ({best_model_name})', fontsize=12, fontweight='bold')
ax.grid(True, alpha=0.3)

# Residuals histogram with normal distribution overlay
ax = axes[1]
ax.hist(residuals, bins=6, color='skyblue', edgecolor='black', alpha=0.7, density=True)
# Add normal distribution curve
mu, sigma = np.mean(residuals), np.std(residuals)
x_norm = np.linspace(min(residuals), max(residuals), 100)
ax.plot(x_norm, stats.norm.pdf(x_norm, mu, sigma), 'r-', linewidth=2, label='Normal fit')
ax.set_xlabel('Residuals', fontsize=11)
ax.set_ylabel('Density', fontsize=11)
ax.set_title('Residual Distribution', fontsize=12, fontweight='bold')
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
fig3_path = storage_manager.resolve_output_path('Residual_Analysis.png')
fig3.savefig(fig3_path, dpi=300, bbox_inches='tight')
plt.close(fig3)
print(f"Residual analysis saved to: {fig3_path}")

# Figure 4: Time series comparison
fig4, ax = plt.subplots(figsize=(12, 6))
ax.plot(years, y, 'bo-', linewidth=2, markersize=8, label='Observed GDP', zorder=5)
ax.plot(years, best_model_results['predictions'], 'rs--', linewidth=2, markersize=8, label='Predicted GDP')
ax.fill_between(years, y, best_model_results['predictions'], alpha=0.3, color='gray', label='Residual')
ax.set_xlabel('Year', fontsize=12)
ax.set_ylabel('GDP (100 million CNY)', fontsize=12)
ax.set_title(f'Shanghai GDP Time Series: Observed vs Predicted ({best_model_name})', fontsize=14, fontweight='bold')
ax.legend(loc='best', fontsize=11)
ax.grid(True, alpha=0.3)
ax.set_xticks(years)
plt.tight_layout()

fig4_path = storage_manager.resolve_output_path('GDP_TimeSeries_Comparison.png')
fig4.savefig(fig4_path, dpi=300, bbox_inches='tight')
plt.close(fig4)
print(f"Time series comparison saved to: {fig4_path}")

# ============================================================
# 8. Final Summary
# ============================================================

print("\n" + "="*60)
print("ANALYSIS COMPLETE")
print("="*60)
print(f"\nStudy Area: Shanghai")
print(f"Time Period: 2013-2022 ({len(X)} years)")
print(f"\nBest Model: {best_model_name}")
print(f"Equation: {best_model_results['equation']}")
print(f"R² = {best_model_results['R2']:.6f}")
print(f"RMSE = {best_model_results['RMSE']:.4f} (100 million CNY)")
print(f"AIC = {best_model_results['AIC']:.4f}")
print(f"BIC = {best_model_results['BIC']:.4f}")
print(f"Cross-Validation R² = {best_model_results['CV_R2_mean']:.4f} ± {best_model_results['CV_R2_std']:.4f}")

print("\nOutput Files:")
print(f"  - {summary_csv}")
print(f"  - {json_path}")
print(f"  - {predictions_csv}")
print(f"  - {fig1_path}")
print(f"  - {fig2_path}")
print(f"  - {fig3_path}")
print(f"  - {fig4_path}")
