"""
Shanghai GDP-NTL Regression Analysis (2013-2022)
Models: Linear, Log-Linear, Quadratic, Power, Exponential
Author: 地缘环境智能计算平台 Code_Assistant
"""
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score, mean_squared_error
import statsmodels.api as sm
import matplotlib.pyplot as plt
import json
import os

# Paths
ANTL_CSV = "outputs/shanghai_antl_stats.csv"
GDP_CSV = "inputs/shanghai_gdp_2013_2022.csv"
OUTPUT_DIR = "outputs"

def load_data():
    """Load ANTL and GDP data"""
    # Load ANTL data from NTL_raster_statistics output
    antl_data = {
        2013: 11.7923,
        2014: 11.2265,
        2015: 11.5896,
        2016: 11.5140,
        2017: 12.6655,
        2018: 12.7607,
        2019: 12.8280,
        2020: 13.1001,
        2021: 13.5075,
        2022: 13.8498
    }
    
    # Load GDP data (from China_Official_GDP_tool)
    gdp_data = {
        2013: 23809.4,
        2014: 25964.5,
        2015: 27821.6,
        2016: 30963.9,
        2017: 34378.3,
        2018: 37769.1,
        2019: 40241.2,
        2020: 41603.9,
        2021: 47059.4,
        2022: 48594.5
    }
    
    df = pd.DataFrame({
        'year': list(antl_data.keys()),
        'ANTL': list(antl_data.values()),
        'GDP': list(gdp_data.values())
    })
    return df

def calculate_metrics(y_true, y_pred, n_params):
    """Calculate regression metrics: R², RMSE, AIC, BIC"""
    n = len(y_true)
    residuals = y_true - y_pred
    rss = np.sum(residuals**2)
    tss = np.sum((y_true - np.mean(y_true))**2)
    
    r2 = r2_score(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    
    # AIC and BIC
    if rss > 0:
        aic = n * np.log(rss/n) + 2 * n_params
        bic = n * np.log(rss/n) + n_params * np.log(n)
    else:
        aic = -np.inf
        bic = -np.inf
    
    return r2, rmse, aic, bic

def fit_linear_model(X, y):
    """Linear: GDP = β0 + β1 * ANTL"""
    X_const = sm.add_constant(X)
    model = sm.OLS(y, X_const).fit()
    y_pred = model.predict(X_const)
    r2, rmse, aic, bic = calculate_metrics(y, y_pred, 2)
    return {
        'model_name': 'Linear',
        'r2': r2,
        'rmse': rmse,
        'aic': aic,
        'bic': bic,
        'y_pred': y_pred,
        'equation': f"GDP = {model.params[0]:.2f} + {model.params[1]:.2f} * ANTL",
        'model': model
    }

def fit_log_linear_model(X, y):
    """Log-Linear: ln(GDP) = β0 + β1 * ANTL"""
    y_log = np.log(y)
    X_const = sm.add_constant(X)
    model = sm.OLS(y_log, X_const).fit()
    y_pred_log = model.predict(X_const)
    y_pred = np.exp(y_pred_log)
    r2, rmse, aic, bic = calculate_metrics(y, y_pred, 2)
    return {
        'model_name': 'Log-Linear',
        'r2': r2,
        'rmse': rmse,
        'aic': aic,
        'bic': bic,
        'y_pred': y_pred,
        'equation': f"ln(GDP) = {model.params[0]:.4f} + {model.params[1]:.4f} * ANTL",
        'model': model
    }

def fit_quadratic_model(X, y):
    """Quadratic: GDP = β0 + β1 * ANTL + β2 * ANTL²"""
    poly = PolynomialFeatures(degree=2, include_bias=False)
    X_poly = poly.fit_transform(X.reshape(-1, 1))
    X_const = sm.add_constant(X_poly)
    model = sm.OLS(y, X_const).fit()
    y_pred = model.predict(X_const)
    r2, rmse, aic, bic = calculate_metrics(y, y_pred, 3)
    return {
        'model_name': 'Quadratic',
        'r2': r2,
        'rmse': rmse,
        'aic': aic,
        'bic': bic,
        'y_pred': y_pred,
        'equation': f"GDP = {model.params[0]:.2f} + {model.params[1]:.2f} * ANTL + {model.params[2]:.2f} * ANTL²",
        'model': model
    }

def fit_power_model(X, y):
    """Power: GDP = α * ANTL^β -> ln(GDP) = ln(α) + β * ln(ANTL)"""
    X_log = np.log(X)
    y_log = np.log(y)
    X_const = sm.add_constant(X_log)
    model = sm.OLS(y_log, X_const).fit()
    y_pred_log = model.predict(X_const)
    y_pred = np.exp(y_pred_log)
    r2, rmse, aic, bic = calculate_metrics(y, y_pred, 2)
    alpha = np.exp(model.params[0])
    beta = model.params[1]
    return {
        'model_name': 'Power',
        'r2': r2,
        'rmse': rmse,
        'aic': aic,
        'bic': bic,
        'y_pred': y_pred,
        'equation': f"GDP = {alpha:.4f} * ANTL^{beta:.4f}",
        'model': model
    }

def fit_exponential_model(X, y):
    """Exponential: GDP = α * e^(β * ANTL) -> ln(GDP) = ln(α) + β * ANTL"""
    y_log = np.log(y)
    X_const = sm.add_constant(X)
    model = sm.OLS(y_log, X_const).fit()
    y_pred_log = model.predict(X_const)
    y_pred = np.exp(y_pred_log)
    r2, rmse, aic, bic = calculate_metrics(y, y_pred, 2)
    alpha = np.exp(model.params[0])
    beta = model.params[1]
    return {
        'model_name': 'Exponential',
        'r2': r2,
        'rmse': rmse,
        'aic': aic,
        'bic': bic,
        'y_pred': y_pred,
        'equation': f"GDP = {alpha:.4f} * e^({beta:.4f} * ANTL)",
        'model': model
    }

def select_best_model(results):
    """Select best model based on R² (highest) and AIC (lowest)"""
    # Sort by R² (descending)
    sorted_by_r2 = sorted(results, key=lambda x: x['r2'], reverse=True)
    best_by_r2 = sorted_by_r2[0]
    
    # Sort by AIC (ascending)
    sorted_by_aic = sorted(results, key=lambda x: x['aic'])
    best_by_aic = sorted_by_aic[0]
    
    return best_by_r2, best_by_aic

def plot_results(df, results, best_model):
    """Create visualization of regression results"""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Plot 1: Actual vs Predicted (Best Model)
    ax1 = axes[0, 0]
    ax1.scatter(df['GDP'], best_model['y_pred'], s=100, alpha=0.7, edgecolors='black')
    ax1.plot([df['GDP'].min(), df['GDP'].max()], 
             [df['GDP'].min(), df['GDP'].max()], 'r--', linewidth=2, label='1:1 Line')
    ax1.set_xlabel('Actual GDP (100 million CNY)', fontsize=12)
    ax1.set_ylabel('Predicted GDP (100 million CNY)', fontsize=12)
    ax1.set_title(f"Best Model: {best_model['model_name']}\nR² = {best_model['r2']:.4f}", fontsize=14)
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Time Series Comparison
    ax2 = axes[0, 1]
    ax2.plot(df['year'], df['GDP'], 'bo-', linewidth=2, markersize=8, label='Actual GDP')
    ax2.plot(df['year'], best_model['y_pred'], 'rs--', linewidth=2, markersize=8, label='Predicted GDP')
    ax2.set_xlabel('Year', fontsize=12)
    ax2.set_ylabel('GDP (100 million CNY)', fontsize=12)
    ax2.set_title('GDP Time Series: Actual vs Predicted', fontsize=14)
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(df['year'])
    
    # Plot 3: Model Comparison (R²)
    ax3 = axes[1, 0]
    model_names = [r['model_name'] for r in results]
    r2_values = [r['r2'] for r in results]
    colors = ['green' if r == best_model else 'skyblue' for r in results]
    bars = ax3.bar(model_names, r2_values, color=colors, edgecolor='black')
    ax3.set_ylabel('R²', fontsize=12)
    ax3.set_title('Model Comparison by R²', fontsize=14)
    ax3.set_ylim(0, 1.0)
    for bar, val in zip(bars, r2_values):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, 
                f'{val:.4f}', ha='center', va='bottom', fontsize=10)
    ax3.axhline(y=best_model['r2'], color='red', linestyle='--', linewidth=2, label=f"Best: {best_model['r2']:.4f}")
    ax3.legend()
    ax3.tick_params(axis='x', rotation=45)
    
    # Plot 4: Residuals
    ax4 = axes[1, 1]
    residuals = df['GDP'] - best_model['y_pred']
    ax4.scatter(best_model['y_pred'], residuals, s=100, alpha=0.7, edgecolors='black')
    ax4.axhline(y=0, color='red', linestyle='--', linewidth=2)
    ax4.set_xlabel('Predicted GDP', fontsize=12)
    ax4.set_ylabel('Residuals', fontsize=12)
    ax4.set_title('Residual Plot', fontsize=14)
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'shanghai_gdp_ntl_regression_plots.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plots saved to: {output_path}")
    return output_path

def main():
    print("=" * 60)
    print("Shanghai GDP-NTL Regression Analysis (2013-2022)")
    print("=" * 60)
    
    # Load data
    df = load_data()
    print(f"\nData loaded: {len(df)} years (2013-2022)")
    print(df.to_string(index=False))
    
    X = df['ANTL'].values
    y = df['GDP'].values
    
    # Fit all models
    print("\n" + "=" * 60)
    print("Fitting Regression Models")
    print("=" * 60)
    
    results = []
    results.append(fit_linear_model(X, y))
    results.append(fit_log_linear_model(X, y))
    results.append(fit_quadratic_model(X, y))
    results.append(fit_power_model(X, y))
    results.append(fit_exponential_model(X, y))
    
    # Print model results
    for result in results:
        print(f"\n{result['model_name']}:")
        print(f"  Equation: {result['equation']}")
        print(f"  R² = {result['r2']:.6f}")
        print(f"  RMSE = {result['rmse']:.2f}")
        print(f"  AIC = {result['aic']:.2f}")
        print(f"  BIC = {result['bic']:.2f}")
    
    # Select best model
    best_by_r2, best_by_aic = select_best_model(results)
    
    print("\n" + "=" * 60)
    print("MODEL SELECTION RESULTS")
    print("=" * 60)
    print(f"\nBest by R²: {best_by_r2['model_name']}")
    print(f"  R² = {best_by_r2['r2']:.6f}")
    print(f"  Equation: {best_by_r2['equation']}")
    
    print(f"\nBest by AIC: {best_by_aic['model_name']}")
    print(f"  AIC = {best_by_aic['aic']:.2f}")
    print(f"  Equation: {best_by_aic['equation']}")
    
    # Overall best (prioritize R²)
    overall_best = best_by_r2
    print(f"\n*** FINAL BEST MODEL: {overall_best['model_name']} ***")
    print(f"Equation: {overall_best['equation']}")
    print(f"R² = {overall_best['r2']:.6f}")
    print(f"RMSE = {overall_best['rmse']:.2f}")
    
    # Generate plots
    plot_path = plot_results(df, results, overall_best)
    
    # Save results to JSON
    summary = {
        'analysis_period': '2013-2022',
        'region': 'Shanghai Municipality',
        'data_points': len(df),
        'models_compared': [r['model_name'] for r in results],
        'best_model': {
            'name': overall_best['model_name'],
            'equation': overall_best['equation'],
            'r2': overall_best['r2'],
            'rmse': overall_best['rmse'],
            'aic': overall_best['aic'],
            'bic': overall_best['bic']
        },
        'all_models': [
            {
                'name': r['model_name'],
                'equation': r['equation'],
                'r2': r['r2'],
                'rmse': r['rmse'],
                'aic': r['aic'],
                'bic': r['bic']
            }
            for r in results
        ]
    }
    
    json_path = os.path.join(OUTPUT_DIR, 'shanghai_gdp_ntl_regression_summary.json')
    with open(json_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"\nResults saved to: {json_path}")
    
    # Save detailed CSV
    results_df = pd.DataFrame([
        {
            'Model': r['model_name'],
            'R2': r['r2'],
            'RMSE': r['rmse'],
            'AIC': r['aic'],
            'BIC': r['bic'],
            'Equation': r['equation']
        }
        for r in results
    ])
    results_df = results_df.sort_values('R2', ascending=False)
    csv_path = os.path.join(OUTPUT_DIR, 'shanghai_regression_models_comparison.csv')
    results_df.to_csv(csv_path, index=False)
    print(f"Model comparison saved to: {csv_path}")
    
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    
    return summary

if __name__ == "__main__":
    main()

