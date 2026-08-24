import ee
import geopandas as gpd
import pandas as pd
import numpy as np
from scipy import stats
from storage_manager import storage_manager

PROJECT_ID = "empyrean-caster-430308-m2"
ee.Initialize(project=PROJECT_ID)

# Load Shanghai boundary
boundary_path = storage_manager.resolve_input_path('shanghai_boundary.shp')
gdf = gpd.read_file(boundary_path)
union_geom = gdf.union_all()
region = ee.Geometry(union_geom.__geo_interface__)

# Load NPP-VIIRS-Like annual collection (2000-2020)
collection = (
    ee.ImageCollection("projects/sat-io/open-datasets/npp-viirs-ntl")
    .filterDate("2000-01-01", "2020-12-31")
    .filterBounds(region)
    .select("b1")
)

# Extract annual mean NTL values using server-side processing
def per_image_stat(img):
    value = img.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=region,
        scale=500,
        maxPixels=1e13,
        bestEffort=True,
    ).get("b1")
    return ee.Feature(None, {
        "year": img.date().format("YYYY"),
        "date": img.date().format("YYYY-MM-dd"),
        "ntl_mean": value,
    })

stats_fc = ee.FeatureCollection(collection.map(per_image_stat))
rows = [f["properties"] for f in stats_fc.getInfo()["features"]]
df = pd.DataFrame(rows).sort_values("year")
df["year"] = df["year"].astype(int)
df["ntl_mean"] = pd.to_numeric(df["ntl_mean"])

print(f"Extracted {len(df)} annual NTL values for Shanghai (2000-2020)")
print(df.head())

# Mann-Kendall trend test
def mann_kendall_test(y):
    """
    Perform Mann-Kendall trend test.
    Returns: tau (Kendall's tau), p_value, trend direction
    """
    n = len(y)
    if n < 2:
        return np.nan, np.nan, "insufficient data"
    
    # Calculate S statistic
    s = 0
    for i in range(n - 1):
        for j in range(i + 1, n):
            diff = y[j] - y[i]
            if diff > 0:
                s += 1
            elif diff < 0:
                s -= 1
    
    # Calculate variance of S (accounting for ties)
    unique, counts = np.unique(y, return_counts=True)
    tie_sum = np.sum(counts * (counts - 1) * (2 * counts + 5))
    var_s = (n * (n - 1) * (2 * n + 5) - tie_sum) / 18.0
    
    # Calculate Z statistic
    if s > 0:
        z = (s - 1) / np.sqrt(var_s)
    elif s < 0:
        z = (s + 1) / np.sqrt(var_s)
    else:
        z = 0
    
    # Two-tailed p-value
    p_value = 2 * (1 - stats.norm.cdf(abs(z)))
    
    # Kendall's tau
    tau = s / (n * (n - 1) / 2)
    
    # Trend direction
    if p_value < 0.05:
        if s > 0:
            trend = "significant increasing"
        else:
            trend = "significant decreasing"
    else:
        trend = "no significant trend"
    
    return tau, p_value, trend

# Sen's slope estimator
def sens_slope(y, x=None):
    """
    Calculate Sen's slope estimator.
    Returns: slope, intercept, confidence interval (95%)
    """
    n = len(y)
    if x is None:
        x = np.arange(n)
    
    # Calculate all pairwise slopes
    slopes = []
    for i in range(n - 1):
        for j in range(i + 1, n):
            if x[j] != x[i]:
                slope = (y[j] - y[i]) / (x[j] - x[i])
                slopes.append(slope)
    
    if len(slopes) == 0:
        return np.nan, np.nan, np.nan, np.nan
    
    slopes = np.array(slopes)
    slope_median = np.median(slopes)
    
    # Calculate intercept
    intercepts = y - slope_median * x
    intercept_median = np.median(intercepts)
    
    # 95% confidence interval for slope
    slope_ci_lower = np.percentile(slopes, 2.5)
    slope_ci_upper = np.percentile(slopes, 97.5)
    
    return slope_median, intercept_median, slope_ci_lower, slope_ci_upper

# Perform trend analysis
ntl_values = df["ntl_mean"].values
years = df["year"].values

tau, p_value, trend_direction = mann_kendall_test(ntl_values)
slope, intercept, slope_ci_lower, slope_ci_upper = sens_slope(ntl_values, years)

print(f"\n=== Trend Analysis Results for Shanghai (2000-2020) ===")
print(f"Mann-Kendall Tau: {tau:.4f}")
print(f"P-value: {p_value:.4f}")
print(f"Trend: {trend_direction}")
print(f"Sen's Slope: {slope:.4f} (NTL units/year)")
print(f"95% CI: [{slope_ci_lower:.4f}, {slope_ci_upper:.4f}]")
print(f"Intercept: {intercept:.4f}")

# Prepare output dataframe
trend_results = {
    "region": "Shanghai",
    "start_year": int(years.min()),
    "end_year": int(years.max()),
    "n_years": len(years),
    "mean_ntl": float(ntl_values.mean()),
    "std_ntl": float(ntl_values.std()),
    "min_ntl": float(ntl_values.min()),
    "max_ntl": float(ntl_values.max()),
    "mann_kendall_tau": float(tau),
    "p_value": float(p_value),
    "trend_direction": trend_direction,
    "sens_slope": float(slope),
    "sens_slope_ci_lower": float(slope_ci_lower),
    "sens_slope_ci_upper": float(slope_ci_upper),
    "intercept": float(intercept),
}

# Save annual NTL values
annual_csv = storage_manager.resolve_output_path('shanghai_ntl_annual_2000_2020.csv')
df.to_csv(annual_csv, index=False)
print(f"\nAnnual NTL values saved to: {annual_csv}")

# Save trend analysis results
trend_csv = storage_manager.resolve_output_path('shanghai_ntl_trend_results.csv')
pd.DataFrame([trend_results]).to_csv(trend_csv, index=False)
print(f"Trend analysis results saved to: {trend_csv}")

print("\n=== Analysis Complete ===")
