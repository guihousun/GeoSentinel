---
name: thematic-modeling
description: Execute assigned urban, classification, regression, indicator, and other thematic nighttime-light models with reproducible validation.
---

# Thematic Modeling

- Freeze the response, predictors, analysis unit, train/test scope, threshold or model, seed, and requested outputs before fitting.
- Use accepted features and observations; do not silently alter the product, target, training population, or scientific question.
- For a named sensor-specific index, cited threshold/classification, or other declared method, inspect the matching allowlisted dedicated tool first. When it implements the requested semantics, use its documented formula, threshold, reducer, and units as the primary method; a generic script may add an output or validation step but may not substitute an alternative formula, threshold scan, or proxy.
- When a task declares a model-selection rule, apply that rule exactly. For a minimum-RMSE rule, choose the valid model with the lowest RMSE; break an exact RMSE tie by higher R2 and then the declared model order. A small but nonzero metric difference is not a tie: unless the assignment declares a tolerance, every lower finite RMSE wins. A weak advantage is a limitation to report, not grounds to select another model.
- When an exponential model is required on the original response scale, fit `y = a * exp(b*x)` directly by nonlinear least squares and calculate its R2/RMSE from original-scale predictions. Do not replace that with log-linear OLS unless the assignment explicitly asks for log-linear estimation.
- Validate input ranges, leakage risks, class balance or residual behavior, applicable domain, and output completeness.
- Report uncertainty and limitations with the actual model artifacts and parameters.
- A historical target count or expected headline is not a tuning objective unless the accepted contract explicitly defines it.

