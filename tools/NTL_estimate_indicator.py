import os
import pickle
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional
from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field
from storage_manager import storage_manager

# 1. 定义输入 Schema (保持不变)
class NTL_Estimate_Indicator_Input(BaseModel):
    tntl: float = Field(..., description="Total Nighttime Light (TNTL) value for the province (e.g., 12345.67)")
    indicator: str = Field(..., description="Indicator to estimate: 'GDP', 'electric power consumption', 'population', 'CO2 emissions' (case-insensitive)")
    province: Optional[str] = Field(None, description="Province/municipality name in Chinese (e.g., '上海市', '浙江省'). Required for CO2 estimation.")

# 2. 定义核心函数 (已修复 Warning)
def NTL_estimate_indicator_provincial(tntl: float, indicator: str, province: Optional[str] = None):
    """
    Estimate a provincial indicator from TNTL using the corresponding pre-trained model.
    """
    
    # --- 1. 路径配置 ---
    current_dir = Path(os.getcwd())
    # 尝试多种路径策略以适应不同的运行环境
    possible_paths = [
        # Preferred: unified shared base from storage manager (stable across E:/D: copies)
        storage_manager.shared_dir / "Model",
        # Backward-compatible fallbacks
        current_dir / "base_data" / "Model",
        Path(__file__).parent.parent / "base_data" / "Model",
    ]
    
    model_dir = None
    for p in possible_paths:
        if p.exists():
            model_dir = p
            break
            
    if model_dir is None:
        return {"error": True, "message": "Critical Error: Model directory not found. Please check project structure."}

    # --- 2. 指标映射 ---
    norm = str(indicator).strip().lower()
    alias_map = {
        'gdp': 'gdp', 'gross domestic product': 'gdp',
        'electric power consumption': 'electric_power_consumption', 'epc': 'electric_power_consumption',
        'population': 'population', 'population_count': 'population',
        'co2': 'co2', 'co2 emissions': 'co2', 'co2 emission': 'co2'
    }

    canonical = alias_map.get(norm)
    if canonical is None:
        return {"error": True, "message": f"Unsupported indicator '{indicator}'."}

    # --- 3. 模型加载 ---
    model_files = {
        'gdp': 'GDP_TNTL_best_model.pkl',
        'electric_power_consumption': 'EPC_TNTL_best_model.pkl',
        'population': 'population_TNTL_best_model.pkl',
        'co2': 'CO2_TNTL_best_model.pkl'
    }
    unit_map = {
        'gdp': '100 million CNY', 'electric_power_consumption': '10^8 kWh', 
        'population': '10^4 people', 'co2': '10^6 tons'
    }

    target_file = model_files.get(canonical)
    model_path = model_dir / target_file

    if canonical == 'co2' and (province is None or str(province).strip() == ''):
        return {"error": True, "message": "CO₂ model requires a 'province' argument."}

    try:
        with open(model_path, 'rb') as f:
            saved_obj = pickle.load(f)
    except Exception as e:
        return {"error": True, "message": f"Error loading model: {str(e)}"}

    # 提取模型
    model = saved_obj
    scaler = None
    poly = None
    if isinstance(saved_obj, dict):
        model = saved_obj.get('model') or saved_obj.get('pipeline') or saved_obj.get('estimator') or saved_obj.get('best_estimator_')
        scaler = saved_obj.get('scaler')
        poly = saved_obj.get('poly') or saved_obj.get('poly_transformer')

    # --- 4. 执行预测 (核心修改部分) ---
    try:
        predicted = None
        
        # 场景 A: CO2 模型
        if canonical == 'co2':
            # 构建 DataFrame 保证列顺序正确
            input_df = pd.DataFrame({'Province': [province], 'TNTL': [float(tntl)]})
            
            # [关键修改] 使用 .values 转换为 numpy array
            # 这样 sklearn 就不会抱怨 "X has feature names but StandardScaler was fitted without feature names"
            input_array = input_df.values 
            
            try:
                predicted = model.predict(input_array)[0]
            except Exception:
                # 备用：如果 pipeline 里有步骤强制依赖列名（极少见），则回退到 DataFrame
                predicted = model.predict(input_df)[0]

        # 场景 B: 其他数值模型
        else:
            X_num = np.array([[float(tntl)]], dtype=float)
            if scaler is not None:
                X_in = scaler.transform(X_num)
                if poly is not None:
                    X_in = poly.transform(X_in)
                predicted = model.predict(X_in)[0]
            else:
                # 兼容可能的 Pipeline 输入要求
                try:
                    predicted = model.predict(X_num)[0]
                except:
                    # 如果必须要有列名（虽然这与你的warning相反，但作为防御性编程）
                    predicted = model.predict(pd.DataFrame({'TNTL': [float(tntl)]}))[0]

    except Exception as e:
        return {"error": True, "message": f"Prediction failed: {str(e)}"}

    # --- 5. 返回结果 ---
    unit = unit_map.get(canonical, '')
    loc_str = f" in {province}" if province else ""
    
    return {
        "error": False,
        "indicator": canonical,
        "tntl": float(tntl),
        "province": province,
        "predicted_value": float(predicted),
        "unit": unit,
        "message": f"Estimated {canonical.upper()}{loc_str} based on TNTL {tntl:.2f}: {predicted:,.2f} {unit}"
    }

# 3. 注册工具
NTL_estimate_indicator_provincial_tool = StructuredTool.from_function(
    func=NTL_estimate_indicator_provincial,
    name="NTL_Estimate_Indicator_Provincial",
    description="Estimate provincial indicators (GDP, EPC, Population, CO2) using TNTL. For CO2, 'province' is required.",
    args_schema=NTL_Estimate_Indicator_Input,
)

import os
import pickle
import numpy as np
from typing import Optional
from langchain_core.tools import StructuredTool
from pydantic.v1 import BaseModel, Field

class DEI_Estimate_City_Input(BaseModel):
    antl: float = Field(..., description="Annual Nighttime Light (ANTL) value for the city")
    year: Optional[int] = Field(None, description="Year of estimation, must be between 2017 and 2024 inclusive")

def DEI_estimate_city(antl: float, year: Optional[int] = None):
    """
    Estimate city-level DEI from ANTL (optionally for a given year).
    Supports two saved formats:
      1) a single saved model file (e.g. base_data/best_model_dei_city.pkl) containing either:
         - a model/pipeline object, or
         - a dict {'model': model, 'scaler': scaler, 'poly': poly (optional)}
      2) a yearly models file (e.g. base_data/yearly_dei_models.pkl) containing a dict:
         { year_int: {'model': model, 'scaler': scaler, ...}, ... }
    If a yearly models dict is found and year is not provided, the function will choose the latest year available.
    Returns a structured dict with prediction or an informative error message.
    """
    # Candidate paths (adjust if your files live elsewhere)
    yearly_candidates = [
        storage_manager.shared_dir / "Model" / "yearly_dei_models.pkl",
        Path("base_data/Model/yearly_dei_models.pkl"),
    ]
    yearly_path = next((str(p) for p in yearly_candidates if Path(p).exists()), str(yearly_candidates[0]))

    saved = None
    used_path = None

    # Prefer yearly models if exists
    if os.path.exists(yearly_path):
        used_path = yearly_path
        try:
            with open(yearly_path, 'rb') as f:
                saved = pickle.load(f)
        except Exception as e:
            return {"error": True, "message": f"Error loading yearly models file '{yearly_path}': {e}"}
    else:
        return {"error": True, "message": f"Yearly models file '{yearly_path}' not found. Place your model file in 'base_data/Model/'."}

    # If saved appears to be a yearly dict (keys are years)
    model = None
    scaler = None
    poly = None

    if isinstance(saved, dict):
        # Check whether this dict looks like a yearly dictionary (keys are years mapping to model dicts)
        keys = list(saved.keys())
        year_like = False
        if keys:
            try:
                int(keys[0])
                year_like = True
            except Exception:
                year_like = False

        if year_like:
            # saved is a mapping year->model_info
            yearly_models = {}
            for k, v in saved.items():
                try:
                    ky = int(k)
                except Exception:
                    continue
                yearly_models[ky] = v

            if not yearly_models:
                return {"error": True, "message": f"Yearly models file '{used_path}' contains no integer year keys."}

            # choose year
            if year is None:
                selected_year = max(yearly_models.keys())
            else:
                selected_year = int(year)
                if selected_year not in yearly_models:
                    available = sorted(yearly_models.keys())
                    return {
                        "error": True,
                        "message": f"Requested year {selected_year} not available in yearly models. Available years: {available}."
                    }

            entry = yearly_models[selected_year]
            # entry may itself be a dict like {'model':..., 'scaler':...} or may be the model directly
            if isinstance(entry, dict):
                model = entry.get('model') or entry.get('pipeline') or entry.get('estimator')
                scaler = entry.get('scaler')
                poly = entry.get('poly') or entry.get('poly_transformer')
                if model is None and len(entry) == 1:
                    model = list(entry.values())[0]
            else:
                model = entry
        else:
            # Not a yearly dict: assume a single model saved as dict with 'model' key
            model = saved.get('model') or saved.get('pipeline') or saved.get('estimator')
            scaler = saved.get('scaler')
            poly = saved.get('poly') or saved.get('poly_transformer')
            if model is None and hasattr(saved, 'predict'):
                model = saved
    else:
        # saved is not a dict, assume it's a model/pipeline directly
        if hasattr(saved, 'predict') or hasattr(saved, 'transform'):
            model = saved

    if model is None:
        return {"error": True, "message": f"Loaded object from '{used_path}' does not contain a usable model."}

    # Prepare inputs
    X_antl = np.array([[float(antl)]], dtype=float)
    X_antl_year = None
    if year is not None:
        X_antl_year = np.array([[float(antl), float(year)]], dtype=float)

    # Helper to try prediction and capture errors
    def try_predict(inp):
        pred = model.predict(inp)
        if hasattr(pred, '__len__'):
            return float(pred[0])
        else:
            return float(pred)

    debug_errors = []
    predicted = None

    # 1) If model has n_features_in_ use it
    try:
        if hasattr(model, 'n_features_in_'):
            n_in = int(getattr(model, 'n_features_in_'))
            if n_in == 1:
                try:
                    if scaler is not None:
                        X_scaled = scaler.transform(X_antl)
                        if poly is not None:
                            X_scaled = poly.transform(X_scaled)
                        predicted = try_predict(X_scaled)
                    else:
                        predicted = try_predict(X_antl)
                except Exception as e:
                    debug_errors.append(f"n_in==1 attempt failed: {e}")
            elif n_in == 2:
                if X_antl_year is None:
                    return {"error": True, "message": "Model expects two features (likely ANTL and year). Please provide 'year'."}
                try:
                    if scaler is not None and not hasattr(model, 'named_steps'):
                        try:
                            scaled_antl = scaler.transform(np.array([[float(antl)]]))
                            X_comb = np.hstack([scaled_antl, np.array([[float(year)]])])
                            if poly is not None:
                                X_comb = poly.transform(X_comb)
                            predicted = try_predict(X_comb)
                        except Exception as inner_e:
                            debug_errors.append(f"n_in==2 scaler path failed: {inner_e}")
                            predicted = try_predict(X_antl_year)
                    else:
                        predicted = try_predict(X_antl_year)
                except Exception as e:
                    debug_errors.append(f"n_in==2 attempt failed: {e}")
            else:
                debug_errors.append(f"Model expects {n_in} features, falling back to pipeline/DataFrame attempts.")
    except Exception as e:
        debug_errors.append(f"n_features_in_ check error: {e}")

    # 2) If model is a pipeline, try DataFrame approach
    if predicted is None:
        try:
            if hasattr(model, 'named_steps') or 'pipeline' in str(type(model)).lower():
                try:
                    import pandas as pd
                    df_try = pd.DataFrame({
                        'ANTL': [float(antl)],
                        'antl': [float(antl)],
                        'year': [int(year)] if year is not None else [np.nan]
                    })
                    predicted = try_predict(df_try)
                except Exception as e:
                    debug_errors.append(f"pipeline DataFrame attempt failed: {e}")
        except Exception as e:
            debug_errors.append(f"pipeline check error: {e}")

    # 3) Fallback numeric attempts with scaler/poly
    if predicted is None:
        try:
            if scaler is not None and poly is None:
                X_scaled = scaler.transform(X_antl)
                predicted = try_predict(X_scaled)
            elif scaler is not None and poly is not None:
                X_scaled = scaler.transform(X_antl)
                X_poly = poly.transform(X_scaled)
                predicted = try_predict(X_poly)
            else:
                predicted = try_predict(X_antl)
        except Exception as e:
            debug_errors.append(f"numeric fallback failed: {e}")

    # 4) If year provided and still not predicted, try direct [antl, year]
    if predicted is None and X_antl_year is not None:
        try:
            predicted = try_predict(X_antl_year)
        except Exception as e:
            debug_errors.append(f"antl+year direct attempt failed: {e}")

    if predicted is None:
        return {"error": True, "message": "All prediction attempts failed. Debug hints: " + "; ".join(debug_errors[:8])}

    return {
        "error": False,
        "antl": float(antl),
        "year": int(year) if year is not None else None,
        "predicted_dei": float(predicted),
        "message": f"Estimated DEI for ANTL {antl:.4f}" + (f", year {year}" if year is not None else "") + f": {predicted:.4f}"
    }

# Tool Definition
DEI_estimate_city_tool = StructuredTool.from_function(
    func=DEI_estimate_city,
    name="DEI_Estimate_City",
    description=(
        "Estimate city-level Digital Economy Indicator (DEI) from ANTL. "
        "The 'year' parameter is REQUIRED and MUST be an integer between 2017 and 2024 (inclusive). "
        "Only years 2017–2024 are supported due to model availability."
    ),
    args_schema=DEI_Estimate_City_Input,
)

# Option A: call the underlying function directly (if function is in scope)
# result = DEI_estimate_city(antl=0.1894242893764177, year=2023)
# print(result)

# Option B: call through the StructuredTool object (if tool object is in scope)
# result2 = DEI_estimate_city_tool.func(antl=0.1894242893764177, year=2023)
# print(result2)

# CO2 usually requires province argument (depending on how the model was trained)
# res_co2 = NTL_estimate_indicator_provincial(tntl=23456.78, indicator='CO2', province='上海市')
# print(res_co2)

# via tool object
# res_co2b = NTL_estimate_indicator_provincial_tool.func(tntl=23456.78, indicator='CO2', province='上海市')
# print(res_co2b)
