from __future__ import annotations

import pytest

import model_config


def test_frontend_model_options_only_expose_deepseek_v4() -> None:
    assert model_config.MODEL_OPTIONS == ["deepseek-v4-flash", "deepseek-v4-pro"]


@pytest.mark.parametrize("model_name", model_config.MODEL_OPTIONS)
def test_deepseek_models_use_project_env_channel(model_name: str) -> None:
    config = model_config.get_model_config(model_name)

    assert config.provider == "deepseek"
    assert config.api_model == model_name
    assert config.api_key_env == "DeepSeek_API_KEY"
    assert config.base_url_env == "DeepSeek_Coding_URL"
    assert config.uses_env_api_key is True


@pytest.mark.parametrize("model_name", ["qwen3.6-plus", "MiniMax-M2.7", "GPT-5.4"])
def test_removed_frontend_models_are_rejected(model_name: str) -> None:
    with pytest.raises(ValueError, match="Unsupported frontend model"):
        model_config.get_model_config(model_name)
