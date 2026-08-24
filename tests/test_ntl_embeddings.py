from __future__ import annotations

import os
import unittest

from utils.ntl_embeddings import build_embedding_config


class NTLEmbeddingConfigTests(unittest.TestCase):
    def tearDown(self) -> None:
        for name in (
            "OPENAI_API_KEY",
            "DASHSCOPE_API_KEY",
            "DASHSCOPE_Qwen_plus_KEY",
            "NTL_EMBEDDING_API_KEY",
            "NTL_EMBEDDING_BASE_URL",
            "NTL_EMBEDDING_DIMENSIONS",
            "NTL_EMBEDDING_MODEL",
            "NTL_EMBEDDING_PROVIDER",
        ):
            os.environ.pop(name, None)

    def test_dashscope_is_default_embedding_provider(self) -> None:
        os.environ.pop("OPENAI_API_KEY", None)
        os.environ["DASHSCOPE_API_KEY"] = "dashscope-coding-key"
        os.environ["DASHSCOPE_Qwen_plus_KEY"] = "dashscope-qwen-plus-key"

        config = build_embedding_config()

        self.assertEqual(config.provider, "dashscope")
        self.assertEqual(config.api_key, "dashscope-qwen-plus-key")
        self.assertEqual(config.model, "text-embedding-v4")
        self.assertEqual(config.base_url, "https://dashscope.aliyuncs.com/compatible-mode/v1")
        self.assertEqual(config.dimensions, 1024)

    def test_embedding_api_key_overrides_dashscope_qwen_plus_key(self) -> None:
        os.environ["DASHSCOPE_Qwen_plus_KEY"] = "dashscope-qwen-plus-key"
        os.environ["NTL_EMBEDDING_API_KEY"] = "embedding-override-key"

        config = build_embedding_config()

        self.assertEqual(config.provider, "dashscope")
        self.assertEqual(config.api_key, "embedding-override-key")

    def test_openai_provider_still_available_when_explicitly_selected(self) -> None:
        os.environ["NTL_EMBEDDING_PROVIDER"] = "openai"
        os.environ["OPENAI_API_KEY"] = "openai-test-key"

        config = build_embedding_config()

        self.assertEqual(config.provider, "openai")
        self.assertEqual(config.api_key, "openai-test-key")
        self.assertIsNone(config.base_url)
        self.assertEqual(config.model, "text-embedding-3-small")
        self.assertIsNone(config.dimensions)


if __name__ == "__main__":
    unittest.main()
