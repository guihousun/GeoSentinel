from __future__ import annotations

import unittest


class StorageGEEContextTests(unittest.TestCase):
    def test_gee_context_vars_are_exported_with_empty_defaults(self) -> None:
        from storage_manager import (
            current_gee_encrypted_refresh_token,
            current_gee_project_id,
            current_gee_token_scopes,
        )

        self.assertEqual(current_gee_project_id.get(), "")
        self.assertEqual(current_gee_encrypted_refresh_token.get(), "")
        self.assertEqual(current_gee_token_scopes.get(), "")


if __name__ == "__main__":
    unittest.main()
