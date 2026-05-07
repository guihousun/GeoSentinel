import unittest
import sys
import types

from langchain_core.messages import AIMessage, HumanMessage

if "app_agents" not in sys.modules:
    stub = types.ModuleType("app_agents")
    stub.get_ntl_graph = lambda *args, **kwargs: None
    sys.modules["app_agents"] = stub
if "file_context_service" not in sys.modules:
    stub = types.ModuleType("file_context_service")
    stub.extract_file_contexts = lambda *args, **kwargs: []
    sys.modules["file_context_service"] = stub

import app_logic


class AppLogicFinalAnswerTests(unittest.TestCase):
    def test_state_fallback_ignores_messages_seen_before_run(self):
        old_answer = AIMessage(content="old terrain answer", name="Engineer")
        new_user = HumanMessage(content="please self-evolve")
        initial_seen = {app_logic._message_fingerprint(old_answer)}

        messages = app_logic._messages_not_seen_before_run([old_answer, new_user], initial_seen)

        self.assertEqual(messages, [new_user])
        self.assertIsNone(app_logic._extract_meaningful_ai_text(messages, preferred_agents=["Engineer"]))

    def test_state_fallback_can_use_new_engineer_answer(self):
        old_answer = AIMessage(content="old terrain answer", name="Engineer")
        new_answer = AIMessage(content="evolution completed", name="Engineer")
        initial_seen = {app_logic._message_fingerprint(old_answer)}

        messages = app_logic._messages_not_seen_before_run([old_answer, new_answer], initial_seen)

        self.assertEqual(messages, [new_answer])
        self.assertEqual(
            app_logic._extract_meaningful_ai_text(messages, preferred_agents=["Engineer"]),
            "evolution completed",
        )


if __name__ == "__main__":
    unittest.main()
