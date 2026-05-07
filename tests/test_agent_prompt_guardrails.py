import unittest

from agents.NTL_Engineer import system_prompt_text
from agents.NTL_Code_Assistant import Code_Assistant_system_prompt_text


class AgentPromptGuardrailTests(unittest.TestCase):
    def test_official_census_requests_cannot_use_population_proxy_by_default(self):
        prompt = system_prompt_text.content

        self.assertIn("OFFICIAL CENSUS DATA GUARDRAIL", prompt)
        self.assertIn("LandScan/WorldPop/GPW", prompt)
        self.assertIn("must not be used as substitutes", prompt)

    def test_self_evolution_command_must_not_repeat_previous_task_answer(self):
        prompt = system_prompt_text.content

        self.assertIn("SELF-EVOLUTION COMMAND HANDLING", prompt)
        self.assertIn("Never answer by repeating the previous analytical result", prompt)

    def test_china_34_province_ntl_contract_requires_complete_regions_and_b1(self):
        engineer_prompt = system_prompt_text.content
        code_prompt = Code_Assistant_system_prompt_text.content

        self.assertIn("CHINA 34 PROVINCE-LEVEL NTL STATISTICS GUARDRAIL", engineer_prompt)
        self.assertIn("projects/sat-io/open-datasets/npp-viirs-ntl", engineer_prompt)
        self.assertIn("band `b1`", engineer_prompt)
        self.assertIn("exactly 34 rows", engineer_prompt)
        self.assertIn("Taiwan", engineer_prompt)
        self.assertIn("Hong Kong", engineer_prompt)
        self.assertIn("Macau", engineer_prompt)

        self.assertIn("CHINA 34 PROVINCE-LEVEL EXECUTION GUARDRAIL", code_prompt)
        self.assertIn("feature.get('mean')", code_prompt)
        self.assertIn("never feature.get('b1_mean')", code_prompt)
        self.assertIn("0 regions", code_prompt)


if __name__ == "__main__":
    unittest.main()
