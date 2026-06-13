# -*- coding: utf-8 -*-
"""Verify that intraday-monitor.yml and 00-daily-analysis.yml share the same
core LLM environment variables so that model resolution, channel config, and
fallback behavior are consistent across both workflows.

Any variable present in the daily analysis workflow but missing from the
intraday workflow (or vice versa) must be listed in the allowed-differences
sets; otherwise the test fails to prevent silent drift.
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
WORKFLOW_DIR = ROOT_DIR / ".github/workflows"
DAILY_PATH = WORKFLOW_DIR / "00-daily-analysis.yml"
INTRADAY_PATH = WORKFLOW_DIR / "intraday-monitor.yml"

# --- Channel list from the provider templates (mirrors llmProviderTemplates.ts) ---
CORE_CHANNELS = (
    "GEMINI",
    "DEEPSEEK",
    "AIHUBMIX",
    "ANSPIRE",
    "OPENAI",
    "ANTHROPIC",
    "MOONSHOT",
    "DASHSCOPE",
    "ZHIPU",
    "MINIMAX",
    "VOLCENGINE",
    "SILICONFLOW",
    "OPENROUTER",
    "OLLAMA",
)

CHANNEL_SUFFIXES = (
    "PROTOCOL",
    "BASE_URL",
    "API_KEY",
    "API_KEYS",
    "MODELS",
    "ENABLED",
    "EXTRA_HEADERS",
)

# Top-level LLM env vars expected in BOTH workflows
CORE_TOP_LEVEL_VARS = frozenset({
    "LITELLM_CONFIG",
    "LITELLM_CONFIG_YAML",
    "LITELLM_API_KEY",
    "LITELLM_MODEL",
    "LITELLM_FALLBACK_MODELS",
    "LLM_TEMPERATURE",
    "LLM_CHANNELS",
    "OPENAI_MODEL",
    "ANTHROPIC_MODEL",
    "GEMINI_MODEL_FALLBACK",
    "GEMINI_REQUEST_DELAY",
})

# Channel-scoped vars expected in BOTH workflows
CORE_CHANNEL_VARS = frozenset({
    f"LLM_{ch}_{suf}" for ch in CORE_CHANNELS for suf in CHANNEL_SUFFIXES
})

CORE_PRIMARY_SECONDARY_VARS = frozenset({
    f"LLM_{role}_{suf}"
    for role in ("PRIMARY", "SECONDARY")
    for suf in CHANNEL_SUFFIXES
})

# All core vars combined
CORE_VARS = CORE_TOP_LEVEL_VARS | CORE_CHANNEL_VARS | CORE_PRIMARY_SECONDARY_VARS

# --- Allowed differences ---
# Vars that MAY legitimately differ between the two workflows.

INTRADAY_ONLY_ALLOWED = frozenset({
    # Legacy single-provider env vars; intraday keeps them for the
    # IntradayMonitor path (these are absent from the daily workflow env
    # because the daily workflow relies entirely on the LLM_*_* channel vars).
    "GEMINI_API_KEY",
    "GEMINI_API_KEYS",
    "GEMINI_MODEL",
    "AIHUBMIX_KEY",
    "OPENAI_API_KEY",
    "OPENAI_API_KEYS",
    "OPENAI_BASE_URL",
    "DEEPSEEK_API_KEY",
    "DEEPSEEK_API_KEYS",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_API_KEYS",
})

DAILY_ONLY_ALLOWED = frozenset({
    # Agent-specific model override (not relevant for intraday LLM calls)
    "AGENT_LITELLM_MODEL",
    # Anspire LLM / provider config (used by daily analysis agent flows only)
    "ANSPIRE_LLM_MODEL",
    "ANSPIRE_LLM_BASE_URL",
    "ANSPIRE_LLM_ENABLED",
    # Report cosmetic toggle
    "REPORT_SHOW_LLM_MODEL",
    # Vision model env (intraday doesn't do image analysis)
    "VISION_MODEL",
    "OPENAI_VISION_MODEL",
    # Agent-first-page model (daily-only)
    "FIRST_PAGE_MODEL",
})


def _extract_env_keys(workflow_path: Path, step_name: str) -> set[str]:
    """Return the set of env var names in the named step's `env` block."""
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    jobs = workflow.get("jobs", {})
    # Try all jobs
    steps = []
    for _job_name, job_def in jobs.items():
        if not isinstance(job_def, dict):
            continue
        steps = job_def.get("steps", [])
        break  # Only first job

    step = next((s for s in steps if s.get("name") == step_name), None)
    available_names = [s.get("name", "<unnamed>") for s in steps]
    assert step is not None, (
        f"Step '{step_name}' not found in {workflow_path.name}; "
        f"available: {available_names}"
    )
    return set(step.get("env", {}).keys())


def _load_daily_env() -> set[str]:
    return _extract_env_keys(DAILY_PATH, "执行股票分析")


def _load_intraday_env() -> set[str]:
    return _extract_env_keys(INTRADAY_PATH, "执行盘中任务")


class TestIntradayWorkflowLLMEnvConsistency:
    """Assert core LLM env vars are present in both workflows."""

    @classmethod
    def setup_class(cls) -> None:
        cls.daily = _load_daily_env()
        cls.intraday = _load_intraday_env()

    def test_core_vars_present_in_both(self) -> None:
        """Every core LLM var must exist in BOTH workflow env blocks."""
        # OLLAMA API_KEY/API_KEYS are optional (local-only provider)
        core_required = CORE_VARS - {"LLM_OLLAMA_API_KEY", "LLM_OLLAMA_API_KEYS"}

        missing_in_daily = core_required - self.daily
        missing_in_intraday = core_required - self.intraday

        assert not missing_in_daily, (
            f"Core LLM vars missing from 00-daily-analysis.yml: {sorted(missing_in_daily)}"
        )
        assert not missing_in_intraday, (
            f"Core LLM vars missing from intraday-monitor.yml: {sorted(missing_in_intraday)}"
        )

    def test_no_unexpected_extra_llm_vars_in_daily(self) -> None:
        """Daily workflow should not have LLM_* vars beyond core + allowed set."""
        llm_vars = {k for k in self.daily if k.startswith("LLM_")}
        allowed = CORE_VARS | DAILY_ONLY_ALLOWED
        unexpected = llm_vars - allowed
        assert not unexpected, (
            f"Unexpected LLM_* vars in 00-daily-analysis.yml (not in core set or allowed list): "
            f"{sorted(unexpected)}"
        )

    def test_no_unexpected_extra_llm_vars_in_intraday(self) -> None:
        """Intraday workflow should not have LLM_* vars beyond core + allowed set."""
        llm_vars = {k for k in self.intraday if k.startswith("LLM_")}
        allowed = CORE_VARS | INTRADAY_ONLY_ALLOWED
        unexpected = llm_vars - allowed
        assert not unexpected, (
            f"Unexpected LLM_* vars in intraday-monitor.yml (not in core set or allowed list): "
            f"{sorted(unexpected)}"
        )

    def test_shared_llm_vars_are_superset_of_core(self) -> None:
        """The intersection of both workflows' LLM_* vars must include all core vars
        (minus OLLAMA API_KEY/API_KEYS which are optional)."""
        shared = self.daily & self.intraday
        needed = {k for k in CORE_VARS if k.startswith("LLM_")}
        needed.discard("LLM_OLLAMA_API_KEY")
        needed.discard("LLM_OLLAMA_API_KEYS")
        missing = needed - shared
        assert not missing, (
            f"LLM vars in core set but NOT shared by both workflows: {sorted(missing)}"
        )
