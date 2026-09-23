"""Agent-owned reasoning budget for z.ai GLM-5 via the door (task 7874f4f9).

Main-turn ``build_api_kwargs`` injects ``extra_body.thinking = {type: enabled,
budget_tokens: int(max_tokens * budget_fraction)}`` when the ``reasoning:``
config flag is ON, the model is glm-5*, and base_url is the z.ai door. Flag OFF
= byte-identical payload. Explicit thinking always wins. Aux paths untouched.
"""

from __future__ import annotations

from unittest.mock import patch

from agent.chat_completion_helpers import build_api_kwargs
from agent.reasoning_budget import apply_reasoning_budget, reasoning_budget_settings
from run_agent import AIAgent

_MSGS = [{"role": "user", "content": "hello"}]
_DOOR = "http://localhost:8780/v1"


def _agent(model="glm-5.3", base_url=_DOOR, max_tokens=16384):
    return AIAgent(
        api_key="test-key", base_url=base_url, model=model, provider="litellm",
        api_mode="chat_completions", quiet_mode=True, skip_context_files=True,
        skip_memory=True, session_id="test-budget",
        max_tokens=max_tokens,
    )


def _config(reasoning=None):
    def _load():
        cfg = {"reasoning": reasoning} if reasoning is not None else {}
        return cfg
    return patch("hermes_cli.config.load_config_readonly", return_value=_load())


class TestFlagOff:
    """AC1: flag OFF = byte-identical payloads."""

    def test_flag_off_no_thinking_anywhere(self):
        with _config():
            kwargs = build_api_kwargs(_agent(), _MSGS)
        assert "thinking" not in kwargs
        assert "thinking" not in (kwargs.get("extra_body") or {})

    def test_flag_absent_no_thinking(self):
        with _config():
            kwargs = build_api_kwargs(_agent(), _MSGS)
        assert "thinking" not in (kwargs.get("extra_body") or {})

    def test_missing_config_block_is_off(self):
        with _config(None):
            kwargs = build_api_kwargs(_agent(), _MSGS)
        assert "thinking" not in (kwargs.get("extra_body") or {})


class TestFlagOn:
    """Spec: inject only on door + glm-5 + main turns, explicit wins."""

    def test_flag_on_injects_budget(self):
        with _config({"enabled": True, "budget_fraction": 0.6}):
            kwargs = build_api_kwargs(_agent(max_tokens=16384), _MSGS)
        thinking = kwargs["extra_body"]["thinking"]
        assert thinking["type"] == "enabled"
        assert thinking["budget_tokens"] == int(16384 * 0.6)  # 9830

    def test_custom_fraction(self):
        with _config({"enabled": True, "budget_fraction": 0.5}):
            kwargs = build_api_kwargs(_agent(max_tokens=10000), _MSGS)
        assert kwargs["extra_body"]["thinking"]["budget_tokens"] == 5000

    def test_fraction_bounds_coerce_to_default(self):
        for bad in (0, 1, -3, 1.5, "junk", None):
            with _config({"enabled": True, "budget_fraction": bad}):
                settings = reasoning_budget_settings(_agent())
            assert settings["budget_fraction"] == 0.6, bad

    def test_explicit_extra_body_thinking_wins(self):
        with _config({"enabled": True, "budget_fraction": 0.6}):
            agent = _agent()
            kwargs = build_api_kwargs(agent, _MSGS)
            # Simulate a caller-pinned thinking via request_overrides-style extra_body
            # already present in kwargs — the injector must not overwrite it.
            explicit = {"type": "enabled", "budget_tokens": 2222}
            kwargs2 = build_api_kwargs(agent, _MSGS)
            kwargs2["extra_body"]["thinking"] = explicit
            apply_reasoning_budget(agent, kwargs2)
        assert kwargs2["extra_body"]["thinking"] == explicit

    def test_not_glm5_model_untouched(self):
        with _config({"enabled": True, "budget_fraction": 0.6}):
            kwargs = build_api_kwargs(_agent(model="glm-4.5-flash"), _MSGS)
        assert "thinking" not in (kwargs.get("extra_body") or {})

    def test_not_door_base_url_untouched(self):
        with _config({"enabled": True, "budget_fraction": 0.6}):
            kwargs = build_api_kwargs(_agent(base_url="http://localhost:4000/v1"), _MSGS)
        assert "thinking" not in (kwargs.get("extra_body") or {})

    def test_small_max_tokens_untouched(self):
        with _config({"enabled": True, "budget_fraction": 0.6}):
            kwargs = build_api_kwargs(_agent(max_tokens=2048), _MSGS)
        assert "thinking" not in (kwargs.get("extra_body") or {})

    def test_ephemeral_max_output_tokens_split_root(self):
        # Continuation retries boost max_tokens via _ephemeral_max_output_tokens;
        # the budget must split the boosted value (16384*2=32768 -> 19660).
        with _config({"enabled": True, "budget_fraction": 0.6}):
            agent = _agent(max_tokens=16384)
            setattr(agent, "_ephemeral_max_output_tokens", 32768)  # dynamic attr (turn_overflow.py)
            kwargs = build_api_kwargs(agent, _MSGS)
        assert kwargs["extra_body"]["thinking"]["budget_tokens"] == 19660

    def test_max_tokens_missing_no_crash_no_inject(self):
        with _config({"enabled": True, "budget_fraction": 0.6}):
            agent = _agent()
            kwargs = {"model": "glm-5.3", "messages": _MSGS}  # no max_tokens
            apply_reasoning_budget(agent, kwargs)
        assert "thinking" not in (kwargs.get("extra_body") or {})


class TestAuxPaths:
    """Spec: aux (title/compression/approval/bg-review) stays no-think."""

    def test_aux_build_call_kwargs_no_thinking(self):
        from agent import auxiliary_client as aux
        with _config({"enabled": True, "budget_fraction": 0.6}):
            token = aux.set_runtime_main("litellm", "glm-5.3", base_url=_DOOR, session_id="s")
            try:
                aux_kwargs = aux._build_call_kwargs("litellm", "glm-5.3", _MSGS, base_url=_DOOR)
            finally:
                aux._RUNTIME_MAIN_CONTEXT.reset(token)
        assert "thinking" not in (aux_kwargs.get("extra_body") or {})
        assert "thinking" not in aux_kwargs


class TestDoorMatch:
    def test_door_variants(self):
        from agent.reasoning_budget import _door_match
        assert _door_match("http://localhost:8780/v1")
        assert _door_match("https://127.0.6.6:8780/v1".replace("127.0.6.6", "127.0.0.1"))
        assert not _door_match("http://localhost:4000/v1")
        assert not _door_match(None)
        assert not _door_match("")


class TestSandbox:
    """Boundary: injection never escapes to other providers on the same box."""

    def test_litellm_4000_fallback_not_injected(self):
        # b02's fallback provider is litellm :4000 — must stay untouched.
        with _config({"enabled": True, "budget_fraction": 0.6}):
            kwargs = build_api_kwargs(_agent(base_url="http://localhost:4000/v1"), _MSGS)
        assert "thinking" not in (kwargs.get("extra_body") or {})


# AC3 (48h zero ceiling events) is an operational gate, not a unit test —
# verified post-flip via watchtower + turn logs, gated in the rollout runbook.
