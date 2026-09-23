"""Agent-owned reasoning budget for z.ai GLM-5 models (task 7874f4f9).

z.ai glm-5.3 runs unlimited thinking by default; reasoning can consume the entire
max_tokens output budget including all 4 continuation retries -> empty answers
("No visible answer was produced", agent/turn_truncation.py _CEILING_NO_TEXT).
The door (zai-proxy) currently band-aids this with a 60% thinking budget injected
at the door (09-18 patch). The caller must own the budget instead.

This module injects a caller-set ``thinking`` budget on MAIN agent turns when:

* ``reasoning.enabled: true`` in config (ships dark, default OFF), AND
* the provider base_url is the z.ai door (localhost:8780 — the fleet's z.ai
  rail), AND
* the model starts with ``glm-5``.

Explicit per-call ``thinking`` (request_overrides / profile extra_body) always
wins — we never overwrite a value the caller set. Aux paths (title/compression/
approval/bg-review on glm-4.5-flash) do not go through ``build_api_kwargs`` and
are never touched.

Injected into ``extra_body`` (not top-level kwargs) because the OpenAI SDK
rejects unknown top-level create() kwargs; the SDK merges ``extra_body``
into the JSON body's top level, so on the wire this is a top-level
``thinking`` field — exactly what zai-proxy checks (``payload.get("thinking")``).
"""

from __future__ import annotations

from typing import Any

# The z.ai door rails. base_url matching is by substring so http/https and
# trailing-path variants of the same door host all qualify.
_DOOR_HOSTS = ("localhost:8780", "127.0.0.1:8780")

DEFAULT_BUDGET_FRACTION = 0.6


def _door_match(base_url: Any) -> bool:
    base = str(base_url or "").lower()
    return any(host in base for host in _DOOR_HOSTS)


def reasoning_budget_settings(agent: Any) -> dict[str, Any]:
    """Read the ``reasoning:`` config block; missing/invalid -> flag-off."""
    try:
        from hermes_cli.config import load_config_readonly
        reasoning = (load_config_readonly() or {}).get("reasoning") or {}
    except Exception:
        return {"enabled": False, "budget_fraction": DEFAULT_BUDGET_FRACTION}
    if not isinstance(reasoning, dict):
        return {"enabled": False, "budget_fraction": DEFAULT_BUDGET_FRACTION}
    enabled = reasoning.get("enabled", False) is True
    try:
        fraction = float(reasoning.get("budget_fraction", DEFAULT_BUDGET_FRACTION))
        if not 0.0 < fraction < 1.0:
            fraction = DEFAULT_BUDGET_FRACTION
    except (TypeError, ValueError):
        fraction = DEFAULT_BUDGET_FRACTION
    return {"enabled": enabled, "budget_fraction": fraction}


def apply_reasoning_budget(agent: Any, api_kwargs: dict[str, Any]) -> None:
    """Inject caller-set ``thinking`` on main turns for glm-5 via the z.ai door.

    Mutates ``api_kwargs`` in place. No-op when the flag is off, the route is
    not the z.ai door, the model is not glm-5*, the request already carries an
    explicit ``thinking`` field, or max_tokens is missing/too small to split.
    """
    settings = reasoning_budget_settings(agent)
    if not settings["enabled"]:
        return
    model = str(getattr(agent, "model", "") or "")
    if not model.startswith("glm-5"):
        return
    if not _door_match(getattr(agent, "base_url", "")):
        return

    # The wire max_tokens is the budget we split (top-level or OpenAI-style
    # max_completion_tokens). The door re-checks against its own view; ours is
    # the caller's authoritative cap.
    max_tokens = api_kwargs.get("max_tokens")
    if max_tokens is None:
        max_tokens = api_kwargs.get("max_completion_tokens")
    if not isinstance(max_tokens, int) or isinstance(max_tokens, bool) or max_tokens < 4096:
        # Too small to split safely (mirrors the door's own >=4096 floor).
        return

    # Explicit per-call thinking always wins — check both the top-level kwargs
    # (already unusual) and extra_body (the standard carrier for provider
    # extension fields on the OpenAI wire).
    extra_body = api_kwargs.get("extra_body")
    if api_kwargs.get("thinking") is not None or (
        isinstance(extra_body, dict) and extra_body.get("thinking") is not None
    ):
        return

    budget = int(max_tokens * settings["budget_fraction"])
    thinking = {"type": "enabled", "budget_tokens": budget}
    if isinstance(extra_body, dict):
        extra_body["thinking"] = thinking
    else:
        api_kwargs["extra_body"] = {"thinking": thinking}
