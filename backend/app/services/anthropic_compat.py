"""Per-model request shaping for the Anthropic Messages API.

Every call site in this repo was written for Haiku 4.5 / pre-4.7 behaviour:
forced ``tool_choice`` and ``resp.content[0].text``. Newer models differ
(per docs.claude.com "Thinking", checked 2026-09-25):

- Opus 4.7+, Opus 5, Opus 5.5, Sonnet 5, Fable, Mythos: thinking is on by
  default, thinking tokens count against ``max_tokens``, the first content
  block can be a thinking block, and non-default ``temperature`` / ``top_p``
  / ``top_k`` return 400 on every request.
- Opus 5.5, Fable 5.1, Mythos 5.1: forced tool use (``tool_choice`` type
  ``tool`` / ``any``) returns 400 on every request; ``auto`` is required.
- Opus 5.5, Fable, Mythos: ``thinking: {"type": "disabled"}`` returns 400.

``adapt_request`` rewrites a request body (SDK kwargs or raw HTTP JSON, same
keys) so it is valid for its model; ``create`` / ``post`` also retry once on
a 400 that names one of those parameters, so a model this table does not
know yet degrades instead of failing open to "0 tariffs".

Env knobs (all optional):
- ``ANTHROPIC_THINKING``: ``disabled`` turns thinking off where the model
  allows it (Sonnet 5, Opus 5); ``adaptive`` asks for it explicitly.
  Unset keeps the model default.
- ``ANTHROPIC_EFFORT``: ``output_config.effort`` for thinking models.
- ``ANTHROPIC_THINKING_MIN_MAX_TOKENS`` (default 16000): ``max_tokens``
  floor while thinking is on, so reasoning cannot crowd out the tool call.
"""
from __future__ import annotations

import copy
import logging
import os
import re
from typing import Any, Callable

log = logging.getLogger(__name__)

_THINKING_DEFAULT_ON = re.compile(r"claude-(?:opus-(?:4-7|4-8|5)|sonnet-5|fable|mythos)", re.I)
_FORCED_TOOL_REJECTED = re.compile(r"claude-(?:opus-5-5|fable-5-1|mythos-5-1)", re.I)
_THINKING_DISABLE_REJECTED = re.compile(r"claude-(?:opus-5-5|fable|mythos)", re.I)
_SAMPLING_KEYS = ("temperature", "top_p", "top_k")


def thinking_default_on(model: str) -> bool:
    return bool(_THINKING_DEFAULT_ON.search(model or ""))


def forced_tool_rejected(model: str) -> bool:
    return bool(_FORCED_TOOL_REJECTED.search(model or ""))


def thinking_disable_allowed(model: str) -> bool:
    return thinking_default_on(model) and not _THINKING_DISABLE_REJECTED.search(model or "")


def _min_max_tokens() -> int:
    try:
        return int(os.environ.get("ANTHROPIC_THINKING_MIN_MAX_TOKENS", "16000"))
    except ValueError:
        return 16000


def _append_system(body: dict, text: str) -> None:
    system = body.get("system")
    if not system:
        body["system"] = text
    elif isinstance(system, str):
        body["system"] = f"{system}\n\n{text}"
    else:
        # Appended after any cache_control block so the cached prefix holds.
        body["system"] = [*system, {"type": "text", "text": text}]


def _unforce_tool_choice(body: dict) -> None:
    choice = body.get("tool_choice") or {}
    if choice.get("type") not in ("tool", "any"):
        return
    name = choice.get("name")
    body["tool_choice"] = {"type": "auto"}
    _append_system(
        body,
        f"Respond only by calling the `{name}` tool exactly once." if name
        else "Respond only by calling one of the provided tools.",
    )


def _thinking_mode(body: dict) -> str:
    """'on' | 'off' after shaping (what the model will actually do)."""
    thinking = body.get("thinking")
    if thinking is None:
        thinking = (body.get("extra_body") or {}).get("thinking")
    if thinking is None:
        return "on" if thinking_default_on(body.get("model", "")) else "off"
    return "off" if thinking.get("type") == "disabled" else "on"


def adapt_request(body: dict, *, sdk: bool = False) -> dict:
    """Return a copy of ``body`` that is valid for ``body['model']``.

    ``sdk=True`` routes ``thinking`` / ``output_config`` through
    ``extra_body`` so older SDK versions without those kwargs still work.
    """
    out = copy.copy(body)
    model = str(out.get("model") or "")
    extra: dict = dict(out.pop("extra_body", None) or {})

    if thinking_default_on(model):
        for k in _SAMPLING_KEYS:
            out.pop(k, None)

    mode = (os.environ.get("ANTHROPIC_THINKING") or "").strip().lower()
    if mode == "disabled" and thinking_disable_allowed(model):
        extra["thinking"] = {"type": "disabled"}
    elif mode == "adaptive" and thinking_default_on(model):
        extra["thinking"] = {"type": "adaptive"}
    effort = (os.environ.get("ANTHROPIC_EFFORT") or "").strip().lower()
    if effort and thinking_default_on(model) and extra.get("thinking", {}).get("type") != "disabled":
        extra["output_config"] = {**extra.get("output_config", {}), "effort": effort}

    if forced_tool_rejected(model):
        _unforce_tool_choice(out)

    for key in ("thinking", "output_config"):
        if key in out and key not in extra:
            extra[key] = out.pop(key)

    if _thinking_mode({**out, "extra_body": extra}) == "on":
        out["max_tokens"] = max(int(out.get("max_tokens") or 0), _min_max_tokens())

    if sdk:
        if extra:
            out["extra_body"] = extra
    else:
        out.update(extra)
    return out


def degrade_for_error(body: dict, message: str) -> dict | None:
    """One-step fallback for a 400 naming a parameter; None if no change."""
    msg = (message or "").lower()
    out = copy.copy(body)
    extra = dict(out.get("extra_body") or {})
    changed = False
    if "tool_choice" in msg or "forced tool" in msg:
        before = out.get("tool_choice")
        _unforce_tool_choice(out)
        changed |= out.get("tool_choice") != before
    if any(k in msg for k in _SAMPLING_KEYS):
        for k in _SAMPLING_KEYS:
            changed |= out.pop(k, None) is not None
    if "thinking" in msg:
        changed |= out.pop("thinking", None) is not None
        changed |= extra.pop("thinking", None) is not None
    if "output_config" in msg or "effort" in msg:
        changed |= out.pop("output_config", None) is not None
        changed |= extra.pop("output_config", None) is not None
    if not changed:
        return None
    if "extra_body" in out or extra:
        out["extra_body"] = extra
        if not extra:
            out.pop("extra_body")
    return out


def create(messages_api: Any, **kwargs) -> Any:
    """``messages_api.create`` with per-model shaping and one 400 retry."""
    body = adapt_request(kwargs, sdk=True)
    try:
        return messages_api.create(**body)
    except Exception as e:  # noqa: BLE001 — only BadRequest-shaped errors retry
        if getattr(e, "status_code", None) != 400:
            raise
        retry = degrade_for_error(body, str(getattr(e, "message", "") or e))
        if retry is None:
            raise
        log.warning("Anthropic 400 for %s, retrying with compatible params: %s",
                    body.get("model"), str(e)[:200])
        return messages_api.create(**retry)


def post(post_fn: Callable[..., Any], url: str, *, json: dict, **kwargs) -> Any:
    """Raw-HTTP ``post_fn(url, json=...)`` with shaping and one 400 retry."""
    body = adapt_request(json)
    resp = post_fn(url, json=body, **kwargs)
    if getattr(resp, "status_code", None) == 400:
        retry = degrade_for_error(body, getattr(resp, "text", "") or "")
        if retry is not None:
            log.warning("Anthropic 400 for %s, retrying with compatible params", body.get("model"))
            resp = post_fn(url, json=retry, **kwargs)
    return resp


def response_text(content: Any) -> str:
    """Text of the first text block (skips thinking blocks); '' if none."""
    for block in content or []:
        btype = block.get("type") if isinstance(block, dict) else getattr(block, "type", None)
        if btype == "text":
            return (block.get("text") if isinstance(block, dict) else getattr(block, "text", "")) or ""
    return ""
