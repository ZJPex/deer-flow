"""Temporary, token-free MCP state tracing for the isolated Fork diagnosis."""

import asyncio
import json
import logging
import sys
import time

import pytest

_TARGET = "test_reconcile_retains_detached_owners_when_retirement_is_dropped"
_FUNCTIONS = {_TARGET, "get_session", "_run_session", "_guarded_cancel", "_owner_unwinding_after_failure", "_track_owner_teardown", "_reap", "initialize", "__aexit__"}


def _future_state(value):
    if not isinstance(value, asyncio.Future):
        return None
    state = {"id": id(value), "done": value.done(), "cancelled": value.cancelled()}
    if isinstance(value, asyncio.Task):
        state["cancelling"] = value.cancelling()
    if value.done() and not value.cancelled():
        error = value.exception()
        state["exception_type"] = type(error).__name__ if error else None
    return state


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    if item.name != _TARGET:
        yield
        return
    from deerflow.mcp import session_pool

    previous_trace = sys.gettrace()
    seen = set()
    cancel_owner = session_pool.MCPSessionPool._cancel_owner
    cancel_code = getattr(getattr(cancel_owner, "__func__", cancel_owner), "__code__", None)
    initial = {
        "clock_module": getattr(time.monotonic, "__module__", None),
        "shield_module": getattr(asyncio.shield, "__module__", None),
        "shield_name": getattr(asyncio.shield, "__qualname__", None),
        "cancel_owner_source": getattr(cancel_code, "co_filename", None),
        "cancel_owner_type": type(cancel_owner).__name__,
        "root_level": logging.getLogger().level,
        "mcp_level": session_pool.logger.getEffectiveLevel(),
        "root_handlers": [f"{type(handler).__module__}.{type(handler).__name__}" for handler in logging.getLogger().handlers],
    }
    print("[DEBUG-pr6714] initial " + json.dumps(initial), flush=True)

    def trace(frame, event, arg):
        filename = frame.f_code.co_filename
        name = frame.f_code.co_name
        if not filename.endswith(("session_pool.py", "test_mcp_binding_fencing.py")) or name not in _FUNCTIONS:
            return None
        key = (name, event, frame.f_lineno)
        if key in seen or len(seen) >= 140:
            return trace
        seen.add(key)
        values = frame.f_locals
        clock = time.monotonic()
        record = {"function": name, "event": event, "line": frame.f_lineno, "clock": clock if isinstance(clock, (int, float)) else type(clock).__name__}
        for variable in ("ready", "task", "creator", "join"):
            state = _future_state(values.get(variable))
            if state is not None:
                record[variable] = state
        if event == "exception":
            record["exception_type"] = arg[0].__name__
        pool = values.get("self") or values.get("pool")
        if isinstance(pool, session_pool.MCPSessionPool):
            record["entries"] = len(pool._entries)
            record["inflight"] = len(pool._inflight)
            record["teardowns"] = len(pool._teardown_tasks)
        print("[DEBUG-pr6714] " + json.dumps(record), flush=True)
        return trace

    sys.settrace(trace)
    try:
        yield
    finally:
        sys.settrace(previous_trace)
