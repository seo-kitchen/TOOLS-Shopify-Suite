"""Run execution/ scripts from within Streamlit pages.

Two modes:
  - ``run_fn(fn, *args, **kwargs)`` — call a pure function that returns a result.
     Simple path. Use when the work is quick (<10s).
  - ``run_fn_threaded(fn, *args, **kwargs)`` — run in a thread so Streamlit
     can poll progress and the websocket won't time out.

Also provides:
  - ``streamlit_progress(bar, log_container)`` — factory that returns a
    ``progress(i, n, msg)`` callback matching the refactored script signature.
"""
from __future__ import annotations

import concurrent.futures
from typing import Any, Callable

import streamlit as st


_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None


def _executor() -> concurrent.futures.ThreadPoolExecutor:
    global _EXECUTOR
    if _EXECUTOR is None:
        _EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    return _EXECUTOR


def run_fn(fn: Callable, *args: Any, **kwargs: Any) -> Any:
    """Call fn synchronously. Returns fn's result or raises."""
    return fn(*args, **kwargs)


def run_fn_threaded(fn: Callable, *args: Any, **kwargs: Any) -> concurrent.futures.Future:
    """Fire-and-forget: returns a Future. Poll in your page with a timer."""
    return _executor().submit(fn, *args, **kwargs)


def streamlit_progress(bar, log_container):
    """Return a progress callback with signature (i, n, msg).

    ``bar``           : st.progress() instance
    ``log_container`` : st.empty()  (so we can overwrite the message)
    """
    def _cb(i: int, n: int, msg: str = "") -> None:
        try:
            bar.progress(min(max(i / max(n, 1), 0.0), 1.0))
        except Exception:
            pass
        if msg and log_container is not None:
            log_container.write(msg)
    return _cb
