"""Advisory lock for long-running pipeline steps.

Prevents two users (or two tabs) from kicking off 'Run match fase 4' at the
same time. Backed by the ``seo_job_locks`` table which has a unique partial
index on (fase, step) WHERE status='running'.

Usage:

    from ui.job_lock import acquire, release, current_holder

    lock = acquire(fase="4", step="match")
    if lock is None:
        holder = current_holder("4", "match")
        st.warning(f"Vergrendeld door {holder['started_by']} sinds {holder['started_at']}")
        st.stop()
    try:
        ... do the work ...
        release(lock["id"], success=True)
    except Exception as e:
        release(lock["id"], success=False, details=str(e))
        raise
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from .supabase_client import current_user_email, get_supabase


TABLE = "seo_job_locks"
STALE_AFTER_MIN = 30


def acquire(fase: str, step: str, details: dict | None = None) -> dict | None:
    """Try to insert a running lock. Returns the lock row, or None if already held.

    If an existing 'running' lock is older than STALE_AFTER_MIN, we force-release
    it and take over (prevents permanently-stuck locks from crashed sessions).
    """
    sb = get_supabase()
    existing = (
        sb.table(TABLE)
        .select("*")
        .eq("fase", fase)
        .eq("step", step)
        .eq("status", "running")
        .limit(1)
        .execute()
    ).data or []

    if existing:
        lock = existing[0]
        started = datetime.fromisoformat(lock["started_at"].replace("Z", "+00:00"))
        if datetime.now(started.tzinfo) - started > timedelta(minutes=STALE_AFTER_MIN):
            sb.table(TABLE).update(
                {"status": "failed", "released_at": datetime.utcnow().isoformat(),
                 "details": {"reason": "stale_force_release"}}
            ).eq("id", lock["id"]).execute()
        else:
            return None

    row = {
        "fase": fase,
        "step": step,
        "started_by": current_user_email(),
        "status": "running",
        "details": details or {},
    }
    res = sb.table(TABLE).insert(row).execute()
    return (res.data or [{}])[0]


def heartbeat(lock_id: int) -> None:
    sb = get_supabase()
    sb.table(TABLE).update({"heartbeat_at": datetime.utcnow().isoformat()}).eq("id", lock_id).execute()


def release(lock_id: int, success: bool = True, details: str | dict | None = None) -> None:
    sb = get_supabase()
    upd: dict[str, Any] = {
        "status": "released" if success else "failed",
        "released_at": datetime.utcnow().isoformat(),
    }
    if details is not None:
        upd["details"] = details if isinstance(details, dict) else {"error": str(details)}
    sb.table(TABLE).update(upd).eq("id", lock_id).execute()


def current_holder(fase: str, step: str) -> dict | None:
    sb = get_supabase()
    res = (
        sb.table(TABLE)
        .select("started_by, started_at, heartbeat_at, details")
        .eq("fase", fase)
        .eq("step", step)
        .eq("status", "running")
        .limit(1)
        .execute()
    )
    return (res.data or [None])[0]
