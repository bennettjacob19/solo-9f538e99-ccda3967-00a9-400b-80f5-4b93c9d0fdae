"""Two-worker circuit-breaker gateway for artifact metadata lookups.

Architecture
------------
* FastAPI/uvicorn runs two worker processes.  Each worker has its own asyncio
  event loop, so a slow upstream query never blocks another query.
* Breaker state lives in SQLite (WAL mode, ``BEGIN IMMEDIATE`` for writes), so
  it is shared by both workers and survives restarts.
* Every upstream call is bounded to ``REQUEST_TIMEOUT`` seconds end-to-end.
* A breaker failure is a connect failure, a timeout, or a 5xx response.  Any
  other completed response (2xx/3xx/4xx) resets the failure counter.

Breaker semantics (per artifact name)
-------------------------------------
* CLOSED: calls pass through.  Failures are counted in completion order.  The
  third consecutive failure opens the circuit; a healthy response resets to 0.
* OPEN: every request is answered locally with 503; the upstream is not
  contacted.  After ``COOLDOWN`` seconds the first caller atomically becomes
  the single HALF_OPEN probe; everybody else is still rejected.
* A failed/timed-out probe re-opens the breaker with a fresh cooldown.
* A probe that crashes or silently disappears releases the probe slot after a
  lease, so a new probe can eventually run.
* Results carry the epoch observed at admission.  When the breaker has tripped
  since then the result is stale: a slow success that started before the trip
  can neither reset the counter nor close the circuit.
* Probe results carry the probe generation.  A late result from an older probe
  generation is discarded, so it cannot overwrite a newer verdict.

In-flight request coalescing (single-flight per artifact name)
--------------------------------------------------------------
* Every request is still admitted by the breaker rules above first.  Requests
  rejected while OPEN/HALF_OPEN never join a shared call.
* Among the requests that are admitted while one directory call for the same
  artifact is still in flight, exactly one becomes the call leader; the rest
  become waiters and receive the leader's result.  Different artifacts use
  different flights and stay fully parallel.
* Coalescing spans both worker processes: the leadership election is an atomic
  SQLite compare-and-swap (one ``shared_calls`` row per artifact), so two
  workers hit by the same name at the same instant still issue a single
  upstream request.
* A shared call's success or failure is folded into the breaker exactly once,
  by the leader.  If it trips the circuit, every waiter already joined to that
  call receives this failure; later queries are rejected by the breaker as
  usual.
* The upstream call keeps its hard 1-second total timeout.  Waiters are bounded
  as well: a dead leader's call is taken over after a lease (or detected via
  liveness checks), and no waiter ever waits indefinitely.
* Call rows carry a per-artifact call generation.  A late result from an older
  generation is fenced off: it neither overwrites the newer call's result nor
  touches the breaker verdict.
* Cancelling a waiting request (client disconnect / worker shutdown) never
  cancels the shared call while it has other waiters; an unconfirmed shared
  call is therefore safe to retry.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("GATEWAY_DB", BASE_DIR / "data" / "breaker.db"))
UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://upstream:9000").rstrip("/")
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "1.0"))
COOLDOWN = float(os.environ.get("COOLDOWN", "2.0"))
FAILURE_THRESHOLD = int(os.environ.get("FAILURE_THRESHOLD", "3"))
# A live probe is bounded by REQUEST_TIMEOUT; anything older than this lease is
# considered abandoned (e.g. its process died before writing a verdict).
PROBE_LEASE = max(2.0, REQUEST_TIMEOUT + 1.0)

# -- in-flight coalescing tuning -------------------------------------------
# A shared upstream call is bounded by REQUEST_TIMEOUT; this lease also has to
# cover the small SQLite round-trips around the call.  A row older than the
# lease whose owner is not live may be taken over by another worker.
SHARED_CALL_LEASE = max(2.0, REQUEST_TIMEOUT + 1.0)
# How long waiters sleep between polls of the shared-call row.
SHARED_POLL_INTERVAL = 0.02
# Completed rows retained per artifact so a cross-process waiter that joined an
# older generation can still read its exact result even after newer calls start.
SHARED_HISTORY_KEEP = 20
# Age after which published (done) rows are pruned.
SHARED_DONE_RETENTION = 30.0
# Hard ceiling on how long a waiter waits for a shared call before giving up
# with a locally generated, non-breaker failure (the call itself stays in
# flight and is still folded once by whoever owns it).
SHARED_WAIT_BUDGET = SHARED_CALL_LEASE + 2.0

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"

# Unique id of this worker process.  Used to tell our own live probe apart from
# a stale half-open record left behind by a dead process.
INSTANCE_ID = f"{os.getpid()}-{time.time_ns()}"
ACTIVE_PROBES: set[str] = set()


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS breakers (
    artifact        TEXT PRIMARY KEY,
    state           TEXT NOT NULL DEFAULT 'closed',
    failures        INTEGER NOT NULL DEFAULT 0,
    opened_at       REAL NOT NULL DEFAULT 0,
    probe_gen       INTEGER NOT NULL DEFAULT 0,
    probe_holder    TEXT,
    epoch           INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    last_transition REAL NOT NULL DEFAULT 0,
    updated_at      REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       REAL NOT NULL,
    artifact TEXT NOT NULL,
    kind     TEXT NOT NULL,
    detail   TEXT
);
CREATE TABLE IF NOT EXISTS upstream_stats (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    hits         INTEGER NOT NULL DEFAULT 0,
    hits_by_mode TEXT NOT NULL DEFAULT '{}',
    shared_joins INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO upstream_stats (id, hits) VALUES (1, 0);
/* One row per artifact with an in-flight (or just-finished) coalesced call.
   Ownership is claimed with an atomic compare-and-swap inside BEGIN IMMEDIATE,
   so only one worker becomes the call leader even when both race.  ``gen``
   fences late results from an older call generation. */
/* One row per call generation; completed rows are retained so a waiter on an
   older generation can read its exact result after newer calls begin.
   Ownership of a new generation is claimed with an atomic compare-and-swap
   inside BEGIN IMMEDIATE, so only one worker becomes the call leader even when
   both race.  ``gen`` fences late results from an older generation. */
CREATE TABLE IF NOT EXISTS shared_calls (
    artifact     TEXT NOT NULL,
    gen          INTEGER NOT NULL,
    leader_pid   INTEGER NOT NULL,
    leader_inst  TEXT NOT NULL,
    started_at   REAL NOT NULL,
    lease_until  REAL NOT NULL,
    finished_at  REAL NOT NULL DEFAULT 0,
    state        TEXT NOT NULL DEFAULT 'running',
    result       TEXT,
    PRIMARY KEY (artifact, gen)
);
"""

# Older databases predate upstream_stats.shared_joins / shared_calls.finished_at.
# The composite-primary-key table layout is recreated wholesale when an older
# single-row layout is detected (the table holds only transient in-flight data,
# never durable breaker state).
MIGRATIONS = (
    "ALTER TABLE upstream_stats ADD COLUMN shared_joins INTEGER NOT NULL DEFAULT 0",
)


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    last: Optional[Exception] = None
    for _ in range(50):
        try:
            conn = connect()
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                # Recreate the transient shared_calls table if an older
                # single-row (artifact-only primary key) layout is present.
                existing = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='shared_calls'"
                ).fetchone()
                if existing is not None:
                    pks = [
                        r["name"]
                        for r in conn.execute("PRAGMA table_info(shared_calls)")
                        if r["pk"]
                    ]
                    if pks != ["artifact", "gen"]:
                        conn.execute("DROP TABLE shared_calls")
                conn.executescript(SCHEMA)
                for stmt in MIGRATIONS:
                    try:
                        conn.execute(stmt)
                    except sqlite3.OperationalError as exc:
                        if "duplicate column" not in str(exc).lower():
                            raise
                # Shared-call rows describe live in-process work; nothing in
                # them survives a restart, so start every boot with a clean
                # table (breaker state itself is untouched and remains durable).
                conn.execute("DELETE FROM shared_calls")
            finally:
                conn.close()
            return
        except sqlite3.OperationalError as exc:  # two workers racing at startup
            last = exc
            time.sleep(0.1)
    raise RuntimeError(f"unable to initialise breaker database: {last}")


@contextlib.contextmanager
def write_tx():
    """Exclusive write transaction, serialising both worker processes."""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
    finally:
        conn.close()


def log_event(conn: sqlite3.Connection, artifact: str, kind: str, detail: str = "") -> None:
    conn.execute(
        "INSERT INTO events (ts, artifact, kind, detail) VALUES (?, ?, ?, ?)",
        (time.time(), artifact, kind, detail[:300]),
    )
    conn.execute("DELETE FROM events WHERE id <= (SELECT MAX(id) - 500 FROM events)")


# ---------------------------------------------------------------------------
# Breaker state machine
# ---------------------------------------------------------------------------


@dataclass
class Admission:
    allowed: bool
    role: str  # "normal" | "probe" | "none"
    epoch: int
    probe_gen: int = 0
    holder: Optional[str] = None
    state: str = CLOSED
    retry_after: float = 0.0


@dataclass
class Outcome:
    kind: str  # "success" | "failure"
    detail: str
    status: Optional[int] = None
    payload: Any = None


def _probe_is_live(row: sqlite3.Row, now: float) -> bool:
    holder = row["probe_holder"] or ""
    if not holder:
        return False
    pid_text = holder.split("|", 1)[0]
    try:
        pid = int(pid_text)
    except ValueError:
        return False
    if pid == os.getpid():
        # Only probes this process is actively running count as live; a
        # half-open row from an earlier incarnation is stale.
        return holder in ACTIVE_PROBES
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    else:
        # A SIGKILLed worker can linger as a zombie until its parent reaps it;
        # ``kill(pid, 0)`` still succeeds for zombies, so check /proc when
        # present.
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
        except OSError:
            pass
        else:
            if stat.rsplit(")", 1)[-1].split()[0] == "Z":
                return False
    # Backstop against PID reuse / a live process that abandoned its probe: a
    # probe is bounded by REQUEST_TIMEOUT, so anything older than the lease is
    # considered dead.
    return now - row["opened_at"] <= PROBE_LEASE


def effective_state(row: sqlite3.Row, now: float) -> tuple[str, float]:
    """Persisted state with crash recovery applied.

    A HALF_OPEN row whose probe owner is gone is treated as OPEN measured from
    the probe start, so the normal cooldown rules (and its 503 responses)
    still apply before another probe is elected.
    """
    if row["state"] == HALF_OPEN and not _probe_is_live(row, now):
        return OPEN, row["opened_at"]
    return row["state"], row["opened_at"]


def admit(artifact: str) -> Admission:
    """Atomically decide whether this request may call the upstream.

    Fast paths (CLOSED and rejection-while-open) are read-only, so normal
    traffic and bursts of rejections never serialise on the SQLite write lock.
    Only the probe election takes ``BEGIN IMMEDIATE``.
    """
    now = time.time()
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM breakers WHERE artifact = ?", (artifact,)).fetchone()
    finally:
        conn.close()

    if row is not None:
        fast = _fast_admission(row, now)
        if fast is not None:
            return fast

    with write_tx() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO breakers (artifact, updated_at) VALUES (?, ?)",
            (artifact, now),
        )
        row = conn.execute("SELECT * FROM breakers WHERE artifact = ?", (artifact,)).fetchone()
        now = time.time()
        state, opened_at = effective_state(row, now)

        if state == CLOSED:
            return Admission(True, "normal", row["epoch"], state=CLOSED)

        if state == HALF_OPEN:
            # A live probe is already in flight; nobody else may pass.
            return Admission(False, "none", row["epoch"], state=HALF_OPEN, retry_after=0.2)

        remaining = COOLDOWN - (now - opened_at)
        if remaining > 0:
            return Admission(False, "none", row["epoch"], state=OPEN, retry_after=remaining)

        # Cooldown elapsed: promote exactly one caller to probe.  BEGIN
        # IMMEDIATE serialises this across both worker processes.
        probe_gen = row["probe_gen"] + 1
        holder = f"{os.getpid()}|{INSTANCE_ID}|{probe_gen}"
        conn.execute(
            "UPDATE breakers SET state = ?, probe_gen = ?, probe_holder = ?, opened_at = ?, "
            "updated_at = ? WHERE artifact = ?",
            (HALF_OPEN, probe_gen, holder, now, now, artifact),
        )
        log_event(conn, artifact, "probe_started", f"generation {probe_gen}")
        ACTIVE_PROBES.add(holder)
        return Admission(
            True, "probe", row["epoch"], probe_gen, holder, HALF_OPEN, 0.0
        )


def _fast_admission(row: sqlite3.Row, now: float) -> Optional[Admission]:
    """Admission decision that needs no write, or ``None`` if a write is needed."""
    state, opened_at = effective_state(row, now)
    if state == CLOSED:
        return Admission(True, "normal", row["epoch"], state=CLOSED)
    if state == HALF_OPEN:
        return Admission(False, "none", row["epoch"], state=HALF_OPEN, retry_after=0.2)
    remaining = COOLDOWN - (now - opened_at)
    if remaining > 0:
        return Admission(False, "none", row["epoch"], state=OPEN, retry_after=remaining)
    return None  # cooldown elapsed: must take the write lock to elect the probe


def record_outcome(artifact: str, adm: Admission, outcome: Outcome, hit_role: Optional[str] = None) -> dict[str, Any]:
    """Fold one completed upstream result into the shared state.

    ``hit_role`` counts this call towards the gateway's per-mode upstream
    counters in the same transaction, so each request needs one write.
    """
    with write_tx() as conn:
        if hit_role:
            _bump_upstream_hit(conn, hit_role)
        return _record_outcome_conn(conn, artifact, adm, outcome)


def _record_outcome_conn(
    conn: sqlite3.Connection, artifact: str, adm: Admission, outcome: Outcome
) -> dict[str, Any]:
    """State-machine update inside an already-open write transaction."""
    now = time.time()
    result: dict[str, Any] = {"effect": "ignored", "state": None}
    row = conn.execute("SELECT * FROM breakers WHERE artifact = ?", (artifact,)).fetchone()
    if row is None:
        return result

    if adm.role == "probe":
        own_probe = (
            row["probe_gen"] == adm.probe_gen
            and row["state"] == HALF_OPEN
            and (row["probe_holder"] or "") == (adm.holder or "")
        )
        if not own_probe:
            # A newer probe generation (or a newer decision) already won.
            result.update(effect="stale_probe_ignored", state=row["state"])
            log_event(conn, artifact, "probe_stale", f"generation {adm.probe_gen}")
            return result
        if outcome.kind == "success":
            conn.execute(
                "UPDATE breakers SET state = ?, failures = 0, probe_holder = NULL, "
                "epoch = epoch + 1, last_error = NULL, last_transition = ?, updated_at = ? "
                "WHERE artifact = ?",
                (CLOSED, now, now, artifact),
            )
            log_event(conn, artifact, "probe_success", "circuit closed")
            result.update(effect="closed", state=CLOSED)
        else:
            conn.execute(
                "UPDATE breakers SET state = ?, failures = ?, opened_at = ?, "
                "probe_holder = NULL, last_error = ?, last_transition = ?, updated_at = ? "
                "WHERE artifact = ?",
                (OPEN, FAILURE_THRESHOLD, now, outcome.detail, now, now, artifact),
            )
            log_event(conn, artifact, "probe_failed", outcome.detail)
            result.update(effect="reopened", state=OPEN)
        return result

    # Normal (non-probe) completed attempt.
    if row["state"] != CLOSED:
        if outcome.kind == "success":
            # Slow success started before the trip: it must not recover.
            result.update(effect="stale_success_ignored", state=row["state"])
            log_event(conn, artifact, "stale_success_ignored", "late success after trip")
        else:
            result.update(effect="stale_failure_ignored", state=row["state"])
        return result

    if adm.epoch != row["epoch"]:
        # The breaker opened (and possibly closed again) while this call
        # was in flight; this attempt belongs to an older epoch.
        result.update(effect="stale_attempt_ignored", state=row["state"])
        return result

    if outcome.kind == "success":
        conn.execute(
            "UPDATE breakers SET failures = 0, last_error = NULL, updated_at = ? "
            "WHERE artifact = ?",
            (now, artifact),
        )
        result.update(effect="reset", state=CLOSED)
    else:
        failures = row["failures"] + 1
        if failures >= FAILURE_THRESHOLD:
            conn.execute(
                "UPDATE breakers SET state = ?, failures = ?, opened_at = ?, "
                "epoch = epoch + 1, probe_holder = NULL, last_error = ?, "
                "last_transition = ?, updated_at = ? WHERE artifact = ?",
                (OPEN, failures, now, outcome.detail, now, now, artifact),
            )
            log_event(conn, artifact, "opened", f"{failures} consecutive failures: {outcome.detail}")
            result.update(effect="opened", state=OPEN)
        else:
            conn.execute(
                "UPDATE breakers SET failures = ?, last_error = ?, updated_at = ? "
                "WHERE artifact = ?",
                (failures, outcome.detail, now, artifact),
            )
            log_event(conn, artifact, "failure", f"{failures}/{FAILURE_THRESHOLD} {outcome.detail}")
            result.update(effect="failure_counted", state=CLOSED)
    return result


def clean_stale_probes() -> None:
    """Release HALF_OPEN rows left behind by dead processes at startup."""
    now = time.time()
    with write_tx() as conn:
        rows = conn.execute("SELECT * FROM breakers WHERE state = ?", (HALF_OPEN,)).fetchall()
        for row in rows:
            if not _probe_is_live(row, now):
                conn.execute(
                    "UPDATE breakers SET state = ?, probe_holder = NULL, updated_at = ? "
                    "WHERE artifact = ? AND state = ?",
                    (OPEN, now, row["artifact"], HALF_OPEN),
                )
                log_event(conn, row["artifact"], "probe_abandoned", "released at startup")


# ---------------------------------------------------------------------------
# Upstream call
# ---------------------------------------------------------------------------


# One pooled client per worker process: connection setup is not repeated for
# every lookup, which keeps 1s timeouts meaningful under concurrency.
_client: Optional[httpx.AsyncClient] = None


def upstream_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=50)
        _client = httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT), limits=limits)
    return _client


async def close_upstream_client() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
    _client = None


async def _get(url: str) -> httpx.Response:
    return await upstream_client().get(url, headers={"Accept": "application/json"})


async def call_upstream(artifact: str) -> Outcome:
    """One GET bounded by REQUEST_TIMEOUT in total (not per phase)."""
    url = f"{UPSTREAM_URL}/artifacts/{quote(artifact, safe='')}"
    try:
        resp = await asyncio.wait_for(_get(url), timeout=REQUEST_TIMEOUT)
    except (asyncio.TimeoutError, httpx.TimeoutException, httpx.ConnectError,
            httpx.NetworkError, httpx.RemoteProtocolError, httpx.HTTPError) as exc:
        return Outcome("failure", f"{type(exc).__name__}: {exc}"[:300])
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # defensive: any transport oddity counts as failure
        return Outcome("failure", f"{type(exc).__name__}: {exc}"[:300])

    if resp.status_code >= 500:
        return Outcome(
            "failure",
            f"upstream {resp.status_code}: {resp.text[:200]}",
            status=resp.status_code,
        )
    try:
        payload = resp.json()
    except ValueError:
        payload = resp.text[:2000]
    return Outcome("success", f"{resp.status_code}", status=resp.status_code, payload=payload)


# ---------------------------------------------------------------------------
# In-flight coalescing (single-flight), spanning both worker processes
# ---------------------------------------------------------------------------


@dataclass
class CallEnvelope:
    """What every participant of a coalesced call receives."""

    outcome: Outcome
    role: str  # breaker admission role, always "normal" for shared calls
    epoch: int
    call_gen: int
    leader: bool
    shared: bool
    breaker_effect: Optional[str] = None
    breaker_state: Optional[str] = None
    detail_override: Optional[str] = None  # e.g. local waiter-give-up reason
    elapsed_ms: float = 0.0


@dataclass
class LocalFlight:
    """One in-flight coalesced call inside this worker's event loop."""

    artifact: str
    call_gen: int
    leader: bool
    epoch: int
    future: "asyncio.Future[CallEnvelope]"
    waiters: int = 0
    driver_task: Optional[asyncio.Task] = None


# artifact -> LocalFlight, only for flights this process currently participates in
LOCAL_FLIGHTS: dict[str, LocalFlight] = {}
# Strong references to detached driver tasks so the garbage collector cannot
# reap a shared call while requests are still waiting on it.  Cleared on
# shutdown; a request disconnect never touches these.
BACKGROUND_TASKS: set[asyncio.Task] = set()
# Lazily created and rebound per running event loop (uvicorn gives each worker
# one loop; tests create fresh loops) so the lock never outlives its loop.
_FLIGHT_LOCK: Optional[asyncio.Lock] = None
_FLIGHT_LOCK_LOOP: Optional[asyncio.AbstractEventLoop] = None


def flight_lock() -> asyncio.Lock:
    global _FLIGHT_LOCK, _FLIGHT_LOCK_LOOP
    loop = asyncio.get_running_loop()
    if _FLIGHT_LOCK is None or _FLIGHT_LOCK_LOOP is not loop:
        _FLIGHT_LOCK = asyncio.Lock()
        _FLIGHT_LOCK_LOOP = loop
    return _FLIGHT_LOCK


def _spawn_driver(artifact: str, flight: LocalFlight, adm: Admission) -> None:
    """Create a detached driver task that outlives any single request."""
    task = asyncio.create_task(_drive(artifact, flight, adm))
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    flight.driver_task = task


async def shutdown_flights() -> None:
    """Cancel every detached shared call during graceful worker shutdown."""
    tasks = list(BACKGROUND_TASKS)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _process_is_live(pid: int) -> bool:
    if pid == os.getpid():
        return True
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    # A SIGKILLed worker may linger as a zombie until reaped; kill(pid, 0)
    # still succeeds for zombies, so consult /proc when available.
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return True
    return stat.rsplit(")", 1)[-1].split()[0] != "Z"


def _latest_row(conn: sqlite3.Connection, artifact: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM shared_calls WHERE artifact = ? ORDER BY gen DESC LIMIT 1",
        (artifact,),
    ).fetchone()


def _prune_shared_history(conn: sqlite3.Connection, artifact: str) -> None:
    conn.execute(
        "DELETE FROM shared_calls WHERE artifact = ? AND gen <= "
        "(SELECT MAX(gen) - ? FROM shared_calls WHERE artifact = ?)",
        (artifact, SHARED_HISTORY_KEEP, artifact),
    )
    conn.execute(
        "DELETE FROM shared_calls WHERE artifact = ? AND state = 'done' "
        "AND finished_at > 0 AND finished_at < ?",
        (artifact, time.time() - SHARED_DONE_RETENTION),
    )


def _claim_shared_call(artifact: str, adm: Admission) -> tuple[bool, int]:
    """Atomically claim leadership of the next call generation for ``artifact``.

    Returns ``(is_leader, call_gen)``.  A new generation is appended when the
    latest call has already published a result, or when a running call's owner
    is gone past its lease.  A still-live, in-lease running call makes the
    caller a waiter on that generation instead.  The write lock makes this
    compare-and-swap atomic across both worker processes.
    """
    now = time.time()
    with write_tx() as conn:
        row = _latest_row(conn, artifact)
        if row is None:
            gen, takeover = 1, False
        elif row["state"] != "running":
            # The previous call has finished: start a brand-new generation but
            # keep the finished row so waiters on it can still read the result.
            gen, takeover = row["gen"] + 1, False
        else:
            owner_live = _process_is_live(row["leader_pid"])
            expired = now >= row["lease_until"]
            if owner_live and not expired:
                return False, row["gen"]
            gen, takeover = row["gen"] + 1, True
        conn.execute(
            "INSERT INTO shared_calls (artifact, gen, leader_pid, leader_inst, "
            "started_at, lease_until, finished_at, state, result) "
            "VALUES (?, ?, ?, ?, ?, ?, 0, 'running', NULL)",
            (
                artifact,
                gen,
                os.getpid(),
                INSTANCE_ID,
                now,
                now + SHARED_CALL_LEASE,
            ),
        )
        _prune_shared_history(conn, artifact)
        log_event(
            conn,
            artifact,
            "shared_call_taken_over" if takeover else "shared_call_started",
            f"generation {gen}",
        )
        return True, gen


def _read_shared_call(artifact: str, call_gen: Optional[int] = None) -> Optional[sqlite3.Row]:
    """Read one generation (latest by default), including finished history."""
    conn = connect()
    try:
        if call_gen is None:
            return _latest_row(conn, artifact)
        return conn.execute(
            "SELECT * FROM shared_calls WHERE artifact = ? AND gen = ?",
            (artifact, call_gen),
        ).fetchone()
    finally:
        conn.close()


def _finish_shared_call(
    artifact: str, call_gen: int, adm: Admission, outcome: Outcome
) -> tuple[Optional[dict[str, Any]], str]:
    """Leader-only completion: fold into the breaker once and publish the result.

    Runs as a single write transaction so the generation fence, the breaker
    update and the result publication are atomic for waiters in both worker
    processes.  Returns ``(breaker_record, publish_state)`` where
    ``publish_state`` is one of ``published`` / ``stale_gen_skipped``.  A call
    that lost leadership (its generation was superseded by a takeover) is
    fenced: its result neither overwrites the newer call nor changes the
    breaker.  A generation that finished normally is never overwritten.
    """
    with write_tx() as conn:
        row = conn.execute(
            "SELECT * FROM shared_calls WHERE artifact = ? AND gen = ?",
            (artifact, call_gen),
        ).fetchone()
        if row is None or row["state"] != "running":
            log_event(conn, artifact, "shared_call_stale", f"generation {call_gen}")
            return None, "stale_gen_skipped"
        # Exactly one breaker accounting for the whole shared call, and the
        # gateway's upstream-hit counter moves in this same transaction.
        _bump_upstream_hit(conn, adm.role)
        record = _record_outcome_conn(conn, artifact, adm, outcome)
        envelope = {
            "kind": outcome.kind,
            "detail": outcome.detail,
            "status": outcome.status,
            "payload": outcome.payload,
            "breaker_effect": record["effect"],
            "breaker_state": record["state"],
        }
        conn.execute(
            "UPDATE shared_calls SET state = 'done', result = ?, finished_at = ? "
            "WHERE artifact = ? AND gen = ? AND state = 'running'",
            (json.dumps(envelope), time.time(), artifact, call_gen),
        )
        log_event(
            conn,
            artifact,
            "shared_call_completed",
            f"generation {call_gen} {outcome.kind} -> {record['effect']}",
        )
        return record, "published"


# -- in-process fan-out ------------------------------------------------------


def _envelope_from_row(
    row: sqlite3.Row, adm: Admission, flight: LocalFlight
) -> CallEnvelope:
    data = json.loads(row["result"])
    outcome = Outcome(
        data["kind"],
        data["detail"],
        status=data.get("status"),
        payload=data.get("payload"),
    )
    return CallEnvelope(
        outcome=outcome,
        role=adm.role,
        epoch=adm.epoch,
        call_gen=flight.call_gen,
        # This worker is a waiter on another worker's leader call.
        leader=False,
        shared=True,
        breaker_effect=data.get("breaker_effect"),
        breaker_state=data.get("breaker_state"),
    )


def _stale_envelope(flight: LocalFlight, adm: Admission) -> CallEnvelope:
    """Result of a leader whose generation was superseded before it finished."""
    return CallEnvelope(
        outcome=Outcome("failure", "shared call superseded by a newer call"),
        role=adm.role,
        epoch=adm.epoch,
        call_gen=flight.call_gen,
        leader=flight.leader,
        shared=flight.waiters > 0,
        breaker_effect="stale_shared_call_ignored",
        detail_override="superseded",
    )


async def _leader_work(
    artifact: str, flight: LocalFlight, adm: Admission
) -> CallEnvelope:
    """Perform the single upstream call and fold its result in once.

    The driving task is detached from any individual request, so a request
    disconnect never reaches here; only process shutdown cancels it.  In that
    case the breaker bookkeeping is shielded so the attempt is still recorded
    exactly once, leaving the shared call safe to retry.
    """
    started = time.time()
    try:
        outcome = await call_upstream(artifact)
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await asyncio.shield(
                asyncio.to_thread(
                    _finish_shared_call,
                    artifact,
                    flight.call_gen,
                    adm,
                    Outcome("failure", "cancelled"),
                )
            )
        raise
    elapsed = time.time() - started
    record, publish_state = await asyncio.to_thread(
        _finish_shared_call, artifact, flight.call_gen, adm, outcome
    )
    if publish_state == "stale_gen_skipped":
        env = _stale_envelope(flight, adm)
    else:
        env = CallEnvelope(
            outcome=outcome,
            role=adm.role,
            epoch=adm.epoch,
            call_gen=flight.call_gen,
            leader=True,
            shared=flight.waiters > 0,
            breaker_effect=(record or {}).get("effect"),
            breaker_state=(record or {}).get("state"),
            elapsed_ms=round(elapsed * 1000, 1),
        )
    return env


async def _remote_waiter_work(
    artifact: str, flight: LocalFlight, adm: Admission
) -> CallEnvelope:
    """Wait for another worker's leader call, taking over if it dies.

    The waiter always reads the generation it joined, so a newer call starting
    on the same artifact neither supersedes it nor changes what result it gets.
    """
    deadline = time.time() + SHARED_WAIT_BUDGET
    while True:
        await asyncio.sleep(SHARED_POLL_INTERVAL)
        row = await asyncio.to_thread(_read_shared_call, artifact, flight.call_gen)
        if row is not None and row["state"] == "done" and row["result"]:
            return _envelope_from_row(row, adm, flight)
        if row is None:
            break  # our row vanished (pruned/reset): fall through and take over
        now = time.time()
        if now >= deadline:
            return CallEnvelope(
                outcome=Outcome(
                    "failure",
                    f"shared call wait budget ({SHARED_WAIT_BUDGET:.0f}s) exhausted",
                ),
                role=adm.role,
                epoch=adm.epoch,
                call_gen=flight.call_gen,
                leader=False,
                shared=True,
                breaker_effect="wait_budget_exhausted",
                detail_override="wait_budget",
            )
        if now >= row["lease_until"] or not _process_is_live(row["leader_pid"]):
            break  # leader dead/overdue: take over below

    # Take over leadership of the same artifact with a fresh generation.
    is_leader, new_gen = await asyncio.to_thread(_claim_shared_call, artifact, adm)
    if not is_leader:
        # Someone else claimed the next generation, or our row simply finished:
        # re-read and keep waiting on whichever generation now belongs to us.
        row = await asyncio.to_thread(_read_shared_call, artifact, new_gen)
        if row is not None and row["state"] == "done" and row["result"]:
            flight.call_gen = new_gen
            return _envelope_from_row(row, adm, flight)
        flight.call_gen = new_gen
        return await _remote_waiter_work(artifact, flight, adm)
    flight.leader = True
    flight.call_gen = new_gen
    return await _leader_work(artifact, flight, adm)


async def _drive(artifact: str, flight: LocalFlight, adm: Admission) -> None:
    """Detached driver: run the shared work and resolve every local waiter.

    The task is owned by :data:`BACKGROUND_TASKS`, not by the originating
    request, so it runs to completion (or shutdown) even if every local
    request disconnects.  Cross-process waiters rely solely on the published
    ``shared_calls`` row, which this task always writes.
    """
    try:
        try:
            if flight.leader:
                envelope = await _leader_work(artifact, flight, adm)
            else:
                envelope = await _remote_waiter_work(artifact, flight, adm)
        except asyncio.CancelledError:
            # Worker shutdown: the leader's bookkeeping was already shielded
            # inside _leader_work; just let the task finish.
            if not flight.future.done():
                flight.future.cancel()
            raise
        except Exception as exc:  # defensive: never leave waiters hanging
            envelope = CallEnvelope(
                outcome=Outcome("failure", f"{type(exc).__name__}: {exc}"[:300]),
                role=adm.role,
                epoch=adm.epoch,
                call_gen=flight.call_gen,
                leader=flight.leader,
                shared=flight.waiters > 0,
                breaker_effect="driver_error",
            )
        if not flight.future.done():
            flight.future.set_result(envelope)
    finally:
        async with flight_lock():
            if LOCAL_FLIGHTS.get(artifact) is flight:
                LOCAL_FLIGHTS.pop(artifact, None)


async def coalesced_meta(artifact: str, adm: Admission) -> tuple[CallEnvelope, bool]:
    """Join (or create) the single in-flight call for ``artifact``.

    The breaker admission has already happened; only allowed requests reach
    here.  Returns ``(envelope, joined_existing)`` where ``joined_existing``
    means this request was answered by a call another request initiated.
    """
    async with flight_lock():
        flight = LOCAL_FLIGHTS.get(artifact)
        if flight is not None:
            # Same-process single-flight: attach to the existing local flight.
            flight.waiters += 1
            joined = True
        else:
            # No local flight: race the other worker process for leadership in
            # SQLite (atomic compare-and-swap).
            is_leader, call_gen = await asyncio.to_thread(
                _claim_shared_call, artifact, adm
            )
            future: asyncio.Future[CallEnvelope] = asyncio.get_running_loop().create_future()
            flight = LocalFlight(
                artifact=artifact,
                call_gen=call_gen,
                leader=is_leader,
                epoch=adm.epoch,
                future=future,
                # The creator itself is the first participant; extra local
                # joiners increment ``waiters``.
                waiters=0,
            )
            LOCAL_FLIGHTS[artifact] = flight
            # Detached driver: cancelling this request must not cancel the call.
            _spawn_driver(artifact, flight, adm)
            joined = False

    try:
        # Shield the shared future: cancelling one waiting request must not
        # cancel the flight for anybody else.  Only this coroutine's wait is
        # interrupted; the detached driver keeps the call running.
        envelope = await asyncio.shield(flight.future)
    except asyncio.CancelledError:
        # This participant went away; the shared call keeps running for the
        # other participants (and for cross-process waiters via the DB row) and
        # remains safe to retry afterwards.
        async with flight_lock():
            if joined:
                flight.waiters = max(0, flight.waiters - 1)
        raise
    return envelope, joined


async def _probe_only(artifact: str, adm: Admission) -> CallEnvelope:
    """HALF_OPEN probe path: a globally unique call that never coalesces."""
    started = time.time()
    try:
        outcome = await call_upstream(artifact)
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await asyncio.shield(
                asyncio.to_thread(
                    _finish, artifact, adm, Outcome("failure", "cancelled"), None
                )
            )
        raise
    elapsed = time.time() - started
    effect = await asyncio.to_thread(_finish, artifact, adm, outcome, adm.role)
    return CallEnvelope(
        outcome=outcome,
        role="probe",
        epoch=adm.epoch,
        call_gen=0,
        leader=True,
        shared=False,
        breaker_effect=effect["effect"],
        breaker_state=effect["state"],
        elapsed_ms=round(elapsed * 1000, 1),
    )
    return await flight.future


# ---------------------------------------------------------------------------
# Auxiliary shared counters
# ---------------------------------------------------------------------------


def _bump_upstream_hit(conn: sqlite3.Connection, role: str) -> None:
    row = conn.execute("SELECT hits, hits_by_mode FROM upstream_stats WHERE id = 1").fetchone()
    try:
        by_mode = json.loads(row["hits_by_mode"] or "{}")
    except ValueError:
        by_mode = {}
    by_mode[role] = by_mode.get(role, 0) + 1
    conn.execute(
        "UPDATE upstream_stats SET hits = ?, hits_by_mode = ? WHERE id = 1",
        (row["hits"] + 1, json.dumps(by_mode, sort_keys=True)),
    )


def _bump_shared_join() -> None:
    """Count one request that was answered by an in-flight coalesced call."""
    with write_tx() as conn:
        conn.execute(
            "UPDATE upstream_stats SET shared_joins = shared_joins + 1 WHERE id = 1"
        )


def get_gateway_hits() -> dict[str, Any]:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT hits, hits_by_mode, shared_joins FROM upstream_stats WHERE id = 1"
        ).fetchone()
        return {
            "hits": row["hits"],
            "by_mode": json.loads(row["hits_by_mode"] or "{}"),
            "shared_joins": row["shared_joins"],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    clean_stale_probes()
    try:
        yield
    finally:
        # Cancel detached shared calls first (their breaker bookkeeping is
        # shielded), then close the pooled HTTP client.
        await shutdown_flights()
        await close_upstream_client()


app = FastAPI(title="Artifact Metadata Circuit-Breaker Gateway", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "pid": str(os.getpid())}


def _finish(artifact: str, adm: Admission, outcome: Outcome, hit_role: Optional[str]) -> dict[str, Any]:
    try:
        return record_outcome(artifact, adm, outcome, hit_role=hit_role)
    finally:
        if adm.holder:
            ACTIVE_PROBES.discard(adm.holder)


def _envelope_to_response(
    artifact: str, adm: Admission, env: CallEnvelope, joined_shared: bool
) -> JSONResponse:
    outcome = env.outcome
    if env.detail_override == "wait_budget":
        # The local waiter gave up; the shared call is still in flight and will
        # still be folded into the breaker once.  Do not double-count here.
        return JSONResponse(
            status_code=502,
            content={
                "artifact": artifact,
                "role": adm.role,
                "upstream": "failure",
                "detail": outcome.detail,
                "breaker_effect": env.breaker_effect,
                "breaker_state": None,
                "shared": False,
                "call_leader": False,
                "call_gen": env.call_gen,
                "pid": os.getpid(),
            },
            headers={"X-Breaker-Role": adm.role, "X-Worker-Pid": str(os.getpid())},
        )

    body: dict[str, Any] = {
        "artifact": artifact,
        "role": env.role,
        "upstream": outcome.kind,
        "detail": outcome.detail,
        "elapsed_ms": env.elapsed_ms,
        "breaker_effect": env.breaker_effect,
        "breaker_state": env.breaker_state,
        # True when this request piggy-backed on a flight already established
        # inside this worker (a remote worker's waiter creates its own local
        # flight, so it reports shared=false but call_leader=false).
        "shared": joined_shared,
        # Only the single request that actually drives the upstream call is the
        # leader; local joiners and cross-process waiters are not.
        "call_leader": env.leader and not joined_shared,
        "call_gen": env.call_gen,
        "pid": os.getpid(),
    }
    if outcome.payload is not None:
        body["metadata"] = outcome.payload

    headers = {
        "X-Breaker-Role": env.role,
        "X-Worker-Pid": str(os.getpid()),
        "X-Coalesced": "1" if joined_shared else "0",
        "X-Call-Generation": str(env.call_gen),
    }
    if outcome.kind == "success":
        return JSONResponse(status_code=outcome.status or 200, content=body, headers=headers)
    return JSONResponse(status_code=502, content=body, headers=headers)


@app.get("/api/meta/{artifact:path}")
async def get_meta(artifact: str) -> JSONResponse:
    artifact = artifact.strip()
    if not artifact:
        raise HTTPException(status_code=400, detail="artifact name required")

    # 1) Breaker admission first, unchanged: rejected requests never reach the
    #    coalescing layer and never contact the upstream.
    try:
        # SQLite writes serialise across processes; run them off the event loop
        # so a request waiting on the write lock never stalls other queries in
        # the same worker.
        adm = await asyncio.to_thread(admit, artifact)
    except sqlite3.Error as exc:
        raise HTTPException(status_code=503, detail=f"breaker store unavailable: {exc}") from exc
    if not adm.allowed:
        return JSONResponse(
            status_code=503,
            content={
                "artifact": artifact,
                "error": "circuit_open",
                "state": adm.state,
                "retry_after": round(adm.retry_after, 3),
                "pid": os.getpid(),
            },
            headers={"Retry-After": f"{max(0.0, adm.retry_after):.2f}"},
        )

    # 2) Single-flight: same-name admitted requests share one directory call
    #    until it completes; different artifacts remain independent.
    try:
        if adm.role == "probe":
            env = await _probe_only(artifact, adm)
            joined_shared = False
        else:
            env, joined_shared = await coalesced_meta(artifact, adm)
    except asyncio.CancelledError:
        # Client disconnect / worker shutdown.  Probe bookkeeping is handled in
        # _probe_only; shared calls keep running for their other waiters.
        raise

    if joined_shared:
        # One extra request was served without an additional upstream call.
        with contextlib.suppress(sqlite3.Error):
            await asyncio.to_thread(_bump_shared_join)

    return _envelope_to_response(artifact, adm, env, joined_shared)


@app.get("/api/breakers")
async def breakers() -> dict[str, Any]:
    return await asyncio.to_thread(_snapshot)


def _snapshot() -> dict[str, Any]:
    now = time.time()
    conn = connect()
    try:
        rows = conn.execute("SELECT * FROM breakers ORDER BY artifact").fetchall()
        items = []
        for row in rows:
            state, opened_at = effective_state(row, now)
            remaining = (
                max(0.0, COOLDOWN - (now - opened_at)) if state == OPEN else 0.0
            )
            items.append(
                {
                    "artifact": row["artifact"],
                    "state": state,
                    "failures": row["failures"],
                    "opened_at": round(row["opened_at"], 3),
                    "cooldown_remaining": round(remaining, 3),
                    "probe_gen": row["probe_gen"],
                    "probe_holder": row["probe_holder"],
                    "epoch": row["epoch"],
                    "last_error": row["last_error"],
                }
            )
        events = [
            dict(r)
            for r in conn.execute(
                "SELECT ts, artifact, kind, detail FROM events ORDER BY id DESC LIMIT 40"
            ).fetchall()
        ]
    finally:
        conn.close()
    return {
        "now": now,
        "cooldown": COOLDOWN,
        "threshold": FAILURE_THRESHOLD,
        "timeout": REQUEST_TIMEOUT,
        "breakers": items,
        "events": events,
        "gateway_upstream": get_gateway_hits(),
        "pid": os.getpid(),
    }


@app.post("/api/reset")
async def reset() -> dict[str, Any]:
    await asyncio.to_thread(_reset)
    return {"ok": True}


def _reset() -> None:
    with write_tx() as conn:
        conn.execute("DELETE FROM breakers")
        conn.execute("DELETE FROM events")
        conn.execute("DELETE FROM shared_calls")
        conn.execute(
            "UPDATE upstream_stats SET hits = 0, hits_by_mode = '{}', shared_joins = 0 "
            "WHERE id = 1"
        )
        log_event(conn, "*", "reset")


# -- control-plane proxy to the example upstream ----------------------------


async def _proxy(method: str, path: str, payload: Any = None) -> JSONResponse:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(3.0)) as client:
            resp = await client.request(method, f"{UPSTREAM_URL}{path}", json=payload)
        try:
            content = resp.json()
        except ValueError:
            content = {"raw": resp.text[:1000]}
        return JSONResponse(status_code=resp.status_code, content=content)
    except Exception as exc:
        return JSONResponse(status_code=502, content={"error": str(exc)[:300]})


@app.get("/api/upstream-mode")
async def get_upstream_mode() -> JSONResponse:
    return await _proxy("GET", "/control/mode")


@app.post("/api/upstream-mode")
async def set_upstream_mode(request: Request) -> JSONResponse:
    payload = await request.json()
    return await _proxy(
        "POST",
        "/control/mode",
        {"mode": payload.get("mode"), "delay": payload.get("delay")},
    )


@app.get("/api/upstream-stats")
async def upstream_stats() -> JSONResponse:
    return await _proxy("GET", "/control/stats")


@app.post("/api/upstream-stats/reset")
async def reset_upstream_stats() -> JSONResponse:
    return await _proxy("POST", "/control/reset")


@app.post("/api/upstream-script")
async def set_upstream_script(request: Request) -> JSONResponse:
    """Install scripted upstream responses (used by the automated tests)."""
    payload = await request.json()
    return await _proxy("POST", "/control/script", payload)


# -- web UI ----------------------------------------------------------------


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
