"""In-process tests for in-flight request coalescing (single-flight).

These exercise both layers of the design without a running server:

* the in-process fan-out (``LOCAL_FLIGHTS``) and
* the cross-process leadership election / generation fencing stored in SQLite.

No third-party async pytest plugin is needed; every scenario is driven through
``asyncio.run``.  The upstream HTTP call is replaced by an in-memory fake, so
the tests count exactly how many directory calls would have happened.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
import time
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture()
def gw(tmp_path, monkeypatch):
    monkeypatch.setenv("GATEWAY_DB", str(tmp_path / "breaker.db"))
    monkeypatch.setenv("REQUEST_TIMEOUT", "1.0")
    monkeypatch.setenv("COOLDOWN", "2.0")
    monkeypatch.setenv("FAILURE_THRESHOLD", "3")
    monkeypatch.setenv("UPSTREAM_URL", "http://127.0.0.1:1")
    import app as app_module

    importlib.reload(app_module)
    app_module.init_db()

    calls: list[float] = []

    def make_handler(delay: float = 0.2, status: int = 200):
        async def fake_get(url):
            calls.append(time.time())
            await asyncio.sleep(delay)
            if status >= 500:
                return httpx.Response(status, text="boom")
            if status >= 400:
                return httpx.Response(status, json={"error": "missing"})
            return httpx.Response(status, json={"artifact": "a", "v": 1})

        return fake_get

    app_module._calls = calls  # type: ignore[attr-defined]
    app_module._make_handler = make_handler  # type: ignore[attr-defined]
    return app_module


def _admit(gw, artifact="a"):
    return gw.admit(artifact)


def test_concurrent_same_artifact_shares_one_upstream_call(gw):
    gw._get = gw._make_handler(delay=0.3)

    async def scenario():
        adms = [gw.admit("a") for _ in range(10)]
        assert all(a.allowed and a.role == "normal" for a in adms)
        results = await asyncio.gather(
            *(gw.coalesced_meta("a", adm) for adm in adms)
        )
        return results

    results = asyncio.run(scenario())
    assert len(gw._calls) == 1, f"expected exactly one upstream call, got {len(gw._calls)}"
    joined = [joined for _, joined in results]
    assert joined.count(False) == 1  # one leader
    assert joined.count(True) == 9   # nine waiters
    envs = [env for env, _ in results]
    assert all(e.outcome.kind == "success" for e in envs)
    assert all(e.call_gen == 1 for e in envs)
    # Exactly one request drives the upstream call; the nine local joiners are
    # followers even though they share the driver's envelope.
    leaders = [
        env.leader and not joined for env, joined in results
    ]
    assert leaders.count(True) == 1
    # The breaker is still closed and the failure counter never moved.
    row = gw.connect().execute("SELECT * FROM breakers WHERE artifact='a'").fetchone()
    assert row["state"] == gw.CLOSED and row["failures"] == 0
    # Gateway forwarding counter moved once; join counter is bumped by the HTTP
    # layer, so here it stays at zero.
    assert gw.get_gateway_hits()["hits"] == 1
    assert gw.get_gateway_hits()["shared_joins"] == 0


def test_different_artifacts_stay_parallel(gw):
    gw._get = gw._make_handler(delay=0.3)

    async def scenario():
        names = ["a", "b", "c", "d"]
        adms = [(n, gw.admit(n)) for n in names]
        started = time.time()
        results = await asyncio.gather(
            *(gw.coalesced_meta(n, adm) for n, adm in adms)
        )
        wall = time.time() - started
        return results, wall

    results, wall = asyncio.run(scenario())
    assert len(gw._calls) == 4, "each distinct artifact gets its own call"
    # Four independent 0.3s calls must overlap rather than serialise.
    assert wall < 0.9, f"distinct artifacts should run in parallel, took {wall:.2f}s"
    assert all(env.outcome.kind == "success" for env, _ in results)


def test_shared_failure_counts_once_and_trips_after_three_calls(gw):
    # Every shared call fails; three coalesced batches must trip the breaker,
    # and a batch of N waiters may add at most one failure each.
    gw._get = gw._make_handler(delay=0.05, status=503)

    async def batch(n):
        adms = [gw.admit("a") for _ in range(n)]
        adms = [a for a in adms if a.allowed]
        pairs = await asyncio.gather(*(gw.coalesced_meta("a", a) for a in adms))
        return [env for env, _ in pairs]

    first = asyncio.run(batch(5))
    assert len(gw._calls) == 1
    assert all(e.outcome.kind == "failure" for e in first)
    assert all(e.breaker_effect == "failure_counted" for e in first)
    row = gw.connect().execute("SELECT * FROM breakers WHERE artifact='a'").fetchone()
    assert row["failures"] == 1  # five requests, one counted failure

    asyncio.run(batch(5))
    assert len(gw._calls) == 2
    third = asyncio.run(batch(5))
    assert len(gw._calls) == 3
    assert all(e.breaker_effect == "opened" for e in third)
    row = gw.connect().execute("SELECT * FROM breakers WHERE artifact='a'").fetchone()
    assert row["state"] == gw.OPEN and row["failures"] == 3
    assert gw.get_gateway_hits()["hits"] == 3

    # Once open, later queries are rejected at admission and never coalesce.
    later = gw.admit("a")
    assert not later.allowed
    assert len(gw._calls) == 3


def test_waiters_all_receive_the_trip_failure(gw):
    """If the shared call trips the breaker, joined waiters get that failure."""
    # Arrange two earlier failures first, then one big coalesced failing batch.
    gw._get = gw._make_handler(delay=0.05, status=503)

    async def preheat():
        for _ in range(2):
            adm = gw.admit("a")
            env, _ = await gw.coalesced_meta("a", adm)
            assert env.outcome.kind == "failure"

    asyncio.run(preheat())

    async def batch():
        adms = [gw.admit("a") for _ in range(6)]
        pairs = await asyncio.gather(*(gw.coalesced_meta("a", a) for a in adms))
        return [env for env, _ in pairs]

    envs = asyncio.run(batch())
    assert len(gw._calls) == 3
    assert all(e.breaker_effect == "opened" for e in envs), "every waiter sees the trip"
    assert all(e.outcome.kind == "failure" for e in envs)


def test_sequential_calls_advance_generation_and_reuse_done_row(gw):
    gw._get = gw._make_handler(delay=0.01)

    async def once():
        adm = gw.admit("a")
        env, joined = await gw.coalesced_meta("a", adm)
        return env, joined

    e1, j1 = asyncio.run(once())
    e2, j2 = asyncio.run(once())
    assert (j1, j2) == (False, False)
    assert e1.call_gen == 1 and e2.call_gen == 2
    row = gw._read_shared_call("a")
    assert row["state"] == "done" and row["gen"] == 2
    assert len(gw._calls) == 2


def test_stale_leader_result_cannot_overwrite_newer_call(gw):
    """A late result from an older generation is fenced off entirely."""
    gw._get = gw._make_handler(delay=0.3)
    adm = gw.admit("a")
    # Generation 1 owned by a dead process past its lease, mirroring a leader
    # whose worker exited before publishing anything.
    past = time.time() - 5
    with gw.write_tx() as conn:
        conn.execute(
            "INSERT INTO shared_calls (artifact, gen, leader_pid, leader_inst, "
            "started_at, lease_until, finished_at, state, result) "
            "VALUES ('a', 1, 999999, 'dead', ?, ?, 0, 'running', NULL)",
            (past, past + 2),
        )

    # A takeover inserts a separate generation-2 row (history rows are keyed by
    # (artifact, gen)) and publishes a success.
    adm2 = gw.admit("a")
    leader2, gen2 = gw._claim_shared_call("a", adm2)
    assert leader2 and gen2 == 2
    record2, state2 = gw._finish_shared_call(
        "a", 2, adm2, gw.Outcome("success", "200", payload={"v": "new"})
    )
    assert state2 == "published"

    # The old generation 1 now reports late.  Its row was superseded by the
    # generation-2 takeover (simulate the takeover marking gen 1 superseded),
    # so it must not overwrite gen 2 nor touch the breaker.
    with gw.write_tx() as conn:
        conn.execute(
            "UPDATE shared_calls SET state = 'superseded' WHERE artifact = 'a' AND gen = 1"
        )
    record, state = gw._finish_shared_call(
        "a", 1, adm, gw.Outcome("failure", "late 503")
    )
    assert state == "stale_gen_skipped" and record is None
    row = gw._read_shared_call("a", 2)
    assert row["gen"] == 2 and row["state"] == "done"
    assert '"new"' in row["result"]
    # Generation 1's row stays untouched, never carrying the new call's result.
    row1 = gw._read_shared_call("a", 1)
    assert row1["gen"] == 1 and row1["result"] is None
    breaker = gw.connect().execute(
        "SELECT * FROM breakers WHERE artifact='a'"
    ).fetchone()
    assert breaker["failures"] == 0  # the late failure must not count


def test_waiter_takes_over_after_leader_death(gw, monkeypatch):
    """A dead leader's running row is taken over; waiters still get a result."""
    gw._get = gw._make_handler(delay=0.05)
    adm = gw.admit("a")

    # Insert a running row owned by a definitely-dead pid.
    with gw.write_tx() as conn:
        conn.execute(
            "INSERT INTO shared_calls (artifact, gen, leader_pid, leader_inst, "
            "started_at, lease_until, state, result) VALUES ('a', 1, 999999, 'x', "
            "?, ?, 'running', NULL)",
            (time.time() - 10, time.time() - 9),
        )

    # A fresh claim must take over with generation 2.
    is_leader, gen = gw._claim_shared_call("a", adm)
    assert is_leader and gen == 2

    async def drive_as_waiter():
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        flight = gw.LocalFlight(
            artifact="a", call_gen=2, leader=True, epoch=adm.epoch, future=fut
        )
        task = asyncio.create_task(gw._drive("a", flight, adm))
        return await fut, task

    env, _ = asyncio.run(drive_as_waiter())
    assert env.outcome.kind == "success"
    assert env.call_gen == 2 and env.leader is True
    assert len(gw._calls) == 1
    row = gw._read_shared_call("a")
    assert row["state"] == "done" and row["gen"] == 2


def test_lease_expiry_allows_takeover_while_owner_absent(gw):
    adm = gw.admit("a")
    now = time.time()
    with gw.write_tx() as conn:
        conn.execute(
            "INSERT INTO shared_calls (artifact, gen, leader_pid, leader_inst, "
            "started_at, lease_until, state, result) VALUES ('a', 1, ?, ?, ?, ?, "
            "'running', NULL)",
            (999998, "dead", now - 10, now - 9),
        )
    is_leader, gen = gw._claim_shared_call("a", adm)
    assert is_leader and gen == 2


def test_waiter_cancel_does_not_cancel_shared_call(gw):
    """A disconnecting waiter leaves the flight usable for the others."""
    gw._get = gw._make_handler(delay=0.3)
    adm1 = gw.admit("a")
    adm2 = gw.admit("a")

    async def scenario():
        env_task = asyncio.create_task(gw.coalesced_meta("a", adm1))
        await asyncio.sleep(0.05)  # let the leader flight establish itself
        waiter = asyncio.create_task(gw.coalesced_meta("a", adm2))
        await asyncio.sleep(0.05)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        leader_env, joined = await env_task
        return leader_env, joined

    env, joined = asyncio.run(scenario())
    assert joined is False  # the first participant is the leader
    assert env.outcome.kind == "success"
    assert len(gw._calls) == 1, "cancelling a waiter must not abort the shared call"


def test_shared_call_row_cleaned_on_reset(gw):
    adm = gw.admit("a")
    gw._claim_shared_call("a", adm)
    assert gw._read_shared_call("a") is not None
    gw._reset()
    assert gw._read_shared_call("a") is None
    assert gw.get_gateway_hits()["shared_joins"] == 0
