"""HTTP-level tests for in-flight request coalescing.

Runs against the live two-worker deployment (Docker Compose or a local pair of
uvicorn processes), exactly like tests/test_breaker.py:

    GATEWAY=http://127.0.0.1:8000 python -m pytest -q tests/test_coalescing_http.py

The two uvicorn workers are separate processes, so these tests exercise the
cross-process leadership election in shared_calls, not just in-process fan-out.
"""

from __future__ import annotations

import concurrent.futures
import time

import httpx
import pytest

import os

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:8000").rstrip("/")
ART = "example-app.json"
OTHER = "shared-lib.json"
CLIENT = httpx.Client(timeout=20.0)


def reset() -> None:
    CLIENT.post(f"{GATEWAY}/api/reset")
    CLIENT.post(f"{GATEWAY}/api/upstream-stats/reset")
    CLIENT.post(f"{GATEWAY}/api/upstream-script", json={"responses": []})
    CLIENT.post(f"{GATEWAY}/api/upstream-mode", json={"mode": "normal"})


def set_mode(mode: str, delay: float | None = None) -> None:
    payload: dict[str, object] = {"mode": mode}
    if delay is not None:
        payload["delay"] = delay
    r = CLIENT.post(f"{GATEWAY}/api/upstream-mode", json=payload)
    assert r.status_code == 200, r.text


def upstream_hits() -> int:
    return CLIENT.get(f"{GATEWAY}/api/upstream-stats").json()["hits"]


def gateway_stats() -> dict:
    return CLIENT.get(f"{GATEWAY}/api/breakers").json()["gateway_upstream"]


def breaker(artifact: str = ART) -> dict:
    data = CLIENT.get(f"{GATEWAY}/api/breakers").json()
    return next(b for b in data["breakers"] if b["artifact"] == artifact)


def concurrent_meta(n: int, artifact: str = ART) -> list[httpx.Response]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        return list(
            pool.map(
                lambda _: CLIENT.get(f"{GATEWAY}/api/meta/{artifact}"), range(n)
            )
        )


@pytest.fixture(autouse=True)
def _reset():
    reset()
    yield


def test_concurrent_same_name_becomes_one_upstream_call_across_workers():
    set_mode("delay", 0.5)
    results = concurrent_meta(12)
    assert all(r.status_code == 200 for r in results)
    assert upstream_hits() == 1, "all twelve same-name lookups share one directory call"
    bodies = [r.json() for r in results]
    assert sum(1 for b in bodies if b["call_leader"]) == 1
    assert len({b["call_gen"] for b in bodies}) == 1
    # Requests really were spread over both worker processes.
    assert len({b["pid"] for b in bodies}) == 2
    stats = gateway_stats()
    assert stats["hits"] == 1
    # Local joiners (same worker) are counted; cross-process waiters are not,
    # so the exact split depends on the kernel's load balancing.
    assert 0 <= stats["shared_joins"] <= 11


def test_distinct_artifacts_remain_parallel_and_separate():
    set_mode("delay", 0.5)
    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(
                lambda name: CLIENT.get(f"{GATEWAY}/api/meta/{name}"),
                [ART, OTHER, "reports-tool.json", "missing-artifact.json"],
            )
        )
    wall = time.time() - started
    assert upstream_hits() == 4
    assert wall < 1.2, f"distinct artifacts must run in parallel, took {wall:.2f}s"
    assert {r.status_code for r in results} == {200, 404}


def test_one_coalesced_failure_counts_once_and_trips_after_three_batches():
    # Slow failing responses keep every batch's call in flight long enough for
    # all workers to join, so each batch is provably a single upstream call.
    CLIENT.post(
        f"{GATEWAY}/api/upstream-script",
        json={"responses": [{"status": 503, "delay": 0.4}] * 6},
    )
    tripping = None
    for batch in range(3):
        results = concurrent_meta(10)
        assert upstream_hits() == batch + 1
        assert sum(1 for r in results if r.json().get("call_leader")) == 1
        tripping = results
    b = breaker()
    assert b["state"] == "open" and b["failures"] == 3
    # Every waiter already joined to the tripping call receives that failure
    # (the single call's outcome, shared) and sees the breaker open.
    assert all(r.status_code == 502 for r in tripping)
    assert all(r.json()["breaker_effect"] == "opened" for r in tripping)
    # Later queries are rejected at admission and never reach the upstream.
    rejected = concurrent_meta(10)
    assert all(r.status_code == 503 for r in rejected)
    assert upstream_hits() == 3


def test_sequential_lookups_advance_call_generation_and_reuse_row():
    set_mode("normal")
    first = CLIENT.get(f"{GATEWAY}/api/meta/{ART}").json()
    second = CLIENT.get(f"{GATEWAY}/api/meta/{ART}").json()
    assert first["call_gen"] == 1 and second["call_gen"] == 2
    assert first["call_leader"] and second["call_leader"]
    assert upstream_hits() == 2


def test_probe_is_never_coalesced():
    set_mode("503")
    for _ in range(3):
        CLIENT.get(f"{GATEWAY}/api/meta/{ART}")
    assert breaker()["state"] == "open"
    time.sleep(2.1)
    # A slow healthy probe stays in flight so every competing request arrives
    # while the breaker is still half-open and must be rejected.
    CLIENT.post(
        f"{GATEWAY}/api/upstream-script",
        json={"responses": [{"status": 200, "delay": 0.5}]},
    )
    results = concurrent_meta(10)
    roles = [r.json().get("role") for r in results]
    assert roles.count("probe") == 1, "the single recovery probe must not be shared"
    assert sum(1 for r in results if r.status_code == 503) == 9
    assert breaker()["state"] == "closed"


def test_waiter_disconnect_does_not_abort_the_shared_call():
    set_mode("delay", 0.6)

    def impatient(_):
        with httpx.Client(timeout=0.1) as client:
            try:
                return client.get(f"{GATEWAY}/api/meta/{ART}")
            except httpx.TimeoutException:
                return None

    def patient(_):
        with httpx.Client(timeout=10) as client:
            return client.get(f"{GATEWAY}/api/meta/{ART}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        gone = list(pool.map(impatient, range(3)))
        survivors = list(pool.map(patient, range(5)))
    assert gone.count(None) == 3
    assert all(r is not None and r.status_code == 200 for r in survivors)
    assert upstream_hits() == 1, "the detached shared call survives disconnects"
    assert gateway_stats()["hits"] == 1


def test_leader_request_disconnect_keeps_other_workers_waiters_alive():
    set_mode("delay", 0.6)

    def quick_leader(_):
        with httpx.Client(timeout=0.08) as client:
            try:
                client.get(f"{GATEWAY}/api/meta/{ART}")
            except httpx.TimeoutException:
                return "disconnected"
            return "finished"

    def follower(_):
        with httpx.Client(timeout=10) as client:
            r = client.get(f"{GATEWAY}/api/meta/{ART}")
            return r.status_code, r.json()["pid"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        leader_outcome = list(pool.map(quick_leader, range(2)))
        follower_outcomes = list(pool.map(follower, range(6)))
    assert "disconnected" in leader_outcome
    assert all(code == 200 for code, _ in follower_outcomes)
    assert upstream_hits() == 1, "the detached shared call survives a disconnecting leader"


def test_counters_distinguish_forwarded_calls_and_shared_joins():
    set_mode("delay", 0.4)
    concurrent_meta(8)
    stats = gateway_stats()
    assert stats["hits"] == 1  # one real directory call, accounted once
    # Every admitted request is either the one leader call or a join; cross-
    # process waiters are leaders of their own local flight but still share the
    # single upstream call, so joins are bounded by the remaining seven.
    assert 0 <= stats["shared_joins"] <= 7
    assert upstream_hits() == 1
