"""Self-tests for the circuit-breaker gateway.

Run against a live deployment (the Docker Compose stack or a locally started
pair of uvicorn processes):

    GATEWAY=http://127.0.0.1:8000 python -m pytest -q tests/test_breaker.py

The tests use only the gateway's public control proxy (``/api/upstream-*``),
so they work whether the example upstream is reachable directly or only from
inside the Compose network.  Each test resets breakers and upstream counters
first.  The persistence test additionally needs ``GATEWAY_RESTART_CMD`` (a
shell command restarting the gateway) and is skipped otherwise.
"""

from __future__ import annotations

import concurrent.futures
import os
import subprocess
import time

import httpx
import pytest

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:8000").rstrip("/")
RESTART_CMD = os.environ.get("GATEWAY_RESTART_CMD", "")

ART = "example-app.json"
CLIENT = httpx.Client(timeout=15.0)


def reset() -> None:
    CLIENT.post(f"{GATEWAY}/api/reset")
    CLIENT.post(f"{GATEWAY}/api/upstream-stats/reset")
    set_mode("normal")
    CLIENT.post(f"{GATEWAY}/api/upstream-script", json={"responses": []})


def set_mode(mode: str, delay: float | None = None) -> None:
    payload: dict[str, object] = {"mode": mode}
    if delay is not None:
        payload["delay"] = delay
    r = CLIENT.post(f"{GATEWAY}/api/upstream-mode", json=payload)
    assert r.status_code == 200, r.text


def script(responses: list[dict]) -> None:
    r = CLIENT.post(f"{GATEWAY}/api/upstream-script", json={"responses": responses})
    assert r.status_code == 200, r.text


def meta(artifact: str = ART) -> httpx.Response:
    return CLIENT.get(f"{GATEWAY}/api/meta/{artifact}")


def breaker(artifact: str = ART) -> dict:
    data = CLIENT.get(f"{GATEWAY}/api/breakers").json()
    return next(b for b in data["breakers"] if b["artifact"] == artifact)


def gateway_hits() -> int:
    return CLIENT.get(f"{GATEWAY}/api/breakers").json()["gateway_upstream"]["hits"]


def upstream_hits() -> int:
    return CLIENT.get(f"{GATEWAY}/api/upstream-stats").json()["hits"]


@pytest.fixture(autouse=True)
def _reset():
    reset()
    yield


def test_healthy_lookup_returns_metadata_and_is_spread_across_workers():
    pids: set[int] = set()

    def one(_: int) -> dict:
        with httpx.Client(timeout=10.0, headers={"Connection": "close"}) as client:
            r = client.get(f"{GATEWAY}/api/meta/{ART}")
        assert r.status_code == 200, r.text
        return r.json()

    # Concurrent requests are what the OS load-balances across the two
    # SO_REUSEPORT workers; sequential keep-alive requests may pin to one.
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        for body in pool.map(one, range(12)):
            assert body["upstream"] == "success"
            assert body["metadata"]["metadata"]["name"] == "example-app"
            pids.add(body["pid"])
    assert len(pids) == 2, f"expected both uvicorn workers to serve, got {pids}"


def test_three_consecutive_failures_open_circuit_and_block_upstream():
    set_mode("503")
    codes = [meta().status_code for _ in range(3)]
    assert codes == [502, 502, 502]
    assert breaker()["state"] == "open"
    assert breaker()["failures"] == 3

    hits_before = upstream_hits()
    for _ in range(5):
        r = meta()
        assert r.status_code == 503
        assert r.json()["error"] == "circuit_open"
    assert upstream_hits() == hits_before, "open circuit must not contact upstream"


def test_success_resets_consecutive_failure_count():
    set_mode("503")
    assert meta().status_code == 502
    assert meta().status_code == 502
    set_mode("normal")
    assert meta().status_code == 200
    assert breaker()["failures"] == 0
    set_mode("503")
    assert meta().status_code == 502
    assert meta().status_code == 502
    assert breaker()["state"] == "closed", "two failures after reset must not trip"
    assert meta().status_code == 502
    assert breaker()["state"] == "open"


def test_connect_failure_counts_as_failure():
    # Connect errors are covered deterministically in the unit tests
    # (tests/test_unit.py) by pointing the gateway at a closed port.
    pytest.skip("covered by tests/test_unit.py::test_connect_error_and_timeout_classification")


def test_timeout_is_a_failure_and_bounded():
    set_mode("delay", 3.0)
    started = time.time()
    r = meta()
    elapsed = time.time() - started
    assert r.status_code == 502
    assert 0.9 <= elapsed < 2.0, f"call must be bounded to ~1s, took {elapsed:.2f}s"
    assert "Timeout" in r.json()["detail"] or "timeout" in r.json()["detail"].lower()


def test_cooldown_admits_exactly_one_probe():
    set_mode("503")
    for _ in range(3):
        meta()
    assert breaker()["state"] == "open"
    time.sleep(2.1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(lambda _: meta(), range(10)))

    roles = [r.json().get("role") for r in results if r.json().get("role")]
    rejected = [r for r in results if r.status_code == 503]
    assert roles.count("probe") == 1, f"exactly one probe expected, roles={roles}"
    assert len(rejected) == 9


def test_probe_failure_reopens_and_cooldown_restarts():
    set_mode("503")
    for _ in range(3):
        meta()
    time.sleep(2.1)
    probe = meta()
    assert probe.status_code == 502 and probe.json()["role"] == "probe"
    b = breaker()
    assert b["state"] == "open"
    assert b["cooldown_remaining"] > 1.5, "failed probe must restart the 2s cooldown"
    assert meta().status_code == 503


def test_probe_success_closes_circuit():
    set_mode("503")
    for _ in range(3):
        meta()
    time.sleep(2.1)
    set_mode("normal")
    probe = meta()
    assert probe.status_code == 200 and probe.json()["role"] == "probe"
    assert probe.json()["breaker_effect"] == "closed"
    assert breaker()["state"] == "closed"
    assert meta().status_code == 200


def test_slow_success_started_before_trip_cannot_recover():
    """In-flight coalescing keeps the pre-trip call's result fenced.

    Under coalescing the same-name requests that used to create the trip now
    join the slow call instead, so they cannot trip the breaker while it is in
    flight.  The state-machine guarantee itself ("a slow success that belongs
    to an older epoch cannot close an open breaker") is covered directly by
    tests/test_unit.py::test_slow_pretrip_success_is_ignored.  Here we verify
    the new, required behaviour: a slow 200 coalesces every concurrent
    same-name request into a single upstream call and leaves the breaker
    closed with no failures recorded.
    """
    script([{"status": 200, "delay": 0.7}])
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: meta(), range(6)))
    assert all(r.status_code == 200 for r in results)
    assert upstream_hits() == 1, "concurrent same-name lookups must share one call"
    b = breaker()
    assert b["state"] == "closed" and b["failures"] == 0
    leaders = [r.json().get("call_leader") for r in results]
    assert leaders.count(True) == 1


def test_slow_queries_do_not_block_each_other():
    set_mode("delay", 0.7)
    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda _: meta(), range(12)))
    elapsed = time.time() - started
    assert all(r.status_code == 200 for r in results)
    assert elapsed < 2.5, f"12 x 0.7s queries should overlap (took {elapsed:.2f}s)"


def test_persisted_state_survives_restart():
    if not RESTART_CMD:
        pytest.skip("set GATEWAY_RESTART_CMD to test persistence across restart")
    set_mode("503")
    for _ in range(3):
        meta()
    assert breaker()["state"] == "open"
    subprocess.run(RESTART_CMD, shell=True, check=True)
    for _ in range(60):
        try:
            if CLIENT.get(f"{GATEWAY}/healthz", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    b = breaker()
    assert b["state"] == "open"
    assert b["failures"] == 3
    hits_before = upstream_hits()
    assert meta().status_code == 503
    assert upstream_hits() == hits_before
