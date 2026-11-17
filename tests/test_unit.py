"""In-process unit tests for the breaker state machine.

These do not need network services.  They drive ``app.admit`` and
``app.record_outcome`` directly against a temporary SQLite file, covering the
classification of failures and the stale-result fencing rules.
"""

from __future__ import annotations

import importlib
import sys
import time
from pathlib import Path

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
    return app_module


def _state(gw, artifact="a"):
    conn = gw.connect()
    try:
        row = conn.execute("SELECT * FROM breakers WHERE artifact = ?", (artifact,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _pass(gw, artifact="a", outcome=None, outcomes=None):
    """Perform one admitted call without network."""
    o = outcome or gw.Outcome("success", "200", status=200)
    adm = gw.admit(artifact)
    assert adm.allowed
    return gw.record_outcome(artifact, adm, o)


def test_three_failures_open_and_success_resets(gw):
    fail = gw.Outcome("failure", "boom")
    _pass(gw, outcome=fail)
    assert _state(gw)["failures"] == 1
    _pass(gw, outcome=fail)
    assert _state(gw)["failures"] == 2
    _pass(gw)  # success resets
    assert _state(gw)["failures"] == 0
    assert _state(gw)["state"] == gw.CLOSED
    _pass(gw, outcome=fail)
    _pass(gw, outcome=fail)
    assert _state(gw)["state"] == gw.CLOSED
    _pass(gw, outcome=fail)
    assert _state(gw)["state"] == gw.OPEN
    assert _state(gw)["failures"] == 3


def test_open_rejects_without_admission(gw):
    fail = gw.Outcome("failure", "boom")
    for _ in range(3):
        _pass(gw, outcome=fail)
    adm = gw.admit("a")
    assert not adm.allowed
    assert adm.state == gw.OPEN
    assert 0 < adm.retry_after <= 2.0


def test_probe_election_is_single_and_cooldown_gated(gw, monkeypatch):
    fail = gw.Outcome("failure", "boom")
    for _ in range(3):
        _pass(gw, outcome=fail)
    assert not gw.admit("a").allowed  # cooldown not over
    # Simulate cooldown expiry without sleeping.
    conn = gw.connect()
    try:
        conn.execute("UPDATE breakers SET opened_at = opened_at - 3 WHERE artifact='a'")
    finally:
        conn.close()
    first = gw.admit("a")
    second = gw.admit("a")
    assert first.allowed and first.role == "probe"
    assert not second.allowed and second.state == gw.HALF_OPEN


def test_probe_failure_restarts_cooldown_and_probe_success_closes(gw):
    fail = gw.Outcome("failure", "boom")
    for _ in range(3):
        _pass(gw, outcome=fail)
    conn = gw.connect()
    try:
        conn.execute("UPDATE breakers SET opened_at = opened_at - 3 WHERE artifact='a'")
    finally:
        conn.close()
    adm = gw.admit("a")
    assert adm.role == "probe"
    gw.record_outcome("a", adm, fail)
    row = _state(gw)
    assert row["state"] == gw.OPEN
    assert row["opened_at"] > time.time() - 1.0
    assert not gw.admit("a").allowed

    conn = gw.connect()
    try:
        conn.execute("UPDATE breakers SET opened_at = opened_at - 3 WHERE artifact='a'")
    finally:
        conn.close()
    adm = gw.admit("a")
    assert adm.role == "probe"
    gw.record_outcome("a", adm, gw.Outcome("success", "200"))
    row = _state(gw)
    assert row["state"] == gw.CLOSED
    assert row["failures"] == 0
    assert gw.admit("a").allowed


def test_slow_pretrip_success_is_ignored(gw):
    """A call admitted while CLOSED that finishes after a trip must not close."""
    fail = gw.Outcome("failure", "boom")
    slow_adm = gw.admit("a")
    assert slow_adm.role == "normal"
    for _ in range(3):
        _pass(gw, outcome=fail)
    assert _state(gw)["state"] == gw.OPEN
    result = gw.record_outcome("a", slow_adm, gw.Outcome("success", "200"))
    assert result["effect"] == "stale_success_ignored"
    row = _state(gw)
    assert row["state"] == gw.OPEN and row["failures"] == 3


def test_stale_probe_generation_cannot_close(gw):
    fail = gw.Outcome("failure", "boom")
    for _ in range(3):
        _pass(gw, outcome=fail)
    conn = gw.connect()
    try:
        conn.execute("UPDATE breakers SET opened_at = opened_at - 3 WHERE artifact='a'")
    finally:
        conn.close()
    slow = gw.admit("a")          # generation 1, never completes in time
    assert slow.role == "probe"
    # The probe is abandoned (crash/timeout): release it and let gen 2 fail.
    conn = gw.connect()
    try:
        conn.execute(
            "UPDATE breakers SET state='open', opened_at=opened_at-3, probe_holder=NULL WHERE artifact='a'"
        )
    finally:
        conn.close()
    newer = gw.admit("a")
    assert newer.role == "probe" and newer.probe_gen == slow.probe_gen + 1
    gw.record_outcome("a", newer, gw.Outcome("failure", "still down"))
    assert _state(gw)["state"] == gw.OPEN
    # Old generation 1 now reports success late: it must not close the circuit.
    result = gw.record_outcome("a", slow, gw.Outcome("success", "200"))
    assert result["effect"] == "stale_probe_ignored"
    assert _state(gw)["state"] == gw.OPEN


def test_abandoned_probe_from_dead_process_is_released(gw):
    fail = gw.Outcome("failure", "boom")
    for _ in range(3):
        _pass(gw, outcome=fail)
    conn = gw.connect()
    try:
        conn.execute(
            "UPDATE breakers SET state='half_open', probe_holder='999999|x|1', "
            "opened_at=opened_at-3, probe_gen=1 WHERE artifact='a'"
        )
    finally:
        conn.close()
    # Effective state is OPEN with an expired cooldown => a new probe may run.
    adm = gw.admit("a")
    assert adm.allowed and adm.role == "probe"


def test_connect_error_and_timeout_classification(gw, monkeypatch):
    """Connection refused and timeout are failures; 4xx is healthy."""
    import asyncio
    import httpx

    async def fake_get(url):
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(gw, "_get", fake_get)
    assert asyncio.run(gw.call_upstream("a")).kind == "failure"

    async def slow_get(url):
        await asyncio.sleep(5)
        return httpx.Response(200, json={})

    monkeypatch.setattr(gw, "_get", slow_get)
    monkeypatch.setattr(gw, "REQUEST_TIMEOUT", 0.2)
    started = time.time()
    out = asyncio.run(gw.call_upstream("a"))
    assert out.kind == "failure"
    assert time.time() - started < 1.0

    async def not_found(url):
        return httpx.Response(404, json={"error": "missing"})

    monkeypatch.setattr(gw, "_get", not_found)
    assert asyncio.run(gw.call_upstream("a")).kind == "success"

    async def unavailable(url):
        return httpx.Response(503, text="nope")

    monkeypatch.setattr(gw, "_get", unavailable)
    assert asyncio.run(gw.call_upstream("a")).kind == "failure"
