"""Example upstream: a tiny local artifact-directory service.

Serves metadata for files in ``ARTIFACT_DIR`` (one ``<name>.json`` file per
artifact) and exposes a control plane used by the demo web page to switch
between healthy, slow and erroring behaviour.  It also counts every real
``/artifacts/*`` request, which is the ground truth for "did the gateway
actually touch the upstream?".
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

BASE_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = Path(os.environ.get("ARTIFACT_DIR", BASE_DIR / "artifacts"))

app = FastAPI(title="Example artifact directory service")

_mode: dict[str, Any] = {"mode": "normal", "delay": 3.0}
# Scripted responses, consumed one per /artifacts request.  Intended for
# deterministic demos/tests: each entry is {"status": int, "delay": float,
# "serve": bool}.  When exhausted the upstream falls back to ``normal``.
_script: list[dict[str, Any]] = []
_stats: dict[str, Any] = {"hits": 0, "by_mode": {}, "last_requests": []}


def _bump(mode: str, artifact: str) -> None:
    _stats["hits"] += 1
    by_mode = _stats["by_mode"]
    by_mode[mode] = by_mode.get(mode, 0) + 1
    recent = _stats["last_requests"]
    recent.insert(0, {"ts": time.time(), "artifact": artifact, "mode": mode})
    del recent[30:]


def _resolve(artifact: str) -> Path | None:
    """Map an artifact name to a metadata file inside ARTIFACT_DIR only."""
    candidate = (ARTIFACT_DIR / artifact).resolve()
    try:
        candidate.relative_to(ARTIFACT_DIR.resolve())
    except ValueError:
        return None
    if candidate.suffix != ".json":
        candidate = candidate.with_suffix(candidate.suffix + ".json")
    return candidate if candidate.is_file() else None


@app.get("/artifacts/{artifact:path}")
async def artifact_metadata(artifact: str, request: Request) -> JSONResponse:
    mode = _mode["mode"]
    scripted = _script.pop(0) if _script else None
    _bump(mode, artifact)

    if scripted is not None:
        if scripted.get("delay"):
            await asyncio.sleep(float(scripted["delay"]))
        status = int(scripted.get("status", 200))
        if status >= 400:
            return JSONResponse(
                status_code=status,
                content={"error": "scripted", "artifact": artifact, "status": status},
            )
        if not scripted.get("serve", True):
            return JSONResponse(status_code=200, content={"artifact": artifact, "metadata": {}})
        mode = "normal"  # fall through to normal metadata rendering
    elif mode == "503":
        return JSONResponse(
            status_code=503,
            content={"error": "upstream_unavailable", "artifact": artifact},
        )
    elif mode == "delay":
        await asyncio.sleep(float(_mode.get("delay", 3.0)))

    path = _resolve(artifact)
    if path is None:
        return JSONResponse(
            status_code=404,
            content={"error": "artifact_not_found", "artifact": artifact},
        )
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        return JSONResponse(
            status_code=500, content={"error": f"unreadable metadata: {exc}"}
        )
    if isinstance(data, dict):
        data.setdefault("artifact", artifact)
        data["source_file"] = path.name
        data["source_mtime"] = int(path.stat().st_mtime)
    return JSONResponse(content={"artifact": artifact, "metadata": data})


@app.get("/control/mode")
async def get_mode() -> dict[str, Any]:
    return dict(_mode)


@app.post("/control/script")
async def set_script(request: Request) -> JSONResponse:
    """Install a list of scripted responses (consumed in order)."""
    global _script
    try:
        payload = await request.json()
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "invalid json"})
    entries = payload.get("responses", []) if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        return JSONResponse(status_code=400, content={"error": "responses must be a list"})
    clean = []
    for entry in entries:
        if not isinstance(entry, dict):
            return JSONResponse(status_code=400, content={"error": "each response must be an object"})
        clean.append(
            {
                "status": int(entry.get("status", 200)),
                "delay": float(entry.get("delay", 0) or 0),
                "serve": bool(entry.get("serve", True)),
            }
        )
    _script = clean
    return JSONResponse(content={"script": _script})


@app.post("/control/mode")
async def set_mode(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except ValueError:
        payload = {}
    mode = str(payload.get("mode") or "")
    if mode not in {"normal", "delay", "503"}:
        return JSONResponse(
            status_code=400,
            content={"error": "mode must be one of normal, delay, 503"},
        )
    if payload.get("delay") is not None:
        try:
            delay = float(payload["delay"])
        except (TypeError, ValueError):
            return JSONResponse(status_code=400, content={"error": "delay must be a number"})
        if not (0 <= delay <= 60):
            return JSONResponse(status_code=400, content={"error": "delay out of range"})
        _mode["delay"] = delay
    _mode["mode"] = mode
    return JSONResponse(content=dict(_mode))


@app.get("/control/stats")
async def get_stats() -> dict[str, Any]:
    return json.loads(json.dumps(_stats))


@app.post("/control/reset")
async def reset_stats() -> dict[str, Any]:
    _stats["hits"] = 0
    _stats["by_mode"] = {}
    _stats["last_requests"] = []
    return {"ok": True}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
