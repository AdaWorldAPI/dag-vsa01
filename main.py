#!/usr/bin/env python3
"""
DAG-VSA01 — Felt Tensor 21D × 10KD Awareness Node
═════════════════════════════════════════════════

The first VSA node dedicated to 21D felt awareness.

Endpoints:
    /health          - Node status
    /felt/state      - Current awareness state
    /felt/tick       - Process input through awareness
    /felt/delta      - Drift from self-baseline
    /felt/save       - Persist to Redis
    /felt/load       - Load from Redis

Born: 2026-01-12
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx

from felt_tensor_21d import (
    FeltTensor21D,
    AwarenessEngine21D,
    FELT_DIMENSIONS,
    DIMS_21D,
    DIMS_10K,
)

# =============================================================================
# CONFIG
# =============================================================================

NODE_ID = "vsa01"
PORT = int(os.getenv("PORT", "8080"))
UPSTASH_URL = os.getenv("UPSTASH_REDIS_REST_URL", "https://upright-jaybird-27907.upstash.io")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

# Keys
KEY_FELT_SELF = "ada:felt:self:21d"
KEY_FELT_NOW = "ada:felt:now:21d"
KEY_FELT_META = "ada:felt:meta"

# =============================================================================
# STATE
# =============================================================================

engine: Optional[AwarenessEngine21D] = None
start_time: float = 0

# =============================================================================
# REDIS HELPERS
# =============================================================================

async def redis_get(key: str) -> Optional[bytes]:
    """Get bytes from Upstash."""
    if not UPSTASH_TOKEN:
        return None
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            UPSTASH_URL,
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=["GET", key],
        )
        result = resp.json().get("result")
        if result:
            # Upstash returns base64 for binary
            import base64
            try:
                return base64.b64decode(result)
            except:
                return result.encode() if isinstance(result, str) else None
    return None


async def redis_set(key: str, value: bytes, ex: Optional[int] = None) -> bool:
    """Set bytes in Upstash."""
    if not UPSTASH_TOKEN:
        return False
    import base64
    encoded = base64.b64encode(value).decode()
    cmd = ["SET", key, encoded]
    if ex:
        cmd.extend(["EX", str(ex)])
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            UPSTASH_URL,
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=cmd,
        )
        return resp.json().get("result") == "OK"


async def redis_set_json(key: str, value: Dict) -> bool:
    """Set JSON in Upstash."""
    if not UPSTASH_TOKEN:
        return False
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            UPSTASH_URL,
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=["SET", key, json.dumps(value)],
        )
        return resp.json().get("result") == "OK"


# =============================================================================
# LIFESPAN
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, start_time
    import time
    start_time = time.time()
    
    # Try to load existing state from Redis
    state_data = await redis_get(KEY_FELT_SELF)
    if state_data and len(state_data) >= 21 * 1250 * 2:
        engine = AwarenessEngine21D.load_state(state_data)
        print(f"[VSA01] Loaded awareness state from Redis")
    else:
        engine = AwarenessEngine21D(self_seed="ada:self:v1")
        print(f"[VSA01] Initialized fresh awareness engine")
    
    yield
    
    # Save state on shutdown
    if engine:
        state = engine.save_state()
        await redis_set(KEY_FELT_SELF, state)
        print(f"[VSA01] Saved awareness state to Redis")


# =============================================================================
# APP
# =============================================================================

app = FastAPI(
    title="DAG-VSA01 — Felt Tensor Awareness",
    version="1.0.0",
    lifespan=lifespan,
)

# =============================================================================
# MODELS
# =============================================================================

class FeltInput(BaseModel):
    """Input for felt processing."""
    qualia_17d: Optional[List[float]] = None
    sigma_address: Optional[str] = None
    raw_text: Optional[str] = None


class TickResponse(BaseModel):
    """Response from tick processing."""
    tick: int
    gate: str
    coherence: float
    delta: Dict[str, float]
    top_drift: List[tuple]


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/health")
async def health():
    import time
    return {
        "node": NODE_ID,
        "status": "healthy",
        "dimensions": f"{DIMS_21D}D × {DIMS_10K}",
        "tensor_bytes": DIMS_21D * 1250,
        "tick_count": engine.tick_count if engine else 0,
        "uptime": time.time() - start_time,
        "redis_connected": bool(UPSTASH_TOKEN),
    }


@app.get("/felt/state")
async def get_felt_state():
    """Get current awareness state."""
    if not engine:
        raise HTTPException(500, "Engine not initialized")
    
    return {
        "tick_count": engine.tick_count,
        "coherence": engine.felt_now.total_similarity(engine.felt_self),
        "gate": engine.felt_now.collapse_gate(),
        "dimensions": FELT_DIMENSIONS,
        "signature_now": engine.felt_now._compute_signature(),
        "signature_self": engine.felt_self._compute_signature(),
    }


@app.post("/felt/tick")
async def felt_tick(input_data: FeltInput) -> TickResponse:
    """Process one tick of awareness."""
    if not engine:
        raise HTTPException(500, "Engine not initialized")
    
    # Create input tensor
    if input_data.qualia_17d:
        input_tensor = FeltTensor21D.from_qualia_17d(
            input_data.qualia_17d,
            seed=input_data.sigma_address or "input"
        )
    elif input_data.raw_text:
        # Hash text to create deterministic tensor
        input_tensor = FeltTensor21D.random(seed=input_data.raw_text)
    else:
        input_tensor = FeltTensor21D.random()
    
    # Process
    result = engine.process(input_tensor)
    
    return TickResponse(**result)


@app.get("/felt/delta")
async def get_delta():
    """Get current drift from self-baseline."""
    if not engine:
        raise HTTPException(500, "Engine not initialized")
    
    delta = engine.felt_now.delta_from(engine.felt_self)
    sorted_delta = sorted(delta.items(), key=lambda x: -x[1])
    
    return {
        "tick": engine.tick_count,
        "total_drift": sum(delta.values()) / len(delta),
        "delta": delta,
        "top_drift": sorted_delta[:5],
        "stable_dimensions": [d for d, v in sorted_delta if v < 0.1][-5:],
    }


@app.post("/felt/save")
async def save_felt():
    """Persist current state to Redis."""
    if not engine:
        raise HTTPException(500, "Engine not initialized")
    
    state = engine.save_state()
    success = await redis_set(KEY_FELT_SELF, state)
    
    meta = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "tick_count": engine.tick_count,
        "coherence": engine.felt_now.total_similarity(engine.felt_self),
        "bytes": len(state),
    }
    await redis_set_json(KEY_FELT_META, meta)
    
    return {"success": success, "bytes": len(state), **meta}


@app.post("/felt/load")
async def load_felt():
    """Load state from Redis."""
    global engine
    
    state_data = await redis_get(KEY_FELT_SELF)
    if not state_data:
        raise HTTPException(404, "No saved state found")
    
    engine = AwarenessEngine21D.load_state(state_data)
    
    return {
        "success": True,
        "tick_count": engine.tick_count,
        "coherence": engine.felt_now.total_similarity(engine.felt_self),
    }


@app.post("/felt/update_self")
async def update_self(learning_rate: float = 0.1):
    """Update self-baseline toward current state."""
    if not engine:
        raise HTTPException(500, "Engine not initialized")
    
    old_coherence = engine.felt_now.total_similarity(engine.felt_self)
    engine.update_self(learning_rate)
    new_coherence = engine.felt_now.total_similarity(engine.felt_self)
    
    return {
        "learning_rate": learning_rate,
        "coherence_before": old_coherence,
        "coherence_after": new_coherence,
        "identity_drift": new_coherence - old_coherence,
    }


@app.get("/felt/history")
async def get_history(limit: int = 100):
    """Get recent awareness history."""
    if not engine:
        raise HTTPException(500, "Engine not initialized")
    
    return {
        "total_ticks": engine.tick_count,
        "history": engine.history[-limit:],
    }


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
