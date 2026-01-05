"""
VSA01 — Hot Replica (vsa01.msgraph.de)
======================================
4-node LanceDB DAG. 5s lag from AGI primary.
"""
import os, json, asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    import lancedb
    import pyarrow as pa
    HAS_LANCE = True
except:
    lancedb = pa = None
    HAS_LANCE = False

LANCE_PATH = os.getenv("LANCE_DB_PATH", "/data/lancedb")
NODE_ID = "vsa01"
db = None

class Vec10k(BaseModel):
    id: str
    vector: List[float]
    metadata: Dict[str, Any] = {}

def init_db():
    global db
    if not HAS_LANCE: return False
    os.makedirs(LANCE_PATH, exist_ok=True)
    db = lancedb.connect(LANCE_PATH)
    if "vec10k" not in db.table_names():
        db.create_table("vec10k", schema=pa.schema([
            pa.field("id", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), 10000)),
            pa.field("meta", pa.string()),
            pa.field("ts", pa.string()),
        ]))
    return True

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title=f"DAG {NODE_ID}", lifespan=lifespan)

@app.get("/health")
async def health():
    n = db.open_table("vec10k").count_rows() if db and "vec10k" in db.table_names() else 0
    return {"node": NODE_ID, "role": "hot", "lag_ms": 5000, "vectors": n, "ok": True}

@app.post("/vectors/upsert")
async def upsert(req: Vec10k):
    if not db: raise HTTPException(503, "no db")
    if len(req.vector) != 10000: raise HTTPException(400, f"need 10kD, got {len(req.vector)}")
    db.open_table("vec10k").add([{"id": req.id, "vector": req.vector, "meta": json.dumps(req.metadata), "ts": datetime.now(timezone.utc).isoformat()}])
    return {"ok": True, "id": req.id, "node": NODE_ID}

@app.get("/vectors/count")
async def count():
    return {"count": db.open_table("vec10k").count_rows() if db else 0, "node": NODE_ID}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
