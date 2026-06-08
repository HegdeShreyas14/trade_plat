#!/usr/bin/env python3
import asyncio
import json
import os
import shutil
import subprocess 
import sys
import time
import zipfile

try:
    import redis.asyncio as redis
except ImportError: 
    redis = None

try:
    from fastapi import FastAPI, UploadFile, File, Form, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import StreamingResponse
except ImportError: 
    FastAPI = None
    CORSMiddleware = None
    StreamingResponse = None
    UploadFile = None
    File = None
    Form = None
    HTTPException = None


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
LEADERBOARD_KEY = os.getenv("REDIS_LEADERBOARD_KEY", "leaderboard:scores")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "leaderboard_updates")
TMP_UPLOAD_DIR = "/tmp/sandbox_uploads"

if FastAPI is None:
    raise RuntimeError("fastapi is not installed. Run: pip install -r telemetry/requirements.txt")
if redis is None:
    raise RuntimeError("redis is not installed. Run: pip install -r telemetry/requirements.txt")

app = FastAPI(title="HFT Sandbox Leaderboard API")

# Configure broad CORS parameters for local development workflows
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


async def redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


async def read_leaderboard(limit=25):
    client = await redis_client()
    try:
        rows = []
        scores = await client.zrevrange(LEADERBOARD_KEY, 0, limit - 1, withscores=True)
        for rank, (contestant_id, score) in enumerate(scores, start=1):
            meta = await client.hgetall(f"contestant:meta:{contestant_id}")
            rows.append(
                {
                    "rank": rank,
                    "id": contestant_id,
                    "score": float(score),
                    "tps": float(meta.get("tps", 0)),
                    "p50_lat_us": float(meta.get("p50_lat_us", 0)),
                    "p90_lat_us": float(meta.get("p90_lat_us", 0)),
                    "p99_lat_us": float(meta.get("p99_lat_us", 0)),
                    "correctness": meta.get("correctness", "UNKNOWN"),
                    "total_events": int(float(meta.get("total_events", 0))),
                    "last_match_id": int(float(meta.get("last_match_id", 0))),
                }
            )
        return rows
    finally:
        await client.aclose()


def sse(data, event="message"):
    return f"event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n"


@app.post("/api/v1/submissions/upload")
async def receive_submission(
    file: UploadFile = File(...),
    contestant_id: str = Form(...)
):
    """
    Ingests contestant ZIP archives, extracts artifacts to a dedicated staging 
    workspace, and triggers the orchestrator container pipeline out-of-band.
    """
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Invalid layout footprint. Submission package must be a .zip archive.")

    contestant_dir = os.path.join(TMP_UPLOAD_DIR, contestant_id)
    if os.path.exists(contestant_dir):
        shutil.rmtree(contestant_dir)
    os.makedirs(contestant_dir, exist_ok=True)

    zip_file_path = os.path.join(contestant_dir, "submission.zip")

    try:
        with open(zip_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(contestant_dir)
            
        os.remove(zip_file_path)

        # ASYNC UPGRADE: Execute orchestrator using non-blocking sub-processes
        cmd_args = ["infra/orchestrator.py", "--sub-id", contestant_id]
        
        async def run_detached_orchestrator():
            print(f"[api-gateway] Launching detached sandbox build process for {contestant_id}...")
            process = await asyncio.create_subprocess_exec(
                sys.executable, *cmd_args,
                stdout=None,  
                stderr=None
            )
            await process.wait()  
            print(f"[api-gateway] Orchestration sequence completed for {contestant_id}.")

        # Dispatch task immediately into event loop to prevent server starvation timeouts
        asyncio.create_task(run_detached_orchestrator())

        return {
            "status": "QUEUED",
            "message": f"Successfully extracted submission workspace configurations for {contestant_id}. Sandbox build initiated.",
        }

    except Exception as e:
        if os.path.exists(contestant_dir):
            shutil.rmtree(contestant_dir)
        raise HTTPException(status_code=500, detail=f"Internal evaluation cluster runtime error: {str(e)}")


@app.get("/api/v1/leaderboard")
async def leaderboard():
    return await read_leaderboard()


@app.get("/api/v1/leaderboard/stream")
async def leaderboard_stream():
    async def events():
        client = await redis_client()
        pubsub = client.pubsub()
        await pubsub.subscribe(REDIS_CHANNEL)
        last_snapshot = 0.0
        try:
            yield sse(await read_leaderboard(), event="snapshot")
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                now = time.monotonic()
                if message and message.get("type") == "message":
                    yield sse(await read_leaderboard(), event="update")
                    last_snapshot = now
                elif now - last_snapshot >= 5.0:
                    yield sse(await read_leaderboard(), event="snapshot")
                    last_snapshot = now
                await asyncio.sleep(0.05)
        finally:
            await pubsub.unsubscribe(REDIS_CHANNEL)
            await pubsub.aclose()
            await client.aclose()

    return StreamingResponse(events(), media_type="text/event-stream")