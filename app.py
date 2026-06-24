from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime

from models import (
    Node, Job, JobStatus, ClusterMetrics,
    NodeRegisterRequest, HeartbeatRequest, JobSubmitRequest,
    SchedulingStrategy, Resource, JobType
)
from store import store
from scheduler import run_scheduling_cycle, complete_job

import uuid


@asynccontextmanager
async def lifespan(app: FastAPI):
    task  = asyncio.create_task(scheduling_loop())
    task2 = asyncio.create_task(auto_complete_loop())
    task3 = asyncio.create_task(heartbeat_loop())        # ← add this
    yield
    task.cancel()
    task2.cancel()
    task3.cancel()       


app = FastAPI(title="FluxScheduler API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


async def scheduling_loop():
    while True:
        await asyncio.sleep(3)
        try:
            run_scheduling_cycle()
        except Exception as e:
            print(f"Scheduler error: {e}")


async def auto_complete_loop():
    """Auto-complete running jobs after a simulated duration for demo purposes"""
    while True:
        await asyncio.sleep(5)
        try:
            for job in list(store.jobs.values()):
                if job.status == JobStatus.running and job.started_at:
                    elapsed = (datetime.utcnow() - job.started_at).total_seconds()
                    # Jobs complete after 30-90s depending on type
                    durations = {
                        JobType.embedding: 20,
                        JobType.data_pipeline: 30,
                        JobType.llm_inference: 45,
                        JobType.fine_tuning: 90,
                        JobType.general: 25,
                    }
                    limit = durations.get(job.job_type, 30)
                    if elapsed > limit:
                        complete_job(job)
        except Exception as e:
            print(f"Auto-complete error: {e}")
async def heartbeat_loop():
    """Keep demo nodes alive by refreshing last_seen every 10 seconds"""
    while True:
        await asyncio.sleep(10)
        for node in store.nodes.values():
            node.last_seen = datetime.utcnow()
            node.healthy = True

# ── Health ──────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


# ── Nodes ───────────────────────────────────────────────────────────────────

@app.post("/api/nodes/register")
def register_node(req: NodeRegisterRequest):
    node = Node(
        node_id=req.node_id,
        name=req.name,
        total=req.total,
        available=req.total.model_copy(),
        capabilities=req.capabilities,
        cost_per_hour=req.cost_per_hour,
        address=req.address,
    )
    store.nodes[req.node_id] = node
    return {"message": f"Node {req.name} registered", "node_id": req.node_id}


@app.post("/api/nodes/heartbeat")
def heartbeat(req: HeartbeatRequest):
    node = store.nodes.get(req.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.available = req.available
    node.running_task_ids = req.running_task_ids
    node.last_seen = datetime.utcnow()
    node.healthy = True
    return {"acknowledged": True}


@app.get("/api/nodes")
def get_nodes():
    nodes = store.get_healthy_nodes()
    return [
        {
            **n.model_dump(),
            "utilization_percent": n.utilization_percent(),
        }
        for n in store.nodes.values()
    ]


@app.delete("/api/nodes/{node_id}")
def remove_node(node_id: str):
    if node_id not in store.nodes:
        raise HTTPException(status_code=404, detail="Node not found")
    del store.nodes[node_id]
    return {"message": "Node removed"}


# ── Jobs ────────────────────────────────────────────────────────────────────

@app.post("/api/jobs")
def submit_job(req: JobSubmitRequest):
    job = Job(
        job_id=str(uuid.uuid4())[:8],
        name=req.name,
        job_type=req.job_type,
        required=req.required,
        priority=req.priority,
    )
    store.jobs[job.job_id] = job
    return {"message": "Job submitted", "job_id": job.job_id, "status": job.status}


@app.get("/api/jobs")
def get_jobs():
    return [
        {
            **j.model_dump(),
            "duration_seconds": j.duration_seconds(),
        }
        for j in sorted(store.jobs.values(), key=lambda j: j.created_at, reverse=True)
    ]


@app.post("/api/jobs/{job_id}/complete")
def mark_complete(job_id: str):
    job = store.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    complete_job(job)
    return {"message": "Job completed", "job_id": job_id}


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str):
    if job_id not in store.jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    del store.jobs[job_id]
    return {"message": "Job deleted"}


# ── Metrics ─────────────────────────────────────────────────────────────────

@app.get("/api/metrics", response_model=ClusterMetrics)
def get_metrics():
    all_nodes = list(store.nodes.values())
    healthy = store.get_healthy_nodes()
    all_jobs = list(store.jobs.values())

    running = [j for j in all_jobs if j.status == JobStatus.running]
    pending = [j for j in all_jobs if j.status == JobStatus.pending]
    completed = [j for j in all_jobs if j.status == JobStatus.completed]

    avg_util = (
        round(sum(n.utilization_percent() for n in healthy) / len(healthy), 1)
        if healthy else 0.0
    )

    return ClusterMetrics(
        total_nodes=len(all_nodes),
        healthy_nodes=len(healthy),
        total_jobs=len(all_jobs),
        running_jobs=len(running),
        pending_jobs=len(pending),
        completed_jobs=len(completed),
        avg_utilization=avg_util,
        total_cost_per_hour=round(sum(n.cost_per_hour for n in healthy), 2),
        cost_saved_today=store.cost_saved(),
        active_strategy=store.strategy,
    )


# ── Strategy ────────────────────────────────────────────────────────────────

@app.post("/api/strategy/{strategy}")
def set_strategy(strategy: SchedulingStrategy):
    store.strategy = strategy
    return {"message": f"Strategy set to {strategy}", "active": strategy}


# ── Demo seed ────────────────────────────────────────────────────────────────

@app.post("/api/demo/seed")
def seed_demo():
    """Register 3 simulated worker nodes for demo purposes"""
    store.nodes.clear()
    store.jobs.clear()
    store._naive_cost_total = 0.0
    store._actual_cost_total = 0.0

    workers = [
        NodeRegisterRequest(
            node_id="gpu-worker-01",
            name="gpu-worker-01",
            total=Resource(cpu_millicores=8000, memory_mb=32768, gpu_memory_gb=40),
            capabilities=[JobType.llm_inference, JobType.embedding, JobType.fine_tuning],
            cost_per_hour=1.20,
            address="gpu-01.internal",
        ),
        NodeRegisterRequest(
            node_id="gpu-worker-02",
            name="gpu-worker-02",
            total=Resource(cpu_millicores=4000, memory_mb=16384, gpu_memory_gb=16),
            capabilities=[JobType.embedding, JobType.fine_tuning, JobType.general],
            cost_per_hour=0.55,
            address="gpu-02.internal",
        ),
        NodeRegisterRequest(
            node_id="cpu-worker-01",
            name="cpu-worker-01",
            total=Resource(cpu_millicores=32000, memory_mb=65536, gpu_memory_gb=0),
            capabilities=[JobType.data_pipeline, JobType.general],
            cost_per_hour=0.18,
            address="cpu-01.internal",
        ),
    ]

    for w in workers:
        register_node(w)

    return {"message": "Demo cluster seeded with 3 nodes", "nodes": [w.node_id for w in workers]}


# ── Strategy Comparison ──────────────────────────────────────────────────────

@app.get("/api/compare")
def compare_strategies():
    """
    Run all 4 strategies against current pending+running jobs (dry run).
    No jobs are assigned — pure scoring simulation.
    """
    from scheduler import can_node_run_job, score_node

    all_jobs = [j for j in store.jobs.values()
                if j.status in (JobStatus.pending, JobStatus.running)]
    healthy  = store.get_healthy_nodes()

    if not all_jobs or not healthy:
        return {"strategies": [], "jobs": [], "summary": {}}

    strategies = list(SchedulingStrategy)
    result_jobs = []

    for job in all_jobs:
        candidates = [n for n in healthy if can_node_run_job(n, job)]
        row = {
            "job_id":   job.job_id,
            "name":     job.name,
            "job_type": job.job_type,
            "priority": job.priority,
            "required": job.required.model_dump(),
            "placements": {}
        }
        for strategy in strategies:
            if not candidates:
                row["placements"][strategy] = {
                    "node": None,
                    "reason": "No eligible node",
                    "est_cost": None,
                }
            else:
                best = min(candidates, key=lambda n: score_node(n, job, strategy))
                fraction = job.required.cpu_millicores / max(best.total.cpu_millicores, 1)
                est_cost = round(best.cost_per_hour * fraction, 6)

                reasons = {
                    SchedulingStrategy.best_fit:     f"least remaining CPU after assign ({best.available.cpu_millicores - job.required.cpu_millicores}m left)",
                    SchedulingStrategy.cheapest_fit: f"lowest estimated cost (${est_cost}/hr slice)",
                    SchedulingStrategy.worst_fit:    f"most free CPU before assign ({best.available.cpu_millicores}m available)",
                    SchedulingStrategy.first_fit:    f"first eligible node in list",
                }
                row["placements"][strategy] = {
                    "node":     best.node_id,
                    "reason":   reasons[strategy],
                    "est_cost": est_cost,
                }
        result_jobs.append(row)

    # Summary: total estimated cost per strategy
    summary = {}
    for strategy in strategies:
        total = sum(
            j["placements"][strategy]["est_cost"] or 0
            for j in result_jobs
        )
        summary[strategy] = round(total, 6)

    return {
        "strategies": [s.value for s in strategies],
        "jobs": result_jobs,
        "summary": summary,
    }

# ── in your JobStatus enum (models.py) — already has failed ✓

# Add these two endpoints to app.py before the demo/seed section:

@app.post("/api/jobs/{job_id}/fail")
def mark_failed(job_id: str):
    job = store.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in (JobStatus.running, JobStatus.pending):
        raise HTTPException(status_code=400, detail="Only running/pending jobs can be failed")
    # Free resources if it was running
    node = store.nodes.get(job.assigned_node) if job.assigned_node else None
    if node:
        node.available.cpu_millicores += job.required.cpu_millicores
        node.available.memory_mb      += job.required.memory_mb
        node.available.gpu_memory_gb  += job.required.gpu_memory_gb
        if job.job_id in node.running_task_ids:
            node.running_task_ids.remove(job.job_id)
    job.status        = JobStatus.failed
    job.assigned_node = None
    job.retry_count   = getattr(job, "retry_count", 0)
    return {"message": "Job marked failed", "job_id": job_id}


@app.post("/api/jobs/{job_id}/retry")
def retry_job(job_id: str):
    job = store.jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.failed:
        raise HTTPException(status_code=400, detail="Only failed jobs can be retried")
    job.status        = JobStatus.pending
    job.assigned_node = None
    job.started_at    = None
    job.completed_at  = None
    job.retry_count   = getattr(job, "retry_count", 0) + 1
    return {"message": "Job queued for retry", "job_id": job_id, "retry_count": job.retry_count}