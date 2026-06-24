---
title: FluxScheduler API
emoji: 🖥️
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
---

# FluxScheduler Backend

Distributed job scheduler for AI workloads. Built by **Prakruthi** — M.S. Computer Science, George Washington University.

The control plane handles node registration, resource-aware job placement, and cost optimization across a simulated GPU/CPU cluster. Four scheduling algorithms (cheapest-fit, best-fit, worst-fit, first-fit) with live WebSocket broadcast.

> Originally prototyped in Go with gRPC — rebuilt the orchestration layer in FastAPI to ship the full platform faster.

---

## Quick start

```bash
# Seed the cluster with 3 demo nodes
POST /api/demo/seed

# Submit a job
POST /api/jobs
{
  "name": "embed-batch-42",
  "job_type": "embedding",
  "required": { "cpu_millicores": 500, "memory_mb": 1024, "gpu_memory_gb": 2 },
  "priority": 7
}

# Switch scheduling strategy
POST /api/strategy/cheapest_fit
```

---

## Endpoints

**Nodes**

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/nodes/register` | Register a worker node with resource profile |
| `POST` | `/api/nodes/heartbeat` | Node heartbeat — updates last_seen and available resources |
| `GET` | `/api/nodes` | All nodes with utilization % |
| `DELETE` | `/api/nodes/{node_id}` | Remove a node |

**Jobs**

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/jobs` | Submit a job |
| `GET` | `/api/jobs` | All jobs sorted by created_at desc |
| `POST` | `/api/jobs/{job_id}/complete` | Mark a job complete, free its resources |
| `POST` | `/api/jobs/{job_id}/fail` | Mark a job failed, free its resources |
| `POST` | `/api/jobs/{job_id}/retry` | Requeue a failed job |
| `DELETE` | `/api/jobs/{job_id}` | Delete a job |

**Cluster**

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/metrics` | Cluster-wide metrics — utilization, cost, job counts |
| `GET` | `/api/compare` | Dry-run all 4 strategies against current jobs, no assignments made |
| `POST` | `/api/strategy/{strategy}` | Switch active scheduling strategy |
| `WS` | `/ws` | WebSocket — server pushes full cluster state every 2s |

**Demo**

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/demo/seed` | Register 3 demo nodes (2 GPU, 1 CPU) and reset state |
| `GET` | `/health` | Health check |

---

## Scheduling strategies

| Strategy | Behavior | Best for |
|---|---|---|
| `cheapest_fit` | Routes to lowest-cost node that fits | Cost optimization |
| `best_fit` | Packs jobs onto the most-utilized node | High throughput |
| `worst_fit` | Spreads jobs to least-utilized node | Fault tolerance |
| `first_fit` | First eligible node wins | Simplicity / baseline |

---

## Job types

`llm_inference` · `embedding` · `fine_tuning` · `data_pipeline` · `general`

Jobs have type affinity — `llm_inference` and `fine_tuning` only run on GPU-capable nodes. `data_pipeline` and `general` go anywhere.

---

## Stack

- **FastAPI** — async REST + WebSocket
- **Pydantic v2** — models and validation
- **In-memory store** — `ClusterStore` with dict-backed node and job state
- **Asyncio** — scheduling loop (3s), auto-complete loop (5s), heartbeat loop (10s), WebSocket broadcast (2s)
- **Docker** — deployed on Hugging Face Spaces

---

## Related

- **[fluxscheduler-frontend](https://github.com/Prakruthi19/fluxscheduler-frontend)** — React + TypeScript dashboard with Gantt timeline, strategy comparison, and node sparklines
- **[distributed-scheduler](https://github.com/Prakruthi19/distributed-scheduler)** — original Go/gRPC prototype
