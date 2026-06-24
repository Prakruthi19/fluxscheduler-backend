from typing import Optional, List
from models import Node, Job, JobType, SchedulingStrategy, JobStatus
from store import store
from datetime import datetime


def can_node_run_job(node: Node, job: Job) -> bool:
    # Check resources
    if node.available.cpu_millicores < job.required.cpu_millicores:
        return False
    if node.available.memory_mb < job.required.memory_mb:
        return False
    if job.required.gpu_memory_gb > 0 and node.available.gpu_memory_gb < job.required.gpu_memory_gb:
        return False
    # Job type affinity — general jobs go anywhere
    if job.job_type != JobType.general and job.job_type not in node.capabilities:
        return False
    return True


def score_node(node: Node, job: Job, strategy: SchedulingStrategy) -> float:
    """Lower score = better candidate"""
    if strategy == SchedulingStrategy.best_fit:
        # Prefer nodes with least remaining CPU after assignment
        remaining = node.available.cpu_millicores - job.required.cpu_millicores
        return remaining

    elif strategy == SchedulingStrategy.cheapest_fit:
        # Prefer cheapest node that can run the job
        # Score = cost_per_hour weighted by how much of the node we use
        fraction_used = job.required.cpu_millicores / max(node.total.cpu_millicores, 1)
        estimated_cost = node.cost_per_hour * fraction_used
        return estimated_cost

    elif strategy == SchedulingStrategy.worst_fit:
        # Prefer node with most remaining resources (spread load)
        return -node.available.cpu_millicores

    elif strategy == SchedulingStrategy.first_fit:
        # Pick the first node that fits — return constant so first eligible wins
        return 0.0

    return 0.0


def schedule_job(job: Job) -> Optional[Node]:
    healthy_nodes = store.get_healthy_nodes()
    candidates = [n for n in healthy_nodes if can_node_run_job(n, job)]

    if not candidates:
        return None

    strategy = store.strategy
    best_node = min(candidates, key=lambda n: score_node(n, job, strategy))

    # Cost tracking — compare cheapest_fit vs naive (random first node)
    naive_node = candidates[0]
    naive_fraction = job.required.cpu_millicores / max(naive_node.total.cpu_millicores, 1)
    naive_cost = naive_node.cost_per_hour * naive_fraction

    actual_fraction = job.required.cpu_millicores / max(best_node.total.cpu_millicores, 1)
    actual_cost = best_node.cost_per_hour * actual_fraction

    store.add_cost_tracking(naive_cost, actual_cost)

    return best_node


def assign_job(job: Job, node: Node):
    # Deduct resources
    node.available.cpu_millicores -= job.required.cpu_millicores
    node.available.memory_mb -= job.required.memory_mb
    node.available.gpu_memory_gb = max(0, node.available.gpu_memory_gb - job.required.gpu_memory_gb)

    # Update job
    job.status = JobStatus.running
    job.assigned_node = node.node_id
    job.started_at = datetime.utcnow()

    # Track on node
    node.running_task_ids.append(job.job_id)


def complete_job(job: Job):
    node = store.nodes.get(job.assigned_node)
    if node:
        # Free resources
        node.available.cpu_millicores += job.required.cpu_millicores
        node.available.memory_mb += job.required.memory_mb
        node.available.gpu_memory_gb += job.required.gpu_memory_gb
        if job.job_id in node.running_task_ids:
            node.running_task_ids.remove(job.job_id)

    job.status = JobStatus.completed
    job.completed_at = datetime.utcnow()


def run_scheduling_cycle():
    """Called periodically to assign pending jobs to available nodes"""
    pending = store.get_pending_jobs()
    assigned = []
    for job in pending:
        node = schedule_job(job)
        if node:
            assign_job(job, node)
            assigned.append(job.job_id)
    return assigned
