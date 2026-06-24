from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum
from datetime import datetime
import uuid


class JobType(str, Enum):
    llm_inference = "llm_inference"
    embedding = "embedding"
    fine_tuning = "fine_tuning"
    data_pipeline = "data_pipeline"
    general = "general"


class SchedulingStrategy(str, Enum):
    best_fit = "best_fit"
    cheapest_fit = "cheapest_fit"
    worst_fit = "worst_fit"
    first_fit = "first_fit"


class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class Resource(BaseModel):
    cpu_millicores: int = Field(..., description="CPU in millicores (1000 = 1 CPU)")
    memory_mb: int = Field(..., description="Memory in MB")
    gpu_memory_gb: float = Field(default=0, description="GPU VRAM in GB (0 = no GPU needed)")


class NodeRegisterRequest(BaseModel):
    node_id: str
    name: str
    total: Resource
    capabilities: List[JobType] = []
    cost_per_hour: float = Field(default=0.10, description="Cost in USD per hour")
    address: str = "localhost"


class HeartbeatRequest(BaseModel):
    node_id: str
    available: Resource
    running_task_ids: List[str] = []


class Node(BaseModel):
    node_id: str
    name: str
    total: Resource
    available: Resource
    capabilities: List[JobType]
    cost_per_hour: float
    address: str
    healthy: bool = True
    last_seen: datetime = Field(default_factory=datetime.utcnow)
    running_task_ids: List[str] = []

    def utilization_percent(self) -> float:
        if self.total.cpu_millicores == 0:
            return 0.0
        used_cpu = self.total.cpu_millicores - self.available.cpu_millicores
        return round((used_cpu / self.total.cpu_millicores) * 100, 1)

    def cost_per_compute_unit(self) -> float:
        util = self.utilization_percent()
        if util == 0:
            return float("inf")
        return round(self.cost_per_hour / (util / 100), 4)


class JobSubmitRequest(BaseModel):
    name: str
    job_type: JobType = JobType.general
    required: Resource
    priority: int = Field(default=1, ge=1, le=10, description="1=low, 10=high")


class Job(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    retry_count: int = 0 
    name: str
    job_type: JobType
    required: Resource
    priority: int = 1
    status: JobStatus = JobStatus.pending
    assigned_node: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def duration_seconds(self) -> Optional[float]:
        if self.started_at is None:
            return None
        end = self.completed_at or datetime.utcnow()
        return round((end - self.started_at).total_seconds(), 1)


class ClusterMetrics(BaseModel):
    total_nodes: int
    healthy_nodes: int
    total_jobs: int
    running_jobs: int
    pending_jobs: int
    completed_jobs: int
    avg_utilization: float
    total_cost_per_hour: float
    cost_saved_today: float
    active_strategy: SchedulingStrategy
