from typing import Dict, List
from models import Node, Job, SchedulingStrategy
from datetime import datetime, timedelta

class ClusterStore:
    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.jobs: Dict[str, Job] = {}
        self.strategy: SchedulingStrategy = SchedulingStrategy.cheapest_fit
        self._naive_cost_total: float = 0.0
        self._actual_cost_total: float = 0.0

    def get_healthy_nodes(self) -> List[Node]:
        cutoff = datetime.utcnow() - timedelta(seconds=15)
        for node in self.nodes.values():
            node.healthy = node.last_seen >= cutoff
        return [n for n in self.nodes.values() if n.healthy]

    def get_all_jobs(self) -> List[Job]:
        return list(self.jobs.values())

    def get_pending_jobs(self) -> List[Job]:
        from models import JobStatus
        return sorted(
            [j for j in self.jobs.values() if j.status == JobStatus.pending],
            key=lambda j: -j.priority
        )

    def add_cost_tracking(self, naive: float, actual: float):
        self._naive_cost_total += naive
        self._actual_cost_total += actual

    def cost_saved(self) -> float:
        return round(max(0, self._naive_cost_total - self._actual_cost_total), 4)

store = ClusterStore()
