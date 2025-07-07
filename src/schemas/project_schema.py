from typing import Dict

from pydantic import BaseModel, ConfigDict


class ProjectStatisticSchema(BaseModel):
    model_config = ConfigDict(strict=True)

    project_id: int

    total_tasks: int
    total_user: int

    tasks_by_status: Dict[str, int]
    average_task_completion_time: float
