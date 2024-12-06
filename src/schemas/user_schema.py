from pydantic import BaseModel, ConfigDict


class UserStatisticSchema(BaseModel):
    model_config = ConfigDict(strict=True)

    user_id: int
    total_projects: int
    tasks_completed_last_week: int
    average_task_completion_time: float


class UserStaticProjectSchema(BaseModel):
    model_config = ConfigDict(strict=True)

    user_id: int
    project_id: int

    tasks_completed_last_week: int
