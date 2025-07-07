from pydantic import BaseModel, ConfigDict


class UserStaticProjectSchema(BaseModel):
    model_config = ConfigDict(strict=True)

    user_id: int
    project_id: int

    tasks_completed_last_week: int
