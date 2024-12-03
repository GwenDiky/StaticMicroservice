from fastapi import APIRouter, Depends
from src.services.static_service import UserStatisticService
from src.schemas.user_schema import UserStatisticSchema
from src.services import static_service
from src.models import core_static
from typing import List

statics_router = APIRouter()


@statics_router.post("/add-static")
async def add_statistic(statistic: UserStatisticSchema,
                        db: UserStatisticService = Depends(
                            core_static.get_mongo_client)):
    await UserStatisticService(db).save_user_statistic(statistic)
    return {"message": "User statistic saved successfully."}

@statics_router.get('/show-static-by-user-id')
async def show_static_by_user_id(user_id: str,
                                 db: UserStatisticService = Depends(
                                     core_static.get_mongo_client)) -> (
        UserStatisticSchema):
    ...

@statics_router.get("/show-static-by-id")
async def show_static_by_id(static_id: str,
                            db: UserStatisticService = Depends(
                                core_static.get_mongo_client)) -> UserStatisticSchema:
    return await UserStatisticService(db).get_user_statistic_by_id(
        static_id=static_id)

@statics_router.get("/show-full-static")
async def show_full_static(db: UserStatisticService = Depends(
                            core_static.get_mongo_client)) -> List[UserStatisticSchema]:
    return await UserStatisticService(db).get_all_user_statistics()

