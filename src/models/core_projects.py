from sqlalchemy.ext.declarative import declarative_base
from src.core.config import settings
from sqlalchemy.ext.asyncio import (AsyncSession, async_sessionmaker,
                                    create_async_engine)
from src.core.base import Base


engine_todo = create_async_engine(settings.postgres.todo_database_url,
                                echo=True)

sessionLocalAsyncTodo = async_sessionmaker(
    bind=engine_todo,
    autoflush=False,
    autocommit=False,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_async_session_todo() -> AsyncSession:
    async with sessionLocalAsyncTodo() as session:
        yield session

async def async_init_db_todo():
    async with engine_users.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
