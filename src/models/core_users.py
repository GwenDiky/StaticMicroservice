from sqlalchemy.ext.declarative import declarative_base
from src.core.config import settings
from sqlalchemy.ext.asyncio import (AsyncSession, async_sessionmaker,
                                    create_async_engine)
from src.core.base import Base


engine_users = create_async_engine(settings.postgres.users_database_url,
                                echo=True)

sessionLocalAsyncUsers = async_sessionmaker(
    bind=engine_users,
    autoflush=False,
    autocommit=False,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_async_session_users() -> AsyncSession:
    async with sessionLocalAsyncUsers() as session:
        yield session

async def async_init_db_users():
    async with engine_users.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
