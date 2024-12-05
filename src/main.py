import logging

import uvicorn
from fastapi import FastAPI

from src.core.config import setup_logging
from src.core.consumer import start_listen
from src.models.core_static import get_mongo_client

logger = logging.getLogger(__name__)

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    client = get_mongo_client()
    await start_listen(client)


if __name__ == "__main__":
    setup_logging()
    uvicorn.run("main:app", reload=True, port=8084)
