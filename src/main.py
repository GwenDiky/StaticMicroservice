import uvicorn
from fastapi import FastAPI
from src.api.handlers.statics import statics_router

app = FastAPI()
app.include_router(statics_router, prefix="/static", tags=["static"])

if __name__ == "__main__":
    uvicorn.run("main:app", reload=True, port=8084)

