"""异步任务网关 HTTP 入口。"""
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.models import SubmitRejected, Task, TaskStatus
from app.service import TaskService

service = TaskService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    service.start()
    yield
    service.shutdown()


app = FastAPI(lifespan=lifespan)


class SubmitRequest(BaseModel):
    caller: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    payload: dict[str, Any] = {}
    handler: str = "default"


class TaskView(BaseModel):
    id: str
    caller: str
    idempotency_key: str
    status: TaskStatus
    attempts: int
    error: Optional[str]
    result: Optional[Any]


def _view(t: Task) -> TaskView:
    return TaskView(id=t.id, caller=t.caller, idempotency_key=t.idempotency_key,
                    status=t.status, attempts=t.attempts, error=t.error,
                    result=t.result)


@app.post("/tasks", status_code=201)
def submit_task(req: SubmitRequest) -> TaskView:
    try:
        task = service.submit(req.caller, req.idempotency_key, req.payload,
                              req.handler)
    except SubmitRejected as e:
        raise HTTPException(status_code=429,
                            detail={"reason": e.reason.value, "message": e.message})
    return _view(task)


@app.get("/tasks/{task_id}")
def get_task(task_id: str) -> TaskView:
    task = service.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return _view(task)


@app.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str) -> TaskView:
    try:
        task = service.cancel(task_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="task not found")
    return _view(task)
