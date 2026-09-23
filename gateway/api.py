"""HTTP layer for the async task gateway.

Endpoints
---------
POST /tasks                 submit (headers: X-Caller, optional X-Idempotency-Key)
GET  /tasks/{task_id}       query state
POST /tasks/{task_id}/cancel
GET  /healthz

All :class:`GatewayError` subclasses share one JSON body shape
``{"error": {"code", "message", ["retry_after"]}}`` with a matching HTTP
status, so failure reasons remain distinguishable over HTTP.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from gateway.errors import ErrorCode, GatewayError
from gateway.executor import TaskExecutor

logger = logging.getLogger("gateway.api")

_STATUS_BY_CODE = {
    ErrorCode.IDEMPOTENCY_CONFLICT: 409,
    ErrorCode.RATE_LIMIT_REJECTED: 429,
    ErrorCode.CONCURRENCY_LIMIT_REJECTED: 429,
    ErrorCode.QUEUE_FULL_REJECTED: 429,
    ErrorCode.TASK_NOT_FOUND: 404,
    ErrorCode.TASK_NOT_CANCELLABLE: 409,
    ErrorCode.SHUTTING_DOWN: 503,
    ErrorCode.UNKNOWN_TASK_TYPE: 400,
    ErrorCode.HANDLER_FAILED: 500,
    ErrorCode.CANCELLED: 409,
}


class SubmitBody(BaseModel):
    task_type: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)


def create_app(executor: TaskExecutor) -> FastAPI:
    app = FastAPI(title="async-task-gateway", version="0.1.0")
    app.state.executor = executor

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(request: Request, exc: GatewayError) -> JSONResponse:
        status = _STATUS_BY_CODE.get(exc.code, 400)
        logger.info("error response: path=%s code=%s status=%s message=%s",
                    request.url.path, exc.code, status, exc.message)
        return JSONResponse(status_code=status, content=exc.to_dict())

    @app.post("/tasks", status_code=201)
    async def submit_task(
        body: SubmitBody,
        x_caller: str = Header(..., alias="X-Caller"),
        x_idempotency_key: str | None = Header(default=None, alias="X-Idempotency-Key"),
    ):
        result = await executor.submit(
            caller=x_caller,
            task_type=body.task_type,
            payload=body.payload,
            idempotency_key=x_idempotency_key,
        )
        view = result.task.to_view()
        view["reused"] = result.reused
        return view

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str):
        task = await executor.get_task(task_id)
        return task.to_view()

    @app.post("/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str):
        task = await executor.cancel(task_id)
        return {"task_id": task_id, "state": task.state.value,
                "cancellation_requested": True}

    @app.get("/healthz")
    async def healthz():
        stats = executor.stats()
        stats["status"] = "draining" if stats["shutting_down"] else "ok"
        return stats

    return app
