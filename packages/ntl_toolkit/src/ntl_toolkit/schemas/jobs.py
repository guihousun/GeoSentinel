from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class JobRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    schema_: Literal["ntl.job.v1"] = Field(
        default="ntl.job.v1",
        alias="schema",
    )
    job_id: str
    tool: str
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    created_at: datetime
    updated_at: datetime
    request: dict[str, Any] = Field(default_factory=dict)
    outputs: list[str] = Field(default_factory=list)
