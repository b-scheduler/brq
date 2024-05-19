from __future__ import annotations

import json
from datetime import datetime

from pydantic import BaseModel


class Job(BaseModel):
    args: list
    kwargs: dict
    create_at: int

    @classmethod
    def from_redis(cls, serialized: str) -> Job:
        return cls.model_validate_json(serialized)

    def to_redis(self) -> str:
        return json.dumps(self.model_dump(), sort_keys=True)
