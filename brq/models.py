from __future__ import annotations

import json
from datetime import datetime

from pydantic import BaseModel


class Job(BaseModel):
    args: list
    kwargs: dict
    create_at: int = 0
    """
    Timestamp in milliseconds
    """

    uid: str = "no_uid"

    @classmethod
    def from_redis(cls, serialized: str) -> Job:
        return cls.model_validate_json(serialized)

    @classmethod
    def from_message(cls, message: dict) -> Job:
        return cls.model_validate_json(message["payload"])

    def to_redis(self) -> str:
        return json.dumps(self.model_dump(), sort_keys=True)

    def to_message(self) -> dict:
        return {"payload": self.to_redis()}
