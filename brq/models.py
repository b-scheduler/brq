from datetime import datetime

from pydantic import BaseModel


class Job(BaseModel):
    function_name: str
    args: list
    kwargs: dict
    created_at: datetime
