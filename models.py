from dataclasses import dataclass
from typing import Literal


@dataclass
class Job:
    user_id: int
    chat_id: int
    url: str
    status_message_id: int
    username: str | None = None


@dataclass(frozen=True)
class UserRef:
    platform: Literal["telegram", "bale"]
    user_id: int
    chat_id: int | None = None
    username: str | None = None
