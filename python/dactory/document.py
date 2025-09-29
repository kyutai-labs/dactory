from typing import Annotated

from pydantic import BaseModel, Field


class Document(BaseModel):
    text: str
    date: str
    url: str
    language: str
    language_score: float
    warc_id: Annotated[str, Field(alias="warc-id")]
    scores: dict[str, float]
    group_idx: int
    warc_file: str
    record_idx: int
    repetitions: int | None

    class Config:
        validate_by_name = True
