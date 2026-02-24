from pydantic import BaseModel, Field


class ResearchItem(BaseModel):
    title: str
    authors: list[str]
    year: int | None = None
    pmid: str
    pmid_url: str
    full_text_url: str | None = None


class PostEvidence(BaseModel):
    topic: str
    summary: str
    tags: list[str] = Field(default_factory=list)
    studies: list[ResearchItem] = Field(default_factory=list)
    post_url: str | None = None
    author_username: str | None = None
    published_at: str | None = None
    likes: int | None = None
    comments: int | None = None
