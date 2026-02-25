from pydantic import BaseModel, Field


class ResearchItem(BaseModel):
    title: str
    authors: list[str]
    year: int | None = None
    pmid: str
    pmid_url: str
    full_text_url: str | None = None
    abstract: str | None = None
    tags: list[str] = Field(default_factory=list)


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


class PipelineRunResult(BaseModel):
    """Results + stats for pipeline run."""

    items: list[PostEvidence] = Field(default_factory=list)
    posts_fetched: int = 0
    posts_with_caption: int = 0
    debug_posts_with_images: int = 0
    debug_pmids_from_text: int = 0
    debug_pmids_from_images: int = 0
    debug_pmids_fetch_failed: int = 0
    debug_images_fetched: int = 0
    debug_images_failed: int = 0
    debug_sample_url: str = ""
    debug_sample_status: str = ""
    debug_first_caption_snippet: str = ""
    debug_title_candidates_tried: int = 0
    debug_apify_first_post: str = ""
    debug_total_image_urls: int = 0
    debug_pmids_from_title_search: int = 0
    debug_pubmed_search_error: str = ""
    debug_first_title_candidate: str = ""
