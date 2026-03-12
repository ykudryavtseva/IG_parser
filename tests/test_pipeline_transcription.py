"""Tests for pipeline: raw content mode, transcript, citation_source, no related_pmids."""

from unittest.mock import MagicMock

from app.models import PostEvidence, ResearchItem
from app.services.pipeline import EvidencePipeline


def _make_pipeline(openai_key: str | None = "sk-test") -> EvidencePipeline:
    instagram = MagicMock()
    pubmed = MagicMock()
    relevance = MagicMock()
    transcription = MagicMock()
    return EvidencePipeline(
        instagram_client=instagram,
        pubmed_client=pubmed,
        relevance_checker=relevance,
        openai_api_key=openai_key,
        transcription_service=transcription,
    )


def test_raw_content_mode_returns_post_without_studies() -> None:
    """Post with caption but no PMIDs should be returned (raw content mode)."""
    pipeline = _make_pipeline()
    pipeline._instagram_client.fetch_posts = MagicMock(
        return_value=(
            [
                {
                    "id": "post1",
                    "url": "https://instagram.com/p/1",
                    "caption": "Креатин помогает при тренировках",
                    "owner": {"username": "blogger"},
                    "ownerUsername": "blogger",
                    "createdAt": "2024-01-15T10:00:00Z",
                    "displayUrl": "https://cdn.example.com/img.jpg",
                }
            ],
            None,
        )
    )
    pipeline._pubmed_client.extract_pmids = MagicMock(return_value=[])
    pipeline._pubmed_client.search_pmids_by_title = MagicMock(return_value=[])

    result = pipeline.run(
        topic="креатин",
        sources=["blogger"],
        max_items=5,
        discovery_limit=1,
        skip_relevance=True,
        latest_posts_mode=True,
    )

    assert len(result.items) == 1
    item = result.items[0]
    assert item.studies == []
    assert item.caption == "Креатин помогает при тренировках"
    assert item.content_type is not None
    assert item.author_username == "blogger"


def test_citation_source_from_caption() -> None:
    """PMID from caption should have citation_source='описание'."""
    pipeline = _make_pipeline()
    pipeline._instagram_client.fetch_posts = MagicMock(
        return_value=(
            [
                {
                    "id": "p1",
                    "url": "https://ig.com/p/1",
                    "caption": "Смотрите PMID 12345678",
                    "owner": {"username": "doc"},
                    "ownerUsername": "doc",
                }
            ],
            None,
        )
    )
    pipeline._pubmed_client.extract_pmids = MagicMock(return_value=["12345678"])
    pipeline._pubmed_client.fetch_study = MagicMock(
        return_value=ResearchItem(
            title="Study Title",
            authors=["Smith"],
            year=2023,
            pmid="12345678",
            pmid_url="https://pubmed.ncbi.nlm.nih.gov/12345678/",
            full_text_url=None,
        )
    )
    pipeline._pubmed_client.extract_pmids.side_effect = lambda t: (
        ["12345678"] if "12345678" in t else []
    )

    result = pipeline.run(
        topic="",
        sources=["doc"],
        max_items=5,
        discovery_limit=1,
        skip_relevance=True,
        latest_posts_mode=True,
    )

    assert len(result.items) == 1
    assert len(result.items[0].studies) == 1
    assert result.items[0].studies[0].citation_source == "описание"


def test_extract_video_url_returns_video_url() -> None:
    """_extract_video_url should return videoUrl from post."""
    pipeline = _make_pipeline()
    post = {"videoUrl": "https://cdn.example.com/reel.mp4"}
    url = EvidencePipeline._extract_video_url(post)
    assert url == "https://cdn.example.com/reel.mp4"


def test_extract_video_url_from_child() -> None:
    """_extract_video_url should check childPosts."""
    pipeline = _make_pipeline()
    post = {"childPosts": [{"videoUrl": "https://cdn.example.com/child.mp4"}]}
    url = EvidencePipeline._extract_video_url(post)
    assert url == "https://cdn.example.com/child.mp4"


def test_detect_content_type_reel() -> None:
    """Post with videoUrl or isVideo should be detected as reel."""
    assert EvidencePipeline._detect_content_type({"videoUrl": "http://x"}) == "reel"
    assert EvidencePipeline._detect_content_type({"isVideo": True}) == "reel"
    assert EvidencePipeline._detect_content_type({"mediaType": "Video"}) == "reel"
