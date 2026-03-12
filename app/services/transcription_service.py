"""Transcription service for Reels via OpenAI Whisper API."""

import io
import logging
from typing import Protocol

import httpx

MAX_VIDEO_BYTES = 25 * 1024 * 1024  # 25 MB Whisper limit
VIDEO_DOWNLOAD_TIMEOUT = 60.0
TRANSCRIBE_TIMEOUT = 120.0

_logger = logging.getLogger(__name__)


class TranscriptionProvider(Protocol):
    """Protocol for transcription providers."""

    def transcribe(self, video_url: str) -> str | None:
        """Transcribe video from URL. Returns None on failure."""
        ...


class WhisperTranscriptionService:
    """Transcribe video using OpenAI Whisper API."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "whisper-1",
    ) -> None:
        self._api_key = api_key
        self._model = model

    def transcribe(self, video_url: str) -> str | None:
        """Download video and transcribe via Whisper. Returns None if no key or on error."""
        if not self._api_key or not video_url or not video_url.startswith("http"):
            return None

        try:
            content = self._download_video(video_url)
            if not content or len(content) > MAX_VIDEO_BYTES:
                return None
            return self._call_whisper(content)
        except Exception as e:
            _logger.warning(
                "transcription_failed url=%s error=%s",
                video_url[:80],
                e,
                exc_info=True,
            )
            return None

    @staticmethod
    def _download_video(url: str) -> bytes | None:
        """Fetch video bytes. Returns None on failure (e.g. CDN block)."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "video/*,*/*;q=0.8",
        }
        try:
            with httpx.Client(
                timeout=VIDEO_DOWNLOAD_TIMEOUT, headers=headers
            ) as client:
                response = client.get(url)
                response.raise_for_status()
                return response.content
        except httpx.HTTPError:
            return None

    def _call_whisper(self, content: bytes) -> str | None:
        """Send video to Whisper API and return transcript."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
        }
        files = {"file": ("video.mp4", io.BytesIO(content), "video/mp4")}
        data = {"model": self._model}
        try:
            with httpx.Client(timeout=TRANSCRIBE_TIMEOUT) as client:
                response = client.post(
                    "https://api.openai.com/v1/audio/transcriptions",
                    headers=headers,
                    files=files,
                    data=data,
                )
                response.raise_for_status()
                result = response.json()
                text = result.get("text") if isinstance(result, dict) else None
                return text.strip() if isinstance(text, str) and text.strip() else None
        except (httpx.HTTPError, KeyError, TypeError):
            return None
