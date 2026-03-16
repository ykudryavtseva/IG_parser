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
            content, status = self._download_video(video_url)
            if not content or len(content) > MAX_VIDEO_BYTES:
                if status != "ok":
                    _logger.info(
                        "transcription_skipped url=%s reason=%s",
                        video_url[:80],
                        status,
                    )
                return None
            result = self._call_whisper(content)
            if not result and status == "ok":
                _logger.debug("whisper_returned_empty url=%s", video_url[:80])
            return result
        except Exception as e:
            _logger.warning(
                "transcription_failed url=%s error=%s",
                video_url[:80],
                e,
                exc_info=True,
            )
            return None

    def transcribe_with_reason(
        self, video_url: str
    ) -> tuple[str | None, str]:
        """
        Like transcribe() but returns (text, reason).
        reason: 'ok', 'no_key', 'no_url', 'download_failed_http_403', etc.
        """
        if not self._api_key:
            return None, "no_openai_key"
        if not video_url or not video_url.startswith("http"):
            return None, "no_video_url"

        try:
            content, status = self._download_video(video_url)
            if not content:
                return None, status
            if len(content) > MAX_VIDEO_BYTES:
                return None, "video_too_large"
            result = self._call_whisper(content)
            return result, "ok" if result else "whisper_empty"
        except Exception as e:
            _logger.warning(
                "transcription_failed url=%s error=%s",
                video_url[:80],
                e,
                exc_info=True,
            )
            return None, f"error_{type(e).__name__}"

    @staticmethod
    def _download_video(url: str) -> tuple[bytes | None, str]:
        """Fetch video bytes. Returns (content, status). status: 'ok' or error reason."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "video/*,*/*;q=0.8",
            "Referer": "https://www.instagram.com/",
        }
        try:
            with httpx.Client(
                timeout=VIDEO_DOWNLOAD_TIMEOUT, headers=headers
            ) as client:
                response = client.get(url)
                response.raise_for_status()
                return response.content, "ok"
        except httpx.HTTPStatusError as e:
            status = getattr(e.response, "status_code", None)
            return None, f"download_failed_http_{status}"
        except httpx.HTTPError:
            return None, "download_failed"

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
