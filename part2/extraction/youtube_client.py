import os
import requests
from datetime import datetime, timezone

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


def get_api_key():
    key = os.getenv("YOUTUBE_API_KEY")
    if not key:
        raise ValueError("YOUTUBE_API_KEY not set in environment")
    return key


def get_channel_id(handle: str) -> dict | None:
    """
    Resolves a YouTube handle (@felipeneto) to channel metadata.
    Returns dict with channel_id, title, description.
    """
    handle = handle.lstrip("@")
    resp = requests.get(
        f"{YOUTUBE_API_BASE}/channels",
        params={
            "part": "id,snippet",
            "forHandle": handle,
            "key": get_api_key(),
        },
        timeout=10,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        return None
    item = items[0]
    return {
        "channel_id": item["id"],
        "handle": handle,
        "title": item["snippet"]["title"],
        "description": item["snippet"].get("description", ""),
    }


def get_uploads_playlist_id(channel_id: str) -> str | None:
    """
    Returns the uploads playlist ID for a channel.
    All public videos of a channel live in this playlist.
    """
    resp = requests.get(
        f"{YOUTUBE_API_BASE}/channels",
        params={
            "part": "contentDetails",
            "id": channel_id,
            "key": get_api_key(),
        },
        timeout=10,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        return None
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_videos_from_playlist(playlist_id: str, published_after: str, max_results: int = 50) -> list[dict]:
    """
    Returns videos from a playlist published after a given date.
    published_after: ISO 8601 string, e.g. '2026-01-01T00:00:00Z'
    """
    videos = []
    next_page_token = None

    while True:
        params = {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": max_results,
            "key": get_api_key(),
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        resp = requests.get(f"{YOUTUBE_API_BASE}/playlistItems", params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            snippet = item["snippet"]
            published_at = snippet.get("publishedAt", "")

            if published_at < published_after:
                continue

            videos.append({
                "video_id": snippet["resourceId"]["videoId"],
                "title": snippet["title"],
                "published_at": published_at,
                "channel_id": snippet["channelId"],
            })

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return videos


def get_video_stats(video_ids: list[str]) -> dict[str, dict]:
    """
    Returns stats and tags for a list of video IDs (max 50 per call).
    Returns dict keyed by video_id.
    """
    results = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp = requests.get(
            f"{YOUTUBE_API_BASE}/videos",
            params={
                "part": "statistics,snippet",
                "id": ",".join(batch),
                "key": get_api_key(),
            },
            timeout=10,
        )
        resp.raise_for_status()
        for item in resp.json().get("items", []):
            stats = item.get("statistics", {})
            results[item["id"]] = {
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "tags": item["snippet"].get("tags", []),
            }
    return results
