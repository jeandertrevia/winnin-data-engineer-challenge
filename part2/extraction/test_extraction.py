"""
Script de teste da extracao. Roda localmente antes de subir o Airflow.
Uso: python extraction/test_extraction.py
"""
import csv
from dotenv import load_dotenv
from tqdm import tqdm
from youtube_client import get_channel_id, get_uploads_playlist_id, get_videos_from_playlist, get_video_stats

load_dotenv(dotenv_path="../.env")

PUBLISHED_AFTER = "2026-01-01T00:00:00Z"
SEED_FILE = "creators_seed.csv"


def run():
    with open(SEED_FILE) as f:
        creators = list(csv.DictReader(f))

    for creator in tqdm(creators, desc="Creators", unit="creator"):
        handle = creator["handle"]

        channel = get_channel_id(handle)
        if not channel:
            tqdm.write(f"[SKIP] {handle} -> canal nao encontrado")
            continue

        tqdm.write(f"\n{handle} -> {channel['channel_id']} | {channel['title']}")

        playlist_id = get_uploads_playlist_id(channel["channel_id"])
        if not playlist_id:
            tqdm.write(f"  [SKIP] playlist nao encontrada")
            continue

        videos = get_videos_from_playlist(playlist_id, published_after=PUBLISHED_AFTER)
        tqdm.write(f"  videos em 2026: {len(videos)}")

        if not videos:
            continue

        video_ids = [v["video_id"] for v in videos]
        stats = get_video_stats(video_ids)

        for video in tqdm(videos[:2], desc="  exemplos", leave=False):
            s = stats.get(video["video_id"], {})
            tqdm.write(f"  [{video['published_at'][:10]}] {video['title'][:60]}")
            tqdm.write(f"    views: {s.get('views')} | likes: {s.get('likes')} | tags: {s.get('tags', [])[:3]}")


if __name__ == "__main__":
    run()
