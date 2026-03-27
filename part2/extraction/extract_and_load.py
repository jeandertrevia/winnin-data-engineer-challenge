"""
Extrai dados da YouTube API e carrega no Postgres (raw layer).

Fluxo:
  1. Le creators ativos de raw.creators_seed
  2. Resolve handle -> channel_id via API e upsert em raw.creators
  3. Busca videos de 2026 de cada creator
  4. Upsert em raw.posts
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))
from youtube_client import (
    get_channel_id,
    get_uploads_playlist_id,
    get_videos_from_playlist,
    get_video_stats,
)

PUBLISHED_AFTER = "2026-01-01T00:00:00Z"


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB", "creators_db"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )


def load_seed(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT handle, name FROM raw.creators_seed WHERE active = TRUE")
        rows = cur.fetchall()
    return [{"handle": r[0], "name": r[1]} for r in rows]


def upsert_creator(conn, handle: str, channel_id: str, title: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.creators (handle, channel_id, title, loaded_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (handle) DO UPDATE
                SET channel_id = EXCLUDED.channel_id,
                    title      = EXCLUDED.title,
                    loaded_at  = NOW()
            """,
            (handle.lstrip("@"), channel_id, title),
        )
    conn.commit()


def upsert_posts(conn, posts: list[dict]):
    if not posts:
        return
    with conn.cursor() as cur:
        for post in posts:
            cur.execute(
                """
                INSERT INTO raw.posts (video_id, channel_id, title, published_at, views, likes, tags, loaded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (video_id) DO UPDATE
                    SET views     = EXCLUDED.views,
                        likes     = EXCLUDED.likes,
                        tags      = EXCLUDED.tags,
                        loaded_at = NOW()
                """,
                (
                    post["video_id"],
                    post["channel_id"],
                    post["title"],
                    post["published_at"],
                    post["views"],
                    post["likes"],
                    post["tags"],
                ),
            )
    conn.commit()


def run():
    conn = get_connection()
    creators = load_seed(conn)
    print(f"Creators na seed: {len(creators)}")

    for creator in tqdm(creators, desc="Processando creators", unit="creator"):
        handle = creator["handle"]

        channel = get_channel_id(handle)
        if not channel:
            tqdm.write(f"[SKIP] {handle} -> canal nao encontrado na API")
            continue

        upsert_creator(conn, handle, channel["channel_id"], channel["title"])
        tqdm.write(f"[OK] {handle} -> {channel['channel_id']} | {channel['title']}")

        playlist_id = get_uploads_playlist_id(channel["channel_id"])
        if not playlist_id:
            tqdm.write(f"  [SKIP] playlist nao encontrada")
            continue

        videos = get_videos_from_playlist(playlist_id, published_after=PUBLISHED_AFTER)
        tqdm.write(f"  {len(videos)} videos encontrados em 2026")

        if not videos:
            continue

        video_ids = [v["video_id"] for v in videos]
        stats = get_video_stats(video_ids)

        posts = []
        for video in tqdm(videos, desc=f"  {handle}", leave=False):
            s = stats.get(video["video_id"], {})
            posts.append({
                "video_id":     video["video_id"],
                "channel_id":   video["channel_id"],
                "title":        video["title"],
                "published_at": video["published_at"],
                "views":        s.get("views", 0),
                "likes":        s.get("likes", 0),
                "tags":         s.get("tags", []),
            })

        upsert_posts(conn, posts)
        tqdm.write(f"  {len(posts)} posts carregados no Postgres")

    conn.close()
    print("Extracao concluida.")


if __name__ == "__main__":
    run()
