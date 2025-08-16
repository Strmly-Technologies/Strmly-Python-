import asyncio
import json
import os
from dotenv import load_dotenv
import redis.asyncio as redis  # ✅ async redis client
from mongodb import connect_database
from typing import Any
from pymongo.collection import Collection
from nsfw import detect_nsfw_video
from video_fingerprint import fingerprint_video,compare_hamming_distance
from audio_fingerprint import fingerprint_audio
from redis_client import init_redis  # this must be async
from s3 import init_s3_client
from bson import ObjectId
load_dotenv()

STREAM_KEY = os.getenv("REDIS_STREAM_KEY", "video_events")
RESULT_STREAM_KEY = os.getenv("REDIS_RESULT_STREAM_KEY", "video_results")
GROUP_NAME = os.getenv("REDIS_CONSUMER_GROUP", "video_workers")
CONSUMER_NAME = os.getenv("REDIS_CONSUMER_NAME", "worker_1")




async def check_video_fingerprint_duplicates(
    long_video_collection: Collection,
    r,  # redis asyncio connection
    video_id: str,
    user_id: str,
    video_url: str,
    fingerprint_hex: str,
    RESULT_STREAM_KEY: str
):
    """
    Checks all LongVideo entries for duplicate fingerprints using compare_hamming_distance.
    Fires Redis event if a match is found.
    """
    print("checking video duplicates...")
    # Fetch all other videos with non-empty fingerprint
    other_videos = await asyncio.to_thread(
        lambda: list(
            long_video_collection.find(
                {
                    "_id": {"$ne": ObjectId(video_id)},
                    "fingerprint": {"$ne": ""}
                },
                {
                    "_id": 1,
                    "videoUrl": 1,
                    "fingerprint": 1
                }
            )
        )
    )

    for doc in other_videos:
        db_fingerprint = doc.get("fingerprint")
        if not db_fingerprint:
            continue

        # Compare using your existing function
        if compare_hamming_distance(fingerprint_hex, db_fingerprint):
            print("video duplicate found, firing redis event")
            await r.xadd(RESULT_STREAM_KEY, {
                "event_type": "duplicate_detected_using_video_fingerprint",
                "videoId": video_id,
                "userId": user_id,
                "videoUrl": video_url,
                "fingerprint": fingerprint_hex,
                "matchedVideoId": str(doc.get("_id")),
                "matchedFingerPrint": db_fingerprint,
                "matchedVideoUrl": doc.get("videoUrl"),
            })
    print("video duplicates checked successfully")

async def check_audio_fingerprint_duplicates(
    long_video_collection: Collection,
    r,  # redis asyncio connection
    video_id: str,
    user_id: str,
    video_url: str,
    fingerprint_hex: str,
    RESULT_STREAM_KEY: str
):
    """
    Checks all LongVideo entries for duplicate fingerprints using compare_hamming_distance.
    Fires Redis event if a match is found.
    """
    print("checking audio duplicates...")
    # Fetch all other videos with non-empty fingerprint
    other_videos = await asyncio.to_thread(
        lambda: list(
            long_video_collection.find(
                {
                    "_id": {"$ne": ObjectId(video_id)},
                    "audio_fingerprint": {"$ne": ""}
                },
                {
                    "_id": 1,
                    "videoUrl": 1,
                    "audio_fingerprint": 1
                }
            )
        )
    )


    for doc in other_videos:
        db_fingerprint = doc.get("audio_fingerprint")
        if not db_fingerprint:
            continue

        # Compare using your existing function
        if compare_hamming_distance(fingerprint_hex, db_fingerprint):
            print("audio duplicate found, firing redis event")
            await r.xadd(RESULT_STREAM_KEY, {
                "event_type": "duplicate_detected_using_audio_fingerprint",
                "videoId": video_id,
                "userId": user_id,
                "videoUrl": video_url,
                "audioFingerprint": fingerprint_hex,
                "matchedVideoId": str(doc.get("_id")),
                "matchedAudioFingerprint": db_fingerprint,
                "matchedVideoUrl": doc.get("videoUrl"),
            })
    print("audio duplicates checked successfully")





async def process_event(r: redis.Redis, event_data: dict, msg_id: str,long_video_collection,s3_client):
    video_id = str(event_data.get("videoId", ""))
    video_url = str(event_data.get("videoUrl", ""))
    user_id = str(event_data.get("userId", ""))
    event_type = str(event_data.get("type", ""))

    print(f"video_url:{video_url}")
    print(f"user_id:{user_id}")
    print(f"user_id:{user_id}")
    print(f"event_type:{event_type}")

    try:
        print(f"🚀 Processing {event_type} for video {video_id}")

        if event_type == "nsfw_detection":
            is_nsfw = await asyncio.to_thread(detect_nsfw_video, video_url,video_id,s3_client)
            print(f"✅ NSFW result for {video_id}: {is_nsfw}")
            if is_nsfw:
                print(f"nsfw video found:{video_id}")
                await r.xadd(RESULT_STREAM_KEY, {
                    "event_type": "nsfw_detected",
                    "videoId": video_id,
                    "userId": user_id,
                    "videoUrl": video_url,
                    "is_nsfw": json.dumps(is_nsfw)
                })
            print(f"nsfw check completed {msg_id}")

        elif event_type == "video_fingerprint":
            fingerprint = await asyncio.to_thread(fingerprint_video, video_id,video_url,s3_client)
            fingerprint_hex = fingerprint.hex() if isinstance(fingerprint, bytes) else fingerprint
            print(f"✅ Video fingerprint for {video_id}: {fingerprint}")
            result = await asyncio.to_thread(
                lambda: long_video_collection.update_one(
                {"_id": ObjectId(video_id)},
                {"$set": {"fingerprint": fingerprint_hex}}
                )
            )
            if result.modified_count == 0:
                print(f"⚠️ No document updated for videoId: {video_id}")
            await check_video_fingerprint_duplicates(long_video_collection,r,video_id,user_id,video_url,fingerprint_hex,RESULT_STREAM_KEY)
            print(f"duplicate check using video fingerprints completed {msg_id}")

        elif event_type == "audio_fingerprint":
            fingerprint = await asyncio.to_thread(fingerprint_audio, video_url, video_id,s3_client)
            fingerprint_hex = fingerprint.hex() if isinstance(fingerprint, bytes) else fingerprint
            print(f"✅ Audio fingerprint for {video_id}: {fingerprint}")
            result = await asyncio.to_thread(
                lambda: long_video_collection.update_one(
                {"_id": ObjectId(video_id)},
                {"$set": {"audio_fingerprint": fingerprint_hex}}
                )
            )
            if result.modified_count == 0:
                print(f"⚠️ No document updated for videoId: {video_id}")
            await check_audio_fingerprint_duplicates(long_video_collection,r,video_id,user_id,video_url,fingerprint_hex,RESULT_STREAM_KEY)
            print(f"duplicate check using audio fingerprints completed {msg_id}")

        else:
            raise Exception(f"Invalid event_type: {event_type}")

    except Exception as e:
        print(f"❌ Error processing video {video_id}: {e}")

    finally:
        await r.xack(STREAM_KEY, GROUP_NAME, msg_id)


async def worker():
    r = await init_redis()  # ✅ async init
    s3_client=init_s3_client() #init s3 client
    client, long_video_collection = connect_database()

    # Ensure consumer group exists
    try:
        await r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
        print(f"✅ Consumer group '{GROUP_NAME}' created")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"ℹ️ Consumer group '{GROUP_NAME}' already exists")
        else:
            raise

    print(f"📡 Listening on stream '{STREAM_KEY}' as '{CONSUMER_NAME}'...")

    while True:
        try:
            messages = await r.xreadgroup(
                GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: ">"}, count=5, block=5000
            )

            if messages:
                for stream, events in messages:
                    for msg_id, data in events:
                        try:
                            print(f"task arrived in queue {msg_id}")
                            asyncio.create_task(process_event(r, data, msg_id,long_video_collection,s3_client))
                        except Exception as e:
                            print(f"❌ Failed to schedule event {msg_id}: {e}")

        except redis.ConnectionError as e:
            print(f"⚠️ Redis connection lost: {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(worker())
