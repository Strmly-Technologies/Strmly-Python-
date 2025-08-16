import os
import redis.asyncio as redis



def init_redis():
  redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        password=os.getenv("REDIS_PASSWORD"),
        username="default",
        decode_responses=True
    )
  return redis_client
