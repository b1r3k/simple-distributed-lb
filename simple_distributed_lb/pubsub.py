import asyncio
import logging
from typing import Awaitable, Callable, Dict

import pydantic
import redis.asyncio as aioredis

logger = logging.getLogger()


class PubSubMessage(pydantic.BaseModel):
    type: str
    channel: str
    pattern: str
    data: str
    redis_key: str

    @classmethod
    def from_redis_message(cls, message: dict) -> "PubSubMessage":
        """
        Parses a message from Redis PubSub and creates a PubSubMessage instance

        {type='pmessage' channel=b'__keyspace@0__:slb_/hello' pattern=b'__keyspace@0__:slb_*' data=b'sadd'}
        """
        # Convert bytes to string for each item in the message
        channel = message["channel"].decode("utf-8")
        return cls(
            type=message["type"],
            channel=channel,
            pattern=message["pattern"].decode("utf-8"),
            data=message["data"].decode("utf-8"),
            redis_key=channel.split(":")[-1],
        )


def setup_redis(url: str) -> aioredis.Redis:
    return aioredis.from_url(url)


async def teardown_redis(redis_client: aioredis.Redis):
    await redis_client.close()


class RedisKeyspaceListener:
    KEYSPACE = "__keyspace@{db_name}__"

    def __init__(
        self,
        redis_client: aioredis.Redis,
        callbacks: Dict[str, Callable[[PubSubMessage], Awaitable]],
        *,
        key_pattern: str = "slb_",
        database: int = 0,
    ):
        self.redis_client = redis_client
        self.callbacks = callbacks
        self.key_pattern = key_pattern
        self.database = database
        self.keyspace = self.KEYSPACE.format(db_name=self.database)
        self.channel = None
        self.listener = None

    async def subscribe(self) -> asyncio.Task:
        if self.channel is None:
            self.channel = self.redis_client.pubsub()
        pattern = f"{self.keyspace}:{self.key_pattern}*"
        logger.info("Subscribing to keyspace on: %s", pattern)
        await self.channel.psubscribe(pattern)
        self.listener = asyncio.create_task(self._listen())
        return self.listener

    async def _listen(self):
        logger.info("Starting keyspace notifications listener")
        try:
            while True:
                message = await self.channel.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    logger.info("Received message: %s", message)
                    try:
                        pubsub_message = PubSubMessage.from_redis_message(message)
                    except pydantic.ValidationError:
                        continue
                    try:
                        op = pubsub_message.data
                        callback = self.callbacks.get(op)
                        if callback:
                            await callback(pubsub_message)
                    except Exception:
                        logger.exception("Error in keyspace listener callback")
        except asyncio.CancelledError:
            logger.info("Keyspace listener cancelled")

    async def aclose(self):
        if self.listener is not None:
            self.listener.cancel()
        await self.channel.punsubscribe()
        await self.channel.close()
        logger.info("Keyspace listener closed")
