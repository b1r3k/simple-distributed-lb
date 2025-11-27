import asyncio
import logging
from enum import StrEnum, auto
from typing import AsyncGenerator, Awaitable, Callable, Dict, List

import pydantic
import redis.asyncio as aioredis
from pydantic_core import InitErrorDetails, PydanticCustomError

from simple_distributed_lb.retry import RetryMechanism

logger = logging.getLogger()


class RedisKeyspaceCommand(StrEnum):
    SADD = auto()


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
        errors: List[InitErrorDetails] = []
        # Convert bytes to string for each item in the message
        try:
            message_type = message["type"]
            channel = message["channel"].decode("utf-8")
            message_pattern = message["pattern"].decode("utf-8")
            message_data = message["data"].decode("utf-8").lower()
            redis_key = channel.split(":")[-1]
        except (AttributeError, KeyError, TypeError):
            logger.error("Invalid message format: %s", message)
            errors.append(
                InitErrorDetails(
                    loc=("message parser",),
                    type=PydanticCustomError("invalid_message", "Invalid message format"),
                    input="message",
                    ctx={"message": message},
                )
            )
        else:
            return cls(
                type=message_type, channel=channel, pattern=message_pattern, data=message_data, redis_key=redis_key
            )
        finally:
            if errors:
                raise pydantic.ValidationError.from_exception_data(title="parsing error", line_errors=errors)


def setup_redis(url: str) -> aioredis.Redis:
    return aioredis.from_url(url)


async def teardown_redis(redis_client: aioredis.Redis):
    await redis_client.close()


class RedisKeyspaceListener:
    KEYSPACE = "__keyspace@{db_name}__"

    def __init__(
        self,
        redis_client: aioredis.Redis,
        callbacks: Dict[RedisKeyspaceCommand, Callable[[PubSubMessage], Awaitable]],
        *,
        retry_mechanism: RetryMechanism = None,
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
        self.logger = logging.getLogger(self.__class__.__name__)
        self.retry_mechanism = retry_mechanism or RetryMechanism()

    async def subscribe(self) -> asyncio.Task:
        if self.channel is None:
            self.channel = self.redis_client.pubsub()
        pattern = f"{self.keyspace}:{self.key_pattern}*"
        self.logger.info("Subscribing to keyspace on: %s", pattern)
        await self.channel.psubscribe(pattern)

    async def _get_message(self) -> AsyncGenerator[PubSubMessage, None]:
        while True:
            message = await self.channel.get_message(ignore_subscribe_messages=True)
            self.logger.debug("Received message: %s", message)
            if message:
                try:
                    pubsub_message = PubSubMessage.from_redis_message(message)
                    yield pubsub_message
                except pydantic.ValidationError as e:
                    self.logger.error("Invalid PubSub message: %s", e)
            else:
                await asyncio.sleep(1)

    async def _listen(self):
        self.logger.info("Starting main loop")
        async for pubsub_message in self._get_message():
            try:
                op = RedisKeyspaceCommand(pubsub_message.data)
                callback = self.callbacks.get(op)
                if callback:
                    await callback(pubsub_message)
            except ValueError:
                # ignore unknown operations
                continue
            except Exception:
                self.logger.exception("Unhandled error in callback")

    async def run(self):
        """Main loop to run the listener"""
        for attempt_delay in self.retry_mechanism:
            try:
                await self.subscribe()
                await self._listen()
            except (aioredis.ConnectionError, aioredis.TimeoutError) as e:
                self.logger.warning("Redis connection error: %s. Reconnecting in %s", e, attempt_delay)
                await asyncio.sleep(attempt_delay)
            except asyncio.CancelledError:
                self.logger.debug("main loop cancelled")
            except (StopAsyncIteration, RuntimeError):
                self.logger.info("no more messages?")
            finally:
                await self.aclose()

    async def aclose(self):
        if self.listener is not None:
            self.listener.cancel()
        try:
            await self.channel.punsubscribe()
        except Exception:
            pass
        try:
            await self.channel.close()
        except Exception:
            pass
        self.logger.info("listener closed")
