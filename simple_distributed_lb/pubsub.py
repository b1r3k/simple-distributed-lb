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
        # Use infinite retries with 60-second max backoff for persistent reconnection
        self.retry_mechanism = retry_mechanism or RetryMechanism(max_retries=float("inf"), min_wait=1, max_wait=60)
        self._connected = False

    @property
    def is_connected(self) -> bool:
        """Check if Redis connection is active."""
        return self._connected

    def _set_connected(self, connected: bool) -> None:
        """
        Update connection state and log changes.

        Provides operational visibility into Redis connection health by logging
        state transitions. This enables monitoring and debugging of fail-open behavior.
        """
        previous = self._connected
        self._connected = connected

        # Log state transitions for operational visibility
        if previous and not connected:
            self.logger.warning("Redis connection lost - operating in fail-open mode")
        elif not previous and connected:
            self.logger.info("Redis connection restored")

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
        """
        Main loop to run the listener with persistent reconnection.

        Implements fail-open behavior: during Redis outages, the load balancer
        continues routing traffic using cached targets while attempting to
        reconnect in the background with exponential backoff (max 60 seconds).

        The retry mechanism uses infinite retries, ensuring reconnection attempts
        continue indefinitely during persistent Redis outages.
        """
        self.logger.info("Starting Redis keyspace listener with persistent reconnection")

        for attempt_delay in self.retry_mechanism:
            try:
                await self.subscribe()
                self._set_connected(True)
                self.logger.info("Redis keyspace subscription established")
                await self._listen()
            except (aioredis.ConnectionError, aioredis.TimeoutError) as e:
                self._set_connected(False)
                self.logger.error("Redis connection error: %s. Retrying in %s seconds", e, attempt_delay)
                await asyncio.sleep(attempt_delay)
            except asyncio.CancelledError:
                self._set_connected(False)
                self.logger.debug("Redis listener task cancelled")
                break
            except (StopAsyncIteration, RuntimeError) as e:
                self._set_connected(False)
                self.logger.warning("Redis listener stopped unexpectedly: %s. Retrying in %s seconds", e, attempt_delay)
                await asyncio.sleep(attempt_delay)
            except Exception as e:
                self._set_connected(False)
                self.logger.error("Unexpected error in Redis listener: %s. Retrying in %s seconds", e, attempt_delay)
                await asyncio.sleep(attempt_delay)
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
