import asyncio
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock

import pydantic
import redis.asyncio as aioredis

from simple_distributed_lb.pubsub import PubSubMessage, RedisKeyspaceListener
from simple_distributed_lb.retry import RetryMechanism


class TestPubSubMessage(TestCase):
    def test_redis_message_parsing(self):
        message = {
            "type": "pmessage",
            "channel": b"__keyspace@0__:slb_/hello",
            "pattern": b"__keyspace@0__:slb_*",
            "data": b"sadd",
        }
        pubsub_message = PubSubMessage.from_redis_message(message)
        self.assertEqual(pubsub_message.type, "pmessage")
        self.assertEqual(pubsub_message.channel, "__keyspace@0__:slb_/hello")
        self.assertEqual(pubsub_message.pattern, "__keyspace@0__:slb_*")
        self.assertEqual(pubsub_message.data, "sadd")
        self.assertEqual(pubsub_message.redis_key, "slb_/hello")

    def test_redis_message_parsing_error(self):
        message = {
            "type": "pmessage",
            "channel": "__keyspace@0__:slb_/hello",
            "pattern": b"__keyspace@0__:slb_*",
            "data": b"sadd",
        }
        with self.assertRaises(pydantic.ValidationError):
            PubSubMessage.from_redis_message(message)


class TestRedisKeyspaceListener(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis_mock = AsyncMock(spec=aioredis.Redis)
        self.channel = AsyncMock(spec=aioredis.client.PubSub)
        self.channel_message = {
            "type": "pmessage",
            "channel": b"__keyspace@0__:slb_/hello",
            "pattern": b"__keyspace@0__:slb_*",
            "data": b"sadd",
        }
        self.channel.get_message.side_effect = [self.channel_message]
        self.redis_mock.pubsub.return_value = self.channel

    async def test_keyspace_listener_subscribe(self):
        inst = RedisKeyspaceListener(redis_client=self.redis_mock, callbacks={}, key_pattern="slb_", database=0)
        await inst.subscribe()
        self.redis_mock.pubsub.assert_called_once()
        self.channel.psubscribe.assert_awaited_once_with("__keyspace@0__:slb_*")

    async def test_keyspace_listener_connection_error(self):
        self.channel.psubscribe.side_effect = aioredis.ConnectionError("Connection lost")
        inst = RedisKeyspaceListener(redis_client=self.redis_mock, callbacks={}, key_pattern="slb_", database=0)
        with self.assertRaises(aioredis.ConnectionError):
            await inst.subscribe()
        self.assertEqual(self.channel.psubscribe.call_count, 1)

    async def test_keyspace_listener_timeout_error(self):
        self.channel.psubscribe.side_effect = aioredis.TimeoutError("Operation timed out")
        inst = RedisKeyspaceListener(redis_client=self.redis_mock, callbacks={}, key_pattern="slb_", database=0)

        with self.assertRaises(aioredis.TimeoutError):
            await inst.subscribe()
        self.assertEqual(self.channel.psubscribe.call_count, 1)

    async def test_keyspace_listener_abrupt_disconnection(self):
        self.channel.get_message.side_effect = [
            self.channel_message,
            aioredis.ConnectionError("Connection lost"),
            aioredis.TimeoutError("Operation timed out"),
            aioredis.ConnectionError("Connection lost"),
            asyncio.CancelledError(),
        ]
        retry = RetryMechanism(max_wait=0.01)
        inst = RedisKeyspaceListener(
            redis_client=self.redis_mock, callbacks={}, key_pattern="slb_", database=0, retry_mechanism=retry
        )

        await inst.run()

        # Check that the listener attempted to reconnect
        self.assertEqual(self.channel.psubscribe.await_count, 3)
        self.channel.psubscribe.assert_awaited_with("__keyspace@0__:slb_*")
