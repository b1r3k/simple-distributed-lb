from unittest import TestCase

from simple_distributed_lb.pubsub import PubSubMessage


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
