import logging
import unittest
from os import environ

import dns.resolver
import httpx

log_level = environ.get("LOG_LEVEL", "INFO")

logger = logging.getLogger()
logger.setLevel(level=logging.getLevelName(log_level))


class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loadbalancer_host = environ.get("LOADBALANCER_HOST")
        cls.loadbalancer_port = environ.get("LOADBALANCER_PORT")
        # throw an error if the environment variables are not set'
        assert cls.loadbalancer_host, "LOADBALANCER_HOST env is not set"
        assert cls.loadbalancer_port, "LOADBALANCER_PORT env is not set"


class TestDNSConfiguration(BaseTestCase):
    def test_dns_targets_count(self):
        # use dnspython to get all IPs registered for given domain using system DNS resolver
        response = dns.resolver.resolve(self.loadbalancer_host, "A")
        print(response)
        self.assertEqual(len([r.address for r in response]), 2)


class TestHTTPRoundRobin(BaseTestCase):
    def test_single_registered_endpoint(self):
        lb_base_url = f"http://{self.loadbalancer_host}:{self.loadbalancer_port}"
        lb_proxy_url = f"{lb_base_url}/hello_world"
        lb_stats_url = f"{lb_base_url}/_stats"
        req_count = 1000
        with httpx.Client() as client:
            for i in range(req_count):
                try:
                    response = client.get(lb_proxy_url)
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    logger.debug(f"Request {i + 1} failed: {e.response.status_code}")
                except Exception as e:
                    logger.error(f"Request {i + 1} failed: {str(e)}")
            # check how many targets are there instead of hardcoding the number
            expected_req_per_target = req_count // 2
            try:
                response = client.get(lb_stats_url)
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                self.fail(f"Failed to fetch stats: {e.response.status_code}")
            stats_json = response.json()
            counts = [stat["GET|/"] for ip, stat in stats_json.items()]
            assert all(
                count == expected_req_per_target for count in counts
            ), "All upstream servers should have received"
