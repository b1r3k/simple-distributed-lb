import logging
import sys
from functools import reduce

import httpx
import orjson as json
import pydantic
from pydantic_settings import BaseSettings, SettingsConfigDict
from starlette.applications import Starlette
from starlette.datastructures import URL, MutableHeaders
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from .pubsub import (
    PubSubMessage,
    RedisKeyspaceCommand,
    RedisKeyspaceListener,
    setup_redis,
    teardown_redis,
)
from .targets import Host, RoundRobinTargets


class GlobalSettings(BaseSettings):
    model_config = SettingsConfigDict(
        # `.env.prod` takes priority over `.env`
        env_file=(".env", ".env.prod"),
        extra="ignore",
    )
    # defaults
    LOG_LEVEL: str = "INFO"
    REDIS_DSN: str = "redis://localhost:6379/0"
    DEBUG: bool = False


settings = GlobalSettings()

log_level = logging.getLevelName(settings.LOG_LEVEL)
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger()

http_client = None
lb_targets: RoundRobinTargets | None = None
redis_client = None
redis_keyspace: RedisKeyspaceListener | None = None


def get_sanitized_headers(headers) -> MutableHeaders:
    new_headers = MutableHeaders(headers=headers)
    del new_headers["connection"]
    for header in new_headers:
        if header.lower().startswith("x-forwarded"):
            del new_headers[header]
    return new_headers


async def get_proxied_response(client, incoming_req: Request, target: Host):
    target_url = URL(hostname=target.ip_address, port=target.port)
    scheme = target_url.scheme or "http"
    target_url = incoming_req.url.replace(hostname=target_url.hostname, scheme=scheme, port=target_url.port, path="/")
    logger.debug("Forwarding to: " + str(target_url))
    # Create a new request to the target server
    endpoint = target_url.hostname
    headers = get_sanitized_headers(incoming_req.headers)
    headers["host"] = endpoint
    has_content = int(headers.get("content-length", 0)) > 0
    data = incoming_req.stream() if has_content else None
    proxy_request = client.build_request(incoming_req.method, str(target_url), headers=headers, data=data)
    response = await client.send(proxy_request)
    return response


async def handle_registration(req: Request) -> Response:
    global lb_targets
    global redis_client
    global redis_keyspace

    raw_body = await req.body()
    try:
        json_body = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.error("Invalid JSON: %s", raw_body)
        raise HTTPException(status_code=400, detail="Invalid JSON")
    for item in json_body:
        ip_addr = item.get("ip_address")
        port = item.get("port")
        path = item.get("path", "/")
        try:
            target = Host(ip_address=ip_addr, port=port)
            main_key = f"{redis_keyspace.key_pattern}paths"
            path_key = f"{redis_keyspace.key_pattern}{path}"
            await redis_client.sadd(path_key, f"{ip_addr}:{port}")
            await redis_client.sadd(main_key, path)
            logger.info("Distributed registration request for %s -> %s", path, target)
        except pydantic.ValidationError:
            raise HTTPException(status_code=400, detail="Invalid IP address, port or path")
    return Response(status_code=200)


async def healthcheck(_: Request) -> Response:
    logger.debug("Healthcheck OK")
    return Response(status_code=200, content="OK")


async def get_target_stats(_: Request) -> Response:
    global lb_targets

    stats = {}

    # take dict[str, List[Host]] and return List[Host] for all values, esentially performing a flatten operation
    all_targets = set(reduce(lambda x, y: x + y, lb_targets.targets.values(), []))
    logger.info("Fetching stats for targets: %s", all_targets)

    async with httpx.AsyncClient() as http_client:
        for target in all_targets:
            try:
                response = await http_client.get(f"http://{target.ip_address}:{target.port}/stats")
                response.raise_for_status()
                target_stats = response.json()
                stats[str(target)] = target_stats
            except httpx.RequestError as e:
                logger.error("Error %s while fetching stats from target: %s", str(e), target)
    return JSONResponse(content=stats)


async def handle_registration_notification(_: PubSubMessage | None = None):
    global lb_targets
    global redis_client
    global redis_keyspace

    main_key = f"{redis_keyspace.key_pattern}paths"
    active_paths = await redis_client.smembers(main_key)
    for path in map(lambda p: p.decode(), active_paths):
        path_key = f"{redis_keyspace.key_pattern}{path}"
        targets = await redis_client.smembers(path_key)
        for target in map(lambda t: t.decode(), targets):
            host, port = target.split(":")
            target = Host(ip_address=host, port=int(port))
            if lb_targets.add(path, target):
                logger.info("Registered target: %s on path: %s", target, path)


async def handle_proxy(request: Request):
    global lb_targets
    global http_client

    logger.debug("Handling connection to: %s", request.url.path)
    try:
        target = lb_targets.get_next(request.url.path)
    except ValueError:
        raise HTTPException(status_code=503, detail="No targets available")
    try:
        response = await get_proxied_response(http_client, request, target)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Connection to target timed out")
    except httpx.RequestError:
        raise HTTPException(status_code=502, detail="Upstream unreachable")
    # Create a streaming response to send back to the client
    if response.content:
        return StreamingResponse(response.aiter_bytes(), status_code=response.status_code, headers=response.headers)
    else:
        return Response(status_code=response.status_code, headers=response.headers)


async def app_startup():
    global http_client
    global lb_targets
    global redis_client
    global redis_keyspace

    limits = httpx.Limits(max_keepalive_connections=100, max_connections=1000)
    timeout = httpx.Timeout(None, connect=5)
    http_client = httpx.AsyncClient(follow_redirects=True, limits=limits, timeout=timeout)
    lb_targets = RoundRobinTargets()
    redis_client = setup_redis(settings.REDIS_DSN)
    redis_keyspace = RedisKeyspaceListener(
        redis_client, callbacks={RedisKeyspaceCommand.SADD: handle_registration_notification}
    )
    await redis_keyspace.subscribe()
    # initialize for the first time since keyspace notifications are not triggered if redis operation did not modify
    # value for given key
    await handle_registration_notification()


async def app_shutdown():
    global http_client
    global lb_targets
    global redis_client
    global redis_keyspace

    await http_client.aclose()
    await redis_keyspace.aclose()
    await teardown_redis(redis_client)


async def handle_error(_: Request, exc: HTTPException) -> Response:
    logger.exception("Error processing request", exc_info=exc)
    status_code = getattr(exc, "status_code", 503)
    return Response(status_code=status_code, content="Server error")


def create_app():
    allowed_methods = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"]

    app = Starlette(
        routes=[
            Route("/", handle_proxy, methods=allowed_methods),
            Route("/register", handle_registration, methods=["POST"]),
            Route("/healthcheck", healthcheck, methods=["GET"]),
            Route("/_stats", get_target_stats, methods=["GET"]),
            Route("/{path:path}", handle_proxy, methods=allowed_methods),
        ],
        on_startup=[app_startup],
        on_shutdown=[app_shutdown],
        exception_handlers={Exception: handle_error},
    )
    return app
