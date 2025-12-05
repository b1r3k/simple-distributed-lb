# simple distributed load balancer

companion repository for blog post:

## Problem description

Create a load balancing server to handle HTTP requests and distribute them to microservices. The server should offer the following endpoints:

    /register: Receives parameters for URL path, IP address, and port. Upon receiving this request, the load balancer will start sending requests to the corresponding microservice.

Requests to other endpoints are forwarded to microservices based on a Round Robin load balancing scheme. Replies are then sent back to the client.

Example:

Microservice A registers: http://<load balancer>/register "/test", ip address, port
Microservice B registers: http://<load balancer>/register "/test2", ip address, port
Microservice C registers: http://<load balancer>/register "/test", ip address, port
Microservice D registers: http://<load balancer>/register "/test", ip address, port

When a client calls http://<load balancer>/test, the request will be forwarded to either A, C, or D.

## Howto

### Run demo

Demo is build with docker compose. Demo setup contains DNS server, redis server, two load balancers and two upstream servers. When a upstream server starts, it registers itself to load balancer using `e2e-upstream.sh` script. When both upstream servers are started, e2e tests are executed using `e2e-test.sh` script which sends requests to load balancer and checks if requests are served according to round-robin scheme.

    $ docker compose up

### Use API

1. Register target

```bash
curl -i -L -X POST \
   -H "Content-Type:application/json" \
   -d \
'[{
  "ip_address": "99.83.207.200",
  "port": 80
}]' \
 'http://localhost:8000/register'
```

2. Send request

```bash
curl -i -L -X GET 'http://localhost:8000/hello/world'
```

### Develop

You can start docker compose setup with two load balancers, two upstream servers, DNS server and redis:

    $ docker compose up

Rebuild the docker images for docker compose:

    $ docker compose up --build --force-recreate

Attach to the running container:

    $ docker ps
    $ docker exec -it <container_id> /bin/bash
