# Redis homework

## Environment prerequisites
1. Docker installed
2. JDK 8+
3. Update `C:\Windows\System32\drivers\etc\hosts` file on Windows (you should use notepad in Administrator access mode)
or `/etc/hosts` on Linux, add following entries:
```
127.0.0.1 redis-master-0
127.0.0.1 redis-master-1
127.0.0.1 redis-master-2
```

## Environment check

Open terminal in `src/test/resource/redis` folder

1. Try to start redis cluster with docker compose (detached mode)
```shell
docker compose up -d
```

2. Try to connect to Redis Cluster
```shell
docker run -it --rm --network=redis_default redis:7.0.5-alpine redis-cli -c -h redis-master-0 -p 30000
```

3. Get cluster info (inside Redis CLI):
```shell
CLUSTER INFO
```

5. Clean up environment
```shell
docker compose down
```

## Lab check instructions
Run `FixedWindowRateLimitControllerTest` test.

## Redis Cluster in Docker Compose Details

Redis Cluster [doesn't support](https://redis.io/docs/manual/scaling/#redis-cluster-and-docker) NAT environments.
Following options are available:
* Use host network mode - unavailable on Windows without Docker Desktop WSL backend
* Run everything inside docker network - not handy for development
* Use DNS names for Redis Cluster with different DNS name to IP address resolution inside private network and outside (chosen option)
