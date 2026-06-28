# Game Traffic Proxy

UDP/TCP proxy for [RobustToolbox](https://github.com/space-wizards/RobustToolbox) / Lidgren game servers with IP filtering, rate limiting, session management, and Prometheus metrics.

## Features

- **UDP proxy** — forwards UDP packets between clients and a backend game server
- **TCP relay** — bidirectional TCP stream forwarding (same listen/target ports)
- **IP filtering** — allowlist and denylist with CIDR notation support
- **Per-client rate limiting** — max packets/second per client IP
- **Session management** — per-client upstream sockets with idle timeout & cleanup
- **Prometheus metrics** — built-in HTTP endpoint exposing packet/connection/byte counters
- **Concurrent** — multi-worker UDP receive loop, configurable max clients
- **Docker** — ready-to-use container image and docker-compose
- **`/info` rewriting** — automatically rewrites `connect_address` in game server `/info` JSON responses to point clients to the proxy

## Usage

```
GameTrafficProxy [options]

Options:
  -l, --listen <endpoint>         UDP listen address:port (default: 0.0.0.0:12121)
  -t, --target <endpoint>         Target server address:port (default: 127.0.0.1:1212)
  -aa,--advertised-address <addr> Public address:port advertised in /info (default: listen address)
      --tcp-listen <boolean>      Enable TCP relay on address:port (default: 0.0.0.0:12121)
  -c, --config <path>             JSON config file
  -a, --allow-list <path>         File with allowed IPs/CIDRs (one per line)
  -d, --deny-list <path>          File with denied IPs/CIDRs (one per line)
  -r, --rate-limit <n>            Max packets/sec per client (0 = unlimited)
      --max-clients <n>           Maximum concurrent clients (default: 256)
      --session-timeout <s>       Seconds idle before dropping session (default: 60)
      --metrics-port <port>       Prometheus metrics HTTP port (0 = disabled)
  -v, --verbose                   Log all forwarded packets
  -h, --help                      Show help
```

### Examples

```bash
# Forward UDP on :7777 to a game server at 192.168.1.10:1212
GameTrafficProxy -l 0.0.0.0:7777 -t 192.168.1.10:1212 --tcp-listen 0.0.0.0:7777

# Load config from file, enable Prometheus on :9090, verbose logging
GameTrafficProxy -c config.json --metrics-port 9090 -v

# Apply allowlist/denylist with rate limit of 100 pkt/s per client
GameTrafficProxy -a allowlist.txt -d denylist.txt -r 100

# Under Docker, advertise the public address so clients connect to the proxy
GameTrafficProxy -l 0.0.0.0:12121 -t 192.168.1.10:1212 -aa game.example.com:12121
```

## Configuration File

JSON file format (all fields optional):

```json
{
  "AllowedIPs": ["192.168.1.0/24", "10.0.0.1"],
  "DeniedIPs": ["203.0.113.0/24"],
  "RateLimitPerClient": 100,
  "MaxClients": 512,
  "SessionTimeoutSeconds": 120,
  "MetricsPort": 9090,
  "Verbose": true,
  "TcpListenEnabled": true,
  "AdvertisedAddress": "game.example.com:12121"
}
```

## Docker

`docker-compose.yml`
```yaml
services:
  proxy:
    image: ghcr.io/cinkafox/robusttrafficproxy:master
    ports:
      - "12121:12121/udp"
      - "12121:12121/tcp"
    environment:
      - TARGET_HOST=gameserver
      - TARGET_PORT=1212
    command: ["-l", "0.0.0.0:12121", "-t", "${TARGET_HOST:-gameserver}:${TARGET_PORT:-1212}", "-aa", "${PUBLIC_ADDRESS:-0.0.0.0:12121}"]
    restart: unless-stopped

```

```bash
docker-compose up -d
```

Or build manually:

```bash
docker build -t game-traffic-proxy .
docker run -d --name game-proxy -p 12121:12121/udp -p 12121:12121/tcp game-traffic-proxy -l 0.0.0.0:12121 --tcp-listen 0.0.0.0:12121 -t gameserver:1212
```

## Prometheus Metrics

| Metric                            | Type    | Description                                   |
|-----------------------------------|---------|-----------------------------------------------|
| `proxy_packets_total`             | Counter | Total packets processed (labels: dir, status) |
| `proxy_bytes_total`               | Counter | Total bytes forwarded (labels: dir)           |
| `proxy_connections_total`         | Counter | Total client connections established          |
| `proxy_connections_dropped_total` | Counter | Total connections dropped (label: reason)     |
| `proxy_active_sessions`           | Gauge   | Current active UDP sessions                   |
| `proxy_filter_denials_total`      | Counter | Connections denied by IP filter               |
| `proxy_tcp_connections_total`     | Counter | Total TCP connections established             |
| `proxy_tcp_connections_active`    | Gauge   | Current active TCP connections                |
| `proxy_tcp_bytes_total`           | Counter | Total TCP bytes relayed (labels: dir)         |
| `proxy_tcp_errors_total`          | Counter | Total TCP proxy errors                        |
