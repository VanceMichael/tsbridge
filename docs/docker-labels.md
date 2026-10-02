# Docker Labels

Run tsbridge in Docker and let it discover services automatically via container labels.

## Quick Example

```yaml
services:
  tsbridge:
    image: ghcr.io/jtdowney/tsbridge:latest
    command: ["-provider", "docker"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - tsbridge-state:/var/lib/tsbridge
    environment:
      # Pass the actual OAuth credentials to the container
      - TS_OAUTH_CLIENT_ID=${TS_OAUTH_CLIENT_ID}
      - TS_OAUTH_CLIENT_SECRET=${TS_OAUTH_CLIENT_SECRET}
    labels:
      # Tell tsbridge which env vars contain the credentials (not redundant - both are needed)
      - "tsbridge.tailscale.oauth_client_id_env=TS_OAUTH_CLIENT_ID"
      - "tsbridge.tailscale.oauth_client_secret_env=TS_OAUTH_CLIENT_SECRET"
      - "tsbridge.tailscale.state_dir=/var/lib/tsbridge"
      - "tsbridge.tailscale.default_tags=tag:server" # Must match or be owned by your OAuth client's tag

  myapp:
    image: myapp:latest
    labels:
      - "tsbridge.enabled=true"
      - "tsbridge.service.name=myapp"
      - "tsbridge.service.port=8080"

volumes:
  tsbridge-state:
```

Your app is now at `https://myapp.<tailnet>.ts.net`

> `default_tags` must match the OAuth client's tag or a tag it owns. A service can override them with its `tags` label. See [Tag Ownership and OAuth Security](configuration-reference.md#tag-ownership-and-oauth-security).

## How It Works

1. tsbridge watches Docker events for real-time changes
2. When a container with `tsbridge.enabled=true` starts, it creates a proxy
3. When the container stops, the proxy is removed
4. A periodic poll (default: 1 minute) acts as a safety net for any missed events

## CLI Flags

When using the Docker provider, these flags are available:

| Flag | Default | Description |
|------|---------|-------------|
| `-provider` | `file` | Set to `docker` for Docker label discovery |
| `-docker-socket` | `unix:///var/run/docker.sock` | Docker socket endpoint |
| `-docker-label-prefix` | `tsbridge` | Prefix for container labels |
| `-docker-poll-interval` | `1m` | Periodic config poll interval (0 to disable) |

```bash
tsbridge -provider docker -docker-poll-interval 30s
```

## Docker Socket Configuration

By default, tsbridge connects to the Docker daemon at `unix:///var/run/docker.sock`. You can configure an alternative endpoint using:

1. `-docker-socket` flag (highest priority)
2. `DOCKER_HOST` environment variable (standard Docker convention)
3. Default socket (`unix:///var/run/docker.sock`)

This is useful for:
- Remote Docker daemons (`tcp://docker-host:2375`)
- Docker Desktop on macOS (`unix://$HOME/.docker/run/docker.sock`)
- Rootless Docker (`unix:///run/user/1000/docker.sock`)
- Custom socket paths

Example with DOCKER_HOST:

```yaml
services:
  tsbridge:
    image: ghcr.io/jtdowney/tsbridge:latest
    command: ["-provider", "docker"]
    environment:
      - DOCKER_HOST=unix:///var/run/docker.sock
      - TS_OAUTH_CLIENT_ID=${TS_OAUTH_CLIENT_ID}
      - TS_OAUTH_CLIENT_SECRET=${TS_OAUTH_CLIENT_SECRET}
    # ...
```

Example with explicit flag:

```bash
tsbridge -provider docker -docker-socket tcp://docker-host:2375
```

## Label Reference

### Labels on the tsbridge container

Place Tailscale and global labels on the container running tsbridge. Comma-separated labels produce string lists.

#### Tailscale labels

| Label | Default | Purpose |
|------|---------|---------|
| `tsbridge.tailscale.oauth_client_id` | none | OAuth client ID |
| `tsbridge.tailscale.oauth_client_id_env` | none | Environment variable containing the OAuth client ID |
| `tsbridge.tailscale.oauth_client_id_file` | none | Absolute path containing the OAuth client ID |
| `tsbridge.tailscale.oauth_client_secret` | none | OAuth client secret |
| `tsbridge.tailscale.oauth_client_secret_env` | none | Environment variable containing the OAuth client secret |
| `tsbridge.tailscale.oauth_client_secret_file` | none | Absolute path containing the OAuth client secret |
| `tsbridge.tailscale.auth_key` | none | Tailscale auth key used instead of OAuth |
| `tsbridge.tailscale.auth_key_env` | none | Environment variable containing the auth key |
| `tsbridge.tailscale.auth_key_file` | none | Absolute path containing the auth key |
| `tsbridge.tailscale.state_dir` | platform data directory | Base directory for per-service tsnet state |
| `tsbridge.tailscale.state_dir_env` | none | Environment variable containing the state directory |
| `tsbridge.tailscale.default_tags` | none | Comma-separated service tags |
| `tsbridge.tailscale.control_url` | Tailscale | Alternate control server URL, such as Headscale |
| `tsbridge.tailscale.oauth_preauthorized` | `true` | Preauthorize OAuth-generated auth keys |

Provide either OAuth credentials or an auth key. OAuth services also require tags from `default_tags` or a service label.

#### Global labels

| Label | Default | Purpose |
|------|---------|---------|
| `tsbridge.global.metrics_addr` | disabled | Prometheus listen address |
| `tsbridge.global.read_header_timeout` | `30s` | Time allowed to read request headers |
| `tsbridge.global.write_timeout` | `30s` | Time allowed to write a response |
| `tsbridge.global.idle_timeout` | `120s` | HTTP keep-alive idle timeout |
| `tsbridge.global.startup_timeout` | `30s` | tsnet startup timeout |
| `tsbridge.global.response_header_timeout` | `0s` | Time allowed for backend response headers; zero disables it |
| `tsbridge.global.access_log` | `true` | Enable access logging |
| `tsbridge.global.trusted_proxies` | none | Comma-separated trusted proxy IPs or CIDR ranges |
| `tsbridge.global.dial_timeout` | `30s` | Backend connection timeout |
| `tsbridge.global.keep_alive_timeout` | `30s` | Backend TCP keep-alive interval |
| `tsbridge.global.idle_conn_timeout` | `90s` | Backend idle connection timeout |
| `tsbridge.global.tls_handshake_timeout` | `10s` | Backend TLS handshake timeout |
| `tsbridge.global.expect_continue_timeout` | `1s` | Wait for a backend `100 Continue` response |
| `tsbridge.global.metrics_read_header_timeout` | `5s` | Metrics server header timeout |
| `tsbridge.global.flush_interval` | `0s` | Response flush interval; `-1ms` flushes immediately |
| `tsbridge.global.max_request_body_size` | `50MB` | Request body limit; `-1` disables it |

Global labels are applied as a single configuration generation. When the provider delivers a changed global section (for example when the tsbridge container is recreated with new labels), tsbridge builds and validates the complete candidate before touching anything running; then every service's trusted-proxy judgment, backend transport timeouts, access logging and request body limits switch over atomically, together with the Prometheus listener (address and read-header timeout). Tailscale listeners and backend connections are not recreated. If a candidate cannot be prepared (e.g. the new `metrics_addr` is already bound), the candidate is discarded in full, the previous generation keeps serving, and the same configuration is retried on the next event or poll once the fault clears.

### Labels on service containers

`tsbridge.enabled=true` opts a container into discovery; `tsbridge.enable=true` is an equivalent alias. Set either `tsbridge.service.port` or `tsbridge.service.backend_addr`; when both are absent, tsbridge uses the container's only exposed port.

| Label | Default | Purpose |
|------|---------|---------|
| `tsbridge.enabled` | `false` | Enable discovery for this container |
| `tsbridge.enable` | `false` | Alias for `tsbridge.enabled` |
| `tsbridge.service.name` | container name | Tailscale hostname |
| `tsbridge.service.port` | single exposed port | Port reached through the container's Docker name |
| `tsbridge.service.backend_addr` | derived from port | Explicit backend address; takes precedence over `port` |
| `tsbridge.service.tags` | `default_tags` | Comma-separated Tailscale tags |
| `tsbridge.service.whois_enabled` | `false` | Add trusted `X-Tailscale-*` identity headers |
| `tsbridge.service.whois_timeout` | `5s` | Whois lookup timeout |
| `tsbridge.service.tls_mode` | `auto` | `auto` for Tailscale HTTPS or `off` for HTTP |
| `tsbridge.service.listen_addr` | `:443` or `:80` | Listen address selected from TLS mode |
| `tsbridge.service.funnel_enabled` | `false` | Expose the service through Tailscale Funnel |
| `tsbridge.service.ephemeral` | `false` | Create an ephemeral Tailscale node |
| `tsbridge.service.oauth_preauthorized` | Tailscale setting (`true`) | Override OAuth auth-key preauthorization |
| `tsbridge.service.insecure_skip_verify` | `false` | Skip certificate verification for HTTPS backends |
| `tsbridge.service.startup_timeout` | global value | Override tsnet startup timeout |
| `tsbridge.service.read_header_timeout` | global value | Override request header timeout |
| `tsbridge.service.write_timeout` | global value | Override response write timeout |
| `tsbridge.service.idle_timeout` | global value | Override HTTP keep-alive idle timeout |
| `tsbridge.service.response_header_timeout` | global value | Override backend response header timeout |
| `tsbridge.service.flush_interval` | global value | Override response flush interval |
| `tsbridge.service.access_log` | global value | Override access logging |
| `tsbridge.service.max_request_body_size` | global value | Override request body limit |
| `tsbridge.service.upstream_headers.<name>` | none | Set a request header sent to the backend |
| `tsbridge.service.downstream_headers.<name>` | none | Set a response header sent to the client |
| `tsbridge.service.remove_upstream` | none | Remove comma-separated request headers |
| `tsbridge.service.remove_downstream` | none | Remove comma-separated response headers |

## Backend Address Tips

Use port, not localhost:

```yaml
# Good - uses container name
- "tsbridge.service.port=8080"

# Bad - localhost is the tsbridge container!
- "tsbridge.service.backend_addr=localhost:8080"
```

Why? In Docker, each container has its own network namespace. `localhost` inside tsbridge doesn't reach your service container.

## Advanced Features

### Custom Listen Configuration

```yaml
labels:
  # Listen on specific address and port
  - "tsbridge.service.listen_addr=127.0.0.1:9090"

  # Listen on all interfaces with custom port
  - "tsbridge.service.listen_addr=0.0.0.0:8080"

  # Listen on port only (all interfaces)
  - "tsbridge.service.listen_addr=:8443"
```

### Streaming/SSE

```yaml
labels:
  - "tsbridge.service.write_timeout=0s" # No timeout
  - "tsbridge.service.flush_interval=-1ms" # No buffering
```

### Security Headers

```yaml
labels:
  - "tsbridge.service.downstream_headers.X-Frame-Options=DENY"
  - "tsbridge.service.downstream_headers.Strict-Transport-Security=max-age=31536000"
```

### Custom Headers

```yaml
labels:
  # Add to requests
  - "tsbridge.service.upstream_headers.X-Service-Name=api"

  # Remove from responses
  - "tsbridge.service.remove_downstream=Server,X-Powered-By"
```

### HTTPS Backends

For connecting to HTTPS backend services:

```yaml
labels:
  # For services with valid certificates
  - "tsbridge.service.backend_addr=https://api.example.com:443"

  # For services with self-signed certificates (use with caution)
  - "tsbridge.service.backend_addr=https://internal.lan:8443"
  - "tsbridge.service.insecure_skip_verify=true"
```

> Security warning: `insecure_skip_verify=true` disables TLS certificate validation. Only use this for trusted internal services with self-signed certificates, as it makes connections vulnerable to attacks.

## Complete Example

```yaml
services:
  tsbridge:
    image: ghcr.io/jtdowney/tsbridge:latest
    command: ["-provider", "docker", "-verbose"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - tsbridge-state:/var/lib/tsbridge
    networks:
      - app-network
    environment:
      - TS_OAUTH_CLIENT_ID=${TS_OAUTH_CLIENT_ID}
      - TS_OAUTH_CLIENT_SECRET=${TS_OAUTH_CLIENT_SECRET}
    labels:
      - "tsbridge.tailscale.oauth_client_id_env=TS_OAUTH_CLIENT_ID"
      - "tsbridge.tailscale.oauth_client_secret_env=TS_OAUTH_CLIENT_SECRET"
      - "tsbridge.tailscale.state_dir=/var/lib/tsbridge"
      - "tsbridge.tailscale.default_tags=tag:server"
      - "tsbridge.global.metrics_addr=:9090"
    ports:
      - "9090:9090" # Metrics

  api:
    image: myapp/api:latest
    networks:
      - app-network
    labels:
      - "tsbridge.enabled=true"
      - "tsbridge.service.name=api"
      - "tsbridge.service.port=8080"
      - "tsbridge.service.whois_enabled=true"

  web:
    image: myapp/web:latest
    networks:
      - app-network
    labels:
      - "tsbridge.enabled=true"
      - "tsbridge.service.name=web"
      - "tsbridge.service.port=3000"
      - "tsbridge.service.access_log=false"

volumes:
  tsbridge-state:

networks:
  app-network:
```

## Docker Networking

### Network Requirements

tsbridge and service containers must be on the same Docker network for communication. They don't need to be in the same compose file, but network connectivity is required.

```yaml
# tsbridge can forward traffic to service containers only if they share a network
networks:
  app-network:  # Same network name in both files

# In tsbridge compose file
services:
  tsbridge:
    networks:
      - app-network

# In service compose file
services:
  myservice:
    networks:
      - app-network
```

### Single Compose File (Simplest)

When everything is in one compose file, Docker automatically creates a shared network:

```yaml
services:
  tsbridge:
    image: ghcr.io/jtdowney/tsbridge:latest
    command: ["-provider", "docker"]
    # ... other config

  myapp:
    image: myapp:latest
    labels:
      - "tsbridge.enabled=true"
      - "tsbridge.service.port=8080"
# Both containers automatically share the default network
```

### Multiple Compose Files

For services in separate compose files, use external networks:

1. Create the network first:

```bash
docker network create tsbridge-network
```

2. tsbridge-compose.yml:

```yaml
services:
  tsbridge:
    image: ghcr.io/jtdowney/tsbridge:latest
    command: ["-provider", "docker"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    environment:
      - TS_OAUTH_CLIENT_ID=${TS_OAUTH_CLIENT_ID}
      - TS_OAUTH_CLIENT_SECRET=${TS_OAUTH_CLIENT_SECRET}
    labels:
      - "tsbridge.tailscale.oauth_client_id_env=TS_OAUTH_CLIENT_ID"
      - "tsbridge.tailscale.oauth_client_secret_env=TS_OAUTH_CLIENT_SECRET"
      - "tsbridge.tailscale.default_tags=tag:server"
    networks:
      - tsbridge-network

networks:
  tsbridge-network:
    external: true
```

3. services-compose.yml:

```yaml
services:
  api:
    image: myapi:latest
    labels:
      - "tsbridge.enabled=true"
      - "tsbridge.service.name=api"
      - "tsbridge.service.port=8080"
    networks:
      - tsbridge-network

  web:
    image: myweb:latest
    labels:
      - "tsbridge.enabled=true"
      - "tsbridge.service.name=web"
      - "tsbridge.service.port=3000"
    networks:
      - tsbridge-network

networks:
  tsbridge-network:
    external: true
```

4. Start them:

```bash
# Start tsbridge
docker compose -f tsbridge-compose.yml up -d

# Start services (in any order)
docker compose -f services-compose.yml up -d
```

### Alternative: Define Network in One File

You can also define the network in one compose file and reference it as external in others:

tsbridge-compose.yml (defines network):

```yaml
services:
  tsbridge:
    # ... config
    networks:
      - shared-network

networks:
  shared-network:
    name: tsbridge-shared
```

services-compose.yml (uses external network):

```yaml
services:
  myapp:
    # ... config
    networks:
      - shared-network

networks:
  shared-network:
    external: true
    name: tsbridge-shared
```

### Network Troubleshooting

Why does networking matter?

- tsbridge acts as a reverse proxy
- It needs to reach your service containers over the network
- `localhost` inside the tsbridge container does not refer to service containers
- Docker networks enable container-to-container communication

Common networking issues:

Service not appearing?

- Check `tsbridge.enabled=true` is set
- Verify containers are on same network - use `docker network ls` and `docker inspect <container>`
- Look at tsbridge logs with `-verbose`

Connection refused?

- Don't use `localhost` - use `port` label instead
- Make sure service is listening on the port
- Check container is actually running
- Verify network connectivity: `docker exec tsbridge-container ping service-container`

Cross-compose networking not working?

- Ensure both compose files reference the same network name
- Check network exists: `docker network ls`
- Verify containers joined the network: `docker network inspect <network-name>`
- Make sure network is external in dependent compose files

## Troubleshooting

Label changes ignored?

- Labels are only read when container starts
- Restart container to apply new labels

Cannot connect between compose files?

- Ensure both files use the same network name
- Network must be marked as `external: true` in dependent compose files
- Create network manually if needed: `docker network create <network-name>`
