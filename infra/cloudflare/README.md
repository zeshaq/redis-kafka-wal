# Cloudflare deployment

Two pieces:

1. **Cloudflare Tunnel** — exposes the lab's bridge service (running on
   the VM) to a public hostname. No port-forwarding, no inbound
   firewall rule, just `cloudflared` outbound from the VM.
2. **Cloudflare Pages** — hosts the static frontend in `web/`.

Why this split: the bridge has live data and lives next to Kafka and
Redis on the VM; Pages lives at the edge and serves a static UI.

```
[Browser]
   │ HTTPS + SSE
   ▼
[Pages]  redis-kafka-wal.pages.dev
   │ HTTPS to api.redis-kafka-wal.<your-domain>
   ▼
[Tunnel]
   │ outbound from cloudflared on VM
   ▼
[Bridge]  bridge:8080  (compose service on VM)
```

## Prerequisites

- A Cloudflare API token (see [`token-scopes.md`](token-scopes.md) for the
  exact scope list).
- A zone (domain) in your Cloudflare account.
- The lab stack running on the target host (`docker compose up -d`).

## One-shot setup

```bash
export CF_API_TOKEN=$(cat /path/to/cloudflare-token)   # the file with the API token
export CF_ZONE_NAME=zeteq.com                          # your domain
export CF_API_HOSTNAME=api.redis-kafka-wal             # subdomain to expose the bridge under
export CF_TUNNEL_NAME=redis-kafka-wal-lab

./infra/cloudflare/setup.sh
```

The script:

1. Creates a Cloudflare Tunnel named `$CF_TUNNEL_NAME` (or reuses an
   existing one with the same name).
2. Fetches the connector token and writes it to
   `.local/cloudflared-token` (gitignored).
3. Configures the tunnel's ingress to route
   `${CF_API_HOSTNAME}.${CF_ZONE_NAME}` to `http://bridge:8080`.
4. Creates a DNS CNAME record for the API hostname.
5. Writes the result to `.env` so `docker compose up` picks it up.

After it succeeds:

```bash
docker compose up -d cloudflared
docker compose logs -f cloudflared       # confirm "Connection established"
curl -s https://${CF_API_HOSTNAME}.${CF_ZONE_NAME}/api/health | jq .
```

## Pages deployment

Two paths; pick one.

### A. Connect the GitHub repo (recommended for ongoing work)

1. Cloudflare dashboard → Workers & Pages → Create → Pages →
   Connect to Git.
2. Pick `zeshaq/redis-kafka-wal`, branch `main`.
3. Build settings:
   - Framework preset: `None`
   - Build command: *(empty)*
   - Build output directory: `web`
4. Deploy. From now on, every push to `main` triggers a redeploy.
5. (Optional) Custom domain: e.g. `redis-kafka-wal.zeteq.com`.

### B. Direct upload via wrangler

```bash
npm i -g wrangler                      # one-time
export CLOUDFLARE_API_TOKEN=$(cat /path/to/cloudflare-token)
wrangler pages project create redis-kafka-wal --production-branch=main
wrangler pages deploy web --project-name=redis-kafka-wal --branch=main
```

Either way, after deployment open the Pages URL (e.g.
`https://redis-kafka-wal.pages.dev`), click **settings**, fill in:

- **API base URL:** `https://${CF_API_HOSTNAME}.${CF_ZONE_NAME}`
- **Bearer token:** the same value as `BRIDGE_WRITE_TOKEN` in your `.env`
  (needed only if you want to produce events from the UI)

Save. The dashboard should immediately turn the connection chip
green ("live") and start displaying region state.

## Tightening CORS

By default the bridge sets `Access-Control-Allow-Origin: *`. For a
public dashboard that's fine. To restrict to your Pages URL:

```bash
# in .env
BRIDGE_ALLOW_ORIGIN=https://redis-kafka-wal.pages.dev
```

Then `docker compose up -d bridge` to apply.

## Cleanup

To remove everything created by `setup.sh`:

```bash
./infra/cloudflare/setup.sh --delete
```

This deletes the DNS record, the tunnel, and the local credential
file. It does not delete the Pages project (use the Cloudflare
dashboard for that if you want).
