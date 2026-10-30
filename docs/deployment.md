# Deployment

Run this on a Mumbai-region VPS whose public IPv4 address does not change. Research and backtests can run on any machine. Order calls, when live mode is eventually allowed, must leave from the registered address.

The compose file publishes Postgres, Redis, and the app only on `127.0.0.1`. Leave it that way. Do not put the Kite API secret in an image or a git commit.

## 1. Static IP

On the Kite developer site, open the profile and add the VPS egress address under IP whitelist. The whitelist is for the developer account, not one app. Zerodha allows one change per calendar week. Only order calls are checked against it. Login, instrument downloads, and the market socket still work from other addresses.

Confirm the address from the VPS:

```bash
curl -4 https://ifconfig.me
```

The value must match the whitelist before any live order is considered.

## 2. Host

Install Docker. Create the data directory and give it to the app user in the image, uid 10001:

```bash
sudo mkdir -p /opt/scalper
sudo chown 10001:10001 /opt/scalper/data
```

Copy the repository to `/opt/scalper`. Copy `.env.example` to `.env` beside `docker-compose.yml` and set:

- `SCALPER_EXPECTED_USER_ID`
- `SCALPER_KITE_API_KEY`
- `SCALPER_KITE_API_SECRET`
- `POSTGRES_PASSWORD` to a value other than `scalper`

Leave `SCALPER_TRADING_MODE=paper`.

On the Kite app, set the redirect URL to exactly:

```text
http://127.0.0.1:8765/callback
```

## 3. Start

```bash
cd /opt/scalper
docker compose up -d --build
docker compose exec app python -m scalper db migrate
```

From a laptop, open the login callback through an SSH tunnel so the session file is created on the VPS:

```bash
ssh -L 8765:127.0.0.1:8765 user@vps
```

On the VPS:

```bash
docker compose exec app python -m scalper login --no-browser
```

Open the printed URL in the laptop browser and log in yourself. Then:

```bash
docker compose exec app python -m scalper instruments fetch
curl --fail http://127.0.0.1:8080/health
```

Health should show phase 8 and `"trading_mode": "paper"`.

## 4. Backup

Run `scripts/backup-postgres.sh` from the repository directory. It writes a SQL dump under `backups/`. Copy those files off the VPS. Restore only into an empty database you can throw away, and confirm `ticks` is present before trusting the backup.

## 5. What this host must not do yet

`python -m scalper go-live` exits with a failure and prints the blocking checklist. Do not set live mode because the process starts. Live size is one lot, and the confirmation phrase belongs only in the VPS environment after that checklist is actually satisfied.
