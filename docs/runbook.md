# Runbook

The access token expires at 06:00 IST. Do not start a new trading session between midnight and 06:00. The feed can also be invalidated by Zerodha's end-of-day job before that clock time.

## Daily login

1. On the VPS, with the SSH tunnel from the deployment guide open, run `python -m scalper login`.
2. Complete Zerodha's own login, including TOTP, in the browser.
3. Confirm the process prints your user id and an expiry at 06:00 IST the next morning.
4. Run `python -m scalper instruments fetch` and keep the new `nfo-YYYY-MM-DD.csv`.
5. Check `http://127.0.0.1:8080/health` and `http://127.0.0.1:8080/metrics`.

The session file is `data/session.json` and must stay mode `600`. A group-readable file is refused.

## Feed stall

A Telegram message says the last quote is too old.

1. Check that the session has not expired and that the VPS clock is IST.
2. Stop new entries with the kill switch below.
3. Restart only the recorder after a fresh login if the socket does not resume.
4. Leave the kill switch on until a new quote is arriving and the stall alert has cleared.

## Order rejects

1. Turn the kill switch on. Do not loop the rejected order.
2. Read the decision log for the risk reason or the broker message. The log does not contain the access token.
3. Three rejects inside the risk window already halt new entries. Resume only after the cause is known.

## PnL mismatch or clock drift

The risk gate halts entries when internal PnL and broker PnL differ by more than the tolerance, or when the local clock and the exchange clock differ by more than two seconds.

1. Kill switch on.
2. Compare the broker position with the process position. The broker figure wins.
3. Fix the clock with NTP. Do not resume while the clocks still differ.

## Kill switch

```bash
python -m scalper kill --reason "operator stop"
```

This writes `data/KILL`. New entries are refused. An exit is still allowed so a position can be flattened. Shutdown of the execution engine cancels working paper orders and flattens a position it knows about. It stops if the broker has an order this process does not recognize.

Resume only with the exact phrase:

```bash
python -m scalper resume --confirm I_RESUME_TRADING
```

Any other phrase leaves the file in place.

## Rollback

1. Kill switch on. Confirm there is no open position at the broker.
2. `docker compose stop app`
3. Check out the previous git revision and rebuild: `docker compose up -d --build app`
4. Do not delete the Postgres volume. Migrations apply forward. To undo a bad migration, restore a backup into a new volume and point the app at it.
5. Log in again. The previous access token belongs to that morning and is not a rollback mechanism.

## Backup restore rehearsal

```bash
scripts/backup-postgres.sh
docker compose exec -T postgres psql -U scalper -d scalper -c 'SELECT count(*) FROM ticks;'
```

Restore the dump into a separate database and repeat the count. A backup that has not been restored is not a backup yet.
