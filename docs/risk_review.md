# Risk review

Read-only pass over the order, stop, halt, clock, and broker paths. Nothing under `src/` or `tests/` was modified. No Kite host was contacted. Evidence is the scratch file `/tmp/scalper_risk_review_test.py` (22 failed, 1 passed under `.venv/bin/pytest /tmp/scalper_risk_review_test.py -q --tb=line`) plus the quoted paths below. A failed test is one that asserted the safe outcome.

`scalper paper` does not construct `KiteBroker` and does not post orders (`src/scalper/paper/command.py` lines 1–3). The defects in the runner and the engine are already on that path. The defects in `KiteBroker` are the code that will run once a live broker is wired in. Where a conclusion depends on Kite's behaviour rather than this repository, it is marked **ASSUMPTION - verify in Kite docs**.

Rupee scale used below: P&L is index points × quantity. The repo tests use lot size 15 and tick 0.05. Live lot size and tick come from the instrument dump, not from a constant, so scale these figures by the dump's lot and tick. On the test contract, 20 ticks is 1.00 index point, which is ₹15. A 100-point adverse move on quantity 15 is ₹1,500. After the chase gives up, nothing in this process caps the loss. One extra lot doubles it.

There is no second process, no GTT, and no `trigger_price` on any order. Whether Zerodha's MIS product squares a leftover position off by itself is **ASSUMPTION - verify in Kite docs**. This repository does not schedule that, and it does not check that it happened.

## Verdicts

| Item | Verdict |
| --- | --- |
| A1 Only `place_live_limit` posts | PASS |
| A2 Same intent sent twice | FAIL |
| A3 Crash between send and record | FAIL |
| A4 Partial fill vs 1-lot cap and the stop | FAIL |
| A5 Cancel/fill race uses a stale remaining size | FAIL |
| A6 Rejections and unknown statuses | FAIL |
| A7 Order book treated as complete | FAIL |
| B1 Position with nobody managing an exit | FAIL |
| B2 Feed drop while in a position | FAIL |
| B3 Circuit, halt, gap through the stop | FAIL |
| B4 Crash while holding | FAIL |
| B5 Stops only run on ticks | FAIL |
| C1 Halt reasons and what the tests assert | FAIL (coverage of persistence) |
| C2 Restart clears a halt | FAIL |
| C3 KILL path depends on the working directory | FAIL |
| C4 Exit still places while halted | PASS |
| C5 Snapshot tasks and the reject window | FAIL |
| D1 Clock drift vs a quiet tape | FAIL |
| D2 Timezones | FAIL |
| D3 Candle construction | FAIL |
| D4 Recorder durability | FAIL |
| D5 Expiry selection | FAIL |
| E1 Static IP is a typed string | FAIL |
| E2 Token death mid-session | FAIL |
| E3 Rate limits | FAIL |

## A. Order lifecycle

### A1. Who can post — PASS

`ExecutionEngine` places only through `place_live_limit`. `src/scalper/execution/engine.py` calls it at line 387 and does not call `.place_limit(`. The repo test `test_the_engine_places_only_through_place_live_limit` asserts that. The only production caller of `broker.place_limit` is `place_live_limit` at `src/scalper/execution/submit.py` line 66. `PaperBroker.place` is called only from `PaperBroker.place_limit` (`src/scalper/execution/paper.py` lines 58–59).

The kill-file wrapper sits in `place_live_limit`, not inside `KiteBroker.place_limit`. A future caller of `broker.place_limit` skips it. Nothing in `src/` does that today.

### A2. Same intent sent twice — FAIL

Several independent paths send a second order, or swallow an exit because the tag collided.

**ConnectError is posted again.** `src/scalper/execution/kite_broker.py` lines 37–38 and 84–86 catch `httpx.ConnectError` and `httpx.ConnectTimeout`, then call `_post_limit` a second time. The scratch test `test_connect_error_does_not_post_a_second_order` recorded two `POST /orders/regular` calls. There is no idempotency header. The tag is only a form field (`kite_broker.py` lines 132–149).

Sequence: first POST raises `ConnectError`; the method posts the same tag again. Worst case, if the first POST had reached Kite, two lots (test contract: quantity 30). Loss is two open lots, points × quantity, with no dedupe.

The installed httpcore raises `ConnectError` only from `_connect`, before `handle_async_request` (`httpcore/_async/connection.py` around the connect loop). **ASSUMPTION - verify in Kite docs** and in the deployed httpx: a `ConnectError` means the HTTP request was not written. If that holds, this particular retry does not double-fill. The code will still double-post if a proxy or a later httpx reports `ConnectError` after the server has the order. **ASSUMPTION - verify in Kite docs:** the tag is not a unique order key. This code already treats two rows with one tag as ambiguous (`kite_broker.py` lines 175–176), which is the behaviour of a label, not of an idempotency key.

**A duplicate tag is reported as a successful new order.** `engine.py` lines 365–367: if `self.orders` already has the tag, `_submit` returns that order with `allowed=True, reason="duplicate tag"`. It does not compare side, price, or quantity.

Sequence: `make_order_tag` for strategy ids `az` and `c2` at the same millisecond is the same string. Scratch test `test_same_millisecond_different_strategy_ids_cannot_share_a_tag` failed because both ids hash to prefix `9c0a` (`src/scalper/kite/tags.py` lines 36–48, 4 hex characters, 16 bits). The engine then returns the stored order. A sell decision comes back as the earlier buy, and the sell is never posted. Worst case: the long stays open with no exit order. On the test lot that is quantity 15, unbounded after that.

The built-in ids `trend_pullback-entry`, `trend_pullback-exit`, and `trend_pullback-flatten` (and the vwap set) do not share a prefix. The paper command's three roles will not collide with each other. Any other pair that shares 4 hex digits will, and a repeated call with the same strategy id and the same millisecond returns the old order even when the new limit is different. Tags do not wrap across days: 8 base62 digits hold a millisecond timestamp well past this century, so the layout itself does not reuse a tag tomorrow.

**An exit is allowed when the account is flat, so a "protective" order can open risk.** Scratch test `test_exit_from_flat_is_refused` placed a sell of 15 with reason `protective order`. `assess` allows every `EXIT` and `FLATTEN` once the halt checks have been acknowledged (`src/scalper/risk/gate.py` lines 64–75). The lot cap, the margin check, and the order-rate checks run only for `ENTRY` (lines 110–127). Worst case: one lot opened on the wrong side. Test contract, quantity 15, unbounded until something else flattens it.

**Replacement quantity is taken before the latest book.** Covered in A5. That path posted a second sell of 15 after the book already showed the first sell filled.

### A3. Crash between send and record — FAIL

Nothing durable is written before the POST. `_our_tags.add` is memory (`engine.py` line 384). `self.orders[tag]` is filled only after `_confirm_tag` (lines 404–405). The paper journal's `record_order` runs after `submit_entry` returns (`runner.py` lines 312–323). `ExecutionEngine.recover` exists (`engine.py` lines 226–242) and can adopt a tagged fill. `src/scalper/paper/command.py` never calls it. Scratch test `test_the_paper_command_never_calls_recover` failed on that absence.

Sequence: POST succeeds, process dies, a new process starts.

1. The in-memory tag set is empty, so `refresh` classifies the tagged order as foreign (`engine.py` lines 215–220).
2. `position_quantity` counts only our tags (line 221), so it stays 0 while the broker is long one lot.
3. `_check_broker_position` raises `PositionMismatch` (lines 488–499).
4. `shutdown` calls `refresh` first (line 245), so the mismatch aborts flatten. No flatten order is sent.

Scratch test `test_a_fresh_process_leaves_an_existing_fill_open`: shutdown raised `PositionMismatch: broker 15 fills 0` and the broker quantity stayed 15. Worst case: one lot left open. Test contract, quantity 15, no working exit. Unbounded until a person flattens it.

`refuse_to_start` (`src/scalper/paper/startup.py` lines 28–29) refuses to boot when `data/KILL` exists, and it does not flatten either. A kill file left by an earlier ambiguous place blocks the only process that could have closed the position.

SIGKILL skips `finally`. No kill file is written. The next start is allowed, and it still does not adopt the fill.

### A4. Partial fills — FAIL

Position size for the gate is filled quantity of our tags, integer-divided by lot size (`engine.py` lines 368–369). A working remainder is not counted. A fill of 5 with lot size 15 is 0 open lots.

Sequence, scratch test `test_a_partial_then_cancel_still_allows_another_full_lot`:

1. Buy 15, print fills 5, cancel the rest. Engine position is 5.
2. A second buy of 15 is allowed (`reason='entry allowed'`).

If that second order fills, the position is 20 on a 1-lot cap. Worst case: one extra lot. Test contract, 20 units versus a cap of 15. The extra 15 is a full lot of unbounded risk on top of the leftover 5.

The paper runner will not send that second entry while `pending` or `position` is set (`runner.py` lines 277–278). The engine will. The runner's own partial path is worse:

Sequence, scratch test `test_partial_stop_does_not_drop_the_remainder`, then the same steps traced under `/tmp/scalper-trace`:

1. Entry of 15 fills 5. `pending` stays because status is not `FILLED` (`runner.py` lines 384–397). The comment on line 384 says to revisit this before a live runner.
2. Price prints 99, which is the stop. The runner submits a sell of **5**.
3. `PaperBroker.place` rejects any quantity that is not a multiple of `lot_size` (`paper.py` lines 48–49). The sell is stored as `REJECTED`, filled 0.
4. `submit_exit` still returns that order, because a rejected order is not `None` (`runner.py` lines 417–419). The runner arms `WorkingExit` on it.
5. `_fill_protective_exit` then calls `on_trade`, and `PaperBroker.on_trade` fills every working order (`paper.py` lines 131–145). The still-working buy grows from 5 to 10. The rejected sell does not fill.
6. The runner still tracks quantity 5. The engine is long 10.

Worst case on this path: the entry can still complete to 15, with a rejected exit and a tracker that is short of the real quantity. Test contract, up to quantity 15 with no accepted exit. `KiteBroker._require_placeable` (`kite_broker.py` lines 234–243) will post a quantity of 5. **ASSUMPTION - verify in Kite docs:** NFO rejects a quantity that is not a lot multiple. If it does, the live exit is rejected the same way. If it accepts it, the position is an odd lot that this code's paper gate cannot close.

`_cancel_entry` (`runner.py` lines 399–403) clears `pending` for whatever status `cancel` returns. A book row that comes back `FILLED` drops the pending entry without building `OpenPosition`. The engine is then long and the runner does not arm a stop. Paper's own `cancel` cannot return `FILLED` (`states.cancel` requires a working status). A Kite book can. Worst case, one lot with no stop until 15:20, and only if that flatten fills.

### A5. Cancel while the order fills — FAIL

`engine.cancel` ignores the cancel response and reads the book (`engine.py` lines 192–196). `KiteBroker.cancel` also reads the book after DELETE (`kite_broker.py` lines 97–101 and 185–191). A single cancel does not invent a cancelled state when the book says partial. The repo test `test_a_fill_that_arrives_during_cancel_stays_on_the_position` covers that read.

The replacement path does not use the latest remaining size. After 25 modifies, `modify_price` (`engine.py` lines 171–190) cancels, requires `CANCELLED` with `remaining >= 1`, then calls `_submit(..., quantity=booked.remaining)`. `_submit` refreshes again (line 361) and still posts the quantity captured earlier. Exits skip the lot cap (A2).

Sequence, scratch test `test_a_filled_order_is_not_replaced_with_another_sell`:

1. A working sell is at the modify cap.
2. The first two book reads show `CANCELLED`, filled 0, remaining 15.
3. The refresh inside `_submit` shows that same order `FILLED` for 15.
4. `modify_price` still places another sell of 15.

Worst case: the first sell has already flattened the long, and the second sell opens a new short of one lot. Test contract, quantity 15 short. Unbounded. **ASSUMPTION - verify in Kite docs:** a cancel and a fill can be visible in that order across two `GET /orders` calls. The client will trade on it.

### A6. Rejections and unknown statuses — FAIL

HTTP `error_type` values in `KITE_ERROR_TYPES` (`src/scalper/execution/errors.py` lines 59–67) become the matching exception. Any other name becomes `BrokerError` (`kite_broker.py` lines 288–296). That raise is not `_Ambiguous`, so it is not posted again and it does not by itself engage the kill file. The runner's `fail_closed` will if the exception reaches `run`. The message text (margin, circuit, freeze) is not branched on. The engine stores a `REJECTED` row and returns it as the result.

The runner then treats that row as a live order. A4's trace is the instance: a rejected sell became `WorkingExit`.

An unknown **status** string is stricter and worse. `_status` raises `OrderMappingError` (`src/scalper/execution/mapping.py` lines 93–112). `map_kite_orders` aborts the whole list (lines 62–70). Local `OrderStatus.EXPIRED` is never produced by `_status`. Scratch test `test_one_unknown_status_does_not_hide_the_other_orders` put a valid `COMPLETE` row beside `status=FOO` and the mapper raised `unmapped Kite status FOO`. The filled order was invisible.

Sequence: any row in `GET /orders` carries a status this table does not name. `refresh` throws. `shutdown` calls `refresh` first, throws again, and `fail_closed` swallows that (`runner.py` lines 592–598). Worst case: one lot open, and the process can no longer see it. Test contract, quantity 15, no flatten order.

I am not listing extra Kite status spellings. **ASSUMPTION - verify in Kite docs** which status strings the order book can emit beyond the set in `mapping.py` lines 18–28 and the `COMPLETE` / `REJECTED` / `CANCELLED` branches.

### A7. The order book is assumed complete — FAIL

`get_orders` is one `GET /orders` with no page, no cursor, and no length check (`kite_broker.py` lines 120–122). `map_kite_orders` requires a list and keeps every row it could parse. A truncated list is treated as the whole day. Combined with A3, a fill that is missing from that list is a foreign or invisible position, and shutdown will not flatten it.

**ASSUMPTION - verify in Kite docs:** `GET /orders` returns every order for the day in one list. The code has no other behaviour to fall back on. The poison-row failure in A6 is the demonstrated form of "the book is not usable, so the position is abandoned."

## B. Stops and exits

The stop is not a resting trigger. `kite_broker.py` posts `order_type=LIMIT` and `validity=DAY` (lines 132–149). Scratch test `test_no_resting_trigger_order_is_sent` failed because the file contains neither `trigger_price` nor `SL`. The chase limits live in `PaperLimits`: 30 seconds, poll 0.5 seconds, 20 ticks (`src/scalper/paper/limits.py` lines 24–27). On the test tick of 0.05 that is 1.00 index point, ₹15 on quantity 15, and then the order is given up.

The backtest fills a stop inside the same bar via `adverse_exit_price` (`src/scalper/backtest/engine.py` around the stop-hit branch, `fills.py` lines 96–112). It has no abandon path. A backtest can pass while the live chase leaves the lot open.

### B1. Position open, no working exit, nobody left to manage it — FAIL

Each of these ends with a non-zero position and no task still chasing it.

1. **Chase timeout, including the second attempt inside `fail_closed`.** Sequence, scratch test `test_abandon_then_fail_closed_does_not_leave_the_lot_open`: a target sell does not trade, 40 seconds pass, `_work_exit` hits `flatten_timeout` and `_abandon` (`runner.py` lines 474–476 and 631–648) engages KILL and raises. `run` calls `fail_closed`, which calls `engine.shutdown`. Shutdown cancels the resting exit and posts a new limit at the last price (`engine.py` lines 244–281). That limit does not fill. `_drive_flatten` raises `UnmanagedPosition: remaining 15; orders paper-3` at line 304. `_attempt` logs `paper_fail_closed_step_failed` for `flatten` and swallows it (lines 592–598). The engine is still long 15. Worst case: one lot, and the process is exiting. On Kite the last chase limit can remain as a DAY order far from the market. It is not a stop. Test contract, anything beyond the 20-tick / 1-point band is unpaid risk. A 100-point gap is ₹1,500 on quantity 15, and the code will not chase it.

2. **Rejected odd-lot stop.** A4. Long 10 in the engine, tracker says 5, exit status `rejected`. No working close.

3. **Foreign or pre-existing fill.** A3. Mismatch aborts shutdown. Broker stays long 15.

4. **Kill file already present at startup.** `refuse_to_start` returns before the runner exists. The broker position is untouched.

5. **`last_price` is missing.** `shutdown` raises `UnmanagedPosition` without posting (`engine.py` lines 254–255). Same outcome: the lot stays.

6. **Feed is silent and an exit object already exists.** B2. The lot stays until 15:20, and the 15:20 flatten is the same 20-tick chase.

### B2. Feed drops while a position is open — FAIL

`_on_drop` (`runner.py` lines 226–237) sets `gap`, drops the cache entry, and emits `stops_blind`. It does not cancel, modify, or flatten.

The silence path is the only automatic flatten, and it requires all of: no tick within `stale_after` (5s), `exit is None`, and age past `stale_after + stops_blind_grace` (5s + 30s) (`runner.py` lines 211–224). A websocket that stays open and sends nothing still becomes `Silence`, because `_next` races the iterator against `sleep(stale_after)` when `pace=True` (lines 180–197). The live paper command uses `pace=True`. Replay uses `pace=False` and never generates `Silence`.

Sequence, scratch test `test_silence_flattens_a_working_exit_when_ticks_stop`: position is long 15, a target sell is already working, the clock jumps 60 seconds, `_on_silence` logs `feed_stall` and `stops_blind`, and `position_quantity` stays 15. The working sell is whatever limit was posted. If price has traded through it, that limit is not marketable, and nobody chases it until the next tick. Worst case: one lot unmarked for the rest of the session, then the 15:20 chase of 20 ticks. Test contract, a move larger than 1.00 point through that limit is left open (₹15 of band, then unbounded; 100 points is ₹1,500 on quantity 15).

`_on_drop` followed by a later tick does re-check the stop (`test_the_first_tick_after_a_reconnect_still_checks_the_stop` in the repo). The hole is the time with no ticks, and any period where `self.exit` is already set.

### B3. Circuit, trading halt, gap — FAIL

No branch reads a circuit limit, a price band, or a freeze status. A limit that cannot trade sits until the 30s / 20-tick abandon in B1, which the scratch test already left long 15.

A gap that arrives as a tick is paper-filled by `adverse_exit_price` inside `_fill_protective_exit` (`runner.py` lines 434–461). A gap that arrives while the feed is dead is invisible until B2. Blind flatten, when it is allowed to run, posts at `last_price` (`runner.py` lines 561–563), which is the price from before the gap, and then chases 20 ticks. Worst case is the same open lot as B1. **ASSUMPTION - verify in Kite docs** and the NSE circular: a band freeze can leave a DAY limit unfilled. This code will not switch order type.

### B4. Crash while holding — FAIL

Nothing outside the process exits the position.

- Stop management is inside `PaperRunner`.
- The health server always returns `status: ok` and does not read a position (`src/scalper/health.py` lines 15–21).
- `recover` is never called (A3).
- There is no `trigger_price` left at the broker (B1).
- SIGKILL writes no kill file. The next start either refuses (if a kill file happens to exist) or boots flat from its own point of view and then refuses to flatten when it notices the broker quantity (A3).

Worst case: the lot that was open at the crash stays open. Test contract, quantity 15, unbounded until a person or, if it exists, the broker's own MIS cutoff. That cutoff is **ASSUMPTION - verify in Kite docs**. It is not implemented here.

### B5. Stop checks are tick-driven — FAIL

`position_trigger` runs from `_manage_tick`, which runs from `_on_quote` (`runner.py` lines 335–344). `_on_silence` does not evaluate the stop price. The exit timeout in `_work_exit` (line 474) also runs only when a tick arrives. A dead feed freezes the chase even though the wall clock has passed 30 seconds. B2 is that case.

A stuck datetime freezes the silence age as well. `age = self._now() - self.last_tick_at` (line 219). Scratch test `test_a_stuck_clock_still_blind_flattens` called `_on_silence` twice with `now == last_tick_at` and the open-position object was still there. `asyncio.sleep` can still wake the loop, and the age check then returns. Worst case while ticks have also stopped: the lot stays, because blind flatten never starts. Test contract, quantity 15.

Session bounds do not wrap this path. `SessionMinuteBook` drops ticks before 09:15 and after 15:30 (`src/scalper/paper/bars.py` lines 15–21 and 31–35). `_on_quote` still calls `_manage_tick` on those ticks (`runner.py` line 274). A print outside the cash session can arm an exit. The flatten clock at 15:20 normally stops the loop first.

## C. Halt, KILL, fail-closed

### C1. Halt reasons and the tests — the in-process decision is tested; survival is not — FAIL

Reasons that block an entry and still allow an exit or flatten, from `src/scalper/risk/gate.py`:

| Reason | Where | Test asserts the block |
| --- | --- | --- |
| `manual kill` | `_halt_reason` line 84 | `test_a_clean_entry_is_allowed_and_a_kill_still_allows_the_exit`, `test_manual_kill_blocks_the_entry_and_still_sends_the_exit_and_flatten` |
| `repeated order rejects` | line 86 | `test_anomalies_halt_entries_and_still_allow_an_exit`, `test_repeated_rejects_block_the_entry_and_still_send_the_exit_and_flatten`, `test_rejects_spaced_a_minute_apart_still_halt_entries` |
| `pnl mismatch versus broker` | lines 87–89 | both anomaly tests and `test_pnl_mismatch_blocks_the_entry_and_still_sends_the_exit_and_flatten` |
| `clock drift` | lines 90–93 | anomaly test and `test_clock_drift_blocks_the_entry_and_still_sends_the_exit_and_flatten` |
| `feed stall` | lines 94–98 | anomaly test and `test_feed_stall_blocks_the_entry_and_still_sends_the_exit_and_flatten` |
| `flatten time` | `_flatten_reason` lines 103–104 | `test_daily_loss_and_flatten_time_block_entries` |
| `daily loss limit` | lines 105–106 | same test |

Those tests assert `allowed is False` or the reason string. They are not "did not throw" tests.

Entry vetoes, same file lines 110–127: live lot cap, size below 1 lot, position lot cap, loss per trade, orders per second, orders per minute, margin unknown, insufficient margin. `test_entry_caps_and_unknown_margin` asserts the reason strings. `unknown intent` is line 63.

Engine-local blocks:

| Reason | Where | What the test asserts |
| --- | --- | --- |
| memory halt after an ambiguous place | `halt.py` lines 14–19, set from `submit.py` line 30 | `test_a_missing_tag_after_placement_engages_halt_and_kill` asserts the halt, the kill file, and a refused entry |
| `position mismatch` | `engine.py` lines 498–499 | `test_a_position_mismatch_halts_new_entries` asserts the in-process halt and a refused entry. It does not assert a kill file |
| `foreign order …` | `engine.py` lines 362–364 | `test_a_foreign_tag_on_this_instrument_blocks_entries` asserts the halt decision and that shutdown raises. Shutdown does not flatten |
| `risk snapshot unavailable` | `engine.py` lines 377–379 | `test_unavailable_snapshot_blocks_the_entry_and_still_sends_the_exit_and_flatten` asserts the entry block and that the exit is sent |

`duplicate tag` is returned as `allow` (A2). It is not a halt.

The tests assert the decision inside one process. They do not assert that the halt is still there after a new interpreter, except the ambiguous-place path, which also writes the kill file. Position mismatch, reject counts, clock drift, and feed stall do not.

### C2. A restart clears the halt — FAIL

`src/scalper/execution/halt.py` lines 10–23 keep `_halted` in module globals. `halt_new_entries` does not touch the disk. Scratch test `test_memory_halt_is_gone_in_a_new_process` set the halt, confirmed no kill file, and a new interpreter exited 0 because `entries_halted()` was false.

Sequence: position mismatch calls `halt_new_entries` and raises (`engine.py` lines 498–499). The process is restarted. The new process may enter. If the mismatch is still on the book, the next `refresh` raises again and then fails to flatten (A3). If the mismatch was a one-read glitch, entries are open again with no human decision.

`recent_rejects` and `LiveSnapshotProvider._rejects` are also process memory (`snapshot.py` lines 151–158). A restart zeroes the reject halt.

The kill file survives a restart when `engage` finished. `close_on_ambiguous` halts first and writes the file second (`submit.py` lines 30–33). A crash in that gap leaves neither. `engage` itself is an `os.replace` onto the final name (`src/scalper/risk/killfile.py` lines 27–44), so a crash before the replace leaves `KILL.tmp` and `is_killed` is false.

The file is mode `0600` and the owner can delete it. `scalper resume` unlinks it when given `I_RESUME_TRADING` (`killfile.py` lines 47–52, `cli.py` lines 320–325). `/data/` is gitignored (`.gitignore` line 12). `git clean -fdx` removes ignored files, which deletes `data/KILL`. There is no second copy.

### C3. KILL is relative to the working directory — FAIL

`Settings.data_dir` defaults to `Path("data")` (`src/scalper/config.py` line 67). `kill_path` joins that path and does not resolve it (`killfile.py` lines 19–20). Scratch test `test_kill_file_disappears_when_the_working_directory_changes` engaged `data/KILL` in one directory, changed directory, and `is_killed(kill_path(Settings().data_dir))` was false.

Sequence: the operator starts `python -m scalper paper` from a directory that is not the one where they created `data/KILL`. Startup does not see the kill file. Entries are allowed. A new `data/KILL` may be written in the new directory, leaving the original one behind. Worst case: a new lot on top of whatever the kill file was meant to freeze. Test contract, another quantity of 15.

`load_settings` also reads `.env` from the current directory (`config.py` line 234). A different directory can load a different mode and a different data dir in the same step.

### C4. Exit while halted — PASS

`entries_halted()` is consulted only when `intent is Intent.ENTRY` (`engine.py` line 355). `assess` returns `allowed=True` for `EXIT` and `FLATTEN` when a halt reason is set (`gate.py` lines 65–68). Scratch test `test_exit_still_places_while_the_memory_halt_is_set` passed: after `halt_new_entries("ambiguous place")`, `submit_exit` placed the sell. The repo tests listed in C1 place an exit and a flatten under feed stall, kill, clock drift, pnl mismatch, rejects, and a missing snapshot.

This is an exit of the size the caller passes. It does not check that the exit reduces a position (A2).

### C5. Snapshot tasks and the reject window — FAIL

`build` and `_submit` call `close_reads` in `finally` (`snapshot.py` lines 222–223, `engine.py` lines 408–411). `close_reads` cancels and awaits (`snapshot.py` lines 188–199). That path is covered by `test_build_failure_leaves_no_pending_read_tasks`.

`refresh` → `_check_broker_position` → `LiveSnapshotProvider.positions` calls `begin()` whenever the position task is missing or already done (`snapshot.py` lines 201–205). `begin` starts the quote, position, and margin reads. Nothing in `refresh` calls `close_reads`. `_release_current` (lines 180–186) cancels the previous tasks and does not await them. Scratch test `test_refresh_does_not_leave_snapshot_reads_running` returned from `refresh` while a `_bounded` quote read was still running (`read_timeout` was 30s). Cancelling the caller of `refresh` does not cancel those tasks: they were created with `asyncio.create_task`.

Worst case is a stuck read during a flatten loop, which calls `refresh` on every pass (`engine.py` line 296), piled onto the order-rate burst in E3. It does not by itself send a second order. It does mean a cancelled shutdown can leave broker reads in flight.

The reject window is the whole process when `reject_window` is `None`. `account_limits` sets that (`src/scalper/paper/limits.py` line 44). `_since` then returns `len(events)` (`snapshot.py` lines 394–398). That matches `test_rejects_spaced_a_minute_apart_still_halt_entries`. The list dies with the process (C2).

## D. Time and data

### D1. Clock drift — FAIL

The drift check is `abs(now - exchange_time)` against `clock_drift_limit` (`gate.py` lines 90–93). `exchange_time` is the quote's `exchange_ts` (`snapshot.py` lines 340–352). It is the last tick's exchange timestamp, not a clock source. `stale_after` is 5 seconds and the drift limit is 2 seconds (`limits.py` lines 20–22), so the drift check fires first.

Sequence, scratch test `test_a_quiet_but_fresh_quote_is_not_clock_drift`: `last_quote_at` is now, `exchange_time` is 3 seconds older, decision is `clock drift` and the entry is halted. A quiet few seconds on the contract halts new entries. That is fail-closed for entries. It does not flatten an open position. The repo test `test_anomalies_halt_entries_and_still_allow_an_exit` locks the same 3-second gap in as a halt.

A stuck clock misses the halt. B5: silence age stays 0, so the open lot is not blind-flattened. The same frozen `now` keeps `abs(now - exchange_time)` small if no newer exchange timestamp is applied. Entries stay allowed and the stop is not checked.

### D2. Timezones — FAIL

`now_ist` uses `ZoneInfo("Asia/Kolkata")` (`src/scalper/clock.py` lines 8–19). It does not follow the server's local zone. Asia/Kolkata has no DST. Token expiry is 06:00 IST the next morning for a login at or after 06:00 (lines 32–44), which is outside 09:15–15:30.

Naive datetimes are labeled IST in four places: `snapshot._parse_time` (lines 420–421), `normalize._optional_time` (`src/scalper/data/normalize.py` lines 111–114), `auth._parse_login_time` (`src/scalper/kite/auth.py` lines 45–48), and backtest CSV load (`src/scalper/backtest/engine.py` lines 237–238). `ensure_ist` rejects a naive value (`clock.py` lines 26–29). The paper command passes `now_ist`, which is aware.

`_on_silence` and the flatten check in `_on_quote` compare `_now().timetz().replace(tzinfo=None)` to a naive `15:20` (`runner.py` lines 213 and 242). They do not call `ensure_ist`. A caller that passes UTC will flatten at 15:20 UTC, which is 20:50 IST. The paper command does not do that. A replay or a test clock can.

Midnight does not cross the cash session. Daily PnL resets on the calendar date inside `_note_closed_trade` (`runner.py` lines 539–542). The kill file has no date in its identity, so yesterday's file still blocks today, which is the conservative direction.

### D3. Candles from ticks — FAIL

`MinuteBook` treats volume as a cumulative day total (`src/scalper/data/candles.py` lines 57–65). The first tick contributes 0, which avoids inventing the morning. A decrease, including a reconnect that restarts the cumulative counter, sets the bar volume to the new cumulative value (`return cumulative`). Scratch test `test_negative_volume_does_not_stamp_the_day_total_on_the_bar`: cumulative 1000 then 999 on the next minute produced a bar of volume 999. That bar is what the strategy sees. Worst case is an entry taken off a bad bar, then the stop failures in B. One lot.

Same cumulative on a duplicate tick yields delta 0. The price still folds into high, low, and close. Duplicate ticks do not double-count volume.

A tick whose minute is older than the open bar raises `ValueError` (`candles.py` lines 41–42). `_on_quote` does not catch it. `run` fail-closes. B1 showed that fail-closed can leave quantity 15 when the flatten limit does not fill. An out-of-order minute is a way to enter that path while a position is open.

Ticks before 09:15 or after 15:30 do not update the bar (`bars.py` lines 31–35) and still reach `_manage_tick` (B5).

### D4. Recorder — FAIL

`PostgresWriter.flush` drops the batch out of memory before the transaction (`src/scalper/data/writer.py` lines 106–114). Scratch test `test_a_failed_database_flush_keeps_the_batch` raised `OSError("disk full")` from the transaction and `_quotes` was empty afterwards. Those ticks are not retried. `consume_ticks` does not catch the writer error (`src/scalper/data/recorder.py` lines 69–74), so the recorder task dies and the queue dies with it. Worst case for trading: the quote cache stops, entries halt on feed stall, and an open lot depends on B2. The lost rows also make the journal and the compare path blind to that interval.

Duplicate ticks and duplicate depth are `ON CONFLICT DO NOTHING` (writer lines 25 and 33). A reconnect that inserts the same `(instrument_token, received_at)` does not double-store. The candle upsert does overwrite (lines 40–47).

The subscribed token is the one `select_current_nifty_future` returned at start (`command.py` lines 67–68, `ticker.py` lines 23–30). Nothing re-reads the dump mid-session. Startup checks that today's filename exists (`startup.py` lines 46–47), not that the bytes were downloaded today. A file copied onto today's name is accepted. Worst case: the runner trades the contract in that file. If that contract is the expired one, the position is in a contract that may not be the one the operator meant. Settlement of an expired future is **ASSUMPTION - verify in Kite docs**.

### D5. Expiry — FAIL

`select_current_nifty_future` picks the nearest NFO future with `expiry >= as_of`, and on the expiry date that contract is still chosen (`src/scalper/kite/instruments.py` lines 214–227). The weekday is not hard-coded. Lot size and tick come from the row. The next morning's file is what rolls the symbol, and only if the process is restarted against that file.

Hard-coded clocks that do affect expiry week:

- Cash session 09:15–15:30 (`bars.py` lines 15–16, and `FilterConfig.session_open` / `session_close` in `src/scalper/strategy/filters.py` lines 29–30).
- New entries blocked after 12:30 on a day that appears in the expiry set (`filters.py` lines 31 and 44–45). `command.py` lines 69 puts every Nifty future expiry in the dump into that set, so the block is on the expiry date, not on the whole week.
- Flatten time defaults to 15:20 (`config.py` line 85).

There is no forced roll during expiry week and no extra exit on expiry day beyond that 12:30 entry block and the ordinary 15:20 flatten. If the 15:20 flatten hits B1, the expiring contract is still open into the close. Worst case: one lot into expiry settlement. **ASSUMPTION - verify in Kite docs** and the NSE contract spec for the settlement price and for whether MIS still exists on that afternoon.

## E. Broker and session

### E1. Static IP — FAIL

`assert_broker_call_allowed` compares two strings (`src/scalper/compliance.py` lines 115–138). `KiteBroker` stores `observed_ip` from its constructor (`kite_broker.py` lines 55–71 and 210–216). Nothing in `src/` reads a socket's local address, an egress lookup, or the interface the HTTP client actually bound. Scratch test `test_connect_error_does_not_post_a_second_order` got as far as POST with `static_ip="8.8.8.8"` and `observed_ip="8.8.8.8"` on a `MockTransport`. The check passed because the strings matched.

`GET` is not an order mutation (`compliance.py` lines 99–112), so reads skip the check on purpose.

Sequence: the operator types the registered IP into config and passes that same string as `observed_ip`, while the host's default route leaves from somewhere else. The order POST is allowed. **ASSUMPTION - verify in Kite docs:** Zerodha rejects order calls from an unregistered IP. If they do, the POST fails at Kite and this bug is a false pass of our own gate, not a filled order. If they do not, the order is live from the wrong address. Either way the local gate does not measure the path.

### E2. Token death mid-session — FAIL

The access token is checked in `refuse_to_start` and in `record_until_stopped` / `download_history` before the loop. `run_paper` does not call `assess_session` again. A login after 06:00 IST is modeled as valid until 06:00 the next morning (`clock.py` lines 32–44), so the scheduled expiry is outside the cash session.

`TokenException` is mapped from the error type (`errors.py` lines 19–20 and 59–67) and is not caught by the engine. There is no refresh and no re-login. `fail_closed` will try `shutdown`, and every cancel, modify, and place on `KiteBroker` sends the same dead token, so the flatten fails the same way and is swallowed (B1).

Sequence: Kite starts returning `TokenException` while a lot is open. The process cannot cancel the working order and cannot post the exit. The process exits or sits in `fail_closed`. The lot stays at the broker. **ASSUMPTION - verify in Kite docs:** losing the access token does not cancel DAY orders and does not flatten MIS. Worst case: one lot, no working stop from this process. Test contract, quantity 15, unbounded.

### E3. Rate limits — FAIL

`KITE_ORDERS_PER_SECOND` is 10 and the client cap is 3 (`src/scalper/kite/api_facts.py` lines 28–30, `compliance.py` lines 21–22). `assess` enforces the client caps only on `ENTRY` (`gate.py` lines 119–122). `Settings.max_orders_per_day` is stored and printed. `RiskSnapshot` has no day counter, and `assess` never reads one.

`modify_price` does not call `assess` and does not call `note_order` (`engine.py` lines 157–190 and 435–437). `submit_exit` and `FLATTEN` skip the rate checks (A2).

Scratch test `test_eleven_exits_cannot_exceed_the_order_rate` placed 11 sells with no sleep. Scratch test `test_eleven_immediate_modifies_cannot_exceed_the_order_rate` applied 11 modifies on one order with no sleep. Both are above Kite's published 10 per second and above the client's own 3 per second.

The paper runner spaces chase modifies by `flatten_poll_interval` (0.5s), which is 2 per second on the default. The engine method itself will not. One entry also bursts: `refresh` is `GET /orders` plus positions, the snapshot starts positions and margins, and `_confirm_tag` reads the book five times (`engine.py` lines 413–418). Those reads are not counted against `KITE_OTHER_REQUESTS_PER_SECOND` (10). **ASSUMPTION - verify in Kite docs** that modifies and cancels share the order bucket with places. This code's own `api_facts` names a single orders-per-second number.

Historical candles are limited to 3 per second (`src/scalper/data/historical.py` lines 38–67). That path holds.

Worst case of the unguarded exit loop: more than 10 order calls in one second, Kite rejects or delays the exit that was supposed to close the lot, and the position stays open through that gap. Test contract, quantity 15 for as long as the close is refused. A reject storm also increments `recent_rejects` and can halt entries; it does not by itself flatten.

## What I did not prove

- I did not run `git clean`. The wipe path in C2 follows from `.gitignore` and from `release` unlinking a normal file.
- I did not demonstrate a 5,000-order day. I demonstrated that the daily cap is not an input to `assess`, and that 11 exits and 11 modifies go out with no pause.
- I did not demonstrate a real TCP `ConnectError` after Kite had accepted the body. A2 separates that assumption from the second POST the client does send.
- I did not invent circuit-limit, token-revocation, or MIS square-off behaviour. Those are marked above.
