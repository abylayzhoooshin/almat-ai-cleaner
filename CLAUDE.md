# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this service is

`rieltor-cleaner-almaty` is the second microservice in a pipeline for the **Almaty** market (the first one, `rieltor-collector-almaty`, is not in this repo but is talked to over HTTP). Astana runs the same code as a separate deploy from its own repo; the two share no state, so a change here does not affect it:

```
rieltor-collector  --/baseline/table-->  rieltor-cleaner  --/baseline/clean-->  consumer
   (full listing data)             (verdicts + stored snapshot)  (clean rows)
```

It answers one question per krisha.kz rental listing: **is this a whole apartment for long-term rent** (`usable: true|false` + `reason_code`). Non-standard condition is also `usable=false`: no renovation (`no_renovation`), no furniture (`unfurnished`) and no core appliances — fridge/washer/stove/kitchen (`no_appliances`, text-only: there is no structured field for it) — such prices aren't comparable and break the baseline (owner's decision, reversing the original "condition is not a reason" rule). NOT reasons: old/modest renovation, cheapness, "partially" furnished, or an empty renovation/furniture field ("unknown", not "bad"). A last-resort catch-all `other_red_flag` lets the model reject an obviously price-distorting listing that matches no listed example (only when obvious). It does NOT attempt fraud detection. It is a separate service from the collector on purpose — the collector's failure mode is krisha.kz, this service's is OpenAI, and neither should take the other down.

`README.md` (Russian) has the full product rationale, the env-var table, measured cost figures, and the consumer contract.

## Commands

No unit tests. The only check is the prompt regression set: `python prompt_check.py` runs the labeled cases in `prompt_cases.json` through the real model (needs `OPENAI_API_KEY`, `OPENAI_MODEL=gpt-5-mini`; costs cents). **Run it after every `SYSTEM_PROMPT` edit, before pushing**; exit code 1 = a hard case broke. Add a case whenever a new miss is found. Plain pip, no package manager config beyond `requirements.txt`.

Prompt invariants: `SYSTEM_PROMPT` lists rules in decision order (first matching code wins); `reason` is filled only when `usable=false` (empty for `ok`, to save output tokens); `openai_batch._validate_verdict` rejects contradictory answers (`usable` must equal `reason_code == "ok"`), which become `llm_failed` and are retried.

```bash
python -m venv .venv && .venv\Scripts\Activate.ps1   # Windows
pip install -r requirements.txt
.\run.ps1          # sources env.ps1 (gitignored, holds live keys) and runs service.py
```

`service.py` starts the cycle loop AND the FastAPI app in one process. The first cycle runs immediately at startup, not after the interval.

To dry-run the pipeline without spending OpenAI money, set `MIN_BATCH_SIZE=999999` — the cycle does everything except submit a batch.

To run only the HTTP API without triggering a cycle (useful when testing endpoints, since a cycle can submit paid batches):

```bash
.venv/Scripts/python.exe -m uvicorn verdicts_api:app --port 8099
```

Note: the Bash tool's default `python` is the global 3.10 install, which lacks `fastapi`/`uvicorn`. Use `.venv/Scripts/python.exe` explicitly.

## Architecture: the cycle

Two cadences share one sequential event loop in `service.cycle_loop` (never run concurrently, so `pipeline._last_full_fetch` needs no lock):
- **Full cycle** (`pipeline.run_cycle`, every `CLEANER_CYCLE_INTERVAL_H` hours, default 12) — fetches the whole collector table, diffs, runs rules, submits new batches, publishes.
- **Ingest tick** (`pipeline.run_ingest_tick`, every `CLEANER_INGEST_TICK_S` seconds, default 300) — between full cycles, checks pending OpenAI batches (a free API call) and publishes immediately if any completed, reusing the full cycle's last fetched rows (no extra collector call, same staleness contract). Without this, a batch that finishes in 10 minutes would sit unpublished for up to `CLEANER_CYCLE_INTERVAL_H` hours since only the full cycle used to check batch status.

The full cycle's fixed order — the order is load-bearing, see the `pipeline.py` module docstring:

1. **Ingest completed OpenAI batches first** (`ingest_completed_batches`). Batches are async (24h window), so a previous run's batch is usually still in flight. Must run before submitting new work so listings aren't double-sent.
2. **Fetch + diff against the collector** by `content_hash` over only the fields that affect the verdict (title, description, renovation, dorm flag, area, rooms) — **not** price, `last_seen_at`, or photos. Keeps the AI from re-processing listings whose price merely changed.
3. **Free layer** (`heuristics.classify`): zero-token rules. Returns `None` when undecided, meaning "send to AI" — never a fabricated verdict to dodge an API call.
4. **AI layer** (`openai_batch`): the remainder goes out as up to `MAX_BATCHES_PER_CYCLE` batches of `MAX_BATCH_SIZE` each. Defaults (3000×6=18000) mean the whole ~7k Almaty base is submitted in a single cycle on cold start.
5. **Publish** (`publish_clean_baseline`): rebuild the stored clean snapshot — see "Consumer contract".

### Invariants that are easy to break

- **`cleaner_db.in_flight_ids()`** — listings sitting in a not-yet-completed batch are excluded before new batches are built. Without it, a cycle starting before the previous batch resolves double-submits and double-pays.
- **`llm_attempts` / `MAX_LLM_ATTEMPTS`** — a listing the model returns garbage for gets `source="llm_failed"` and its `content_hash` is deliberately **not** stored, so the next diff re-queues it. After `MAX_LLM_ATTEMPTS` the real hash is stored and it's left `usable=None` ("unknown", not "bad") instead of retrying forever. The attempt count is computed once in `pipeline` and passed to `upsert_verdict(attempts=...)` — don't reintroduce a second query there.
- **A "completed" OpenAI batch can have zero successful outputs** (`output_file_id` empty, reasons only in `error_file_id`). That is not a batch failure to OpenAI. `ingest_completed_batches` special-cases it (mark `failed`, requeue) or listings stay stuck in `in_flight_ids` forever while `/health` still reports OK.
- **Partial batch submission must not lose data.** If `submit_batch` throws mid-loop (typically an OpenAI token-queue/quota limit), already-submitted batches stay recorded and the rest simply isn't marked processed, so it returns next cycle.

## Pagination has a real tie-break requirement

`processed_at` has second resolution and an entire batch is written within one second — on real data 1000 of 1014 rows shared one timestamp. Any paginated query MUST order by `(processed_at, id)`, never `processed_at` alone, or consumers walking pages get duplicates and gaps. `idx_verdicts_page` exists for exactly this ordering.

## Rules are retroactive, the prompt is not

`pipeline.apply_rules_to_known` runs `heuristics.classify` over EVERY fetched row each cycle (free, in-memory), right after batch ingestion and before the diff. A rule verdict overrides an existing AI verdict; if a `source='rule'` verdict's rule no longer matches, its `content_hash` is reset so the listing returns to the queue. So changing a rule needs no manual DB cleanup.

Editing `SYSTEM_PROMPT` does NOT re-label existing listings: the diff keys on `content_hash` of the listing text, which a prompt edit doesn't change. To re-run the AI over the whole base after a prompt change, set env `CLEANER_RELABEL_GEN` to any new value (e.g. `2`) and redeploy: the next cycle resets the hash of every `llm`/`llm_failed` verdict (`cleaner_db.start_relabel`), remembers those ids in table `relabel`, and re-submits them. While `relabel` is non-empty the snapshot is NOT rebuilt (publishing would drop the reset listings), so consumers keep the previous one; when the last id gets a fresh verdict (`upsert_verdict` deletes it from `relabel`; ids gone from the site are pruned) the next cycle publishes normally. Rule verdicts are unaffected.

## The two verdict producers share one output shape

Both `heuristics.classify(row)` and the parsed OpenAI response produce the same dict (`usable`, `reason_code`, `confidence`, `reason`, `source`), so `cleaner_db.upsert_verdict` and everything downstream never needs to know which layer produced a verdict. Preserve this shape when touching either producer.

## Consumer contract

`/baseline/clean` serves a **stored snapshot** (`clean_baseline` table), not a live proxy. `pipeline.publish_clean_baseline` rebuilds it wholesale as the last step of every cycle, from that cycle's full collector fetch, in one transaction. The request path never calls the collector.

Publication rule: a listing is in the snapshot only if it has a verdict whose `content_hash` matches its **current** hash, and that verdict is not `usable=0`. New and changed listings therefore wait (12–24h: one cycle submits, the next ingests) — this is deliberate, so unreviewed junk never reaches the "clean" output. `usable=None` rows are published: a retrying `llm_failed` row stores an empty hash and so waits, but once `MAX_LLM_ATTEMPTS` is exhausted the real hash is stored and it goes out as "unknown, not bad".

Tradeoff accepted by the user: prices are as of the last cycle (up to 12h old) rather than live. Don't "fix" this by reintroducing a live proxy — that brings back unreviewed listings in the clean output.

`publish_clean_baseline` never writes an EMPTY snapshot: on cold start nothing has a matching verdict yet, and storing that would make `/baseline/clean` answer 200 with zero rows instead of 503 — defeating the very guard the endpoint documents (a consumer would read "no listings" and wipe its data). It logs a warning and leaves the previous snapshot (or none) alone.

`total` is the snapshot row count; pagination is by `seq` range, so pages are full until the last. `built_at` changes on every rebuild — consumers restart their walk if it changes mid-walk. Before the first rebuild the endpoint returns 503, not an empty list.

`/baseline/clean.csv` returns the whole snapshot as one file (built in memory inside a single sync request handler — don't turn it into a streaming generator over a live sqlite connection: Starlette iterates sync generators across threadpool threads and sqlite connections are thread-bound).

If the collector is unreachable, the cycle still ingests finished batches but leaves the previous snapshot untouched.

`confidence` must never be used as a filter threshold — see README's error-asymmetry argument. It only prioritizes manual review in `/verdicts/review`.

## Deployment constraint worth remembering

Render **Cron Jobs cannot be used** for this service: they get no persistent disk (losing `cleaner.db` means re-paying for the whole base every run and orphaning paid-for batches) and serve no HTTP (the consumer can't reach them). Free web services are equally unusable — no disk and they spin down after 15 min, which stops the internal timer. Hence: paid web service + 1 GB disk, schedule inside the process. `render.yaml` encodes this.

## Secrets

`env.ps1` holds live `OPENAI_API_KEY` and `COLLECTOR_API_KEY` values and is gitignored along with `run.ps1` and `data/`. Never move real keys into a tracked file.
