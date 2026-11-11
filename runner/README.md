# Auspex permitted external reader

This is deployed execution code, not a browser script. It reads only registered
merchant URLs returned by the authenticated Auspex backend, then submits the raw
result for **server-side durable storage before product matching**. The reader
never assigns compatibility, stock or eligibility itself. It does not send email.

## GitHub Actions deployment

The intended repository is `kordldurham/Auspex`, branch `main`, workflow
`.github/workflows/auspex-scanner.yml`. Publish the workflow **last**, after the
backend trusts the repository's immutable numeric ID, owner ID, branch and
workflow identity. Forks and other repositories cannot use this template to run
against production. Optional Actions variable `AUSPEX_SCANNER_PAUSED=true` pauses
execution without modifying the source.

No AgentMail key, Sites service token or long-lived GitHub secret is required.
The job has `id-token: write`; the reader requests a fresh GitHub OIDC token for
`https://auspex-mini-prices-k7m2.devkorddurham.chatgpt.site/scanner`. Tokens are
refreshed before expiry, never logged, stored as artifacts, or sent to merchants.
The app verifies signature and identity claims; client-side token decoding is
used only for expiry caching.

A scheduled event polls every five minutes. **Polling does not create a scan.**
The backend decides whether an existing queued/due full scan can be claimed. A
poll with no job exits successfully. GitHub concurrency and the backend lease
prevent overlapping writers; scheduled timing is best effort, not an exact SLA.
A physical app click enqueues work; the next available runner claims it.

## Controlled acceptance runs

The workflow also supports dispatch, and a push changing `runner/run-config.json`.
Only push/dispatch events use deliberate force/interruption settings; scheduled
polls always ignore them.

1. Push `{schemaVersion:1,requestId:"full-1",forceFull:true,stopAfterBatches:null}`.
   Wait for a terminal full-catalog run and inspect persisted proof.
2. Push a new request ID with `forceFull:true,stopAfterBatches:1`. The reader must
   release its lease with reason `interrupted`; the job remains resumable.
3. Push a new ID with `forceFull:false,stopAfterBatches:null`. It must resume the
   **same job ID**, preserve completed work, and finish all registered sources.
4. Push another `forceFull:true` request with no interruption. Its completed job
   is the third full completion (four workflow invocations, including the
   intentional interruption). A scan can finish full coverage with individual
   source failures; preserve those reasons rather than claiming all URLs worked.

Use valid JSON files (the notation above illustrates fields). Set the config
back to the checked-in idle values after acceptance. Do not infer success from a
workflow's green status alone: require scope, source totals, raw persistence,
matching counts, runner identity and source failure diagnostics from the app.

## Runtime and safety

Python3.12 and `httpx==0.28.1` are required:

```sh
python -m pip install -r runner/requirements.txt
python -m unittest discover -s runner/tests -v
python -m runner --proof-file runner-proof.json
```

Outside GitHub, an explicitly provisioned `AUSPEX_RUNNER_BEARER` can authenticate
an authorized external host. Never commit it or pass it on command lines. The
reader sends that bearer only to the exact Auspex origin and runner endpoints.
Docker/systemd files are portable deployment templates, not a claim that a host
has already been configured. The default command exits after an idle poll or
finished job; systemd's timer invokes future polls. GitHub uses short-lived OIDC
instead and needs no environment file.

Each merchant has one in-flight request and a one-second minimum interval;
collection concurrency defaults to8. Request/body/JSON collection has a12-second
absolute deadline, bounded retries, exponential jitter and Retry-After handling.
403/access denials and changed redirect destinations are not bypassed. Responses
larger than the collection limit or malformed JSON are persisted as failure
evidence, not fabricated product data. Valid JSON of any shape is sent unchanged
for server matching, including an object whose product ID disagrees with the task.

Successful observe acknowledgements are the checkpoint boundary. HTTP failures
are independent per source. Stop/SIGTERM/deadline releases only the runner lease,
not the scan; a later worker can continue pending tasks. The backend is the source
of truth for retries, leases and product matching. Raw bodies and tokens are not
written to runner proof; the proof includes execution IDs and counters only.

## Backend adapter contract

- `POST /api/runner/claim {runnerId,forceFull}` returns
  `{claimed,busy,job:null|job,runner}`; no queued work means `job:null`.
- `GET /api/runner/tasks?runnerId=...` returns `{job,sources}`.
- `POST /api/runner/observe {runnerId,jobId,observations,shopChecks}` returns `{job}`.
- `POST /api/runner/heartbeat {runnerId,jobId}` renews the same authorized lease.
- `POST /api/runner/release {runnerId,jobId,reason}` releases/requeues safely.

All API calls use `Authorization: Bearer ...`; all mutations carry runnerId.
Collection `observation.collection` records status, attempt count, response hash
and byte count when a complete response was read. The `product` member may contain
any valid JSON type. Collection errors include evidence/status instead of product.
Server code must persist the incoming raw evidence before matching or rejection,
return individual rejection reasons, and make repeated acknowledgements idempotent.

### Proof counter definitions

`collected` and `collectionFailures` are client collection outcomes.
`persistenceAcks` counts successful observe checkpoints.
`rawEvidenceAcknowledged` counts raw observation records explicitly acknowledged
by the server, split into `rawProductEvidenceAcknowledged` and
`rawFailureEvidenceAcknowledged`. These can include retry evidence and are not
unique matched-listing counts. A missing/incomplete raw-persistence receipt fails
the worker rather than implying data was saved.

Server job counters are recorded unchanged: `listingsDiscovered` means raw
collection succeeded, `listingsPersisted` means catalog matching/persistence
succeeded, and `rejectedListings` means collected evidence was rejected during
matching. Proof aliases `rawCollectionSucceededListings`,
`matchedCatalogListings` and `rejectedAfterCollectionListings` make that distinction
explicit. A raw response can be durably saved while matching rejects it.
