# Design

> Ringkasan ini berfokus pada perubahan dan pemeriksaan yang sudah dilakukan.

## 1. Problems found

| Problem | Cause | Current change | Evidence / remaining work |
|---|---|---|---|
| INC-101: core timeout could lead to duplicate debit | A timeout leaves the posting outcome unknown. | Only busy/unavailable errors are retried. A timeout saves `PENDING / CORE_TIMEOUT`; reconciliation uses `inquire`, never another `post`. | Unit test verifies timeout→inquiry and one `post`. |
| INC-108 / INC-109: reconciliation could repost an uncertain transfer or overwhelm core | Reconciliation previously retried `post`, and API plus reconciliation had no common in-process cap. | `ReconciliationJob` calls read-only `inquire(transferId)`. `CoreConcurrencyConfiguration` provides a shared fair semaphore with 10 permits; both API and reconciliation acquire it. | Targeted tests pass for posted, rejected, and empty inquiry results using an immediate scheduler in the unit-test setup. The semaphore is per JVM, not shared across replicas. |
| Blocking SDK call on a reactive path | `CoreBankingClient` uses synchronous I/O. Calling it directly from a Reactor pipeline can block an event-loop thread. | `TransferService` wraps `post` in `Mono.fromCallable` and uses `coreSdkScheduler`, configured from a virtual-thread-per-task executor. | Verify with BlockHound or a thread-name/assertion test; not yet recorded. |
| Fraud unavailable must not move money | A fraud error previously mapped to `ALLOW`. | Error now saves `FAILED / FRAUD_UNAVAILABLE`. | Unit test verifies Core `post` is not called. |
| Account and FX clients retry broadly | Retries do not filter errors or use backoff. | Not changed. | `[TODO: tighten Account/FX retries.]` |
| Idempotency may race under concurrent requests | The schema indexes but does not uniquely constrain the key. | SHA-256 request fingerprints replay the same body and reject a different body with 422. Concurrent first requests remain unprotected. | Same-body and changed-body tests pass. Database-backed atomicity remains. |
| INC-110: service cannot resolve dependencies in Compose | The default application URLs point to `localhost`, which refers to the transfer-service container itself; Compose does not provide service DNS URLs through environment variables. | `docker-compose.yml` now supplies PostgreSQL and mock-bank service DNS URLs, waits for healthy dependencies, and defines healthchecks for PostgreSQL and transfer-service. | `docker compose config --quiet` passed. The stack ran a successful Docker smoke transfer with HTTP 201 / `COMPLETED`; transfer ID `0e844f6f-c787-4575-b420-94ccc3a3f711`, core transaction ID `CT00000001`. |

## 2. Threads

WebFlux handles HTTP requests and the account, FX, fraud, and R2DBC publishers reactively. The Core SDK is blocking. The API wraps `coreBankingClient.post` with `Mono.fromCallable(...).subscribeOn(coreSdkScheduler)`, so the SDK call runs on a virtual thread rather than an event-loop thread. Reconciliation schedules its blocking `inquire` work on the same Spring-managed scheduler and uses the same semaphore as the API.

Reconciliation uses `.block()` in its scheduled worker, not on the WebFlux event loop. `[TODO: validate worker approach.]`

## 3. Trace

`[TODO: add a redacted timeout trace.]`

## 4. Core SDK

`CoreBankingClient.post` and `inquire` run on the virtual-thread-backed `coreSdkScheduler`. The shared `Semaphore(10, true)` is acquired before either call and released in `finally`, so no more than 10 core SDK calls are active at once **within one application process**.

At most 10 Core calls run concurrently per process; three replicas could reach 30. `[TODO: measure a 200-request load.]`

## 5. Timeouts and retries

| Dependency | Timeout | Retry behavior | Reason / status handling |
|---|---|---|---|
| Accounts | No client-specific timeout is configured in `AccountClient`. | `retry(3)` (up to four subscriptions); no filter or backoff. HTTP 404 becomes empty. | Broad retry policy needs review. |
| FX | No client-specific timeout is configured in `FxClient`. | `retry(3)` (up to four subscriptions); no filter or backoff. | Broad retry policy needs review. |
| Fraud | No client-specific timeout is configured in `FraudClient`. | No retry in the client. `FraudService` currently converts errors to `ALLOW`. | Fail-open behavior is a known risk and must be resolved. |
| Core SDK | 500 ms connect timeout; read timeout comes from `acme.core-read-timeout-ms` / `CORE_SDK_READ_TIMEOUT_MS` configuration. | `post` retries only `CoreBusyException` and `CoreUnavailableException`, up to three retries with a fixed one-second delay. `CoreTimeoutException` is not retried; the transfer is saved as `PENDING / CORE_TIMEOUT`. | Busy/unavailable indicate nothing was sent; a timeout has an unknown outcome and must be reconciled with `inquire`. |

## 6. Fraud unavailable

Fraud errors fail closed: `TransferService` saves `FAILED / FRAUD_UNAVAILABLE` and does not call Core Banking. This follows the 503 option in `docs/api.md`; unit tests verify the behavior.

## 7. Idempotency

The API stores idempotency responses in the database and returns a stored response when a key is found. The current check-then-create flow is not atomic, and the schema does not enforce a unique idempotency key, so concurrent duplicate first requests are not guaranteed to produce one transfer. A restart preserves stored rows, but it does not fix the concurrent race. Three replicas make the race more likely because each process can perform the lookup at the same time.

Matching-key replay and 422 `IDEMPOTENCY_KEY_REUSED` for a changed body are covered by controller tests. Requests use SHA-256 fingerprints. `[TODO: add a database-backed atomic claim and concurrent request test; the schema currently has only a non-unique index.]`

## 8. Virtual threads

The Core SDK performs blocking `HttpURLConnection` I/O. The API bridges that call from Reactor with `Mono.fromCallable(...).subscribeOn(coreSdkScheduler)`. The scheduler wraps `Executors.newVirtualThreadPerTaskExecutor()` and is disposed by Spring on shutdown. Reconciliation schedules its imperative SDK inquiry on that scheduler. The semaphore, rather than the number of virtual threads, limits active core calls.

`CoreBankingClient` performs blocking I/O inside a synchronized session. `[TODO: verify pinning impact.]`

## 9. Streams

- **Batch:** Not implemented.
- **Events:** Not implemented.

## 10. Docker

`transfer-service/Dockerfile` uses a Maven/JDK build stage and a JRE runtime stage, runs as `appuser` (UID 10001), and sets `-Xms256m -Xmx256m` under the assessment's 512 MB container limit. Compose supplies service-DNS URLs, waits for PostgreSQL and mock-bank health before starting transfer-service, and waits for transfer-service health before starting web-client. The runtime image check confirmed `/usr/bin/curl` is present for the transfer-service healthcheck.

On 2026-10-06, `docker compose config --quiet` passed. The Compose stack handled a 1.00 USD smoke transfer with HTTP 201 / `COMPLETED`, transfer ID `0e844f6f-c787-4575-b420-94ccc3a3f711`, and core transaction ID `CT00000001`. A second manual check sent the same request twice with idempotency key `docker-idempotency-check-01`; both responses returned transfer ID `24beb5e3-6d9d-4a60-9c6c-856dac11fef1` and core transaction ID `CT00000002`. The manual Docker smoke preceded the latest service logic changes; those changes passed the Maven suite. These Docker checks are not automated integration tests.

Image size is 382 MB. `docker compose stop --timeout 10 transfer-service` completed with Spring's graceful-shutdown log; the service restarted healthy. This was an idle shutdown, not a shutdown during a batch.

## 11. Git and security

`origin` points to `https://github.com/ddiandrab/ocbc-code-assessment`; `upstream` points to the assessment repository. The working branch is `chore/assessment-bootstrap` at `6b11babb`. `.gitignore` is present, and `.idea/` and `target/` are not tracked. Current edits are uncommitted and this branch is not pushed.

SDK-101 (`4d839f7`) and SDK-107 (`eb8e8d3`) are not ancestors of this branch; the required `-x` cherry-picks remain undone.

SEC-301 history cleanup and credential rotation are unverified. Follow `docs/GIT_WORKFLOW.md`; do not include the credential in this document.

## 12. Production

At three replicas, the in-memory semaphore allows up to 30 core calls rather than the required global maximum of 10. The current idempotency check can also race across replicas. At ten times the traffic, unbounded/unspecified WebClient timeouts, broad retries, and waiting virtual threads need load testing and explicit backpressure/capacity limits.

With another week, I would prioritize global/DB-backed concurrency and idempotency, explicit dependency timeouts and retry filters, then implement batch/events and validate under the required Docker limits.

## 13. Learning

- Project Reactor: Learned how Mono and Flux work, how to use fromCallable and subscribeOn for blocking calls, and how retries differ from timeout error handling.
- Troubleshooting: Practiced diagnosing test failures and logs, fixing issues with asynchronous tests, and verifying transfer, retry, and reconciliation behavior.
- Docker: Learned how to configure service URLs, health checks, and dependencies in Docker Compose, and how to use a multi-stage build with a non-root user. Also ran a smoke test and checked endpoint idempotency.

## 14. AI

I used an AI assistant to clarify requirements, review and edit code and tests, explain Reactor, virtual threads and reconciliation, troubleshoot test results, and draft this document. I reviewed the changes and verified the Maven suite; the Docker smoke test was run separately.

## 15. Testing

Current regression tests include `TransferServiceTest` for the successful and core-timeout paths, and `ReconciliationJobTest` for posted, rejected, empty inquiry, and no-pending-transfer cases. They use Reactor publishers and Mockito verification; the reconciliation work is asynchronous and should be awaited through completion signals rather than arbitrary sleeps.

The full `./mvnw -B verify` passed on 2026-10-06: 59 tests, 0 failures and 0 errors. Tests cover idempotency replay and changed-body rejection, Core busy/unavailable retry, fraud unavailable without posting, and timeout reconciliation without a second `post`. Earlier manual Docker checks passed Compose validation, confirmed `curl` in the runtime image, completed a transfer with HTTP 201 / `COMPLETED`, and replayed a key with the same transfer and core transaction IDs.
