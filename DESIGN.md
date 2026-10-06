# Design

> Draft for review. Replace every `[TODO]` with evidence from your own run or decision before submitting. Do not report a test, measurement, Git operation, or incident fix as complete unless you verified it.

## 1. Problems found

| Problem | Cause | Current change | Evidence / remaining work |
|---|---|---|---|
| INC-101: core timeout could lead to duplicate debit | A broad retry around the transfer flow could repeat a non-idempotent core `post`; timeout means the outcome is unknown. | In `TransferService.post`, only `CoreBusyException` and `CoreUnavailableException` are retried. `CoreTimeoutException` saves `PENDING / CORE_TIMEOUT`; it is not posted again by the request path. | `TransferServiceTest.createTransferTimeout` verifies `PENDING` and one `post`. Add/retain an integration test proving a timeout followed by reconciliation does not post twice. |
| INC-108 / INC-109: reconciliation could repost an uncertain transfer or overwhelm core | Reconciliation previously retried `post`, and API plus reconciliation had no common in-process cap. | `ReconciliationJob` calls read-only `inquire(transferId)`. `CoreConcurrencyConfiguration` provides a shared fair semaphore with 10 permits; both API and reconciliation acquire it. | Targeted tests pass for posted, rejected, and empty inquiry results using an immediate scheduler in the unit-test setup. The semaphore is per JVM, not shared across replicas. |
| Blocking SDK call on a reactive path | `CoreBankingClient` uses synchronous I/O. Calling it directly from a Reactor pipeline can block an event-loop thread. | `TransferService` wraps `post` in `Mono.fromCallable` and uses `coreSdkScheduler`, configured from a virtual-thread-per-task executor. | Verify with BlockHound or a thread-name/assertion test; not yet recorded. |
| Fraud errors currently fail open | `FraudService.check` maps any error to `ALLOW`. | Not changed in this patch. | `[TODO: choose and implement the policy required by the assessment; add a regression test.]` |
| Account and FX clients retry broadly | `AccountClient` and `FxClient` use `retry(3)` without filtering exception types or backoff. | Not changed in this patch. | `[TODO: decide which errors are safe to retry and add tests.]` |
| Idempotency may race under concurrent requests | The controller checks for an existing key before saving it; the schema uses an index rather than a unique constraint. Two concurrent first requests can both pass the check. | Sequential replay with the same key returns the stored response. The concurrent race is not fixed. | Docker smoke check returned the same `transferId` (`24beb5e3-6d9d-4a60-9c6c-856dac11fef1`) and `coreTxnId` (`CT00000002`) for two sequential requests with key `docker-idempotency-check-01`. Add an automated replay test and a concurrency test; decide behavior for the same key with a different request body. |
| INC-110: service cannot resolve dependencies in Compose | The default application URLs point to `localhost`, which refers to the transfer-service container itself; Compose does not provide service DNS URLs through environment variables. | `docker-compose.yml` now supplies PostgreSQL and mock-bank service DNS URLs, waits for healthy dependencies, and defines healthchecks for PostgreSQL and transfer-service. | `docker compose config --quiet` passed. The stack ran a successful Docker smoke transfer with HTTP 201 / `COMPLETED`; transfer ID `0e844f6f-c787-4575-b420-94ccc3a3f711`, core transaction ID `CT00000001`. |

## 2. Threads

WebFlux handles HTTP requests and the account, FX, fraud, and R2DBC publishers reactively. The Core SDK is blocking. The API wraps `coreBankingClient.post` with `Mono.fromCallable(...).subscribeOn(coreSdkScheduler)`, so the SDK call runs on a virtual thread rather than an event-loop thread. Reconciliation schedules its blocking `inquire` work on the same Spring-managed scheduler and uses the same semaphore as the API.

Reconciliation currently calls `.block()` while collecting pending transfers and while persisting a resolved status. Those calls run in the scheduled/job work path, not the WebFlux event loop. `[TODO: decide whether to keep this imperative job design or convert the repository flow to a fully reactive chain; verify thread behavior.]`

## 3. Trace

`[TODO: Run a transfer that times out and paste a redacted log trace here. Include the transfer/correlation ID, the timeout log, the saved PENDING status, the reconciliation inquiry result, and the final status. Do not include credentials or account-sensitive data.]`

## 4. Core SDK

`CoreBankingClient.post` and `inquire` run on the virtual-thread-backed `coreSdkScheduler`. The shared `Semaphore(10, true)` is acquired before either call and released in `finally`, so no more than 10 core SDK calls are active at once **within one application process**.

When 200 requests arrive together, API requests can reach the SDK scheduling point, but at most 10 hold permits. Other virtual threads wait at `Semaphore.acquire()`. This limits open SDK work but does not add an explicit request queue bound or a multi-replica/global limit. With three replicas, the current local semaphore could allow up to 30 simultaneous SDK calls. `[TODO: load-test 200 requests and record latency, queueing, and core session counts.]`

## 5. Timeouts and retries

| Dependency | Timeout | Retry behavior | Reason / status handling |
|---|---|---|---|
| Accounts | No client-specific timeout is configured in `AccountClient`. | `retry(3)` (up to four subscriptions); no filter or backoff. HTTP 404 becomes empty. | Broad retry policy needs review. |
| FX | No client-specific timeout is configured in `FxClient`. | `retry(3)` (up to four subscriptions); no filter or backoff. | Broad retry policy needs review. |
| Fraud | No client-specific timeout is configured in `FraudClient`. | No retry in the client. `FraudService` currently converts errors to `ALLOW`. | Fail-open behavior is a known risk and must be resolved. |
| Core SDK | 500 ms connect timeout; read timeout comes from `acme.core-read-timeout-ms` / `CORE_SDK_READ_TIMEOUT_MS` configuration. | `post` retries only `CoreBusyException` and `CoreUnavailableException`, up to three retries with a fixed one-second delay. `CoreTimeoutException` is not retried; the transfer is saved as `PENDING / CORE_TIMEOUT`. | Busy/unavailable indicate nothing was sent; a timeout has an unknown outcome and must be reconciled with `inquire`. |

The total worst-case transfer latency is **not yet bounded or calculated** because the WebClient dependencies do not have explicit per-client timeouts in the current code. `[TODO: configure/confirm dependency timeouts and calculate the retry budget from those values.]`

## 6. Fraud unavailable

Current behavior is fail-open: `FraudService.check` logs the failure and returns `ALLOW`. This preserves availability but can allow a transfer without a successful fraud decision. `[TODO: state the chosen risk policy, implement it, and link the regression test. Do not describe fail-open as fail-closed.]`

## 7. Idempotency

The API stores idempotency responses in the database and returns a stored response when a key is found. The current check-then-create flow is not atomic, and the schema does not enforce a unique idempotency key, so concurrent duplicate first requests are not guaranteed to produce one transfer. A restart preserves stored rows, but it does not fix the concurrent race. Three replicas make the race more likely because each process can perform the lookup at the same time.

`[TODO: implement and test an atomic database-backed uniqueness/claim strategy, including matching-key replay and same-key/different-request behavior.]`

## 8. Virtual threads

The Core SDK performs blocking `HttpURLConnection` I/O. The API bridges that call from Reactor with `Mono.fromCallable(...).subscribeOn(coreSdkScheduler)`. The scheduler wraps `Executors.newVirtualThreadPerTaskExecutor()` and is disposed by Spring on shutdown. Reconciliation schedules its imperative SDK inquiry on that scheduler. The semaphore, rather than the number of virtual threads, limits active core calls.

`CoreBankingClient` synchronizes on the per-call `Session` while opening, using, and closing it. `[TODO: verify this behavior against the SDK source and explain the Java 21 pinning implications based on the actual synchronized I/O path.]`

`[TODO: record a reproducible measurement: workload, machine/container limits, throughput, latency, and thread/session counts. No measurement is claimed yet.]`

## 9. Streams

- **Batch:** Not implemented yet. `[TODO: document concurrency, per-account ordering, slow-client behavior, and disconnect handling after implementation and tests.]`
- **Events:** Not implemented yet. `[TODO: document safe event publication from event-loop and virtual-thread paths, slow-subscriber policy, disconnect behavior, and subscriber gauge after implementation and tests.]`

## 10. Docker

`transfer-service/Dockerfile` uses a Maven/JDK build stage and a JRE runtime stage, runs as `appuser` (UID 10001), and sets `-Xms256m -Xmx256m` under the assessment's 512 MB container limit. Compose supplies service-DNS URLs, waits for PostgreSQL and mock-bank health before starting transfer-service, and waits for transfer-service health before starting web-client. The runtime image check confirmed `/usr/bin/curl` is present for the transfer-service healthcheck.

On 2026-10-06, `docker compose config --quiet` passed. The Compose stack handled a 1.00 USD smoke transfer with HTTP 201 / `COMPLETED`, transfer ID `0e844f6f-c787-4575-b420-94ccc3a3f711`, and core transaction ID `CT00000001`. A second manual check sent the same request twice with idempotency key `docker-idempotency-check-01`; both responses returned transfer ID `24beb5e3-6d9d-4a60-9c6c-856dac11fef1` and core transaction ID `CT00000002`, confirming sequential replay returned the stored result in this run. These are manual Docker checks, not automated integration tests.

`[TODO: record the built image size and verify graceful shutdown by measuring `docker stop` during a batch. Confirm health status for all services after a clean `docker compose up --build -d` before submission.]`

## 11. Git and security

`.gitignore` and removal of tracked `.idea/` and `target/` files are part of the current staged changes. `[TODO: verify the resulting commit and fork state.]`

The required SDK-101 and SDK-107 changes have not been confirmed as cherry-picked in this draft. `[TODO: record both `-x` cherry-picks and how the conflict was resolved, with commit IDs.]`

For SEC-301, remove the exposed credential from the repository history/tip according to `docs/GIT_WORKFLOW.md`, and have the credential owner rotate/revoke it outside this repository. `[TODO: record what was actually done; never paste the credential here.]`

## 12. Production

At three replicas, the in-memory semaphore allows up to 30 core calls rather than the required global maximum of 10. The current idempotency check can also race across replicas. At ten times the traffic, unbounded/unspecified WebClient timeouts, broad retries, and waiting virtual threads need load testing and explicit backpressure/capacity limits.

With another week, I would prioritize a global/DB-backed concurrency and idempotency strategy, explicit dependency timeouts and retry filters, fail-closed fraud behavior (subject to the risk decision), then implement the batch/event features and validate under the required Docker limits.

## 13. Learning

`[TODO: Write this in your own words: what was unfamiliar at the start, which repository documents/tests you used, and what you can now explain or implement without assistance.]`

## 14. AI

I used an AI assistant to clarify assessment requirements, review code and test changes, explain Reactor/virtual-thread and reconciliation behavior, troubleshoot compiler/test output, and prepare this `DESIGN.md` draft. I reviewed the suggestions and made the implementation changes myself. `[TODO: adjust this statement to match your actual use and review process.]`

## 15. Testing

Current regression tests include `TransferServiceTest` for the successful and core-timeout paths, and `ReconciliationJobTest` for posted, rejected, empty inquiry, and no-pending-transfer cases. They use Reactor publishers and Mockito verification; the reconciliation work is asynchronous and should be awaited through completion signals rather than arbitrary sleeps.

The latest targeted run was `./mvnw -B -pl transfer-service -Dtest=ReconciliationJobTest,TransferServiceTest test`: 5 tests passed, with 0 failures and 0 errors (3 reconciliation tests and 2 transfer-service tests). The full `./mvnw -B verify` also passed: 53 tests across the SDK, mock bank, and transfer service, with 0 failures and 0 errors. The reconciliation unit tests inject `Schedulers.immediate()` so scheduled work completes before assertions; they do not measure virtual-thread performance or replace the integration test. Manual Docker checks on 2026-10-06 passed Compose syntax validation, confirmed `curl` exists in the transfer-service runtime image, completed a transfer with HTTP 201 / `COMPLETED`, and replayed an idempotency key twice with the same transfer and core transaction IDs.

Coverage still needs to include retry filtering/backoff, concurrent idempotency, fraud unavailability, and automated integration behavior against the real `mock-bank` container. `[TODO: add automated checks for the manual Docker observations and record graceful-shutdown validation.]`
