package com.acme.transfer.service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.core.sdk.CoreBusyException;
import com.acme.core.sdk.CoreTimeoutException;
import com.acme.core.sdk.CoreUnavailableException;
import com.acme.core.sdk.PostingRequest;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.client.Account;
import com.acme.transfer.client.AccountClient;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransferService {

  private final AccountClient accountClient;
  private final FxService fxService;
  private final FraudService fraudService;
  private final CoreBankingClient coreBankingClient;
  private final TransferRepository transferRepository;
  private final R2dbcEntityTemplate template;
  private final AuditService auditService;
  private final Scheduler coreSdkScheduler;
  private final Semaphore coreSessionPermits;

  public Mono<TransferEntity> createTransfer(TransferRequest request) {
    String transferId = UUID.randomUUID().toString();
    BigDecimal amount = new BigDecimal(request.amount());
    log.info("Creating transfer {}: {} {} from {} to {}", transferId, amount, request.currency(),
        request.sourceAccount(), request.destinationAccount());

    return accountClient.getAccount(request.sourceAccount())
        .flatMap(source -> accountClient.getAccount(request.destinationAccount())
            .flatMap(destination -> process(transferId, request, amount, source, destination)))
        .switchIfEmpty(Mono.defer(() -> save(transferId, request, amount, null, "REJECTED", "ACCOUNT_NOT_FOUND", null)))
        .doOnNext(transfer -> auditService.recordTransfer(transfer).subscribe());
  }

  public Mono<TransferEntity> getTransfer(String transferId) {
    return transferRepository.findById(transferId);
  }

  private Mono<TransferEntity> process(String transferId, TransferRequest request, BigDecimal amount,
      Account source, Account destination) {
    if (!"ACTIVE".equals(source.status()) || !"ACTIVE".equals(destination.status())) {
      return save(transferId, request, amount, null, "REJECTED", "ACCOUNT_NOT_ACTIVE", null);
    }
    if (!source.currency().equals(request.currency())) {
      return save(transferId, request, amount, null, "REJECTED", "CURRENCY_MISMATCH", null);
    }
    if (source.available().compareTo(amount) < 0) {
      return save(transferId, request, amount, null, "REJECTED", "INSUFFICIENT_FUNDS", null);
    }
    return fxService.convert(amount, source.currency(), destination.currency())
        .flatMap(conversion -> fraudService.check(transferId, request, amount)
            .flatMap(decision -> switch (decision) {
              case "DENY" -> save(transferId, request, amount, conversion, "REJECTED", "FRAUD_DENIED", null);
              case "REVIEW" -> save(transferId, request, amount, conversion, "PENDING_REVIEW", "FRAUD_REVIEW", null);
              default -> post(transferId, request, amount, conversion);
            }));
  }

  private Mono<TransferEntity> post(String transferId, TransferRequest request, BigDecimal amount,
      Conversion conversion) {
    PostingRequest posting = new PostingRequest(transferId, request.sourceAccount(),
        request.destinationAccount(), amount, request.currency(), conversion.creditAmount(),
        conversion.creditCurrency());

    return Mono.fromCallable(() -> this.postWithPermit(posting))
        .subscribeOn(coreSdkScheduler)
        .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))
            .filter(ex -> ex instanceof CoreBusyException || ex instanceof CoreUnavailableException))
        .flatMap(result -> result.status() == PostingResult.Status.POSTED
            ? save(transferId, request, amount, conversion, "COMPLETED", null, result.coreTxnId())
            : save(transferId, request, amount, conversion, "REJECTED", "CORE_REJECTED", null))
        .onErrorResume(CoreTimeoutException.class, e -> {
          log.error("Core banking timeout for transfer {}", transferId, e);
          return save(transferId, request, amount, conversion, "PENDING", "CORE_TIMEOUT", null);
        });
  }

  private PostingResult postWithPermit(PostingRequest request) throws InterruptedException {
    coreSessionPermits.acquire();
    try {
      return coreBankingClient.post(request);
    } finally {
      coreSessionPermits.release();
    }
  }

  private Mono<TransferEntity> save(String transferId, TransferRequest request, BigDecimal amount,
      Conversion conversion, String status, String reasonCode,
      String coreTxnId) {
    Instant now = Instant.now();
    TransferEntity transfer = new TransferEntity(transferId, status, reasonCode,
        request.sourceAccount(), request.destinationAccount(), amount, request.currency(),
        conversion == null ? null : conversion.creditAmount(),
        conversion == null ? null : conversion.creditCurrency(),
        conversion == null ? null : conversion.rate(),
        coreTxnId, request.description(), now, now);
    log.info("Transfer {} {} {}", transferId, status, reasonCode == null ? "" : reasonCode);
    return template.insert(transfer);
  }
}
