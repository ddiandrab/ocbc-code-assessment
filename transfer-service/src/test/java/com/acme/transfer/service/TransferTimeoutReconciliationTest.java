package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.core.sdk.CoreTimeoutException;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.client.Account;
import com.acme.transfer.client.AccountClient;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.job.ReconciliationJob;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class TransferTimeoutReconciliationTest {

  @Test
  void timeoutIsResolvedByInquiryWithoutPostingAgain() {
    AccountClient accountClient = mock(AccountClient.class);
    FxService fxService = mock(FxService.class);
    FraudService fraudService = mock(FraudService.class);
    CoreBankingClient coreBankingClient = mock(CoreBankingClient.class);
    TransferRepository repository = mock(TransferRepository.class);
    R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
    AuditService auditService = mock(AuditService.class);
    Semaphore permits = new Semaphore(10);

    when(accountClient.getAccount("2000000001")).thenReturn(Mono.just(account("2000000001")));
    when(accountClient.getAccount("2000000002")).thenReturn(Mono.just(account("2000000002")));
    when(fxService.convert(any(), anyString(), anyString()))
        .thenReturn(Mono.just(new Conversion(new BigDecimal("1.00"), "USD", null)));
    when(fraudService.check(anyString(), any(), any())).thenReturn(Mono.just("ALLOW"));
    when(coreBankingClient.post(any()))
        .thenThrow(new CoreTimeoutException("Core banking timeout"));
    when(template.insert(any(TransferEntity.class)))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    when(auditService.recordTransfer(any())).thenReturn(Mono.just(1));

    TransferService transferService = new TransferService(accountClient, fxService, fraudService,
        coreBankingClient, repository, template, auditService, Schedulers.immediate(), permits);
    TransferEntity pending = transferService.createTransfer(new TransferRequest(
        "2000000001", "2000000002", "1.00", "USD", "timeout then reconcile")).block();
    assertEquals("PENDING", pending.status());

    when(repository.findByStatusAndReasonCode("PENDING", "CORE_TIMEOUT"))
        .thenReturn(Flux.just(pending));
    when(coreBankingClient.inquire(pending.transferId())).thenReturn(Optional.of(
        new PostingResult("ref", "core-txn-1", PostingResult.Status.POSTED, null, Instant.now())));
    when(repository.updateStatus(anyString(), anyString(), nullable(String.class),
        nullable(String.class), any(Instant.class))).thenReturn(Mono.just(1));

    ReconciliationJob job = new ReconciliationJob(repository, coreBankingClient, permits,
        Schedulers.immediate());
    job.retryTimedOutTransfers();

    verify(coreBankingClient).inquire(pending.transferId());
    verify(coreBankingClient, times(1)).post(any());
    verify(repository).updateStatus(eq(pending.transferId()), eq("COMPLETED"),
        nullable(String.class), eq("core-txn-1"), any(Instant.class));
  }

  private static Account account(String number) {
    return new Account(number, "Test", "USD", "ACTIVE", new BigDecimal("1000.00"));
  }
}
