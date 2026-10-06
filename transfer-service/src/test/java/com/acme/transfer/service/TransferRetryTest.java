package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.core.sdk.CoreBusyException;
import com.acme.core.sdk.CoreUnavailableException;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.client.Account;
import com.acme.transfer.client.AccountClient;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class TransferRetryTest {

  @Test
  void retriesBusyAndUnavailableCoreErrorsThenCompletes() {
    AccountClient accountClient = mock(AccountClient.class);
    FxService fxService = mock(FxService.class);
    FraudService fraudService = mock(FraudService.class);
    CoreBankingClient coreBankingClient = mock(CoreBankingClient.class);
    R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
    AuditService auditService = mock(AuditService.class);
    TransferService service = new TransferService(accountClient, fxService, fraudService,
        coreBankingClient, mock(TransferRepository.class), template, auditService,
        Schedulers.immediate(), new Semaphore(10));

    when(accountClient.getAccount("2000000001")).thenReturn(Mono.just(account("2000000001")));
    when(accountClient.getAccount("2000000002")).thenReturn(Mono.just(account("2000000002")));
    when(fxService.convert(any(), anyString(), anyString()))
        .thenReturn(Mono.just(new Conversion(new BigDecimal("1.00"), "USD", null)));
    when(fraudService.check(anyString(), any(), any())).thenReturn(Mono.just("ALLOW"));
    when(coreBankingClient.post(any()))
        .thenThrow(new CoreBusyException("No core session available"))
        .thenThrow(new CoreUnavailableException("Core unavailable", new RuntimeException("offline")))
        .thenReturn(new PostingResult("ref", "core-txn-1", PostingResult.Status.POSTED, null,
            Instant.now()));
    when(template.insert(any(TransferEntity.class)))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    when(auditService.recordTransfer(any())).thenReturn(Mono.just(1));

    StepVerifier.create(service.createTransfer(
        new TransferRequest("2000000001", "2000000002", "1.00", "USD", "retry test")))
        .assertNext(transfer -> assertEquals("COMPLETED", transfer.status()))
        .verifyComplete();

    verify(coreBankingClient, times(3)).post(any());
  }

  private static Account account(String number) {
    return new Account(number, "Test", "USD", "ACTIVE", new BigDecimal("1000.00"));
  }
}
