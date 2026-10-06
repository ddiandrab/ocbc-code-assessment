package com.acme.transfer.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.transfer.client.Account;
import com.acme.transfer.client.AccountClient;
import com.acme.transfer.client.FraudClient;
import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;
import java.math.BigDecimal;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class FraudServiceFailureTest {

  @Test
  void doesNotTurnFraudServiceFailureIntoAllow() {
    FraudClient fraudClient = mock(FraudClient.class);
    AcmeProperties properties = new AcmeProperties("accounts", "fx", "fraud", "core", 2000,
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000"), new BigDecimal("5000"),
            new BigDecimal("6500")));
    FraudService fraudService = new FraudService(fraudClient, properties);
    TransferRequest request = new TransferRequest(
        "2000000001", "2000000002", "7000.00", "USD", "fraud outage test");
    when(fraudClient.assess("transfer-1", "7000.00", "USD", "2000000001", "2000000002"))
        .thenReturn(Mono.error(new IllegalStateException("fraud service unavailable")));

    StepVerifier.create(fraudService.check("transfer-1", request, new BigDecimal("7000.00")))
        .expectError(IllegalStateException.class)
        .verify();

    verify(fraudClient).assess("transfer-1", "7000.00", "USD", "2000000001", "2000000002");
  }

  @Test
  void fraudUnavailableMarksTransferFailedWithoutPostingToCore() {
    FraudClient fraudClient = mock(FraudClient.class);
    AcmeProperties properties = new AcmeProperties("accounts", "fx", "fraud", "core", 2000,
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000"), new BigDecimal("5000"),
            new BigDecimal("6500")));
    FraudService fraudService = new FraudService(fraudClient, properties);
    AccountClient accountClient = mock(AccountClient.class);
    FxService fxService = mock(FxService.class);
    CoreBankingClient coreBankingClient = mock(CoreBankingClient.class);
    R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
    AuditService auditService = mock(AuditService.class);
    TransferService transferService = new TransferService(accountClient, fxService, fraudService,
        coreBankingClient, mock(TransferRepository.class), template, auditService,
        Schedulers.immediate(), new Semaphore(10));
    TransferRequest request = new TransferRequest(
        "2000000001", "2000000002", "7000.00", "USD", "fraud outage test");

    when(accountClient.getAccount("2000000001")).thenReturn(Mono.just(account("2000000001")));
    when(accountClient.getAccount("2000000002")).thenReturn(Mono.just(account("2000000002")));
    when(fxService.convert(any(), anyString(), anyString()))
        .thenReturn(Mono.just(new Conversion(new BigDecimal("7000.00"), "USD", null)));
    when(fraudClient.assess(anyString(), eq("7000.00"), eq("USD"), eq("2000000001"),
        eq("2000000002")))
        .thenReturn(Mono.error(new IllegalStateException("fraud service unavailable")));
    when(template.insert(any(TransferEntity.class)))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    when(auditService.recordTransfer(any())).thenReturn(Mono.just(1));

    StepVerifier.create(transferService.createTransfer(request))
        .assertNext(transfer -> {
          org.junit.jupiter.api.Assertions.assertEquals("FAILED", transfer.status());
          org.junit.jupiter.api.Assertions.assertEquals("FRAUD_UNAVAILABLE", transfer.reasonCode());
        })
        .verifyComplete();

    verify(coreBankingClient, never()).post(any());
  }

  private static Account account(String number) {
    return new Account(number, "Test", "USD", "ACTIVE", new BigDecimal("10000.00"));
  }
}
