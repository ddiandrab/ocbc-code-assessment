package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.core.sdk.CoreTimeoutException;
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

class TransferServiceTest {

    @Test
    void createTransfer() throws Exception {
        AccountClient accountClient = mock(AccountClient.class);
        FxService fxService = mock(FxService.class);
        FraudService fraudService = mock(FraudService.class);
        CoreBankingClient coreBankingClient = mock(CoreBankingClient.class);
        R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
        AuditService auditService = mock(AuditService.class);
        TransferService transferService = new TransferService(accountClient, fxService, fraudService,
                coreBankingClient, mock(TransferRepository.class), template, auditService, Schedulers.immediate(), new Semaphore(10));

        when(accountClient.getAccount("2000000001")).thenReturn(Mono.just(
                new Account("2000000001", "Test", "USD", "ACTIVE", new BigDecimal("100000.00"))));
        when(accountClient.getAccount("2000000002")).thenReturn(Mono.just(
                new Account("2000000002", "Test", "USD", "ACTIVE", new BigDecimal("100000.00"))));
        when(fxService.convert(any(), anyString(), anyString()))
                .thenReturn(Mono.just(new Conversion(new BigDecimal("150.00"), "USD", null)));
        when(fraudService.check(anyString(), any(), any())).thenReturn(Mono.just("ALLOW"));
        when(coreBankingClient.post(any())).thenReturn(new PostingResult("x", "CT1",
                PostingResult.Status.POSTED, null, Instant.now()));
        when(template.insert(any(TransferEntity.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(auditService.recordTransfer(any())).thenReturn(Mono.just(1));

        TransferEntity transfer = transferService.createTransfer(
                new TransferRequest("2000000001", "2000000002", "150.00", "USD", "test")).block();

        // wait for the audit event to be written
        Thread.sleep(500);

        assertEquals("COMPLETED", transfer.status());
        verify(auditService).recordTransfer(any());
    }

    @Test
    void createTransferTimeout() throws Exception {
        AccountClient accountClient = mock(AccountClient.class);
        FxService fxService = mock(FxService.class);
        FraudService fraudService = mock(FraudService.class);
        CoreBankingClient coreBankingClient = mock(CoreBankingClient.class);
        R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
        AuditService auditService = mock(AuditService.class);
        TransferService transferService = new TransferService(accountClient, fxService, fraudService,
                coreBankingClient, mock(TransferRepository.class), template, auditService, Schedulers.immediate(), new Semaphore(10));

        when(accountClient.getAccount("2000000001")).thenReturn(Mono.just(
                new Account("2000000001", "Test", "USD", "ACTIVE", new BigDecimal("100000.00"))));
        when(accountClient.getAccount("2000000002")).thenReturn(Mono.just(
                new Account("2000000002", "Test", "USD", "ACTIVE", new BigDecimal("100000.00"))));
        when(fxService.convert(any(), anyString(), anyString()))
                .thenReturn(Mono.just(new Conversion(new BigDecimal("150.00"), "USD", null)));
        when(fraudService.check(anyString(), any(), any())).thenReturn(Mono.just("ALLOW"));
        when(coreBankingClient.post(any())).thenThrow(new CoreTimeoutException("Core banking timeout"));
        when(template.insert(any(TransferEntity.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(auditService.recordTransfer(any())).thenReturn(Mono.just(1));

        StepVerifier.create(transferService.createTransfer(
                new TransferRequest("2000000001", "2000000002", "150.00", "USD", "test")))
                .expectNextMatches(
                        transfer -> transfer.status().equals("PENDING") && transfer.reasonCode().equals("CORE_TIMEOUT"))
                .verifyComplete();
        verify(auditService).recordTransfer(any());
        verify(coreBankingClient, org.mockito.Mockito.times(1)).post(any());
    }
}
