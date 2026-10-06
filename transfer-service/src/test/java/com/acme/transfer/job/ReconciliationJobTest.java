package com.acme.transfer.job;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@DisplayName("ReconciliationJobTest Test")
public class ReconciliationJobTest {

    private ReconciliationJob reconciliationJob;

    @Mock
    private TransferRepository transferRepository;

    @Mock
    private CoreBankingClient coreBankingClient;

    @Mock
    private Semaphore coreSessionPermits;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        reconciliationJob = new ReconciliationJob(
                transferRepository,
                coreBankingClient,
                coreSessionPermits,
                Schedulers.immediate());
    }

    @Test
    @DisplayName("Test retryTimedOutTransfers method when there are no transfers to retry")
    public void testRetryTimedOutTransfersWithNoTransfers() {
        when(transferRepository.findByStatusAndReasonCode("PENDING", "CORE_TIMEOUT"))
                .thenReturn(Flux.empty());
        reconciliationJob.retryTimedOutTransfers();
        verify(coreBankingClient, never()).inquire(anyString());
    }

    @Test
    @DisplayName("Test retryTimedOutTransfers method when inquire returns empty result")
    public void testRetryTimedOutTransfersWithEmptyInquireResult() throws InterruptedException {
        var transfer = mock(TransferEntity.class);
        when(transfer.transferId()).thenReturn("transfer1");

        when(transferRepository.findByStatusAndReasonCode("PENDING", "CORE_TIMEOUT"))
                .thenReturn(Flux.just(transfer));
        when(coreBankingClient.inquire("transfer1")).thenReturn(Optional.empty());

        reconciliationJob.retryTimedOutTransfers();

        verify(coreBankingClient).inquire("transfer1");
        verify(transferRepository, never()).updateStatus(anyString(), anyString(), any(), any(), any());
        verify(coreSessionPermits).acquire();
        verify(coreSessionPermits).release();
    }

    @Test
    @DisplayName("Test retryTimedOutTransfers method when there are transfers to retry")
    public void testRetryTimedOutTransfersWithTransfers() throws InterruptedException {
        var transfer1 = mock(TransferEntity.class);
        when(transfer1.transferId()).thenReturn("transfer1");

        var transfer2 = mock(TransferEntity.class);
        when(transfer2.transferId()).thenReturn("transfer2");

        var posted = new PostingResult("ref1", "coreTxnId1", PostingResult.Status.POSTED, null, null);
        var rejected = new PostingResult("ref2", "coreTxnId2", PostingResult.Status.REJECTED, "INSUFFICIENT_FUNDS",
                null);

        when(transferRepository.findByStatusAndReasonCode("PENDING", "CORE_TIMEOUT"))
                .thenReturn(Flux.just(transfer1, transfer2));

        // Stub per reference supaya hasil tidak bergantung pada urutan worker.
        when(coreBankingClient.inquire("transfer1")).thenReturn(Optional.of(posted));
        when(coreBankingClient.inquire("transfer2")).thenReturn(Optional.of(rejected));

        // reconcile(...).block() membutuhkan publisher hasil yang tidak null.
        when(transferRepository.updateStatus(
                anyString(), anyString(), nullable(String.class),
                nullable(String.class), any(Instant.class)))
                .thenReturn(Mono.just(1));

        reconciliationJob.retryTimedOutTransfers();

        // Verifikasi bahwa metode inquire dipanggil untuk setiap transfer.
        verify(coreBankingClient).inquire("transfer1");
        verify(coreBankingClient).inquire("transfer2");

        // Verifikasi bahwa updateStatus dipanggil dengan parameter yang sesuai.
        verify(transferRepository).updateStatus(
                eq("transfer1"), eq("COMPLETED"), isNull(),
                eq("coreTxnId1"), any(Instant.class));
        verify(transferRepository).updateStatus(
                eq("transfer2"), eq("REJECTED"), eq("INSUFFICIENT_FUNDS"),
                isNull(), any(Instant.class));

        // Verifikasi bahwa semaphore digunakan untuk membatasi akses ke core banking inquiry.
        verify(coreSessionPermits, times(2)).acquire();
        verify(coreSessionPermits, times(2)).release();
        verify(coreBankingClient, never()).post(any());
    }
}
