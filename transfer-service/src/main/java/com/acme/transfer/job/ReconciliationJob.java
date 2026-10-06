package com.acme.transfer.job;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Scheduler;

/**
 * Retries transfers that failed because core banking did not answer in time.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReconciliationJob {

  private final TransferRepository transferRepository;
  private final CoreBankingClient coreBankingClient;
  private final Semaphore coreSessionPermits;
  private final Scheduler coreSdkScheduler;

  @Scheduled(fixedRate = 30_000, initialDelay = 30_000)
  public void retryTimedOutTransfers() {
    List<TransferEntity> transfers = transferRepository
        .findByStatusAndReasonCode("PENDING", "CORE_TIMEOUT")
        .collectList()
        .block();
    log.info("Reconciliation: {} transfers to retry", transfers.size());
    for (TransferEntity transfer : transfers) {
      coreSdkScheduler.schedule(() -> {
        try {
          inquireWithPermit(transfer);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Reconciliation for {} interrupted", transfer.transferId());
        }
      });
    }
  }

  private void inquireWithPermit(TransferEntity transfer) throws InterruptedException {
    coreSessionPermits.acquire();
    try {
      this.reconcile(transfer);
    } finally {
      coreSessionPermits.release();
    }
  }

  private void reconcile(TransferEntity transfer) {
    try {
      Optional<PostingResult> found = coreBankingClient.inquire(transfer.transferId());

      if (found.isEmpty()) {
        // Core belum menemukan posting. Biarkan PENDING, lalu cek lagi sesuai kebijakan reconciliation.
        return;
      }

      PostingResult result = found.get();

      if (result.status() == PostingResult.Status.POSTED) {
        transferRepository.updateStatus(
            transfer.transferId(),
            "COMPLETED",
            null,
            result.coreTxnId(),
            Instant.now()).block();
      } else {
        transferRepository.updateStatus(
            transfer.transferId(),
            "REJECTED",
            result.reasonCode(),
            null,
            Instant.now()).block();
      }
    } catch (Exception e) {
      log.warn("Reconciliation untuk {} gagal: {}",
          transfer.transferId(), e.getMessage());
    }
  }
}
