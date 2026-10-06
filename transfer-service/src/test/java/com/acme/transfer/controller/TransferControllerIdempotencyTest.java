package com.acme.transfer.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import com.acme.transfer.dto.Money;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.IdempotencyEntity;
import com.acme.transfer.service.IdempotencyService;
import com.acme.transfer.service.TransferService;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class TransferControllerIdempotencyTest {

  private static final String KEY = "idempotency-key-001";

  @Mock
  private TransferService transferService;

  @Mock
  private IdempotencyService idempotencyService;

  private TransferController controller;

  @BeforeEach
  void setUp() {
    controller = new TransferController(transferService, idempotencyService);
  }

  @Test
  void replaysStoredResponseForSameKeyAndSameRequest() {
    TransferRequest request = request("invoice-1");
    TransferResource storedResponse = response("transfer-1");
    IdempotencyEntity stored = stored(KEY, request, storedResponse);
    when(idempotencyService.findExisting(KEY)).thenReturn(Mono.just(stored));
    when(idempotencyService.requestHash(request)).thenReturn(stored.requestHash());
    when(idempotencyService.readResponse(stored)).thenReturn(storedResponse);

    StepVerifier.create(controller.createTransfer(KEY, request))
        .assertNext(actual -> {
          assertEquals(HttpStatus.CREATED, actual.getStatusCode());
          assertSame(storedResponse, actual.getBody());
        })
        .verifyComplete();

    verify(transferService, never()).createTransfer(request);
    verify(idempotencyService, never()).save(org.mockito.ArgumentMatchers.anyString(),
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt(),
        org.mockito.ArgumentMatchers.any());
  }

  @Test
  void rejectsReusingKeyWithDifferentRequestBody() {
    TransferRequest originalRequest = request("invoice-1");
    TransferRequest changedRequest = request("invoice-2");
    IdempotencyEntity stored = stored(KEY, originalRequest, response("transfer-1"));
    when(idempotencyService.findExisting(KEY)).thenReturn(Mono.just(stored));
    when(idempotencyService.requestHash(changedRequest)).thenReturn("different-request-hash");

    StepVerifier.create(controller.createTransfer(KEY, changedRequest))
        .expectErrorMatches(error -> error instanceof ResponseStatusException statusException
            && statusException.getStatusCode() == HttpStatus.UNPROCESSABLE_ENTITY)
        .verify();

    verify(transferService, never()).createTransfer(changedRequest);
  }

  private static TransferRequest request(String description) {
    return new TransferRequest("2000000001", "2000000002", "1.00", "USD", description);
  }

  private static TransferResource response(String transferId) {
    Instant now = Instant.parse("2026-10-06T00:00:00Z");
    return new TransferResource(transferId, "COMPLETED", null, "2000000001", "2000000002",
        new Money("1.00", "USD"), new Money("1.00", "USD"), null, "CT1", now, now);
  }

  private static IdempotencyEntity stored(String key, TransferRequest request,
      TransferResource response) {
    return new IdempotencyEntity(key, Integer.toHexString(request.hashCode()), response.transferId(),
        201, "{}", Instant.parse("2026-10-06T00:00:00Z"));
  }
}
