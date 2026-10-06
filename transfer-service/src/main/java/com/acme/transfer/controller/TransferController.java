package com.acme.transfer.controller;

import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.service.IdempotencyService;
import com.acme.transfer.service.TransferService;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/api/v1/transfers")
@RequiredArgsConstructor
public class TransferController {

  private static final Set<String> CURRENCIES = Set.of("IDR", "USD", "SGD");

  private final TransferService transferService;
  private final IdempotencyService idempotencyService;

  @PostMapping
  public Mono<ResponseEntity<Object>> createTransfer(
      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
      @RequestBody TransferRequest request) {
    log.info("Transfer request {}: {}", idempotencyKey, request);
    String error = validate(request);
    if (error != null) {
      return Mono.just(ResponseEntity.badRequest().body(Map.of("error", error)));
    }
    if (idempotencyKey == null) {
      return transferService.createTransfer(request).map(this::toResponse);
    }
    return idempotencyService.findExisting(idempotencyKey)
        .flatMap(existing -> {
          if (!existing.requestHash().equals(idempotencyService.requestHash(request))) {
            return Mono.error(new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                "IDEMPOTENCY_KEY_REUSED"));
          }
          return Mono.just(ResponseEntity.status(existing.responseStatus())
              .body((Object) idempotencyService.readResponse(existing)));
        })
        .switchIfEmpty(Mono.defer(() -> transferService.createTransfer(request)
            .map(this::toResponse)
            .flatMap(response -> idempotencyService.save(idempotencyKey, request,
                    response.getStatusCode().value(), (TransferResource) response.getBody())
                .thenReturn(response))))
        .onErrorMap(e -> !(e instanceof ResponseStatusException),
            e -> new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage(), e))
        .doOnError(e -> ResponseEntity.internalServerError().body(Map.of("error", e.getMessage())));
  }

  @GetMapping("/{transferId}")
  public Mono<TransferResource> getTransfer(@PathVariable String transferId) {
    return transferService.getTransfer(transferId)
        .map(TransferResource::from)
        .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,
            "Transfer " + transferId + " not found")));
  }

  private ResponseEntity<Object> toResponse(TransferEntity transfer) {
    HttpStatus status = switch (transfer.status()) {
      case "COMPLETED" -> HttpStatus.CREATED;
      case "PENDING_REVIEW" -> HttpStatus.ACCEPTED;
      case "REJECTED" -> HttpStatus.UNPROCESSABLE_ENTITY;
      default -> HttpStatus.SERVICE_UNAVAILABLE;
    };
    return ResponseEntity.status(status).body(TransferResource.from(transfer));
  }

  private static String validate(TransferRequest request) {
    if (request.sourceAccount() == null || !request.sourceAccount().matches("\\d{10}")) {
      return "Invalid source account";
    }
    if (request.destinationAccount() == null || !request.destinationAccount().matches("\\d{10}")) {
      return "Invalid destination account";
    }
    if (request.currency() == null || !CURRENCIES.contains(request.currency())) {
      return "Invalid currency";
    }
    try {
      if (request.amount() == null || new BigDecimal(request.amount()).signum() <= 0) {
        return "Invalid amount";
      }
    } catch (NumberFormatException e) {
      return "Invalid amount";
    }
    return null;
  }
}
