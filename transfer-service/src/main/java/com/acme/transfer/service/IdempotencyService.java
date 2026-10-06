package com.acme.transfer.service;

import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.IdempotencyEntity;
import com.acme.transfer.repository.IdempotencyRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class IdempotencyService {

  private final IdempotencyRepository idempotencyRepository;
  private final R2dbcEntityTemplate template;
  private final ObjectMapper objectMapper;

  /** The stored response for this key, if the request was already processed. */
  public Mono<IdempotencyEntity> findExisting(String idempotencyKey) {
    return idempotencyRepository.findByIdempotencyKey(idempotencyKey).next();
  }

  public Mono<IdempotencyEntity> save(String idempotencyKey, TransferRequest request, int status,
                                      TransferResource response) {
    String body;
    try {
      body = objectMapper.writeValueAsString(response);
    } catch (JsonProcessingException e) {
      return Mono.error(e);
    }
    return template.insert(new IdempotencyEntity(idempotencyKey,
        requestHash(request), response.transferId(), status, body, Instant.now()));
  }

  /** Stable SHA-256 fingerprint used to reject reuse of a key with a different request body. */
  public String requestHash(TransferRequest request) {
    try {
      byte[] requestBody = objectMapper.writeValueAsBytes(request);
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(requestBody));
    } catch (JsonProcessingException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Could not fingerprint idempotent request", e);
    }
  }

  public TransferResource readResponse(IdempotencyEntity entity) {
    try {
      return objectMapper.readValue(entity.responseBody(), TransferResource.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Stored response is not readable", e);
    }
  }
}
