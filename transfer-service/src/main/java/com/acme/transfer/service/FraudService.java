package com.acme.transfer.service;

import com.acme.transfer.client.FraudClient;
import com.acme.transfer.client.FraudDecision;
import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.dto.TransferRequest;
import java.math.BigDecimal;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class FraudService {

  private final FraudClient fraudClient;
  private final AcmeProperties properties;

  /** ALLOW, DENY or REVIEW. Transfers below the threshold are not checked. */
  public Mono<String> check(String transferId, TransferRequest request, BigDecimal amount) {
    if (amount.compareTo(threshold(request.currency())) < 0) {
      return Mono.just("ALLOW");
    }
    return fraudClient.assess(transferId, amount.toPlainString(), request.currency(),
            request.sourceAccount(), request.destinationAccount())
        .map(FraudDecision::decision);
  }

  private BigDecimal threshold(String currency) {
    return switch (currency) {
      case "IDR" -> properties.fraudThreshold().idr();
      case "SGD" -> properties.fraudThreshold().sgd();
      default -> properties.fraudThreshold().usd();
    };
  }
}
