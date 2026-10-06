package com.acme.transfer.config;

import java.util.concurrent.Semaphore;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration 
public class CoreConcurrencyConfiguration {
  @Bean 
  Semaphore coreSessionPermits() {
    return new Semaphore(10, true);
  }
}
