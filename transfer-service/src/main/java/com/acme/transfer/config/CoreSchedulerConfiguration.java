package com.acme.transfer.config;

import java.util.concurrent.Executors;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Configuration 
class CoreSchedulerConfiguration {

  @Bean(destroyMethod = "dispose")
  Scheduler coreSdkScheduler() {
    return Schedulers.fromExecutorService(
        Executors.newVirtualThreadPerTaskExecutor());
  }
}
