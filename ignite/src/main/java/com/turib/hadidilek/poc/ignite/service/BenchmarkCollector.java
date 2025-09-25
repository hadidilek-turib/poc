package com.turib.hadidilek.poc.ignite.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import java.util.concurrent.Callable;

@Slf4j
public class BenchmarkCollector {

  @Getter
  StopWatch stopWatch = new StopWatch();

  public void timed(String taskName, Runnable runnable) {
    stopWatch.start(taskName);
    try {
      log.info("{} -> started... ", taskName);
      runnable.run();
    } finally {
      stopWatch.stop();
      log.info("ended in {} ms.\n", stopWatch.getLastTaskTimeMillis());
    }
  }

  public <T> T timed(String taskName, Callable<T> callable) throws Exception {
    stopWatch.start(taskName);
    try {
      log.info("{} -> started... ", taskName);
      return callable.call();
    } finally {
      stopWatch.stop();
      log.info("ended in {} ms.\n", stopWatch.getLastTaskTimeMillis());
    }
  }
}
