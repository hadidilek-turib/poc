package com.turib.hadidilek.poc.ignite.controller;

import com.turib.hadidilek.poc.ignite.service.BenchmarkService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping("/benchmark")
public class BenchmarkController {

  private final BenchmarkService service;

  public BenchmarkController(BenchmarkService service) {
    this.service = service;
  }

  @GetMapping("/prepare")
  public String prepare(
      @RequestParam(name = "start-key") int startKey,
      @RequestParam(name = "item-count") int itemCount) {

    CompletableFuture.runAsync(() -> {
      service.reset();
      service.populate(startKey, itemCount);
    });
    return "Data preparation started.";
  }

  @GetMapping("/benchmark")
  public String benchmark(
      @RequestParam(name = "stepSize", required = false, defaultValue = "10000") int stepSize,
      @RequestParam(name = "stepCount", required = false, defaultValue = "10") int stepCount,
      @RequestParam(name = "iterations", required = false, defaultValue = "3") int iterations) {

    CompletableFuture.runAsync(() -> {
      try {
        service.benchmark(
            stepSize,
            stepCount,
            iterations
        );
      } catch (Exception e) {
        log.error("Benchmark test failed.", e);
      }
    });

    return "Test started.";
  }
}
