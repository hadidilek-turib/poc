package com.turib.hadidilek.poc.ignite.service;

import com.turib.hadidilek.poc.ignite.model.Person;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import org.springframework.util.StopWatch.TaskInfo;

import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.stream.Collectors;

import static com.turib.hadidilek.poc.ignite.config.IgniteConfig.Caches.PERSON_SCAN;
import static com.turib.hadidilek.poc.ignite.config.IgniteConfig.Caches.PERSON_SQL;

@Service
public class BenchmarkService {

  public enum BenchmarkType {
    SCAN_QUERY,
    SQL_QUERY
  }

  public void destroy() {
    PERSON_SCAN.getCache().destroy();
    PERSON_SQL.getCache().destroy();
  }

  public void populate(int itemCount) {
    for (int i = 0; i < itemCount; i++) {
      Person person = new Person(i, "name-" + i);
      PERSON_SCAN.getCache().put(itemCount, person);
      PERSON_SQL.getCache().put(itemCount, person);
    }
  }

  public void benchmark(
      int stepSize,
      int stepCount,
      int iterations) throws Exception {

    BenchmarkCollector benchmarkCollector = new BenchmarkCollector();
    destroy();
    int totalCount = 0;
    for (int step = 0; step < stepCount; step++) {
      if (stepSize != 0) {
        populate(stepSize);
      }

      totalCount += stepSize;

      System.out.printf("Step %s - Total count: %s\n", step, totalCount);

      for (int i = 0; i < iterations; i++) {
        System.out.printf("Iteration %s \n", i);

        ScanQuery<Long, BinaryObject> scanQuery = new ScanQuery();
        scanQuery.setPageSize(1024);
        scanQuery.setLocal(false);
        String taskName = String.format("Scan Query - step %s - total count: %s", step, totalCount);
        benchmarkCollector.timed(
            taskName,
            () -> {
              try (var cursor = PERSON_SCAN.getCache().query(scanQuery)) {
                cursor.spliterator().forEachRemaining(e -> {
                });
              }
            });

        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery("SELECT _key, _val FROM Person ORDER BY name DESC");
        sqlFieldsQuery.setPageSize(1024);
        sqlFieldsQuery.setLocal(false);
        taskName = String.format("Scan Query - step %s - total count: %s", step, totalCount);
        benchmarkCollector.timed(
            taskName,
            () -> {
              try (var cursor = PERSON_SQL.getCache().query(sqlFieldsQuery)) {
                cursor.spliterator().forEachRemaining(e -> {
                });
              }
            });
      }
    }

    StopWatch stopWatch = benchmarkCollector.getStopWatch();
    System.out.println(stopWatch.prettyPrint());

    Map<String, List<TaskInfo>> stats = Arrays
        .stream(stopWatch.getTaskInfo())
        .collect(Collectors.groupingBy(e -> e.getTaskName()));

    for (var entry : stats.entrySet()) {
      System.out.println(entry.getKey());
      printSummary(entry.getValue());
    }
  }

  private static void printSummary(List<TaskInfo> taskInfos) {
    LongSummaryStatistics statsForScanQuery = taskInfos.stream().map(TaskInfo::getTimeMillis).mapToLong(Long::longValue).summaryStatistics();
    System.out.println("Sum: " + statsForScanQuery.getSum());
    System.out.println("Avg: " + statsForScanQuery.getAverage());
    System.out.println("Min: " + statsForScanQuery.getMin());
    System.out.println("Max: " + statsForScanQuery.getMax());
  }
}
