package com.turib.hadidilek.poc.ignite.service;

import com.turib.hadidilek.poc.ignite.config.IgniteConfig;
import com.turib.hadidilek.poc.ignite.model.Person;
import org.apache.ignite.Ignite;
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
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.turib.hadidilek.poc.ignite.config.IgniteConfig.Cache.PERSON_SCAN;
import static com.turib.hadidilek.poc.ignite.config.IgniteConfig.Cache.PERSON_SQL;

@Service
public class BenchmarkService {

  private Ignite client;

  public BenchmarkService(Ignite client) {
    this.client = client;
  }

  public void reset() {
    IgniteConfig.Cache.resetAll(client);
  }

  public void populate(int startKey, int itemCount) {
    StopWatch stopWatch = new StopWatch();
    stopWatch.start("populate");
    int totalCount = startKey + itemCount;
    for (int id = startKey; id < totalCount; id++) {
      Person person = Person.builder()
          .id(id)
          .name("name-" + id)
          .surname("surname-" + id)
          .build();
      PERSON_SCAN.getCache().put(id, person);
      PERSON_SQL.getCache().put(id, person);
    }
    stopWatch.stop();
    System.out.println(stopWatch.prettyPrint());
  }

  public void benchmark(
      int stepSize,
      int stepCount,
      int iterations) {

    reset();

    BenchmarkCollector benchmarkCollector = new BenchmarkCollector();
    int totalCount = 0;
    for (int step = 0; step < stepCount; step++) {
      if (stepSize != 0) {
        populate(step * stepSize, stepSize);
      }

      totalCount += stepSize;

      String stepInfo = String.format("step %02d - total: %d", step, totalCount);
      System.out.println(stepInfo);

      for (int i = 0; i < iterations; i++) {
        System.out.printf("Iteration %d \n", i);

        ScanQuery<Long, BinaryObject> scanQuery = new ScanQuery();
        scanQuery.setPageSize(1024);
        scanQuery.setLocal(false);
        String taskName = String.format("%s - scan", stepInfo);
        benchmarkCollector.timed(
            taskName,
            () -> {
              try (var cursor = PERSON_SCAN.getCache().query(scanQuery)) {
                cursor.spliterator().forEachRemaining(e -> {
                });
              }
            });

        String queryStr = String.format("SELECT _key, _val FROM Person ORDER BY name DESC, surname DESC");
        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(queryStr);
        sqlFieldsQuery.setPageSize(1024);
        sqlFieldsQuery.setLocal(false);
        taskName = String.format("%s - sql", stepInfo);
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

    Map<String, List<TaskInfo>> statsByTasks = new TreeMap<>(Arrays
        .stream(stopWatch.getTaskInfo())
        .collect(Collectors.groupingBy(TaskInfo::getTaskName)));

    printCsv(statsByTasks);
  }

  private static void printCsv(Map<String, List<TaskInfo>> stats) {
    System.out.println("Count(x1000)\tScan(ms)\tSQL(ms)\n");
    for (var entry : stats.entrySet()) {
      if (entry.getKey().endsWith("scan")) {
        System.out.printf("%s\t%.2f\t",
            entry.getKey().replaceAll(".*total:\\s*(\\d+).*", "$1"),
            summary(entry.getValue()).getAverage());
      } else {
        System.out.printf("%.2f\n", summary(entry.getValue()).getAverage());
      }
    }
  }

  private static LongSummaryStatistics summary(List<TaskInfo> taskInfos) {
    return taskInfos.stream().map(TaskInfo::getTimeMillis).mapToLong(Long::longValue).summaryStatistics();
  }
}
