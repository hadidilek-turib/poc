package com.turib.hadidilek.poc.ignite.service;

import com.turib.hadidilek.poc.ignite.config.CacheContext;
import com.turib.hadidilek.poc.ignite.model.Person;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import org.springframework.util.StopWatch.TaskInfo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.turib.hadidilek.poc.ignite.config.CacheContext.Cache.PERSON_SCAN;
import static com.turib.hadidilek.poc.ignite.config.CacheContext.Cache.PERSON_SQL;

@Service
public class BenchmarkService {

  private CacheContext cacheContext;

  public BenchmarkService(CacheContext cacheContext) {
    this.cacheContext = cacheContext;
  }

  public void reset() {
    cacheContext.resetAll();
  }

  public void populate(int startKey, int itemCount) {
    StopWatch stopWatch = new StopWatch();
    stopWatch.start("populate");
    int totalCount = startKey + itemCount;
    for (int id = startKey; id < totalCount; id++) {
      Person person = Person.builder()
          .field1(id)
          .field2("field2-" + id)
          .field3("field3-" + id)
          .field4(id * 1000L)
          .field5(BigDecimal.valueOf(id * 1.5))
          .field6(LocalDate.now().plusDays(id))
          .field7("field7-" + id)
          .field8("field8-" + id)
          .field9("field9-" + id)
          .field10("field10-" + id)
          .build();

      BinaryObject binaryObject = cacheContext.getClient().binary().builder("Person")
          .setField("field1", person.getField1())
          .setField("field2", person.getField2())
          .setField("field3", person.getField3())
          .setField("field4", person.getField4())
          .setField("field5", person.getField5())
          .setField("field6", person.getField6())
          .setField("field7", person.getField7())
          .setField("field8", person.getField8())
          .setField("field9", person.getField9())
          .setField("field10", person.getField10())
          .build();

      PERSON_SCAN.getCache().put(id, binaryObject);
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

      IgniteBiPredicate<Integer, BinaryObject> predicate = (key, person) -> true;
      String sql = "SELECT _key, _val FROM person " +
          "ORDER BY " +
          "field3 DESC, " +
          "field4 DESC, " +
          "field5 DESC" +
          "";

      for (int i = 0; i < iterations; i++) {
        System.out.printf("Iteration %d \n", i);
        ScanQuery<Integer, BinaryObject> scanQuery = new ScanQuery();
        scanQuery.setFilter(predicate);
        scanQuery.setPageSize(1024);
        scanQuery.setLocal(false);
        String taskName = String.format("%s - scan", stepInfo);
        benchmarkCollector.timed(
            taskName,
            () -> {
              try (var cursor = PERSON_SCAN.getCache().withKeepBinary().query(scanQuery)) {
                cursor.spliterator().forEachRemaining(e -> {
                });
              }
            });

        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
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
