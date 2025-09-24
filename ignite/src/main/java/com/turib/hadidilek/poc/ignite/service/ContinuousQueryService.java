package com.turib.hadidilek.poc.ignite.service;

import com.turib.hadidilek.poc.ignite.model.Person;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.turib.hadidilek.poc.ignite.config.IgniteConfig.Caches.PERSON_SQL;

@Slf4j
@Service
public class ContinuousQueryService {

  @PostConstruct
  public void postConstruct() {
//    populateInitialData();

//    ContinuousQuery<Integer, Person> continuousQuery = buildContinuousQuery();
//    cache.query(continuousQuery);
//    log.info("Continuous Query started.");

//    executeInitialQuery();

//    cache.put(4, new Person(104, "Bruce"));
//    cache.put(6, new Person(106, "Lee"));
  }

  private void populateInitialData() {
    PERSON_SQL.getCache().put(1, new Person(101, "Charlie"));
    PERSON_SQL.getCache().put(2, new Person(102, "Alice"));
    PERSON_SQL.getCache().put(3, new Person(103, "Bob"));
    PERSON_SQL.getCache().put(5, new Person(105, "Mark"));
  }

  private void executeInitialQuery() {
    SqlFieldsQuery countQuery = new SqlFieldsQuery("select count(*) from person");
    try (FieldsQueryCursor<List<?>> cursor = PERSON_SQL.getCache().query(countQuery)) {
      long count = (Long) cursor.iterator().next().get(0);
      log.info("Total count: {}", count);
    }

    SqlFieldsQuery initialQuery = new SqlFieldsQuery("select _key, _val from person order by name offset 0 limit 2");
    try (FieldsQueryCursor<List<?>> cursor = PERSON_SQL.getCache().query(initialQuery)) {
      List<? extends Map.Entry<?, ?>> items = cursor
          .getAll()
          .stream()
          .map(entry -> Map.entry(entry.get(0), entry.get(1)))
          .toList();

      log.info("Initial results:");
      items.forEach(item -> log.info("item: {}", item));
    }
  }

  private ContinuousQuery<Integer, Person> buildContinuousQuery() {
    ContinuousQuery<Integer, Person> continuousQuery = new ContinuousQuery<>();
    IgniteBiPredicate<Integer, Person> predicate = (key, person) -> person.getId() > 101 && person.getId() < 105;
    continuousQuery.setRemoteFilterFactory(() -> event -> predicate.apply(event.getKey(), event.getValue()));
    continuousQuery.setLocalListener(events -> {
      for (var e : events) {
        log.info("Continuous Query Event received: key:{} old:{} new:{}", e.getKey(), e.getOldValue(), e.getValue());
      }
    });

    return continuousQuery;
  }
}

