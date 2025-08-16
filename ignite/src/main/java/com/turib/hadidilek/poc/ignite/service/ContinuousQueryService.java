package com.turib.hadidilek.poc.ignite.service;

import com.turib.hadidilek.poc.ignite.config.IgniteConfig;
import com.turib.hadidilek.poc.ignite.model.Person;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ContinuousQueryService {

  private final IgniteCache<Integer, Person> cache;

  public ContinuousQueryService(Ignite ignite) {
    this.cache = ignite.cache(IgniteConfig.PERSON_CACHE);
  }

  @PostConstruct
  public void postConstruct() {
    populateInitialData();
    initContinuousQuery();
    cache.put(4, new Person(104, "Bruce"));
    cache.put(6, new Person(106, "Lee"));
  }

  private void populateInitialData() {
    cache.put(1, new Person(101, "Charlie"));
    cache.put(2, new Person(102, "Alice"));
    cache.put(3, new Person(103, "Bob"));
    cache.put(5, new Person(105, "Mark"));
  }

  private void initContinuousQuery() {
    ContinuousQuery<Integer, Person> continuousQuery = buildContinuousQuery();
    Query initialQuery = new SqlFieldsQuery("select * from person order by name offset 0 limit 2");
    continuousQuery.setInitialQuery(initialQuery);

    QueryCursor<?> cursor = cache.query(continuousQuery);
    log.info("Continuous Query started. Initial results:");

    List<Person> items = cursor
        .getAll()
        .stream()
        .map(List.class::cast)
        .map(fields -> Person
            .builder()
            .id((Integer) fields.get(0))
            .name((String) fields.get(1))
            .build())
        .toList();

    items.forEach(item -> log.info("item: {}", item));
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

