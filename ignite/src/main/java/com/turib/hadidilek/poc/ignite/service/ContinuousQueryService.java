package com.turib.hadidilek.poc.ignite.service;

import com.turib.hadidilek.poc.ignite.config.IgniteConfig;
import com.turib.hadidilek.poc.ignite.model.Person;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ContinuousQueryService {

  private final IgniteCache<Integer, Person> cache;

  public ContinuousQueryService(Ignite ignite) {
    this.cache = ignite.cache(IgniteConfig.PERSON_CACHE);
  }

  @PostConstruct
  public void postConstruct() {
    initContinuousQuery();
    populateInitialData();
  }

  private void populateInitialData() {
    cache.put(1, new Person(101, "Charlie"));
    cache.put(2, new Person(102, "Alice"));
    cache.put(3, new Person(103, "Bob"));
  }

  private void initContinuousQuery() {
    ContinuousQuery<Integer, Person> cq = new ContinuousQuery<>();
    cq.setLocalListener(events -> {
      for (var e : events) {
        log.info("Continuous Query Event received: key:{} old:{} new:{}", e.getKey(), e.getOldValue(), e.getValue());
      }
    });

    cq.setRemoteFilterFactory(() -> event -> true);

    cache.query(cq);
    log.info("Continuous Query started...");
  }
}

