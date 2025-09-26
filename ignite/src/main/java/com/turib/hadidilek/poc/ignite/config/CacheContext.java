package com.turib.hadidilek.poc.ignite.config;

import com.turib.hadidilek.poc.ignite.model.Person;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.stereotype.Component;

import static com.turib.hadidilek.poc.ignite.config.CacheContext.Cache.PERSON_SCAN;
import static com.turib.hadidilek.poc.ignite.config.CacheContext.Cache.PERSON_SQL;

@Component
public class CacheContext {

  public enum Cache {
    PERSON_SCAN(null),
    PERSON_SQL(null);

    @Getter
    @Setter
    private IgniteCache cache;

    Cache(IgniteCache cache) {
      this.cache = cache;
    }
  }

  @Getter
  private Ignite client;

  public CacheContext(Ignite client) {
    this.client = client;
  }

  @PostConstruct
  public void postConstruct() {
    resetAll();
  }

  public IgniteCache<Integer, BinaryObject> getCachePersonScan() {
    return PERSON_SCAN.getCache();
  }

  public IgniteCache<Integer, Person> getCachePersonSql() {
    return PERSON_SQL.getCache();
  }

  public void resetAll() {
    for (Cache cache : Cache.values()) {
      reset(cache);
    }
  }

  public void reset(Cache cacheWrapper) {
    String cacheName = cacheWrapper.name();
    IgniteCache cache = client.cache(cacheName);
    if (cache != null) {
      cache.destroy();
    }

    switch (cacheWrapper) {
      case PERSON_SCAN: {
        CacheConfiguration<Integer, BinaryObject> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setStoreKeepBinary(true);
        PERSON_SCAN.setCache(client.getOrCreateCache(cacheCfg));
        break;
      }

      case PERSON_SQL: {
        CacheConfiguration<Integer, Person> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setIndexedTypes(Integer.class, Person.class);
        cacheCfg.setStoreKeepBinary(true);
        PERSON_SQL.setCache(client.getOrCreateCache(cacheCfg));
        break;
      }
    }
  }
}
