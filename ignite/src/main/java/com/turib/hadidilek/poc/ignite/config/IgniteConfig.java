package com.turib.hadidilek.poc.ignite.config;

import com.turib.hadidilek.poc.ignite.model.Person;
import lombok.Getter;
import lombok.Setter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class IgniteConfig {

  public enum Caches {
    PERSON_SCAN(null),
    PERSON_SQL(null);

    @Getter
    @Setter
    private IgniteCache<Integer, Person> cache;

    private Caches(IgniteCache<Integer, Person> cache) {
      this.cache = cache;
    }
  }

  //  @Bean(name = "igniteServer", destroyMethod = "close")
  public Ignite igniteServer() {
    IgniteConfiguration cfg = new IgniteConfiguration();
    cfg.setIgniteInstanceName("ignite-server");
    cfg.setClientMode(false);
    cfg.setPeerClassLoadingEnabled(true);
    return Ignition.start(cfg);
  }

  @Bean(name = "igniteClient", destroyMethod = "close")
//  @DependsOn("igniteServer")
  public Ignite igniteThickClient() {
    TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
    TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
    ipFinder.setAddresses(List.of("127.0.0.1:47500"));
    discoverySpi.setIpFinder(ipFinder);

    IgniteConfiguration cfg = new IgniteConfiguration();
    cfg.setIgniteInstanceName("ignite-client");
    cfg.setClientMode(true);
    cfg.setPeerClassLoadingEnabled(true);
    cfg.setDiscoverySpi(discoverySpi);

    Ignite client = Ignition.start(cfg);
    System.out.println("Thick client started: " + client.name());

    Caches.PERSON_SCAN.setCache(client.getOrCreateCache(Caches.PERSON_SCAN.name()));

    CacheConfiguration<Integer, Person> cacheCfg = new CacheConfiguration<>(Caches.PERSON_SQL.name());
    cacheCfg.setIndexedTypes(Integer.class, Person.class);
    Caches.PERSON_SQL.setCache(client.getOrCreateCache(cacheCfg));

    return client;
  }

  //  @Bean(destroyMethod = "close")
//  @DependsOn("igniteServer")
  public IgniteClient igniteThinClient() {
    ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
    IgniteClient igniteClient = Ignition.startClient(clientCfg);

//    ClientCacheConfiguration clientCacheConfiguration = new ClientCacheConfiguration();
//    clientCacheConfiguration.setName(PERSON_CACHE);
//    clientCacheConfiguration.setSqlSchema(PERSON_CACHE);

//    ClientCache<Integer, Person> cache = igniteClient.getOrCreateCache(PERSON_CACHE);
    igniteClient.query(new SqlFieldsQuery(
        "CREATE TABLE IF NOT EXISTS Person (" +
            " id LONG PRIMARY KEY, " +
            " name VARCHAR " +
            ") WITH \"CACHE_NAME=? \""
    ).setArgs(Caches.PERSON_SQL.name())).getAll();

    igniteClient.query(new SqlFieldsQuery(
        "CREATE INDEX IF NOT EXISTS idx_id ON Person(id)"
    )).getAll();

    igniteClient.query(new SqlFieldsQuery(
        "CREATE INDEX IF NOT EXISTS idx_person_name ON Person(name)"
    )).getAll();

    return igniteClient;
  }
}
