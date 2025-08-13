package com.turib.hadidilek.poc.ignite.config;

import com.turib.hadidilek.poc.ignite.model.Person;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IgniteConfig {

  public static final String PERSON_CACHE = "personCache";

  @Bean(destroyMethod = "close")
  public Ignite igniteInstance() {
    IgniteConfiguration cfg = new IgniteConfiguration();
//    cfg.setIgniteInstanceName("spring-ignite-node");
//    cfg.setPeerClassLoadingEnabled(true);
//    cfg.setMarshaller(new JdkMarshaller());

    CacheConfiguration<Integer, Person> cacheCfg = new CacheConfiguration<>(PERSON_CACHE);
    cacheCfg.setIndexedTypes(Integer.class, Person.class);
    cfg.setCacheConfiguration(cacheCfg);
    return Ignition.start(cfg);
  }
}
