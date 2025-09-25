package com.turib.hadidilek.poc.ignite.controller;

import com.turib.hadidilek.poc.ignite.config.IgniteConfig;
import com.turib.hadidilek.poc.ignite.model.Person;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/cache")
public class CacheController {

  @GetMapping("/all")
  public List<Person> get(@RequestParam(name = "cache") IgniteConfig.Cache cache,
                          @RequestParam(name = "page", required = false, defaultValue = "0") int page,
                          @RequestParam(name = "size", required = false, defaultValue = "200") int size) {
    int offset = page * size;

    Query qry = switch (cache) {
      case PERSON_SCAN ->
          new SqlFieldsQuery("select * from person order by name offset ? limit ?").setArgs(offset, size);

      case PERSON_SQL -> new ScanQuery();
    };

    QueryCursor<List<?>> cursor = cache.getCache().query(qry);
    return cursor
        .getAll()
        .stream()
        .map(fields -> Person
            .builder()
            .id((Integer) fields.get(0))
            .name((String) fields.get(1))
            .build())
        .toList();
  }

  @GetMapping(value = "/{id}")
  public Person get(@RequestParam IgniteConfig.Cache cache, @PathVariable int id) {
    return cache.getCache().get(id);
  }

  @DeleteMapping(value = "/clear")
  public void clear(@RequestParam IgniteConfig.Cache cache) {
    cache.getCache().clear();
  }

  @DeleteMapping(value = "/destroy")
  public void destroy(@RequestParam IgniteConfig.Cache cache) {
    cache.getCache().destroy();
  }
}
