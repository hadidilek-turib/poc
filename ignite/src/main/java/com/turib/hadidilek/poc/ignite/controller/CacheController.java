package com.turib.hadidilek.poc.ignite.controller;

import com.turib.hadidilek.poc.ignite.config.CacheContext;
import com.turib.hadidilek.poc.ignite.model.Person;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
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

  private final CacheContext cacheContext;

  public CacheController(CacheContext cacheContext) {
    this.cacheContext = cacheContext;
  }

  @GetMapping("/person/all")
  public List<Person> get(@RequestParam(name = "cache") CacheContext.Cache cache,
                          @RequestParam(name = "page", required = false, defaultValue = "0") int page,
                          @RequestParam(name = "size", required = false, defaultValue = "200") int size) {
    int offset = page * size;

    Query qry = switch (cache) {
      case PERSON_SCAN ->
          new SqlFieldsQuery("select * from person order by field2 offset ? limit ?").setArgs(offset, size);

      case PERSON_SQL -> new ScanQuery();
    };

    QueryCursor<List<?>> cursor = cache.getCache().query(qry);
    return cursor
        .getAll()
        .stream()
        .map(fields -> Person
            .builder()
            .field1((Integer) fields.get(0))
            .field2((String) fields.get(1))
            .build())
        .toList();
  }

  @GetMapping(value = "/scan/person/{id}")
  public Person getScan(@PathVariable int id) {
    BinaryObject object = cacheContext.getCachePersonScan().get(id);
    return object.deserialize();
  }

  @GetMapping(value = "/sql/person/{id}")
  public Person getSql(@PathVariable int id) {
    SqlFieldsQuery query = new SqlFieldsQuery("SELECT _key, _val FROM person WHERE field1 = ?").setArgs(id);
    FieldsQueryCursor<List<?>> cursor = cacheContext.getCachePersonSql().query(query);
    List<Object> list = (List<Object>) cursor.iterator().next();
    return (Person) list.get(1);
  }

  @DeleteMapping(value = "/clear")
  public void clear(@RequestParam CacheContext.Cache cache) {
    cache.getCache().clear();
  }

  @DeleteMapping(value = "/destroy")
  public void destroy(@RequestParam CacheContext.Cache cache) {
    cache.getCache().destroy();
  }
}
