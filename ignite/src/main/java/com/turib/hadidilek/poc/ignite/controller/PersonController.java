package com.turib.hadidilek.poc.ignite.controller;

import com.turib.hadidilek.poc.ignite.model.Person;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static com.turib.hadidilek.poc.ignite.config.IgniteConfig.Caches.PERSON_SQL;

@RestController
@RequestMapping("/person")
public class PersonController {

  @GetMapping("/all")
  public List<Person> getPeople(@RequestParam(name = "page", required = false, defaultValue = "0") int page,
                                @RequestParam(name = "size", required = false, defaultValue = "2") int size) {
    int offset = page * size;
    SqlFieldsQuery qry =
        new SqlFieldsQuery("select * from person order by name offset ? limit ?")
            .setArgs(offset, size);

    FieldsQueryCursor<List<?>> cursor = PERSON_SQL.getCache().query(qry);
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

  @GetMapping
  public Person getPerson(@RequestParam Integer id) {
    return PERSON_SQL.getCache().get(id);
  }

  @PostMapping
  public String addPerson(@RequestParam int id, @RequestParam String name) {
    PERSON_SQL.getCache().put(id, new Person(id, name));
    return "Person added: " + id + " - " + name;
  }

  @DeleteMapping("/{id}")
  public String removePerson(@PathVariable int id) {
    PERSON_SQL.getCache().remove(id);
    return "Person removed: " + id;
  }
}
