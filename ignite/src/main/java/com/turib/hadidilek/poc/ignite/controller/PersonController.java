package com.turib.hadidilek.poc.ignite.controller;

import com.turib.hadidilek.poc.ignite.config.IgniteConfig;
import com.turib.hadidilek.poc.ignite.model.Person;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteClosure;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/person")
public class PersonController {

  private final IgniteCache<Integer, Person> cache;

  public PersonController(Ignite ignite) {
    this.cache = ignite.cache(IgniteConfig.PERSON_CACHE);
  }

  @GetMapping("/all")
  public List<Person> getPeople(@RequestParam(name = "page", required = false, defaultValue = "0") int page,
                                @RequestParam(name = "size", required = false, defaultValue = "2") int size) {
    int offset = page * size;
    SqlFieldsQuery qry =
        new SqlFieldsQuery("select * from person order by name offset ? limit ?")
            .setArgs(offset, size);

    IgniteClosure<List<?>, Person> transformer = objects -> Person
        .builder()
        .id((Integer) objects.get(0))
        .name((String) objects.get(1))
        .build();

    return cache.query(qry, transformer).getAll();
  }

  @GetMapping
  public Person getPerson(@RequestParam Integer id) {
    return cache.get(id);
  }

  @PostMapping
  public String addPerson(@RequestParam int id, @RequestParam String name) {
    cache.put(id, new Person(id, name));
    return "Person added: " + id + " - " + name;
  }

  @DeleteMapping("/{id}")
  public String removePerson(@PathVariable int id) {
    cache.remove(id);
    return "Person removed: " + id;
  }
}
