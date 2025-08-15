package com.turib.hadidilek.poc.ignite.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Person implements Serializable {

  @QuerySqlField(index = true)
  private int id;

  @QuerySqlField(index = true)
  private String name;
}
