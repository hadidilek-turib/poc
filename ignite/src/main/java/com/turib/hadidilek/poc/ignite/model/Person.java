package com.turib.hadidilek.poc.ignite.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Person implements Serializable {

  @QuerySqlField
  private int field1;

  @QuerySqlField
  private String field2;

  @QuerySqlField
  private String field3;

  @QuerySqlField
  private long field4;

  @QuerySqlField
  private BigDecimal field5;

  @QuerySqlField
  private LocalDate field6;

  @QuerySqlField
  private String field7;

  @QuerySqlField
  private String field8;

  @QuerySqlField
  private String field9;

  @QuerySqlField
  private String field10;
}
