/**
 * Copyright 2021-2022 Nicola Vitucci
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datagrafting.sql2sparql.calcite;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

public class SparqlQueryable<T> extends AbstractTableQueryable<T> {
  public SparqlQueryable(QueryProvider queryProvider, SchemaPlus schema, SparqlTable sparqlTable, String tableName) {
    super(queryProvider, schema, sparqlTable, tableName);
  }

  @Override
  public Enumerator<T> enumerator() {
    final Enumerable<T> enumerable = (Enumerable<T>) getTable().query(null);
    return enumerable.enumerator();
  }

  private SparqlTable getTable() {
    return (SparqlTable) table;
  }

  public Enumerable<T> query(String queryString) {
    return (Enumerable<T>) getTable().query(queryString);
  }
}
