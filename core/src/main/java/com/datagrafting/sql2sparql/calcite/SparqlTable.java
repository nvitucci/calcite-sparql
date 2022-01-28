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

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.datagrafting.sql2sparql.sparql.SparqlEndpoint;

public abstract class SparqlTable extends AbstractTable implements TranslatableTable, QueryableTable {
  protected String tableName;
  protected String prop;
  protected SparqlEndpoint endpoint;

  protected final Type elementType;

  public SparqlTable(String tableName, String prop, SparqlEndpoint endpoint) {
    this.tableName = tableName;
    this.prop = prop;
    this.endpoint = endpoint;

    // TODO: review
    this.elementType = Object[].class;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new SparqlQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override
  public Type getElementType() {
    return elementType;
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, elementType, tableName, clazz);
  }

  public Enumerable<Object> query(String queryString) {
    // TODO Merge with the scan body
    System.out.println(queryString);

    ResultSet results;

    try {
      results = endpoint.query(queryString);
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    }
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new SparqlEnumerator(results);
      }
    };
  }

  public SqlTypeName datatypeToSqlType(String datatype, boolean defaultToString) {
    final String XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#";

    switch (datatype) {
      case XSD_PREFIX + "boolean":
        return SqlTypeName.BOOLEAN;

      // Numbers
      case XSD_PREFIX + "byte":
        return SqlTypeName.TINYINT;

      case XSD_PREFIX + "short":
        return SqlTypeName.SMALLINT;

      case XSD_PREFIX + "int":
        return SqlTypeName.INTEGER;

      case XSD_PREFIX + "integer":
      case XSD_PREFIX + "long":
        return SqlTypeName.BIGINT;

      case XSD_PREFIX + "decimal":
        return SqlTypeName.DECIMAL;

      case XSD_PREFIX + "float":
        return SqlTypeName.FLOAT;

      case XSD_PREFIX + "double":
        return SqlTypeName.DOUBLE;

      // Date and time
      case XSD_PREFIX + "date":
        return SqlTypeName.DATE;

      case XSD_PREFIX + "time":
        return SqlTypeName.TIME;

      case XSD_PREFIX + "dateTime":
        return SqlTypeName.TIMESTAMP;

      case XSD_PREFIX + "dateTimeStamp":
        return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

      case XSD_PREFIX + "string":
        return SqlTypeName.VARCHAR;

      // TODO: review this
      case XSD_PREFIX + "unsignedByte":
      case XSD_PREFIX + "unsignedShort":
      case XSD_PREFIX + "unsignedInt":
      case XSD_PREFIX + "nonNegativeInteger":
      case XSD_PREFIX + "positiveInteger":
      case XSD_PREFIX + "unsignedLong":
      case XSD_PREFIX + "nonPositiveInteger":
      case XSD_PREFIX + "negativeInteger":
        throw new RuntimeException("Datatype " + datatype + " currently not supported");

      default:
        if (defaultToString) {
          return SqlTypeName.VARCHAR;
        } else {
          throw new RuntimeException(datatype + " datatype cannot be converted to a SQL type");
        }
    }
  }
}
