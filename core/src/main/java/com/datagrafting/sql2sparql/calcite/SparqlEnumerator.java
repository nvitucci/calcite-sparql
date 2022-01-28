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

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node_Literal;

public class SparqlEnumerator implements Enumerator<Object> {
  private final ResultSet results;

  public SparqlEnumerator(ResultSet results) {
    this.results = results;
  }

  @Override
  public Object current() {
    try {
      int numFields = results.getMetaData().getColumnCount();
      Object[] row = new Object[numFields];

      for (int col = 1; col <= numFields; col++) {
        row[col - 1] = parseObject(results.getObject(col));
      }

      // When only one field is projected
      if (numFields == 1) {
        return row[0];
      } else {
        return row;
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    }
  }

  private Object parseObject(Object obj) {
    if (obj instanceof Node_Literal) {
      Object value = ((Node_Literal) obj).getLiteralValue();
      RDFDatatype datatype = ((Node_Literal) obj).getLiteralDatatype();
      Object parsedValue = datatype.parse(value.toString());

      if (datatype.getURI().equals("http://www.w3.org/2001/XMLSchema#integer") &&
          !(parsedValue instanceof Long)) {
        // An xsd:integer can be converted to an Integer, a Long or a BigInteger (see
        // https://jena.apache.org/documentation/notes/typed-literals.html#xsd-data-types),
        // but Calcite uses long for the BIGINT type
        parsedValue = parsedValue instanceof BigInteger ?
            ((BigInteger) parsedValue).longValue() :
            // Value is neither Long nor BigInteger, so it must be Integer
            ((Integer) parsedValue).longValue();
      }

      return parsedValue;
    } else {
      return obj;
    }
  }

  @Override
  public boolean moveNext() {
    try {
      return results.next();
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    }
  }

  @Override
  public void reset() {

  }

  @Override
  public void close() {
    try {
      results.close();
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    }
  }
}
