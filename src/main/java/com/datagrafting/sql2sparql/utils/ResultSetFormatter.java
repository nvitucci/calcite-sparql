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
package com.datagrafting.sql2sparql.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class ResultSetFormatter {
  public static String printResults(ResultSet resultSet) throws SQLException {
    return printResults(resultSet, false, false);
  }

  public static String printResults(ResultSet resultSet, boolean withClasses, boolean lineAfterRow)
      throws SQLException {
    StringBuilder output = new StringBuilder();
    int numCol = resultSet.getMetaData().getColumnCount();

    // Can't use streams conveniently as ResultSet methods can throw SQLException
    List<String> columnNames = new ArrayList<>();
    for (int col = 1; col <= numCol; col++) {
      String tableName = resultSet.getMetaData().getTableName(col);
      columnNames.add(
          (tableName == null ? "" : tableName + ".")
              + resultSet.getMetaData().getColumnName(col)
              + (withClasses ? " [" + resultSet.getMetaData().getColumnClassName(col) + "]" : ""));
    }

    String header = String.join(" | ", columnNames);
    String line = StringUtils.repeat("-", header.length());
    output.append(header)
          .append("\n")
          .append(line)
          .append("\n");

    while (resultSet.next()) {
      List<String> columns = new ArrayList<>();
      for (int col = 1; col <= numCol; col++) {
        Object obj;

        try {
          obj = resultSet.getObject(col);
        } catch (ClassCastException e) {
          // The type for this field in this row is different from the inferred column type

          // TODO: log
          // System.out.println("ClassCastException on " + resultSet.getMetaData().getColumnName(col) + ": " + e);

          // Replace the non-assignable value with null
          obj = null;
        }

        columns.add(obj == null ? null : obj + (withClasses ? " [" + obj.getClass().getName() + "]" : ""));
      }

      String row = String.join(" | ", columns);
      String rowLine = StringUtils.repeat("-", row.length());
      output.append(row)
            .append(lineAfterRow ? "\n" + rowLine : "")
            .append("\n");
    }

    return output.toString();
  }
}
