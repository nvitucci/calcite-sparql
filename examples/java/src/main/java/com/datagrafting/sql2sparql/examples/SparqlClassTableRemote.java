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
package com.datagrafting.sql2sparql.examples;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class SparqlClassTableRemote {
  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    URL modelPath = SparqlClassTableRemote.class.getClassLoader().getResource("modelClassRemote.json");

    Class.forName("org.apache.calcite.avatica.remote.Driver");
    Class.forName("org.apache.jena.jdbc.remote.RemoteEndpointDriver");

    Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + Objects.requireNonNull(modelPath).getPath(), info);
    
    String query = ""
        + "SELECT * "
        + "FROM Company "
        + "LIMIT 10";

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
    System.out.println(printResults(resultSet, true, true));

    connection.close();
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
    String line = "---";
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
          // Replace the non-assignable value with null
          obj = null;
        }

        columns.add(obj == null ? null : obj + (withClasses ? " [" + obj.getClass().getName() + "]" : ""));
      }

      String row = String.join(" | ", columns);
      String rowLine = "---";
      output.append(row)
            .append(lineAfterRow ? "\n" + rowLine : "")
            .append("\n");
    }

    return output.toString();
  }
}
