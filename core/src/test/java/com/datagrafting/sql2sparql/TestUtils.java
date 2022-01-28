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
package com.datagrafting.sql2sparql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.datagrafting.sql2sparql.utils.ResultSetFormatter;

public class TestUtils {
  // Cannot use CalciteAssert as it is defined in calcite-core test directory
  public static void checkPlan(Connection connection, String query, String expectedPlan) throws SQLException {
    checkPlan(connection, query, false, expectedPlan);
  }

  public static void checkPlan(Connection connection, String query, boolean showLogical, String expectedPlan)
      throws SQLException {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "EXPLAIN PLAN " + (showLogical ? "WITHOUT IMPLEMENTATION " : "") + "FOR " + query);
    resultSet.next();
    assertThat(resultSet.getString(1)).isEqualTo(expectedPlan);
  }

  public static void checkResults(Connection connection, String query, boolean withClasses, boolean lineAfterRow,
                                  String expectedResults) throws SQLException {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
    assertThat(ResultSetFormatter.printResults(resultSet, withClasses, lineAfterRow)).isEqualTo(expectedResults);
  }
}
