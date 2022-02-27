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
package com.datagrafting.sql2sparql.sparql;

import static com.datagrafting.sql2sparql.TestUtils.checkPlan;
import static com.datagrafting.sql2sparql.TestUtils.checkResults;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparqlMappingTableTest {
  private Connection connection;

  @BeforeEach
  public void setUp() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    URL modelPath = SparqlMappingTableTest.class.getClassLoader().getResource("modelMapping.json");
    connection = DriverManager.getConnection(
        "jdbc:calcite:model=" + Objects.requireNonNull(modelPath).getPath(), info);
  }

  @AfterEach
  public void teardown() throws SQLException {
    connection.close();
  }

  @Test
  public void testSelectAll() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM Person";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], name=[$1], age=[$2])\n"
        + "  SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String] | Person.age [java.lang.Long]\n"
        + "------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String] | 40 [java.lang.Long]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String] | 42 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectAllLimit() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM Person "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(fetch=[1])\n"
        + "  LogicalProject(s=[$0], name=[$1], age=[$2])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassSort(fetch=[1])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String] | Person.age [java.lang.Long]\n"
        + "------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectAllLimit1000() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM Person "
        + "LIMIT 1000";

    checkPlan(connection, query, true, ""
        + "LogicalSort(fetch=[1000])\n"
        + "  LogicalProject(s=[$0], name=[$1], age=[$2])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassSort(fetch=[1000])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String] | Person.age [java.lang.Long]\n"
        + "------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String] | 40 [java.lang.Long]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String] | 42 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectProject() throws SQLException {
    String query = ""
        + "SELECT s, name "
        + "FROM Person";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], name=[$1])\n"
        + "  SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], name=[$1])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectInverted() throws SQLException {
    String query = ""
        + "SELECT name, s "
        + "FROM Person";

    checkPlan(connection, query, true, ""
        + "LogicalProject(name=[$1], s=[$0])\n"
        + "  SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(name=[$1], s=[$0])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.name [java.lang.String] | Person.s [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "Jane Doe [java.lang.String] | http://www.example.com/id/janedoe [java.lang.String]\n"
        + "John Doe [java.lang.String] | http://www.example.com/id/johndoe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectLimit() throws SQLException {
    String query = ""
        + "SELECT s, name "
        + "FROM Person "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(fetch=[1])\n"
        + "  LogicalProject(s=[$0], name=[$1])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], name=[$1])\n"
        + "    SparqlClassSort(fetch=[1])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectLimit1000() throws SQLException {
    String query = ""
        + "SELECT s, name "
        + "FROM Person "
        + "LIMIT 1000";

    checkPlan(connection, query, true, ""
        + "LogicalSort(fetch=[1000])\n"
        + "  LogicalProject(s=[$0], name=[$1])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassSort(fetch=[1000])\n"
        + "    SparqlClassProject(s=[$0], name=[$1])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectAllOrderBy() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM Person "
        + "ORDER BY name DESC";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$1], dir0=[DESC])\n"
        + "  LogicalProject(s=[$0], name=[$1], age=[$2])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassSort(sort0=[$1], dir0=[DESC])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String] | Person.age [java.lang.Long]\n"
        + "------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String] | 42 [java.lang.Long]\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectProjectOrderBy() throws SQLException {
    String query = ""
        + "SELECT s, name "
        + "FROM Person "
        + "ORDER BY name DESC";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$1], dir0=[DESC])\n"
        + "  LogicalProject(s=[$0], name=[$1])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassSort(sort0=[$1], dir0=[DESC])\n"
        + "    SparqlClassProject(s=[$0], name=[$1])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectOrderByLimit() throws SQLException {
    String query = ""
        + "SELECT s, name "
        + "FROM Person "
        + "ORDER BY name DESC "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$1], dir0=[DESC], fetch=[1])\n"
        + "  LogicalProject(s=[$0], name=[$1])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], name=[$1])\n"
        + "    SparqlClassSort(sort0=[$1], dir0=[DESC], fetch=[1])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectOrderBy2Limit() throws SQLException {
    String query = ""
        + "SELECT s, name "
        + "FROM Person "
        + "ORDER BY name DESC, s ASC "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC], fetch=[1])\n"
        + "  LogicalProject(s=[$0], name=[$1])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], name=[$1])\n"
        + "    SparqlClassSort(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC], fetch=[1])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectAllEqFilter() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM Person "
        + "WHERE name = 'John Doe'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], name=[$1], age=[$2])\n"
        + "  LogicalFilter(condition=[=($1, 'John Doe')])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassFilter(condition=[=($1, 'John Doe')])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String] | Person.age [java.lang.Long]\n"
        + "------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String] | 42 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectProjectEqFilter() throws SQLException {
    String query = ""
        + "SELECT name, s "
        + "FROM Person "
        + "WHERE name = 'John Doe'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(name=[$1], s=[$0])\n"
        + "  LogicalFilter(condition=[=($1, 'John Doe')])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(name=[$1], s=[$0])\n"
        + "    SparqlClassFilter(condition=[=($1, 'John Doe')])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.name [java.lang.String] | Person.s [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "John Doe [java.lang.String] | http://www.example.com/id/johndoe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectNeqFilter() throws SQLException {
    String query = ""
        + "SELECT s, name "
        + "FROM Person "
        + "WHERE name <> 'John Doe'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], name=[$1])\n"
        + "  LogicalFilter(condition=[<>($1, 'John Doe')])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], name=[$1])\n"
        + "    SparqlClassFilter(condition=[<>($1, 'John Doe')])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectSearchStringFilter() throws SQLException {
    // Tests when an IN is transformed into a SEARCH

    String query = ""
        + "SELECT s, name "
        + "FROM Person "
        + "WHERE name IN ('John Doe', 'Jane Doe')";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], name=[$1])\n"
        + "  LogicalFilter(condition=[OR(=($1, 'John Doe'), =($1, 'Jane Doe'))])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], name=[$1])\n"
        + "    SparqlClassFilter(condition=[SEARCH($1, Sarg['Jane Doe', 'John Doe']:CHAR(8))])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.name [java.lang.String]\n"
        + "------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectSearchNumFilter() throws SQLException {
    // Tests when an IN is transformed into a SEARCH

    String query = ""
        + "SELECT s, age "
        + "FROM Person "
        + "WHERE age IN (40, 21)";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], age=[$2])\n"
        + "  LogicalFilter(condition=[OR(=($2, 40), =($2, 21))])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], age=[$2])\n"
        + "    SparqlClassFilter(condition=[SEARCH($2, Sarg[21L:BIGINT, 40L:BIGINT]:BIGINT)])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.age [java.lang.Long]\n"
        + "---------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectSearchNumRangeFilter() throws SQLException {
    String query = ""
        + "SELECT s, age "
        + "FROM Person "
        + "WHERE age > 30 AND age < 41";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], age=[$2])\n"
        + "  LogicalFilter(condition=[AND(>($2, 30), <($2, 41))])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], age=[$2])\n"
        + "    SparqlClassFilter(condition=[SEARCH($2, Sarg[(30..41)])])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.age [java.lang.Long]\n"
        + "---------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectGeqFilter() throws SQLException {
    String query = ""
        + "SELECT s, age "
        + "FROM Person "
        + "WHERE age >= 42";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], age=[$2])\n"
        + "  LogicalFilter(condition=[>=($2, 42)])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], age=[$2])\n"
        + "    SparqlClassFilter(condition=[>=($2, 42)])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.age [java.lang.Long]\n"
        + "---------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | 42 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectLeqFilter() throws SQLException {
    String query = ""
        + "SELECT s, age "
        + "FROM Person "
        + "WHERE age <= 40";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], age=[$2])\n"
        + "  LogicalFilter(condition=[<=($2, 40)])\n"
        + "    SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], age=[$2])\n"
        + "    SparqlClassFilter(condition=[<=($2, 40)])\n"
        + "      SparqlMappingTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.age [java.lang.Long]\n"
        + "---------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }
}
