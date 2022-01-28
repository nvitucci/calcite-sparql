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

public class SparqlPropTableTest {
  private Connection connection;

  @BeforeEach
  public void setUp() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    URL modelPath = SparqlPropTableTest.class.getClassLoader().getResource("modelProp.json");
    connection = DriverManager.getConnection(
        "jdbc:calcite:model=" + Objects.requireNonNull(modelPath).getPath(), info);
  }

  @AfterEach
  public void teardown() throws SQLException {
    connection.close();
  }

  @Test
  public void testSelectObject() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM homepage";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  SparqlPropTableScan(table=[[sparql, homepage]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropTableScan(table=[[sparql, homepage]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "homepage.s [java.lang.String] | homepage.o [java.lang.String]\n"
        + "-------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | http://www.example.com/pages/janedoe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | http://www.example.com/pages/johndoe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectString() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM name";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.s [java.lang.String] | name.o [java.lang.String]\n"
        + "-----------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectInt() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM age";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "age.s [java.lang.String] | age.o [java.lang.Long]\n"
        + "-------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | 42 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectLimit() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM name "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(fetch=[1])\n"
        + "  LogicalProject(s=[$0], o=[$1])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropLimit\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.s [java.lang.String] | name.o [java.lang.String]\n"
        + "-----------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectEqFilter() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM name "
        + "WHERE o = 'John Doe'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  LogicalFilter(condition=[=($1, 'John Doe')])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropFilter(condition=[=($1, 'John Doe')])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.s [java.lang.String] | name.o [java.lang.String]\n"
        + "-----------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectNeqFilter() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM name "
        + "WHERE o <> 'John Doe'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  LogicalFilter(condition=[<>($1, 'John Doe')])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropFilter(condition=[<>($1, 'John Doe')])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.s [java.lang.String] | name.o [java.lang.String]\n"
        + "-----------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectSearchStringFilter() throws SQLException {
    // Tests when an IN is transformed into a SEARCH

    String query = ""
        + "SELECT * "
        + "FROM name "
        + "WHERE o IN ('John Doe', 'Jane Doe')";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  LogicalFilter(condition=[OR(=($1, 'John Doe'), =($1, 'Jane Doe'))])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropFilter(condition=[SEARCH($1, Sarg['Jane Doe', 'John Doe']:CHAR(8))])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.s [java.lang.String] | name.o [java.lang.String]\n"
        + "-----------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectSearchNumFilter() throws SQLException {
    // Tests when an IN is transformed into a SEARCH

    String query = ""
        + "SELECT * "
        + "FROM age "
        + "WHERE o IN (40, 21)";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  LogicalFilter(condition=[OR(=($1, 40), =($1, 21))])\n"
        + "    SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropFilter(condition=[SEARCH($1, Sarg[21L:BIGINT, 40L:BIGINT]:BIGINT)])\n"
        + "    SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "age.s [java.lang.String] | age.o [java.lang.Long]\n"
        + "-------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectSearchNumRangeFilter() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM age "
        + "WHERE o > 30 AND o < 41";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  LogicalFilter(condition=[AND(>($1, 30), <($1, 41))])\n"
        + "    SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropFilter(condition=[SEARCH($1, Sarg[(30..41)])])\n"
        + "    SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "age.s [java.lang.String] | age.o [java.lang.Long]\n"
        + "-------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectGeqFilter() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM age "
        + "WHERE o >= 42";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  LogicalFilter(condition=[>=($1, 42)])\n"
        + "    SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropFilter(condition=[>=($1, 42)])\n"
        + "    SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "age.s [java.lang.String] | age.o [java.lang.Long]\n"
        + "-------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | 42 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectLeqFilter() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM age "
        + "WHERE o <= 40";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], o=[$1])\n"
        + "  LogicalFilter(condition=[<=($1, 40)])\n"
        + "    SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropFilter(condition=[<=($1, 40)])\n"
        + "    SparqlPropTableScan(table=[[sparql, age]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "age.s [java.lang.String] | age.o [java.lang.Long]\n"
        + "-------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectProject() throws SQLException {
    String query = ""
        + "SELECT o "
        + "FROM name ";

    checkPlan(connection, query, true, ""
        + "LogicalProject(o=[$1])\n"
        + "  SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropProject(o=[$1])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.o [java.lang.String]\n"
        + "-------------------------\n"
        + "Jane Doe [java.lang.String]\n"
        + "John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectFilterProject() throws SQLException {
    String query = ""
        + "SELECT o "
        + "FROM name "
        + "WHERE o = 'John Doe'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(o=[$1])\n"
        + "  LogicalFilter(condition=[=($1, 'John Doe')])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropProject(o=[$1])\n"
        + "    SparqlPropFilter(condition=[=($1, 'John Doe')])\n"
        + "      SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.o [java.lang.String]\n"
        + "-------------------------\n"
        + "John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectOrderLimitDesc2() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM name "
        + "ORDER BY s DESC "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$0], dir0=[DESC], fetch=[1])\n"
        + "  LogicalProject(s=[$0], o=[$1])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropSort(sort0=[$0], dir0=[DESC], fetch=[1])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.s [java.lang.String] | name.o [java.lang.String]\n"
        + "-----------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectOrderLimitAsc2() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM name "
        + "ORDER BY s ASC "
        + "LIMIT 2";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$0], dir0=[ASC], fetch=[2])\n"
        + "  LogicalProject(s=[$0], o=[$1])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlPropToEnumerableConverter\n"
        + "  SparqlPropSort(sort0=[$0], dir0=[ASC], fetch=[2])\n"
        + "    SparqlPropTableScan(table=[[sparql, name]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "name.s [java.lang.String] | name.o [java.lang.String]\n"
        + "-----------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }
}
