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

public class SparqlClassTableTest {
  private Connection connection;

  @BeforeEach
  public void setUp() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    URL modelPath = SparqlClassTableTest.class.getClassLoader().getResource("modelClass.json");
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
        + "LogicalProject(s=[$0], xmlns_family_name=[$1], xmlns_knows=[$2], xmlns_name=[$3], xmlns_title=[$4], xmlns_homepage=[$5], xmlns_mbox_sha1sum=[$6], xmlns_age=[$7], xmlns_givenname=[$8], xmlns_nick=[$9])\n"
        + "  SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_family_name [java.lang.String] | Person.xmlns_knows [java.lang.String] | Person.xmlns_name [java.lang.String] | Person.xmlns_title [java.lang.String] | Person.xmlns_homepage [java.lang.String] | Person.xmlns_mbox_sha1sum [java.lang.String] | Person.xmlns_age [java.lang.Long] | Person.xmlns_givenname [java.lang.String] | Person.xmlns_nick [java.lang.String]\n"
        + "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Doe [java.lang.String] | http://www.example.com/id/johndoe [java.lang.String] | Jane Doe [java.lang.String] | Mr [java.lang.String] | http://www.example.com/pages/janedoe [java.lang.String] | 90045cddf482e1bd1773fb8bb9cd8a9e75c9c0a4 [java.lang.String] | 40 [java.lang.Long] | Jane [java.lang.String] | Jay [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | Doe [java.lang.String] | http://www.example.com/id/janedoe [java.lang.String] | John Doe [java.lang.String] | Mr [java.lang.String] | http://www.example.com/pages/johndoe [java.lang.String] | 2a6f4e470a1b9ef493f4ac83aa9456102a14f5c4 [java.lang.String] | 42 [java.lang.Long] | John [java.lang.String] | Johnny [java.lang.String]\n"
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
        + "  LogicalProject(s=[$0], xmlns_family_name=[$1], xmlns_knows=[$2], xmlns_name=[$3], xmlns_title=[$4], xmlns_homepage=[$5], xmlns_mbox_sha1sum=[$6], xmlns_age=[$7], xmlns_givenname=[$8], xmlns_nick=[$9])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassLimit\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_family_name [java.lang.String] | Person.xmlns_knows [java.lang.String] | Person.xmlns_name [java.lang.String] | Person.xmlns_title [java.lang.String] | Person.xmlns_homepage [java.lang.String] | Person.xmlns_mbox_sha1sum [java.lang.String] | Person.xmlns_age [java.lang.Long] | Person.xmlns_givenname [java.lang.String] | Person.xmlns_nick [java.lang.String]\n"
        + "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Doe [java.lang.String] | http://www.example.com/id/johndoe [java.lang.String] | Jane Doe [java.lang.String] | Mr [java.lang.String] | http://www.example.com/pages/janedoe [java.lang.String] | 90045cddf482e1bd1773fb8bb9cd8a9e75c9c0a4 [java.lang.String] | 40 [java.lang.Long] | Jane [java.lang.String] | Jay [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProject() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_name "
        + "FROM Person";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], xmlns_name=[$3])\n"
        + "  SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_name [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectInverted() throws SQLException {
    String query = ""
        + "SELECT xmlns_name, s "
        + "FROM Person";

    checkPlan(connection, query, true, ""
        + "LogicalProject(xmlns_name=[$3], s=[$0])\n"
        + "  SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(xmlns_name=[$3], s=[$0])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.xmlns_name [java.lang.String] | Person.s [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "Jane Doe [java.lang.String] | http://www.example.com/id/janedoe [java.lang.String]\n"
        + "John Doe [java.lang.String] | http://www.example.com/id/johndoe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectLimit() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_name "
        + "FROM Person "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(fetch=[1])\n"
        + "  LogicalProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassLimit\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_name [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectAllOrderBy() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM Person "
        + "ORDER BY xmlns_name DESC";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$3], dir0=[DESC])\n"
        + "  LogicalProject(s=[$0], xmlns_family_name=[$1], xmlns_knows=[$2], xmlns_name=[$3], xmlns_title=[$4], xmlns_homepage=[$5], xmlns_mbox_sha1sum=[$6], xmlns_age=[$7], xmlns_givenname=[$8], xmlns_nick=[$9])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassSort(sort0=[$3], dir0=[DESC])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_family_name [java.lang.String] | Person.xmlns_knows [java.lang.String] | Person.xmlns_name [java.lang.String] | Person.xmlns_title [java.lang.String] | Person.xmlns_homepage [java.lang.String] | Person.xmlns_mbox_sha1sum [java.lang.String] | Person.xmlns_age [java.lang.Long] | Person.xmlns_givenname [java.lang.String] | Person.xmlns_nick [java.lang.String]\n"
        + "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | Doe [java.lang.String] | http://www.example.com/id/janedoe [java.lang.String] | John Doe [java.lang.String] | Mr [java.lang.String] | http://www.example.com/pages/johndoe [java.lang.String] | 2a6f4e470a1b9ef493f4ac83aa9456102a14f5c4 [java.lang.String] | 42 [java.lang.Long] | John [java.lang.String] | Johnny [java.lang.String]\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Doe [java.lang.String] | http://www.example.com/id/johndoe [java.lang.String] | Jane Doe [java.lang.String] | Mr [java.lang.String] | http://www.example.com/pages/janedoe [java.lang.String] | 90045cddf482e1bd1773fb8bb9cd8a9e75c9c0a4 [java.lang.String] | 40 [java.lang.Long] | Jane [java.lang.String] | Jay [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectOrderBy() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_name "
        + "FROM Person "
        + "ORDER BY xmlns_name DESC";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$1], dir0=[DESC])\n"
        + "  LogicalProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassSort(sort0=[$1], dir0=[DESC])\n"
        + "    SparqlClassProject(s=[$0], xmlns_name=[$3])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_name [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectOrderByLimit() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_name "
        + "FROM Person "
        + "ORDER BY xmlns_name DESC "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$1], dir0=[DESC], fetch=[1])\n"
        + "  LogicalProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassSort(sort0=[$3], dir0=[DESC], fetch=[1])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_name [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectOrderBy2Limit() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_name "
        + "FROM Person "
        + "ORDER BY xmlns_name DESC, s ASC "
        + "LIMIT 1";

    checkPlan(connection, query, true, ""
        + "LogicalSort(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC], fetch=[1])\n"
        + "  LogicalProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassSort(sort0=[$3], sort1=[$0], dir0=[DESC], dir1=[ASC], fetch=[1])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_name [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectAllEqFilter() throws SQLException {
    String query = ""
        + "SELECT * "
        + "FROM Person "
        + "WHERE xmlns_name = 'John Doe'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], xmlns_family_name=[$1], xmlns_knows=[$2], xmlns_name=[$3], xmlns_title=[$4], xmlns_homepage=[$5], xmlns_mbox_sha1sum=[$6], xmlns_age=[$7], xmlns_givenname=[$8], xmlns_nick=[$9])\n"
        + "  LogicalFilter(condition=[=($3, 'John Doe')])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassFilter(condition=[=($3, 'John Doe')])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_family_name [java.lang.String] | Person.xmlns_knows [java.lang.String] | Person.xmlns_name [java.lang.String] | Person.xmlns_title [java.lang.String] | Person.xmlns_homepage [java.lang.String] | Person.xmlns_mbox_sha1sum [java.lang.String] | Person.xmlns_age [java.lang.Long] | Person.xmlns_givenname [java.lang.String] | Person.xmlns_nick [java.lang.String]\n"
        + "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | Doe [java.lang.String] | http://www.example.com/id/janedoe [java.lang.String] | John Doe [java.lang.String] | Mr [java.lang.String] | http://www.example.com/pages/johndoe [java.lang.String] | 2a6f4e470a1b9ef493f4ac83aa9456102a14f5c4 [java.lang.String] | 42 [java.lang.Long] | John [java.lang.String] | Johnny [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectEqFilter() throws SQLException {
    String query = ""
        + "SELECT xmlns_name, s "
        + "FROM Person "
        + "WHERE xmlns_nick = 'Johnny'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(xmlns_name=[$3], s=[$0])\n"
        + "  LogicalFilter(condition=[=($9, 'Johnny')])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(xmlns_name=[$3], s=[$0])\n"
        + "    SparqlClassFilter(condition=[=($9, 'Johnny')])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.xmlns_name [java.lang.String] | Person.s [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "John Doe [java.lang.String] | http://www.example.com/id/johndoe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectNeqFilter() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_name "
        + "FROM Person "
        + "WHERE xmlns_name <> 'John Doe'";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], xmlns_name=[$3])\n"
        + "  LogicalFilter(condition=[<>($3, 'John Doe')])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassFilter(condition=[<>($3, 'John Doe')])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_name [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectSearchStringFilter() throws SQLException {
    // Tests when an IN is transformed into a SEARCH

    String query = ""
        + "SELECT s, xmlns_name "
        + "FROM Person "
        + "WHERE xmlns_name IN ('John Doe', 'Jane Doe')";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], xmlns_name=[$3])\n"
        + "  LogicalFilter(condition=[OR(=($3, 'John Doe'), =($3, 'Jane Doe'))])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_name=[$3])\n"
        + "    SparqlClassFilter(condition=[SEARCH($3, Sarg['Jane Doe', 'John Doe']:CHAR(8))])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_name [java.lang.String]\n"
        + "------------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | Jane Doe [java.lang.String]\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | John Doe [java.lang.String]\n"
    );
  }

  @Test
  public void testSelectProjectSearchNumFilter() throws SQLException {
    // Tests when an IN is transformed into a SEARCH

    String query = ""
        + "SELECT s, xmlns_age "
        + "FROM Person "
        + "WHERE xmlns_age IN (40, 21)";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], xmlns_age=[$7])\n"
        + "  LogicalFilter(condition=[OR(=($7, 40), =($7, 21))])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_age=[$7])\n"
        + "    SparqlClassFilter(condition=[SEARCH($7, Sarg[21L:BIGINT, 40L:BIGINT]:BIGINT)])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_age [java.lang.Long]\n"
        + "---------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectSearchNumRangeFilter() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_age "
        + "FROM Person "
        + "WHERE xmlns_age > 30 AND xmlns_age < 41";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], xmlns_age=[$7])\n"
        + "  LogicalFilter(condition=[AND(>($7, 30), <($7, 41))])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_age=[$7])\n"
        + "    SparqlClassFilter(condition=[SEARCH($7, Sarg[(30..41)])])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_age [java.lang.Long]\n"
        + "---------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectGeqFilter() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_age "
        + "FROM Person "
        + "WHERE xmlns_age >= 42";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], xmlns_age=[$7])\n"
        + "  LogicalFilter(condition=[>=($7, 42)])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_age=[$7])\n"
        + "    SparqlClassFilter(condition=[>=($7, 42)])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_age [java.lang.Long]\n"
        + "---------------------------------------------------------------\n"
        + "http://www.example.com/id/johndoe [java.lang.String] | 42 [java.lang.Long]\n"
    );
  }

  @Test
  public void testSelectLeqFilter() throws SQLException {
    String query = ""
        + "SELECT s, xmlns_age "
        + "FROM Person "
        + "WHERE xmlns_age <= 40";

    checkPlan(connection, query, true, ""
        + "LogicalProject(s=[$0], xmlns_age=[$7])\n"
        + "  LogicalFilter(condition=[<=($7, 40)])\n"
        + "    SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkPlan(connection, query, ""
        + "SparqlClassToEnumerableConverter\n"
        + "  SparqlClassProject(s=[$0], xmlns_age=[$7])\n"
        + "    SparqlClassFilter(condition=[<=($7, 40)])\n"
        + "      SparqlClassTableScan(table=[[sparql, Person]])\n"
    );

    checkResults(connection, query, true, false, ""
        + "Person.s [java.lang.String] | Person.xmlns_age [java.lang.Long]\n"
        + "---------------------------------------------------------------\n"
        + "http://www.example.com/id/janedoe [java.lang.String] | 40 [java.lang.Long]\n"
    );
  }
}
