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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.jena.graph.Node;
import org.apache.jena.jdbc.JdbcCompatibility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparqlEndpointTest {
  SparqlEndpoint sparqlEndpoint;

  @BeforeEach
  public void setUp() throws SQLException {
    // Use "jdbc:jena:mem:empty=true" for an empty endpoint
    URL modelPath = SparqlEndpointTest.class.getClassLoader().getResource("data.nq");
    String url = "jdbc:jena:mem:dataset=" + modelPath;
    sparqlEndpoint = new SparqlEndpoint(url, JdbcCompatibility.LOW);
  }

  @AfterEach
  public void teardown() throws SQLException {
    sparqlEndpoint.close();
  }

  @Test
  public void readDataTest() throws SQLException {
    String queryString = ""
        + "SELECT * "
        + "WHERE { "
        + "  GRAPH ?g { "
        + "    ?s ?p ?o "
        + "  } "
        + "FILTER (?p = <http://xmlns.com/foaf/0.1/name>)} ORDER BY DESC(?s) LIMIT 1";

    ResultSet results = sparqlEndpoint.query(queryString);

    while (results.next()) {
      // With the LOW compatibility level, results are converted to Nodes
      Node g = (Node) results.getObject("g");
      Node s = (Node) results.getObject("s");
      Node p = (Node) results.getObject("p");
      Node o = (Node) results.getObject("o");

      assertThat(g.isURI()).isTrue();
      assertThat(g.getURI()).isEqualTo("http://www.example.com/graph/a");
      assertThat(s.isURI()).isTrue();
      assertThat(s.getURI()).isEqualTo("http://www.example.com/id/johndoe");
      assertThat(p.isURI()).isTrue();
      assertThat(p.getURI()).isEqualTo("http://xmlns.com/foaf/0.1/name");
      assertThat(o.isLiteral()).isTrue();
      assertThat(o.getLiteralValue()).isEqualTo("John Doe");
    }
  }
}
