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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.calcite.util.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.jdbc.JdbcCompatibility;
import org.apache.jena.jdbc.JenaDriver;

public class SparqlEndpoint {
  private Properties info;
  private Connection conn;
  private String url;
  private int compatibility;

  public SparqlEndpoint(String url) throws SQLException {
    this(url, JdbcCompatibility.LOW);
  }

  public SparqlEndpoint(String url, int compatibility) throws SQLException {
    this.url = url;
    this.compatibility = compatibility;

    Properties info = new Properties();
    info.setProperty(JenaDriver.PARAM_JDBC_COMPATIBILITY, Integer.toString(compatibility));
    this.info = info;

    // Make sure the driver is loaded. Without this, getConnection() may fail.
    try {
      Class.forName("org.apache.jena.jdbc.remote.RemoteEndpointDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Class not found", e);
    }
    conn = DriverManager.getConnection(url, info);
  }

  public ResultSet query(String queryString) throws SQLException {
    try {
      Statement stmt = conn.createStatement();
      return stmt.executeQuery(queryString);
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    }
  }

  public Map<String, String> getProperties() throws SQLException {
    Map<String, String> properties = new HashMap<>();
    ResultSet results = query(""
        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        + "SELECT DISTINCT ?p\n"
        + "WHERE {\n"
        + "  GRAPH ?g {\n"
        + "    ?s ?p ?o\n"
        + "  }\n"
        + "  FILTER (?p NOT IN (rdf:type))\n"
        + "}"
    );

    while (results.next()) {
      Node p = (Node) results.getObject("p");
      assert p.isURI();

      properties.put(p.getLocalName(), p.getURI());
    }

    return properties;
  }

  public Map<String, String> getClasses() throws SQLException {
    Map<String, String> classes = new HashMap<>();
    ResultSet results = query(""
        + "SELECT DISTINCT ?cl\n"
        + "WHERE {\n"
        + "  GRAPH ?g {\n"
        + "    ?s a ?cl\n"
        + "  }\n"
        + "}"
    );

    while (results.next()) {
      Node cl = (Node) results.getObject("cl");
      assert cl.isURI();

      classes.put(cl.getLocalName(), cl.getURI());
    }

    return classes;
  }

  public Set<String> getObjectTypesForProperty(String prop, int limit) throws SQLException {
    Set<String> objectTypes = new HashSet<>();
    ResultSet results = query(String.format(""
        + "SELECT DISTINCT ?o\n"
        + "WHERE {\n"
        + "  GRAPH ?g {\n"
        + "    ?s <%s> ?o\n"
        + "  }\n"
        + "}\n"
        + "LIMIT %d", prop, limit)
    );

    while (results.next()) {
      Node o = (Node) results.getObject("o");

      if (o.isURI()) {
        objectTypes.add(null);
      } else {
        objectTypes.add(o.getLiteralDatatypeURI());
      }
    }

    return objectTypes;
  }

  public List<Pair<String, String>> getPropertiesPerClass(String type, int limit)
      throws SQLException, URISyntaxException {
    Set<String> propNames = new HashSet<>();
    List<Pair<String, String>> props = new ArrayList<>();
    ResultSet results = query(String.format(""
        + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        + "SELECT DISTINCT ?p\n"
        + "WHERE {\n"
        + "  GRAPH ?g {\n"
        + "    ?s a <%s> .\n"
        + "    ?s ?p ?o .\n"
        + "  }\n"
        + "  FILTER (?p NOT IN (rdf:type))\n"
        + "}\n"
        + "LIMIT %d", type, limit)
    );

    while (results.next()) {
      Node p = (Node) results.getObject("p");
      String name = getNamespacedName(p);
      String uri = p.getURI();

      // Temporary hack to prevent similar property names to generate duplicate columns
      if (propNames.contains(name)) {
        name += "_0";
      }

      propNames.add(name);
      props.add(new Pair<>(name, uri));
    }

    return props;
  }

  private String getNamespacedName(Node p) throws URISyntaxException {
    // TODO: try and make actual use of namespaces (predefined customizable list?)
    String[] hostComponents = new URI(p.getNameSpace()).getHost().split("\\.");
    String domain = hostComponents[hostComponents.length - 2];
    return domain + "_" + p.getLocalName();
  }

  public void close() throws SQLException {
    conn.close();
  }
}
