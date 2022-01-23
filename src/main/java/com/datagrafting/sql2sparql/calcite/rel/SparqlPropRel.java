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
package com.datagrafting.sql2sparql.calcite.rel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

public interface SparqlPropRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("SPARQL_PROP", SparqlPropRel.class);

  class Implementor {
    public RelOptTable table;
    public Map<String, String> props = new HashMap<>();
    public int limit;
    public List<String> joinSelect = new ArrayList<>();
    public List<String> joinPattern = new ArrayList<>();
    public Map<Integer, String> sortIndices = new HashMap<>();
    public List<Integer> projects = new ArrayList<>();
    public Map<Integer, String> filters = new HashMap<>();

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((SparqlPropRel) input).implement(this);
    }

    public String getQuery() {
      // No joins, simple select
      if (joinSelect.isEmpty()) {
        joinSelect.add("?s");
        joinSelect.add("?o");
      } else {
        // TODO: refine join to prevent this?
        // Remove duplicate selects (artifact from joins) without changing order
        joinSelect = joinSelect.stream().distinct().collect(Collectors.toList());
      }

      List<String> select;
      if (projects.isEmpty()) {
        select = new ArrayList<>();
        select.add("?s");
        select.add("?o");
      } else {
        select = projects.stream()
                         .sorted()
                         .map(idx -> joinSelect.get(idx))
                         .collect(Collectors.toList());
      }

      StringBuilder orderBy = new StringBuilder();
      for (Map.Entry<Integer, String> sort : sortIndices.entrySet()) {
        orderBy.append(sort.getValue());
        orderBy.append("(");
        orderBy.append(select.get(sort.getKey()));
        orderBy.append(")");
      }

      StringBuilder filterClause = new StringBuilder();
      for (Map.Entry<Integer, String> filter : filters.entrySet()) {
        filterClause.append(filter.getValue().replace(SparqlPropFilter.PLACEHOLDER, joinSelect.get(filter.getKey())));
      }

      return String.format(""
              + "SELECT %s\n"
              + "WHERE {\n"
              + "  GRAPH ?g {\n"
              + "    %s\n"
              + "  }\n"
              + "%s"
              + "}\n"
              + "%s\n"
              + "%s",
          String.join(" ", select),
          joinPattern.isEmpty() ? "?s <" + props.values().toArray()[0] + "> ?o ." : String.join(" . ", new LinkedHashSet<>(joinPattern)),
          filterClause.length() > 0 ? "FILTER (" + filterClause + ")" : "",
          orderBy.length() > 0 ? "ORDER BY " + orderBy.toString() : "",
          limit > 0 ? "LIMIT " + limit : ""
      );
    }
  }
}
