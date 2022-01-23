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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

public interface SparqlClassRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("SPARQL_CLASS", SparqlClassRel.class);

  class Implementor {
    public RelOptTable table;
    public Map<String, String> classes = new HashMap<>();
    public List<Pair<String, String>> props = new ArrayList<>();
    public List<Integer> projects = new ArrayList<>();
    public List<Pair<Integer, String>> sortIndices = new ArrayList<>();
    public Map<Integer, String> filters = new HashMap<>();
    public int limit;

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((SparqlClassRel) input).implement(this);
    }

    public String getQuery() {
      props.add(0, new Pair<>("s", "_ID_"));

      String selectClause;
      if (projects.isEmpty()) {
        selectClause = props.stream()
                            .map(pair -> String.format("?%s", pair.getKey()))
                            .collect(Collectors.joining(" "));
      } else {
        selectClause = projects.stream()
                               .map(idx -> props.get(idx))
                               .map(pair -> String.format("?%s", pair.getKey()))
                               .collect(Collectors.joining(" "));
      }

      String typeClause = String.format("<%s>", classes.values().toArray()[0]);

      String whereClause = props.stream()
                                .map(entry -> String.format("OPTIONAL { ?s <%s> ?%s }", entry.getValue(), entry.getKey()))
                                .collect(Collectors.joining("\n    "));

      String filter = filters.entrySet().stream()
                             .map(entry -> entry.getValue().replace(SparqlClassFilter.PLACEHOLDER, "?" + props.get(entry.getKey()).getKey()))
                             .collect(Collectors.joining(" "));
      String filterClause = filter.length() > 0 ? "FILTER (" + filter + ")" : "";

      String orderBy = sortIndices.stream()
                                  .map(entry -> String.format("%s (?%s)", entry.getValue(), props.get(entry.getKey()).getKey()))
                                  .collect(Collectors.joining(" "));
      String orderByClause = orderBy.length() > 0 ? "ORDER BY " + orderBy : "";

      String limitClause = limit > 0 ? "LIMIT " + limit : "";

      return String.format(""
              + "SELECT DISTINCT %s\n"
              + "WHERE {\n"
              + "  GRAPH ?g {\n"
              + "    ?s a %s .\n"
              + "    %s\n"
              + "  }\n"
              + "%s"
              + "}\n"
              + "%s\n"
              + "%s",
          selectClause, typeClause, whereClause, filterClause, orderByClause, limitClause
      ).trim();
    }
  }
}
