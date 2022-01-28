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
package com.datagrafting.sql2sparql.calcite;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import com.datagrafting.sql2sparql.calcite.rel.SparqlClassRel;
import com.datagrafting.sql2sparql.calcite.rel.SparqlClassTableScan;
import com.datagrafting.sql2sparql.sparql.SparqlEndpoint;

public class SparqlClassTable extends SparqlTable {
  // TODO: init in config
  public static final int MAX_PROBE_OBJECTS = 10;
  public static final int MAX_PROPS_PER_TABLE = 25;

  private List<Pair<String, String>> columns;

  public SparqlClassTable(String tableName, String prop, SparqlEndpoint endpoint) {
    super(tableName, prop, endpoint);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    Set<String> probeObjects;

    try {
      builder.add("s", typeFactory.createSqlType(SqlTypeName.VARCHAR));

      for (Pair<String, String> col : getColumns()) {
        probeObjects = endpoint.getObjectTypesForProperty(col.getValue(), MAX_PROBE_OBJECTS);

        RelDataType type;
        if (probeObjects.size() > 1) {
          // Too many potential types, stick to string type
          type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        } else {
          String potentialType = probeObjects.toArray(new String[0])[0];

          if (potentialType == null) {
            type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
          } else {
            type = typeFactory.createSqlType(datatypeToSqlType(potentialType, true));
          }
        }

        builder.add(col.getKey(), type);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    }

    return builder.build();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new SparqlClassTableScan(cluster, cluster.traitSetOf(SparqlClassRel.CONVENTION), relOptTable, tableName,
        prop, getColumns());
  }

  private List<Pair<String, String>> getColumns() {
    // TODO: move this cache elsewhere?
    if (columns == null) {
      try {
        columns = endpoint.getPropertiesPerClass(prop, MAX_PROPS_PER_TABLE);
      } catch (SQLException | URISyntaxException e) {
        throw new RuntimeException("Runtime exception", e);
      }
    }

    return columns;
  }
}
