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

import java.sql.SQLException;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import com.datagrafting.sql2sparql.calcite.config.ColumnSpec;
import com.datagrafting.sql2sparql.calcite.config.TableMapping;
import com.datagrafting.sql2sparql.calcite.rel.SparqlClassRel;
import com.datagrafting.sql2sparql.calcite.rel.SparqlMappingTableScan;
import com.datagrafting.sql2sparql.sparql.SparqlEndpoint;

public class SparqlMappingTable extends SparqlTable {
  // TODO: init in config
  public static final int MAX_PROBE_OBJECTS = 10;

  private final TableMapping tableMapping;

  public SparqlMappingTable(String tableName, String prop, TableMapping tableMapping, SparqlEndpoint endpoint) {
    // TODO: review this as tableName and prop not needed but SparqlTable constructor uses both
    super(tableName, prop, endpoint);
    this.tableMapping = tableMapping;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    Set<String> probeObjects;

    try {
      builder.add("s", typeFactory.createSqlType(SqlTypeName.VARCHAR));

      for (ColumnSpec col : tableMapping.getColumns()) {
        probeObjects = endpoint.getObjectTypesForProperty(col.getProperty(), MAX_PROBE_OBJECTS);

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

        builder.add(col.getName(), type);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    }

    return builder.build();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new SparqlMappingTableScan(cluster, cluster.traitSetOf(SparqlClassRel.CONVENTION), relOptTable, tableMapping);
  }
}
