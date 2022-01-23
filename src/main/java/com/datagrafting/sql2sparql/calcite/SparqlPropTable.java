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

import com.datagrafting.sql2sparql.calcite.rel.SparqlPropRel;
import com.datagrafting.sql2sparql.calcite.rel.SparqlPropTableScan;
import com.datagrafting.sql2sparql.sparql.SparqlEndpoint;

public class SparqlPropTable extends SparqlTable {
  public SparqlPropTable(String tableName, String prop, SparqlEndpoint endpoint) {
    super(tableName, prop, endpoint);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    Set<String> probeObjects;

    // TODO: init in config
    int limit = 10;

    try {
      probeObjects = endpoint.getObjectTypesForProperty(prop, limit);
    } catch (SQLException e) {
      throw new RuntimeException("SQL Exception", e);
    }

    builder.add("s", typeFactory.createSqlType(SqlTypeName.VARCHAR));

    if (probeObjects.size() > 1) {
      throw new RuntimeException("Too many object types for property " + prop);
    }

    String potentialType = probeObjects.toArray(new String[0])[0];

    if (potentialType == null) {
      // Object property or plain literal, treat as string
      builder.add("o", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    } else {
      builder.add("o", typeFactory.createSqlType(
          datatypeToSqlType(potentialType, true)));
    }

    return builder.build();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new SparqlPropTableScan(cluster, cluster.traitSetOf(SparqlPropRel.CONVENTION), relOptTable, tableName, prop);
  }
}
