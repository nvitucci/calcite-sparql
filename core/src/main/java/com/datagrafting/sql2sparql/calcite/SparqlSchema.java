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
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.datagrafting.sql2sparql.calcite.config.Config;
import com.datagrafting.sql2sparql.calcite.config.TableMapping;
import com.datagrafting.sql2sparql.calcite.config.TableMode;
import com.datagrafting.sql2sparql.sparql.SparqlEndpoint;

public class SparqlSchema extends AbstractSchema {
  private Map<String, Table> tableMap;
  private final SparqlEndpoint endpoint;
  private final TableMode tableMode;
  private final Config config;
  private final SchemaPlus parentSchema;
  private final String name;

  public SparqlSchema(Config config, SchemaPlus parentSchema, String name) throws SQLException {
    this.endpoint = new SparqlEndpoint(config.getEndpoint());
    this.tableMode = config.getTableMode();
    this.config = config;
    this.parentSchema = parentSchema;
    this.name = name;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      try {
        tableMap = createTableMap();
      } catch (SQLException e) {
        throw new RuntimeException("SQL exception", e);
      }
    }

    return tableMap;
  }

  private Map<String, Table> createTableMap() throws SQLException {
    Map<String, Table> tableMap = new HashMap<>();
    Map<String, String> tableProps = new HashMap<>();

    switch (tableMode) {
      case PROPERTY:
        tableProps.putAll(endpoint.getProperties());

        for (Map.Entry<String, String> tableProp : tableProps.entrySet()) {
          tableMap.put(tableProp.getKey(), new SparqlPropTable(tableProp.getKey(), tableProp.getValue(), endpoint));
        }

        break;

      case CLASS:
        tableProps.putAll(endpoint.getClasses());

        for (Map.Entry<String, String> tableProp : tableProps.entrySet()) {
          tableMap.put(tableProp.getKey(), new SparqlClassTable(tableProp.getKey(), tableProp.getValue(), endpoint));
        }

        break;

      case MAPPING:
        for (TableMapping tableMapping : config.getTableMappings()) {
          // TODO: should remove "MAP" param
          tableMap.put(tableMapping.getName(), new SparqlMappingTable(tableMapping.getName(), "MAP", tableMapping, endpoint));
        }

        break;

      default:
        throw new RuntimeException("Unsupported table mode " + tableMode);
    }

    return tableMap;
  }
}
