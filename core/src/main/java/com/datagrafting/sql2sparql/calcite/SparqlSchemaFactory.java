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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.datagrafting.sql2sparql.calcite.config.Config;
import com.datagrafting.sql2sparql.calcite.config.TableMapping;
import com.datagrafting.sql2sparql.calcite.config.TableMode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("UnusedDeclaration")
public class SparqlSchemaFactory implements SchemaFactory {
  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String endpointOp = (String) operand.get("endpoint");
    String tableModeOp = (String) operand.get("tableMode");
    List<?> tableMappingsOp = (List<?>) operand.get("tableMappings");

    TableMode tableMode;
    List<TableMapping> tableMappings = null;

    if (tableModeOp == null) {
      tableMode = TableMode.PROPERTY;
    } else {
      tableMode = TableMode.valueOf(tableModeOp.toUpperCase(Locale.ROOT));
    }

    if (tableMappingsOp != null) {
      ObjectMapper mapper = new ObjectMapper();
      tableMappings = tableMappingsOp.stream()
                           .map(item -> mapper.convertValue(item, TableMapping.class))
                           .collect(Collectors.toList());
    }

    // Maybe evaluate usage of Builder pattern
    Config config = new Config(endpointOp, tableMode, tableMappings);

    try {
      return new SparqlSchema(config, parentSchema, name);
    } catch (SQLException e) {
      throw new RuntimeException("Cannot connect to SPARQL endpoint " + endpointOp, e);
    }
  }
}
