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
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.Pair;

import com.datagrafting.sql2sparql.calcite.config.TableMapping;

public class SparqlMappingTableScan extends TableScan implements SparqlClassRel {
  private final TableMapping tableMapping;

  public SparqlMappingTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relOptTable, TableMapping tableMapping) {
    super(cluster, traitSet, new ArrayList<>(), relOptTable);
    this.tableMapping = tableMapping;

    assert getConvention() == SparqlClassRel.CONVENTION;
  }

  @Override
  public void register(RelOptPlanner planner) {
    // for (RelOptRule rule : planner.getRules()) {
    //     planner.removeRule(rule);
    // }

    // E.g. or removing rule:
    // planner.removeRule(ReduceExpressionsRule.FilterReduceExpressionsRule.Config.DEFAULT.toRule());

    planner.addRule(SparqlClassToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : SparqlClassRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.table = table;
    implementor.props.addAll(tableMapping.getColumns().stream()
                                         .map(item -> new Pair<String, String>(item.getName(), item.getProperty()))
                                         .collect(Collectors.toList())
    );
  }
}
