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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

public class SparqlPropTableScan extends TableScan implements SparqlPropRel {
  private String tableName;
  private String prop;

  public SparqlPropTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relOptTable,
                             String tableName, String prop) {
    super(cluster, traitSet, new ArrayList<>(), relOptTable);
    this.tableName = tableName;
    this.prop = prop;
    assert getConvention() == SparqlPropRel.CONVENTION;
  }

  @Override
  public void register(RelOptPlanner planner) {
    // for (RelOptRule rule : planner.getRules()) {
    //     planner.removeRule(rule);
    // }

    // E.g. or removing rule:
    // planner.removeRule(ReduceExpressionsRule.FilterReduceExpressionsRule.Config.DEFAULT.toRule());

    planner.addRule(SparqlPropToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : SparqlPropRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.table = table;
    implementor.props.put(this.tableName, this.prop);
  }
}
