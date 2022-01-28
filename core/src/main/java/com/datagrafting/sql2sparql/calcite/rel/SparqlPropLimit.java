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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class SparqlPropLimit extends SingleRel implements SparqlPropRel {
  private RexNode limit;

  protected SparqlPropLimit(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode limit) {
    super(cluster, traits, input);
    this.limit = limit;
    assert getConvention() == input.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    if (cost == null) {
      return null;
    }

    // Does not work
    // return planner.getCostFactory().makeZeroCost();
    // Does not work with "small" LIMITs
    // return cost.multiplyBy(0.1);
    return planner.getCostFactory().makeTinyCost().multiplyBy(0.1);
  }

  @Override
  public SparqlPropLimit copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SparqlPropLimit(getCluster(), traitSet, sole(inputs), limit);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    if (limit != null) {
      implementor.limit = RexLiteral.intValue(limit);
    }
  }
}
