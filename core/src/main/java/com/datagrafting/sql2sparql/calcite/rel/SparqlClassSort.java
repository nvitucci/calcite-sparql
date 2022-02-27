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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

public class SparqlClassSort extends Sort implements SparqlClassRel {
  public SparqlClassSort(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child, collation, offset, fetch);

    assert getConvention() == SparqlClassRel.CONVENTION;
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode input,
                   RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new SparqlClassSort(getCluster(), traitSet, input, collation, offset, fetch);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    // TODO: add offset
    if (fetch != null) {
      implementor.limit = RexLiteral.intValue(fetch);
    }

    for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
      implementor.sortIndices.add(new Pair<>(fieldCollation.getFieldIndex(),
          fieldCollation.getDirection().isDescending() ? "DESC" : "ASC"));
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);

    // TODO: what if cost null?
    return cost.multiplyBy(RelOptUtil.EPSILON);
  }

}
