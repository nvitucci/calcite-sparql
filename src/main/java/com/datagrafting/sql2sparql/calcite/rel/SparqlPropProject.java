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
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

public class SparqlPropProject extends Project implements SparqlPropRel {

  protected SparqlPropProject(RelOptCluster cluster, RelTraitSet traits, RelNode input,
                              List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, new ArrayList<>(), input, projects, rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    if (cost == null) {
      return null;
    }

    return cost.multiplyBy(0.1);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new SparqlPropProject(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    System.out.println(getProjects());
    System.out.println(getNamedProjects());

    for (RexNode project : getProjects()) {
      int projectIndex = ((RexInputRef) project).getIndex();
      implementor.projects.add(projectIndex);
    }
  }

  @Override
  public RelOptTable getTable() {
    return input.getTable();
  }
}
