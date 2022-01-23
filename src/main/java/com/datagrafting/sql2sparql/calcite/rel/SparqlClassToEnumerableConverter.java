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

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.runtime.Hook;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.datagrafting.sql2sparql.calcite.SparqlMethod;
import com.datagrafting.sql2sparql.calcite.SparqlQueryable;

public class SparqlClassToEnumerableConverter extends ConverterImpl implements EnumerableRel {
  protected SparqlClassToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, child);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SparqlClassToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    if (cost == null) {
      return null;
    }
    return cost.multiplyBy(0.1);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), rowType,
            pref.prefer(JavaRowFormat.ARRAY));
    final SparqlClassRel.Implementor sparqlImplementor = new SparqlClassRel.Implementor();
    sparqlImplementor.visitChild(0, getInput());

    String sparqlQuery = sparqlImplementor.getQuery();

    final Expression table =
        builder.append("table",
            sparqlImplementor.table.getExpression(
                SparqlQueryable.class));
    Expression enumerable =
        builder.append("enumerable",
            Expressions.call(table, SparqlMethod.SPARQL_QUERY.method, Expressions.constant(sparqlQuery)));
    builder.add(
        Expressions.return_(null, enumerable));
    Hook.QUERY_PLAN.run(sparqlQuery);

    return implementor.result(physType, builder.toBlock());
  }
}
