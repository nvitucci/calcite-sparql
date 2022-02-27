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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;

public class SparqlPropRules {
  public static final List<RelOptRule> RULES = Arrays.asList(
      SparqlSortRule.INSTANCE,
      SparqlProjectRule.INSTANCE,
      SparqlFilterRule.INSTANCE
  );

  private static class SparqlSortRule extends ConverterRule {
    private static final SparqlSortRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalSort.class, Convention.NONE,
            SparqlPropRel.CONVENTION, "SparqlSortRule")
        .withRuleFactory(SparqlSortRule::new)
        .toRule(SparqlSortRule.class);

    protected SparqlSortRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalSort sort = (LogicalSort) rel;
      final RelTraitSet traitSet = sort.getTraitSet().replace(SparqlPropRel.CONVENTION);
      RelNode input = sort.getInput();
      RexNode offset = sort.offset;
      RexNode fetch = sort.fetch;

//            System.out.println("Sort: " + sort);
//            System.out.println("Sort input: " + sort.getInput());
//            System.out.println("Sort collation: " + sort.getCollation());
//            System.out.println("Sort offset: " + offset);
//            System.out.println("Sort fetch: " + fetch);

      return new SparqlPropSort(sort.getCluster(), traitSet,
          convert(input, input.getTraitSet().replace(out)), sort.getCollation(), offset, fetch);
    }
  }

  private static class SparqlProjectRule extends ConverterRule {
    private static final SparqlProjectRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalProject.class, Convention.NONE,
            SparqlPropRel.CONVENTION, "SparqlProjectRule")
        .withRuleFactory(SparqlProjectRule::new)
        .toRule(SparqlProjectRule.class);

    protected SparqlProjectRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(SparqlPropRel.CONVENTION);

      return new SparqlPropProject(project.getCluster(), traitSet,
          convert(project.getInput(), out),
          project.getProjects(), project.getRowType());
    }
  }

  private static class SparqlFilterRule extends ConverterRule {
    private static final SparqlFilterRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalFilter.class, Convention.NONE,
            SparqlPropRel.CONVENTION, "SparqlFilterRule")
        .withRuleFactory(SparqlFilterRule::new)
        .toRule(SparqlFilterRule.class);

    protected SparqlFilterRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(SparqlPropRel.CONVENTION);

      return new SparqlPropFilter(rel.getCluster(), traitSet,
          convert(filter.getInput(), SparqlPropRel.CONVENTION), filter.getCondition());
    }
  }
}
