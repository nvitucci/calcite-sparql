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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class SparqlPropToEnumerableConverterRule extends ConverterRule {
  public static final ConverterRule INSTANCE = Config.INSTANCE
      .withConversion(RelNode.class, SparqlPropRel.CONVENTION,
          EnumerableConvention.INSTANCE, "SparqlToEnumerableConverterRule")
      .withRuleFactory(SparqlPropToEnumerableConverterRule::new)
      .toRule(SparqlPropToEnumerableConverterRule.class);

  private SparqlPropToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new SparqlPropToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}