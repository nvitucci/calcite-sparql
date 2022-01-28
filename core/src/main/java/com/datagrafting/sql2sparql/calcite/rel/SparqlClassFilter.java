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

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import com.google.common.collect.Range;

public class SparqlClassFilter extends Filter implements SparqlClassRel {
  public final static String PLACEHOLDER = "_VAR_";

  protected SparqlClassFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    assert condition instanceof RexCall;

    // TODO: more filters and unify with scan()
    switch (condition.getKind()) {
      // case IN:
      case SEARCH:
        search(implementor);
        break;

      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN_OR_EQUAL:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case LESS_THAN:
        eq(implementor);
        break;

      default:
        throw new UnsupportedOperationException(condition.getKind() + " filter not supported");
    }
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new SparqlClassFilter(getCluster(), traitSet, input, condition);
  }

  public void eq(Implementor implementor) {
    SqlOperator operator = ((RexCall) condition).getOperator();
    String sparqlOperator = operator.getName().equals("<>") ? "!=" : operator.getName();

    RexNode left = ((RexCall) condition).getOperands().get(0);
    RexNode right = ((RexCall) condition).getOperands().get(1);

    System.out.printf("Left: %s %s %s\nRight: %s %s %s\n",
        left, left.getKind(), left.getType(),
        right, right.getKind(), right.getType());

    if (left.getKind() == SqlKind.INPUT_REF && right.getKind() == SqlKind.LITERAL) {
      int index = ((RexInputRef) left).getIndex();
      Object value = ((RexLiteral) right).getValue2();

      // TODO: review this
      if (left.getType().getSqlTypeName().getFamily().equals(SqlTypeFamily.CHARACTER)) {
        implementor.filters.put(index, String.format("%s %s '%s'", PLACEHOLDER, sparqlOperator, value));
      } else {
        implementor.filters.put(index, String.format("%s %s %s", PLACEHOLDER, sparqlOperator, value));
      }
    }
  }

  public void search(Implementor implementor) {
    System.out.println(((RexCall) condition).getOperands());
    RexNode left = ((RexCall) condition).getOperands().get(0);
    RexNode right = ((RexCall) condition).getOperands().get(1);

    System.out.printf("Left: %s %s %s %s\nRight: %s %s %s %s\n",
        left, left.getKind(), left.getType(), left.getClass(),
        right, right.getKind(), right.getType(), right.getClass());

    if (left.getKind() == SqlKind.INPUT_REF && right.getKind() == SqlKind.LITERAL) {
      int index = ((RexInputRef) left).getIndex();
      Sarg<?> value = ((RexLiteral) right).getValueAs(Sarg.class);
      Set<String> filters = new HashSet<>();

      // TODO: review this. It also needs more datatypes.
      if (left.getType().getSqlTypeName().getFamily().equals(SqlTypeFamily.CHARACTER)) {
        for (Range<?> range : value.rangeSet.asRanges()) {
          if (range.lowerEndpoint() instanceof NlsString && range.upperEndpoint() instanceof NlsString) {
            String lower = ((NlsString) range.lowerEndpoint()).getValue();
            String upper = ((NlsString) range.upperEndpoint()).getValue();

            if (lower.equals(upper)) {
              filters.add(String.format("%s = '%s'", PLACEHOLDER, lower));
            } else {
              filters.add(String.format("(%s >= '%s' && %s <= '%s')", PLACEHOLDER, lower, PLACEHOLDER, upper));
            }
          }
        }

        implementor.filters.put(index, filters.stream().map(Object::toString).collect(Collectors.joining(" || ")));
      } else {
        for (Range<?> range : value.rangeSet.asRanges()) {
          if (range.lowerEndpoint() instanceof BigDecimal && range.upperEndpoint() instanceof BigDecimal) {
            long lower = ((BigDecimal) range.lowerEndpoint()).longValue();
            long upper = ((BigDecimal) range.upperEndpoint()).longValue();

            if (lower == upper) {
              filters.add(String.format("%s = %s", PLACEHOLDER, lower));
            } else {
              filters.add(String.format("(%s >= %s && %s <= %s)", PLACEHOLDER, lower, PLACEHOLDER, upper));
            }
          }
        }

        implementor.filters.put(index, filters.stream().map(Object::toString).collect(Collectors.joining(" || ")));
      }
    }
  }
}
