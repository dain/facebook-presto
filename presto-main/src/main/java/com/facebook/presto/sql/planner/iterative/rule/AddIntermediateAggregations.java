/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.matching.Pattern.empty;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.groupingKeys;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.step;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Adds INTERMEDIATE aggregations between an un-grouped FINAL aggregation and its preceding
 * PARTIAL aggregation.
 * <p>
 * From:
 * <pre>
 * - Aggregation (FINAL)
 *   - RemoteExchange (GATHER)
 *     - Aggregation (PARTIAL)
 * </pre>
 * To:
 * <pre>
 * - Aggregation (FINAL)
 *   - LocalExchange (GATHER)
 *     - Aggregation (INTERMEDIATE)
 *       - LocalExchange (ARBITRARY)
 *         - RemoteExchange (GATHER)
 *           - Aggregation (INTERMEDIATE)
 *             - LocalExchange (GATHER)
 *               - Aggregation (PARTIAL)
 * </pre>
 * <p>
 */
public class AddIntermediateAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            // Only consider FINAL un-grouped aggregations
            .with(step().equalTo(AggregationNode.Step.FINAL))
            .with(empty(groupingKeys()))
            // Only consider aggregations without ORDER BY clause
            .matching(node -> !node.hasOrderings());

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return SystemSessionProperties.isEnableIntermediateAggregations(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        Lookup lookup = context.getLookup();
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();
        SymbolAllocator symbolAllocator = context.getSymbolAllocator();
        Session session = context.getSession();

        Optional<PlanNode> rewrittenSource = recurseToPartial(lookup.resolve(aggregation.getSource()), lookup, idAllocator, symbolAllocator);

        if (!rewrittenSource.isPresent()) {
            return Result.empty();
        }

        PlanNode source = rewrittenSource.get();

        if (getTaskConcurrency(session) > 1) {
            source = ExchangeNode.partitionedExchange(
                    idAllocator.getNextId(),
                    ExchangeNode.Scope.LOCAL,
                    source,
                    new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), source.getOutputSymbols()));
            source = new AggregationNode(
                    idAllocator.getNextId(),
                    source,
                    inputsAsOutputs(aggregation.getAggregations()),
                    aggregation.getGroupingSets(),
                    AggregationNode.Step.INTERMEDIATE,
                    aggregation.getHashSymbol(),
                    aggregation.getGroupIdSymbol(),
                    Optional.of(symbolAllocator.newSymbol("rowType", TINYINT)));
            source = ExchangeNode.gatheringExchange(idAllocator.getNextId(), ExchangeNode.Scope.LOCAL, source);
        }

        return Result.ofPlanNode(aggregation.replaceChildren(ImmutableList.of(source)));
    }

    /**
     * Recurse through a series of preceding ExchangeNodes and ProjectNodes to find the preceding PARTIAL aggregation
     */
    private Optional<PlanNode> recurseToPartial(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (node instanceof AggregationNode && ((AggregationNode) node).getStep() == AggregationNode.Step.PARTIAL) {
            return Optional.of(addGatheringIntermediate((AggregationNode) node, idAllocator, symbolAllocator));
        }

        if (!(node instanceof ExchangeNode) && !(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (PlanNode source : node.getSources()) {
            Optional<PlanNode> planNode = recurseToPartial(lookup.resolve(source), lookup, idAllocator, symbolAllocator);
            if (!planNode.isPresent()) {
                return Optional.empty();
            }
            builder.add(planNode.get());
        }
        return Optional.of(node.replaceChildren(builder.build()));
    }

    private PlanNode addGatheringIntermediate(AggregationNode aggregation, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        verify(aggregation.getGroupingKeys().isEmpty(), "Should be an un-grouped aggregation");
        ExchangeNode gatheringExchange = ExchangeNode.gatheringExchange(idAllocator.getNextId(), ExchangeNode.Scope.LOCAL, aggregation);
        return new AggregationNode(
                idAllocator.getNextId(),
                gatheringExchange,
                outputsAsInputs(aggregation.getAggregations()),
                aggregation.getGroupingSets(),
                AggregationNode.Step.INTERMEDIATE,
                aggregation.getHashSymbol(),
                aggregation.getGroupIdSymbol(),
                Optional.of(symbolAllocator.newSymbol("rowType", TINYINT)));
    }

    /**
     * Rewrite assignments so that inputs are in terms of the output symbols.
     * <p>
     * Example:
     * 'a' := sum('b') => 'a' := sum('a')
     * 'a' := count(*) => 'a' := count('a')
     */
    private static List<AggregationNode.Aggregation> outputsAsInputs(List<AggregationNode.Aggregation> assignments)
    {
        ImmutableList.Builder<AggregationNode.Aggregation> builder = ImmutableList.builder();
        for (AggregationNode.Aggregation aggregation : assignments) {
            Symbol output = aggregation.getOutputSymbol();
            checkState(!aggregation.getCall().getOrderBy().isPresent(), "Intermediate aggregation does not support ORDER BY");
            builder.add(new AggregationNode.Aggregation(
                    output,
                    new FunctionCall(QualifiedName.of(aggregation.getSignature().getName()), ImmutableList.of(output.toSymbolReference())),
                    aggregation.getSignature(),
                    Optional.empty()));  // No mask for INTERMEDIATE
        }
        return builder.build();
    }

    /**
     * Rewrite assignments so that outputs are in terms of the input symbols.
     * This operation only reliably applies to aggregation steps that take partial inputs (e.g. INTERMEDIATE and split FINALs),
     * which are guaranteed to have exactly one input and one output.
     * <p>
     * Example:
     * 'a' := sum('b') => 'b' := sum('b')
     */
    private static List<Aggregation> inputsAsOutputs(List<AggregationNode.Aggregation> assignments)
    {
        ImmutableList.Builder<AggregationNode.Aggregation> builder = ImmutableList.builder();
        for (Aggregation aggregation : assignments) {
            // Should only have one input symbol
            Symbol input = getOnlyElement(aggregation.getInputSymbols());
            builder.add(new Aggregation(
                    input,
                    aggregation.getCall(),
                    aggregation.getSignature(),
                    aggregation.getMask()));
        }
        return builder.build();
    }
}
