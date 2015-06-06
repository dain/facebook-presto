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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.Type;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.execution.StageState.RUNNING;
import static com.facebook.presto.execution.StageState.SCHEDULED;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;

@NotThreadSafe
public class PhasedExecutionSchedule
        implements ExecutionSchedule
{
    private final List<Set<SqlStageExecution>> schedulePhases;
    private final Set<SqlStageExecution> activeSources = new HashSet<>();

    public PhasedExecutionSchedule(Collection<SqlStageExecution> stages)
    {
        List<Set<PlanFragmentId>> phases = extractPhases(stages.stream().map(SqlStageExecution::getFragment).collect(toImmutableList()));

        Map<PlanFragmentId, SqlStageExecution> stagesByFragmentId = stages.stream().collect(toImmutableMap(stage -> stage.getFragment().getId()));

        // create a mutable list of mutable sets of stages, so we can remove completed stages
        schedulePhases = new ArrayList<>();
        for (Set<PlanFragmentId> phase : phases) {
            schedulePhases.add(phase.stream()
                    .map(stagesByFragmentId::get)
                    .collect(Collectors.toCollection(HashSet::new)));
        }
    }

    @Override
    public Set<SqlStageExecution> getStagesToSchedule()
    {
        removeCompletedStages();
        addPhasesIfNecessary();
        if (isFinished()) {
            return ImmutableSet.of();
        }
        return activeSources;
    }

    private void removeCompletedStages()
    {
        for (Iterator<SqlStageExecution> stageIterator = activeSources.iterator(); stageIterator.hasNext(); ) {
            StageState state = stageIterator.next().getState();
            if (state == SCHEDULED || state == RUNNING || state.isDone()) {
                stageIterator.remove();
            }
        }
    }

    private void addPhasesIfNecessary()
    {
        // we want at least one source distributed phase in the active sources
        if (hasSourceDistributedStage(activeSources)) {
            return;
        }

        while (!schedulePhases.isEmpty()) {
            Set<SqlStageExecution> phase = schedulePhases.remove(0);
            activeSources.addAll(phase);
            if (hasSourceDistributedStage(phase)) {
                return;
            }
        }
    }

    private static boolean hasSourceDistributedStage(Set<SqlStageExecution> phase)
    {
        return phase.stream().anyMatch(stage -> stage.getFragment().getDistribution() == PlanDistribution.SOURCE);
    }

    @Override
    public boolean isFinished()
    {
        return activeSources.isEmpty() && schedulePhases.isEmpty();
    }

    static List<Set<PlanFragmentId>> extractPhases(Iterable<PlanFragment> fragments)
    {
        DirectedGraph<PlanFragmentId, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        fragments.forEach(fragment -> graph.addVertex(fragment.getId()));

        for (PlanFragment fragment : fragments) {
            fragment.getRoot().accept(new Visitor(fragment.getId(), graph), null);
        }

        // computes all the strongly connected components of the directed graph
        List<Set<PlanFragmentId>> components = new StrongConnectivityInspector<>(graph).stronglyConnectedSets();
        DirectedGraph<Set<PlanFragmentId>, DefaultEdge> componentGraph = new DefaultDirectedGraph<>(DefaultEdge.class);
        components.forEach(componentGraph::addVertex);
        for (Set<PlanFragmentId> sourceComponent : components) {
            for (Set<PlanFragmentId> targetComponent : components) {
                if (containsEdge(graph, sourceComponent, targetComponent)) {
                    componentGraph.addEdge(sourceComponent, targetComponent);
                }
            }
        }

        List<Set<PlanFragmentId>> schedulePhases = ImmutableList.copyOf(new TopologicalOrderIterator<>(componentGraph));
        return schedulePhases;
    }

    private static <T> boolean containsEdge(DirectedGraph<T, ?> graph, Set<T> sources, Set<T> targets)
    {
        for (T source : sources) {
            for (T target : targets) {
                if (graph.containsEdge(source, target)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static class Visitor
            extends PlanVisitor<Void, Set<PlanFragmentId>>
    {
        private final PlanFragmentId planFragmentId;
        private final DirectedGraph<PlanFragmentId, DefaultEdge> graph;

        public Visitor(PlanFragmentId planFragmentId, DirectedGraph<PlanFragmentId, DefaultEdge> graph)
        {
            this.planFragmentId = planFragmentId;
            this.graph = graph;
        }

        @Override
        public Set<PlanFragmentId> visitJoin(JoinNode node, Void context)
        {
            if (node.getType() == Type.RIGHT) {
                return processJoin(node.getLeft(), node.getRight(), context);
            }
            else {
                return processJoin(node.getRight(), node.getLeft(), context);
            }
        }

        @Override
        public Set<PlanFragmentId> visitSemiJoin(SemiJoinNode node, Void context)
        {
            return processJoin(node.getFilteringSource(), node.getSource(), context);
        }

        @Override
        public Set<PlanFragmentId> visitIndexJoin(IndexJoinNode node, Void context)
        {
            return processJoin(node.getIndexSource(), node.getProbeSource(), context);
        }

        private Set<PlanFragmentId> processJoin(PlanNode build, PlanNode probe, Void context)
        {
            Set<PlanFragmentId> buildSources = build.accept(this, context);
            Set<PlanFragmentId> probeSources = probe.accept(this, context);

            for (PlanFragmentId buildSource : buildSources) {
                for (PlanFragmentId probeSource : probeSources) {
                    graph.addEdge(buildSource, probeSource);
                }
            }

            return ImmutableSet.<PlanFragmentId>builder()
                    .addAll(buildSources)
                    .addAll(probeSources)
                    .build();
        }

        @Override
        public Set<PlanFragmentId> visitRemoteSource(RemoteSourceNode node, Void context)
        {
            node.getSourceFragmentIds()
                    .forEach(remoteSource -> graph.addEdge(planFragmentId, remoteSource));

            // If there are multiple sources, this is an unrolled union.  Link
            // the source fragments together, so we only schedule one at a time.
            PlanFragmentId previousSource = null;
            for (PlanFragmentId currentSource : node.getSourceFragmentIds()) {
                if (previousSource != null) {
                    graph.addEdge(previousSource, currentSource);
                }

                previousSource = currentSource;
            }

            return ImmutableSet.copyOf(node.getSourceFragmentIds());
        }

        @Override
        public Set<PlanFragmentId> visitUnion(UnionNode node, Void context)
        {
            ImmutableSet.Builder<PlanFragmentId> allSources = ImmutableSet.builder();

            // Link the source fragments together, so we only schedule one at a time.
            Set<PlanFragmentId> previousSources = ImmutableSet.of();
            for (PlanNode subPlanNode : node.getSources()) {
                Set<PlanFragmentId> currentSources = subPlanNode.accept(this, context);
                allSources.addAll(currentSources);

                for (PlanFragmentId currentSource : currentSources) {
                    for (PlanFragmentId previousSource : previousSources) {
                        graph.addEdge(previousSource, currentSource);
                    }
                }

                previousSources = currentSources;
            }

            return allSources.build();
        }

        @Override
        protected Set<PlanFragmentId> visitPlan(PlanNode node, Void context)
        {
            List<PlanNode> sources = node.getSources();
            if (sources.isEmpty()) {
                return ImmutableSet.of();
            }
            if (sources.size() == 1) {
                return sources.get(0).accept(this, context);
            }
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}
