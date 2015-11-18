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

import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.Distribution;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

public class FixedSourcePartitionedScheduler
        implements StageScheduler
{
    private final SqlStageExecution stage;
    private final Distribution distribution;
    private final SourcePartitionedScheduler sourcePartitionedScheduler;
    private boolean scheduledTasks;

    public FixedSourcePartitionedScheduler(
            SqlStageExecution stage,
            SplitSource splitSource,
            Distribution distribution,
            int splitBatchSize,
            NodeTaskMap nodeTaskMap,
            int maxSplitsPerNode)
    {
        this.stage = stage;
        this.distribution = distribution;
        sourcePartitionedScheduler = new SourcePartitionedScheduler(stage, splitSource, new FixedSplitPlacementPolicy(distribution, nodeTaskMap, maxSplitsPerNode), splitBatchSize);
    }

    @Override
    public ScheduleResult schedule()
    {
        List<RemoteTask> newTasks = ImmutableList.of();
        if (!scheduledTasks) {
            newTasks = distribution.getPartitionToNode().entrySet().stream()
                    .map(entry -> stage.scheduleTask(entry.getValue(), entry.getKey()))
                    .collect(toImmutableList());
            scheduledTasks = true;
        }

        ScheduleResult schedule = sourcePartitionedScheduler.schedule();
        return new ScheduleResult(schedule.isFinished(), newTasks, schedule.getBlocked());
    }

    @Override
    public void close()
    {
        sourcePartitionedScheduler.close();
    }

    private static class FixedSplitPlacementPolicy
            implements SplitPlacementPolicy
    {
        private final Distribution distribution;
        private final NodeTaskMap nodeTaskMap;
        private final int maxSplitsPerNode;

        public FixedSplitPlacementPolicy(Distribution distribution, NodeTaskMap nodeTaskMap, int maxSplitsPerNode)
        {
            this.distribution = distribution;
            this.nodeTaskMap = nodeTaskMap;
            this.maxSplitsPerNode = maxSplitsPerNode;
        }

        @Override
        public Multimap<Node, Split> computeAssignments(Set<Split> splits)
        {
            SetMultimap<Node, Split> assignments = HashMultimap.create();
            Map<Node, Integer> splitCountByNode = new HashMap<>();
            for (Split split : splits) {
                Node node = distribution.getNode(split);

                int nodeSplitCount = splitCountByNode.computeIfAbsent(node, key -> nodeTaskMap.getPartitionedSplitsOnNode(node));
                int totalSplitCount = assignments.get(node).size() + nodeSplitCount;

                if (totalSplitCount < maxSplitsPerNode) {
                    assignments.put(node, split);
                }
            }
            return ImmutableMultimap.copyOf(assignments);
        }

        @Override
        public void lockDownNodes()
        {
        }

        @Override
        public List<Node> allNodes()
        {
            return ImmutableList.copyOf(distribution.getPartitionToNode().values());
        }
    }
}
