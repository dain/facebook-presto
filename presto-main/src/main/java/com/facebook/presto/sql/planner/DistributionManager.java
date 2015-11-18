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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSelector;
import com.facebook.presto.operator.BucketFunction;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.COORDINATOR_ONLY;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.FIXED;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.SINGLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public class DistributionManager
{
    private final NodeScheduler nodeScheduler;

    @Inject
    public DistributionManager(NodeScheduler nodeScheduler)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
    }

    public Distribution getDistribution(Session session, DistributionHandle distributionHandle)
    {
        requireNonNull(distributionHandle, "distributionHandle is null");

        if (distributionHandle instanceof SystemDistributionHandle) {
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(null);
            PlanDistribution planDistribution = ((SystemDistributionHandle) distributionHandle).getPlanDistribution();

            List<Node> nodes;
            if (planDistribution == COORDINATOR_ONLY) {
                nodes = ImmutableList.of(nodeSelector.selectCurrentNode());
            }
            else if (planDistribution == SINGLE) {
                nodes = nodeSelector.selectRandomNodes(1);
            }
            else if (planDistribution == FIXED) {
                nodes = nodeSelector.selectRandomNodes(getHashPartitionCount(session));
            }
            else {
                throw new IllegalArgumentException("Unsupported plan distribution " + planDistribution);
            }

            checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");

            ImmutableMap.Builder<Integer, Node> partitionToNode = ImmutableMap.builder();
            for (int i = 0; i < nodes.size(); i++) {
                Node node = nodes.get(i);
                partitionToNode.put(i, node);
            }
            return new Distribution(partitionToNode.build());
        }
        throw new IllegalArgumentException("Unsupported distribution handle " + distributionHandle.getClass().getName());
    }

    public PartitionFunction getPartitionFunction(Session session, PartitionFunctionBinding functionBinding, List<Type> partitionChannelTypes)
    {
        BucketFunction bucketFunction = functionBinding.getFunctionHandle().createBucketFunction(functionBinding, partitionChannelTypes);
        return new PartitionFunction(bucketFunction, functionBinding.getBucketToPartition().get());
    }
}
