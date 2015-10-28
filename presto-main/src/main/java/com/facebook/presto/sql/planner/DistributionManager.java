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
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.ConnectorBucketFunction;
import com.facebook.presto.spi.ConnectorDistributionProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.SystemPartitionFunctionHandle.SystemPartitionFunctionId;
import com.google.common.base.MoreObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.ToIntFunction;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.COORDINATOR_ONLY;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.FIXED;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.SINGLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DistributionManager
{
    private final NodeScheduler nodeScheduler;
    private final ConcurrentMap<String, ConnectorDistributionProvider> distributionProviders = new ConcurrentHashMap<>();

    @Inject
    public DistributionManager(NodeScheduler nodeScheduler)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
    }

    public void addDistributionProvider(String connectorId, ConnectorDistributionProvider distributionProvider)
    {
        checkState(distributionProviders.putIfAbsent(connectorId, distributionProvider) == null, "DistributionProvider for connector '%s' is already registered", connectorId);
    }

    public PartitionFunction getPartitionFunction(Session session, PartitionFunctionBinding functionBinding, List<Type> partitionChannelTypes)
    {
        Optional<int[]> bucketToPartition = functionBinding.getBucketToPartition();
        checkArgument(bucketToPartition.isPresent(), "Bucket to partition must be set before a partition function can be created");

        PartitionFunctionHandle functionHandle = functionBinding.getFunctionHandle();
        BucketFunction bucketFunction;
        if (!functionHandle.getConnectorId().isPresent()) {
            bucketFunction = getSystemBucketFunction(
                    (SystemPartitionFunctionHandle) functionHandle.getConnectorHandle(),
                    functionBinding.getHashColumn().isPresent(),
                    partitionChannelTypes,
                    bucketToPartition.get().length);
        }
        else {
            ConnectorDistributionProvider distributionProvider = distributionProviders.get(functionHandle.getConnectorId().get());
            checkArgument(distributionProvider != null, "No distribution provider for connector %s", functionHandle.getConnectorId().get());

            ConnectorBucketFunction connectorPartitionGenerator = distributionProvider.getBucketFunction(
                    session.toConnectorSession(),
                    functionHandle.getConnectorHandle(),
                    partitionChannelTypes,
                    bucketToPartition.get().length);

            checkArgument(connectorPartitionGenerator != null, "No function %s", functionHandle);
            bucketFunction = connectorPartitionGenerator::getBucket;
        }
        return new PartitionFunction(bucketFunction, functionBinding.getBucketToPartition().get());
    }

    private static BucketFunction getSystemBucketFunction(SystemPartitionFunctionHandle functionHandle, boolean isPrecomputed, List<Type> partitionChannelTypes, int bucketCount)
    {
        if (functionHandle.getFunction() == SystemPartitionFunctionId.single) {
            checkState(bucketCount == 1, "Single partition can only have one bucket");
            return new SingleBucketFunction();
        }

        if (functionHandle.getFunction() == SystemPartitionFunctionId.roundRobin) {
            return new RoundRobinBucketFunction(bucketCount);
        }

        if (functionHandle.getFunction() == SystemPartitionFunctionId.hash) {
            if (isPrecomputed) {
                return new HashBucketFunction(new PrecomputedHashGenerator(0), bucketCount);
            }
            else {
                int[] hashChannels = new int[partitionChannelTypes.size()];
                for (int i = 0; i < partitionChannelTypes.size(); i++) {
                    hashChannels[i] = i;
                }

                return new HashBucketFunction(new InterpretedHashGenerator(partitionChannelTypes, hashChannels), bucketCount);
            }
        }

        throw new IllegalArgumentException("Unsupported plan function " + functionHandle);
    }

    public Distribution getDistribution(Session session, DistributionHandle distributionHandle)
    {
        requireNonNull(session, "session is null");
        requireNonNull(distributionHandle, "distributionHandle is null");

        if (!distributionHandle.getConnectorId().isPresent()) {
            return getSystemDistribution(session, distributionHandle);
        }

        ConnectorDistributionProvider distributionProvider = distributionProviders.get(distributionHandle.getConnectorId().get());
        checkArgument(distributionProvider != null, "No distribution provider for connector %s", distributionHandle.getConnectorId().get());

        Map<Integer, Node> bucketToNode = distributionProvider.getBucketToNode(session.toConnectorSession(), distributionHandle.getConnectorHandle());
        checkArgument(bucketToNode != null, "No distribution %s", distributionHandle);
        checkArgument(!bucketToNode.isEmpty(), "Distribution %s is empty", distributionHandle);

        int bucketCount = bucketToNode.keySet().stream()
                .mapToInt(Integer::intValue)
                .max()
                .getAsInt() + 1;

        int[] bucketToPartition = new int[bucketCount];
        BiMap<Node, Integer> nodeToPartition = HashBiMap.create();
        int nextPartitionId = 0;
        for (Entry<Integer, Node> entry : bucketToNode.entrySet()) {
            Integer partitionId = nodeToPartition.get(entry.getValue());
            if (partitionId == null) {
                partitionId = nextPartitionId++;
                nodeToPartition.put(entry.getValue(), partitionId);
            }
            bucketToPartition[entry.getKey()] = partitionId;
        }

        ToIntFunction<ConnectorSplit> splitBucketFunction = distributionProvider.getSplitBucketFunction(session.toConnectorSession(), distributionHandle.getConnectorHandle());
        checkArgument(splitBucketFunction != null, "No distribution %s", distributionHandle);

        return new Distribution(nodeToPartition.inverse(), bucketToPartition);
    }

    private Distribution getSystemDistribution(Session session, DistributionHandle distributionHandle)
    {
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(null);
        PlanDistribution planDistribution = ((SystemDistributionHandle) distributionHandle.getConnectorHandle()).getPlanDistribution();

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

        ImmutableMap.Builder<Integer, Node> distribution = ImmutableMap.builder();
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            distribution.put(i, node);
        }
        return new Distribution(distribution.build());
    }

    private static class SingleBucketFunction
            implements BucketFunction
    {
        @Override
        public int getBucket(Page page, int position)
        {
            return 0;
        }
    }

    private static class RoundRobinBucketFunction
            implements BucketFunction
    {
        private final int bucketCount;
        private int counter;

        public RoundRobinBucketFunction(int bucketCount)
        {
            checkArgument(bucketCount > 0, "bucketCount must be at least 1");
            this.bucketCount = bucketCount;
        }

        @Override
        public int getBucket(Page page, int position)
        {
            int bucket = counter % bucketCount;
            counter = (counter + 1) & 0x7fff_ffff;
            return bucket;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("bucketCount", bucketCount)
                    .toString();
        }
    }

    private static class HashBucketFunction
            implements BucketFunction
    {
        private final HashGenerator generator;
        private final int bucketCount;

        public HashBucketFunction(HashGenerator generator, int bucketCount)
        {
            this.generator = requireNonNull(generator, "generator is null");
            checkArgument(bucketCount > 0, "bucketCount must be at least 1");
            this.bucketCount = bucketCount;
        }

        @Override
        public int getBucket(Page page, int position)
        {
            return generator.getPartition(bucketCount, position, page);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("generator", generator)
                    .add("bucketCount", bucketCount)
                    .toString();
        }
    }
}
