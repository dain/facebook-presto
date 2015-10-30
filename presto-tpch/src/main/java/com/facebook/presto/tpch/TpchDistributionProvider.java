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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ConnectorBucketFunction;
import com.facebook.presto.spi.ConnectorDistributionHandle;
import com.facebook.presto.spi.ConnectorDistributionProvider;
import com.facebook.presto.spi.ConnectorPartitionFunctionHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.tpch.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class TpchDistributionProvider
        implements ConnectorDistributionProvider
{
    private final String connectorId;
    private final NodeManager nodeManager;
    private final int splitsPerNode;

    public TpchDistributionProvider(String connectorId, NodeManager nodeManager, int splitsPerNode)
    {
        this.connectorId = connectorId;
        this.nodeManager = nodeManager;
        checkArgument(splitsPerNode > 0, "splitsPerNode must be at least 1");
        this.splitsPerNode = splitsPerNode;
    }

    @Override
    public ConnectorBucketFunction getBucketFunction(
            ConnectorSession session,
            ConnectorPartitionFunctionHandle functionHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        int totalRows = checkType(functionHandle, TpchPartitionFunctionHandle.class, "functionHandle").getTotalRows();
        int rowsPerBucket = totalRows / bucketCount;
        checkArgument(partitionChannelTypes.equals(ImmutableList.of(BIGINT)), "Expected one BIGINT parameter");
        return new TpchBucketFunction(bucketCount, rowsPerBucket);
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorSession session, ConnectorDistributionHandle distributionHandle)
    {
        Set<Node> nodes = nodeManager.getActiveDatasourceNodes(connectorId);
        checkState(!nodes.isEmpty(), "No TPCH nodes available");

        // Split the data using split and skew by the number of nodes available.
        ImmutableMap.Builder<Integer, Node> distribution = ImmutableMap.builder();
        int partNumber = 0;
        for (Node node : nodes) {
            for (int i = 0; i < splitsPerNode; i++) {
                distribution.put(partNumber, node);
                partNumber++;
            }
        }
        return distribution.build();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorSession session, ConnectorDistributionHandle distributionHandle)
    {
        return value -> checkType(value, TpchSplit.class, "value").getPartNumber();
    }
}
