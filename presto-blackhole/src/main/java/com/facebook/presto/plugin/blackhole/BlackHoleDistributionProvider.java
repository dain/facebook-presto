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
package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorBucketFunction;
import com.facebook.presto.spi.ConnectorDistributionHandle;
import com.facebook.presto.spi.ConnectorDistributionProvider;
import com.facebook.presto.spi.ConnectorPartitionFunctionHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.abs;

public class BlackHoleDistributionProvider
        implements ConnectorDistributionProvider
{
    private final String connectorId;
    private final NodeManager nodeManager;

    public BlackHoleDistributionProvider(String connectorId, NodeManager nodeManager)
    {
        this.connectorId = connectorId;
        this.nodeManager = nodeManager;
    }

    @Override
    public ConnectorBucketFunction getBucketFunction(ConnectorSession session, ConnectorPartitionFunctionHandle functionHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        return (page, position) -> {
            int hash = 13;
            for (int i = 0; i < partitionChannelTypes.size(); i++) {
                Type type = partitionChannelTypes.get(i);
                hash = 31 * hash + type.hash(page.getBlock(i), position);
            }
            return abs(hash) % bucketCount;
        };
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorSession session, ConnectorDistributionHandle distributionHandle)
    {
        Set<Node> nodes = nodeManager.getActiveDatasourceNodes(connectorId);
        checkState(!nodes.isEmpty(), "No black hole nodes available");

        // Split the data using split and skew by the number of nodes available.
        ImmutableMap.Builder<Integer, Node> distribution = ImmutableMap.builder();
        int partNumber = 0;
        for (Node node : nodes) {
            distribution.put(partNumber, node);
            partNumber++;
        }
        return distribution.build();
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorSession session, ConnectorDistributionHandle distributionHandle)
    {
        return value -> {
            throw new PrestoException(NOT_SUPPORTED, "Black hole connector does not supported distributed reads");
        };
    }
}
