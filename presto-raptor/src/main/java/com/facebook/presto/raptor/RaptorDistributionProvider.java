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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.spi.ConnectorBucketFunction;
import com.facebook.presto.spi.ConnectorDistributionHandle;
import com.facebook.presto.spi.ConnectorDistributionProvider;
import com.facebook.presto.spi.ConnectorPartitionFunctionHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import static com.facebook.presto.raptor.util.Types.checkType;
import static java.util.Objects.requireNonNull;

public class RaptorDistributionProvider
        implements ConnectorDistributionProvider
{
    private final ShardManager shardManager;

    @Inject
    public RaptorDistributionProvider(ShardManager shardManager)
    {
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
    }

    @Override
    public ConnectorBucketFunction getBucketFunction(
            ConnectorSession session,
            ConnectorPartitionFunctionHandle functionHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        return new RaptorBucketFunction(bucketCount);
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorSession session, ConnectorDistributionHandle distributionHandle)
    {
        RaptorDistributionHandle handle = checkType(distributionHandle, RaptorDistributionHandle.class, "distributionHandle");
        return shardManager.getBucketAssignments(handle.getDistributionId());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorSession session, ConnectorDistributionHandle distributionHandle)
    {
        return value -> checkType(value, RaptorSplit.class, "value").getBucketNumber().getAsInt();
    }
}
