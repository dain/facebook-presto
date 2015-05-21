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

import com.facebook.presto.HashPagePartitionFunction;
import com.facebook.presto.OutputBuffers;
import com.facebook.presto.PagePartitionFunction;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.GuardedBy;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

class HashOutputBufferManager
        implements OutputBufferManager
{
    private final Consumer<OutputBuffers> outputBufferTarget;
    private final BiFunction<Integer, Integer, PagePartitionFunction> pagePartitionFunctionGenerator;

    @GuardedBy("this")
    private final Set<TaskId> bufferIds = new LinkedHashSet<>();

    @GuardedBy("this")
    private boolean noMoreBufferIds;

    public HashOutputBufferManager(Consumer<OutputBuffers> outputBufferTarget, PlanFragment fragment)
    {
        this.outputBufferTarget = requireNonNull(outputBufferTarget, "outputBufferTarget is null");

        requireNonNull(fragment, "fragment is null");
        checkState(fragment.getOutputPartitioning() == OutputPartitioning.HASH, "fragment is not hash partitioned");

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        ImmutableList<Integer> partitionChannels = fragment.getPartitionBy().stream()
                .map(symbol -> fragment.getOutputLayout().indexOf(symbol))
                .collect(toImmutableList());
        Optional<Integer> hashChannel = fragment.getHash().map(fragment.getOutputLayout()::indexOf);
        this.pagePartitionFunctionGenerator = new HashPagePartitionFunctionGenerator(partitionChannels, hashChannel, fragment.getTypes());

    }

    HashOutputBufferManager(Consumer<OutputBuffers> outputBufferTarget, HashPagePartitionFunctionGenerator pagePartitionFunctionGenerator)
    {
        this.outputBufferTarget = requireNonNull(outputBufferTarget, "outputBufferTarget is null");
        this.pagePartitionFunctionGenerator = requireNonNull(pagePartitionFunctionGenerator, "pagePartitionFunctionGenerator is null");
    }

    @Override
    public synchronized void addOutputBuffer(TaskId bufferId)
    {
        if (noMoreBufferIds) {
            // a stage can move to a final state (e.g., failed) while scheduling, so ignore
            // the new buffers
            return;
        }
        bufferIds.add(bufferId);
    }

    @Override
    public void noMoreOutputBuffers()
    {
        OutputBuffers outputBuffers;
        synchronized (this) {
            if (noMoreBufferIds) {
                // already created the buffers
                return;
            }
            noMoreBufferIds = true;

            ImmutableMap.Builder<TaskId, PagePartitionFunction> buffers = ImmutableMap.builder();
            int partition = 0;
            int partitionCount = bufferIds.size();
            for (TaskId bufferId : bufferIds) {
                buffers.put(bufferId, pagePartitionFunctionGenerator.apply(partition, partitionCount));
                partition++;
            }
            outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                    .withBuffers(buffers.build())
                    .withNoMoreBufferIds();
        }

        outputBufferTarget.accept(outputBuffers);
    }

    static class HashPagePartitionFunctionGenerator
            implements BiFunction<Integer, Integer, PagePartitionFunction>
    {
        private final List<Integer> partitionChannels;
        private final Optional<Integer> hashChannel;
        private final List<Type> types;

        public HashPagePartitionFunctionGenerator(List<Integer> partitionChannels, Optional<Integer> hashChannel, List<Type> types)
        {
            this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public PagePartitionFunction apply(Integer partition, Integer partitionCount)
        {
            return new HashPagePartitionFunction(partition, partitionCount, partitionChannels, hashChannel, types);
        }
    }
}
