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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.FLUSHING;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static com.facebook.presto.execution.buffer.BufferState.NO_MORE_PAGES;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class PartitionedBuffer
        implements OutputBuffer
{
    private final String taskInstanceId;
    private final StateMachine<BufferState> state;
    private final OutputBuffers outputBuffers;
    private final SharedBufferMemoryManager memoryManager;

    private final Map<Integer, Partition> partitions;
    private final Map<TaskId, Partition> partitionsByTask;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();

    public PartitionedBuffer(String taskInstanceId, StateMachine<BufferState> state, OutputBuffers outputBuffers, SharedBufferMemoryManager memoryManager)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.state = requireNonNull(state, "state is null");

        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers.getType() == PARTITIONED, "Expected a PARTITIONED output buffer descriptor");
        checkArgument(outputBuffers.isNoMoreBufferIds(), "Expected a final output buffer descriptor");
        this.outputBuffers = outputBuffers;

        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

        ImmutableMap.Builder<Integer, Partition> partitions = ImmutableMap.builder();
        ImmutableMap.Builder<TaskId, Partition> partitionsByTask = ImmutableMap.builder();
        for (Entry<TaskId, Integer> entry : outputBuffers.getBuffers().entrySet()) {
            Partition partition = new Partition(taskInstanceId, entry.getValue(), entry.getKey(), state, memoryManager);
            partitions.put(entry.getValue(), partition);
            partitionsByTask.put(entry.getKey(), partition);
        }
        this.partitions = partitions.build();
        this.partitionsByTask = partitionsByTask.build();

        state.compareAndSet(OPEN, NO_MORE_BUFFERS);
        state.compareAndSet(NO_MORE_PAGES, FLUSHING);
        checkFlushComplete();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    @Override
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    @Override
    public SharedBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = this.state.get();

        int totalBufferedBytes = 0;
        int totalBufferedPages = 0;
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (Partition partition : partitions.values()) {
            BufferInfo bufferInfo = partition.getInfo();
            infos.add(bufferInfo);

            PageBufferInfo pageBufferInfo = bufferInfo.getPageBufferInfo();
            totalBufferedPages += pageBufferInfo.getBufferedPages();
            totalBufferedBytes += pageBufferInfo.getBufferedBytes();
        }

        return new SharedBufferInfo(
                "PARTITIONED",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                totalBufferedBytes,
                totalBufferedPages,
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                infos.build());
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<?> enqueue(Page page)
    {
        throw new UnsupportedOperationException("PartitionedBuffer requires a partition number");
    }

    @Override
    public ListenableFuture<?> enqueue(int partitionNumber, Page page)
    {
        requireNonNull(page, "page is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return immediateFuture(true);
        }

        List<Page> pages = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

        long rowCount = pages.stream().mapToLong(Page::getPositionCount).sum();
        checkState(rowCount == page.getPositionCount());
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(pages.size());

        return partitions.get(partitionNumber).enqueuePages(pages);
    }

    @Override
    public CompletableFuture<BufferResult> get(TaskId outputId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputId, "outputId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        BufferState state = this.state.get();
        if (state != FAILED && partitionsByTask.get(outputId) == null) {
            return completedFuture(emptyResults(taskInstanceId, 0, true));
        }

        return partitionsByTask.get(outputId).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void abort(TaskId outputId)
    {
        requireNonNull(outputId, "outputId is null");

        Partition partition = partitionsByTask.get(outputId);
        if (partition != null) {
            partition.destroy();
        }

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        state.compareAndSet(OPEN, NO_MORE_PAGES);
        state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
        memoryManager.setNoBlockOnFull();

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            partitions.values().forEach(Partition::destroy);
        }
    }

    @Override
    public void fail()
    {
        // ignore fail if the buffer already in a terminal state.
        state.setIf(FAILED, oldState -> !oldState.isTerminal());

        // DO NOT destroy buffers.  The coordinator manages the teardown of failed queries.
    }

    private void checkFlushComplete()
    {
        if (state.get() != FLUSHING) {
            return;
        }

        for (Partition partition : partitions.values()) {
            if (!partition.isFinished()) {
                return;
            }
        }
        destroy();
    }

    @ThreadSafe
    private static final class Partition
    {
        private final String taskInstanceId;
        private final int partitionNumber;
        private final TaskId bufferId;
        private final StateMachine<BufferState> state;
        private final SharedBufferMemoryManager memoryManager;

        private final AtomicLong rowsAdded = new AtomicLong(); // Number of rows added to the masterBuffer
        private final AtomicLong pagesAdded = new AtomicLong(); // Number of pages added to the masterBuffer

        private final AtomicLong bufferedBytes = new AtomicLong();

        private final AtomicLong currentSequenceId = new AtomicLong();
        private final AtomicBoolean finished = new AtomicBoolean();

        @GuardedBy("this")
        private final Deque<Page> pages = new ArrayDeque<>();

        @GuardedBy("this")
        private PendingRead pendingRead;

        private Partition(String taskInstanceId, int partitionNumber, TaskId bufferId, StateMachine<BufferState> state, SharedBufferMemoryManager memoryManager)
        {
            this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
            this.partitionNumber = partitionNumber;
            this.state = requireNonNull(state, "state is null");
            this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
        }

        public BufferInfo getInfo()
        {
            //
            // NOTE: this code must be lock free to we are not hanging state machine updates
            //

            long sequenceId = this.currentSequenceId.get();
            int bufferedPages = Math.max(Ints.checkedCast(pagesAdded.get() - sequenceId), 0);
            PageBufferInfo pageBufferInfo = new PageBufferInfo(partitionNumber, bufferedPages, bufferedBytes.get(), rowsAdded.get(), pagesAdded.get());

            if (finished.get()) {
                return new BufferInfo(bufferId, true, 0, sequenceId, pageBufferInfo);
            }

            return new BufferInfo(bufferId, finished.get(), bufferedPages, sequenceId, pageBufferInfo);
        }

        public boolean isFinished()
        {
            return finished.get();
        }

        public void destroy()
        {
            long bytesRemoved;
            PendingRead pendingRead;
            synchronized (this) {
                pages.clear();
                bytesRemoved = bufferedBytes.getAndSet(0);
                finished.set(true);

                pendingRead = this.pendingRead;
                this.pendingRead = null;
            }

            // update memory manager out side of lock to not trigger callbacks while holding the lock
            memoryManager.updateMemoryUsage(bytesRemoved);

            if (pendingRead != null) {
                pendingRead.abort();
            }
        }

        public ListenableFuture<?> enqueuePages(Collection<Page> pages)
        {
            long bytesAdded;
            PendingRead pendingRead;
            synchronized (this) {
                // ignore pages after finish
                // this can happen with limit queries
                if (finished.get()) {
                    return immediateFuture(null);
                }

                this.pages.addAll(pages);

                long rowCount = pages.stream().mapToLong(Page::getPositionCount).sum();
                rowsAdded.addAndGet(rowCount);
                pagesAdded.addAndGet(pages.size());

                bytesAdded = pages.stream().mapToLong(Page::getSizeInBytes).sum();
                bufferedBytes.addAndGet(bytesAdded);

                pendingRead = this.pendingRead;
                this.pendingRead = null;
            }

            // update memory manager out side of lock to not trigger callbacks while holding the lock
            memoryManager.updateMemoryUsage(bytesAdded);

            // we just added a page, so process the pending read
            if (pendingRead != null) {
                BufferResult bufferResult = processRead(pendingRead.getSequenceId(), pendingRead.getMaxSize());
                pendingRead.getResultFuture().complete(bufferResult);
            }

            return memoryManager.getNotFullFuture();
        }

        public CompletableFuture<BufferResult> getPages(long sequenceId, DataSize maxSize)
        {
            // acknowledge pages first, out side of locks to not trigger callbacks while holding the lock
            acknowledgePages(sequenceId);

            synchronized (this) {
                // Each buffer is private to a single client, and each client should only have one outstanding
                // read.  Therefore, we abort the existing read since it was most likely abandoned by the client.
                if (pendingRead != null) {
                    pendingRead.abort();
                    pendingRead = null;
                }

                // if buffer is finished return an empty page
                // this could be a request for a buffer that never existed, but that is ok since the buffer
                // could have been destroyed before the creation message was received
                if (finished.get()) {
                    return completedFuture(emptyResults(taskInstanceId, currentSequenceId.get(), true));
                }

                // if request is for pages before the current position, just return an empty page
                if (sequenceId < currentSequenceId.get()) {
                    return completedFuture(emptyResults(taskInstanceId, sequenceId, false));
                }

                // Return results immediately if we have data or are already finished
                // Additionally, if the read is for data before the currently acknowledged pages, this is
                // likely an out of order request, so return an empty result.
                if (!pages.isEmpty() || finished.get() || sequenceId < currentSequenceId.get()) {
                    return completedFuture(processRead(sequenceId, maxSize));
                }

                pendingRead = new PendingRead(taskInstanceId, sequenceId, maxSize);
                return pendingRead.getResultFuture();
            }
        }

        /**
         * @return a result with at least one page if we have pages in buffer, empty result otherwise
         */
        private synchronized BufferResult processRead(long sequenceId, DataSize maxSize)
        {
            // if request is for pages before the current position, just return an empty result
            // if it is for pages after the current position, there was an error somewhere, so return
            // and empty result also, and the error will be resolved when the client reconnects
            if (sequenceId != currentSequenceId.get()) {
                return emptyResults(taskInstanceId, sequenceId, false);
            }

            // if buffer is finished, return an empty result
            if (finished.get()) {
                return emptyResults(taskInstanceId, currentSequenceId.get(), true);
            }

            // read the new pages
            long maxBytes = maxSize.toBytes();
            List<Page> pages = new ArrayList<>();
            long bytes = 0;

            for (int i = 0; i < pages.size(); i++) {
                Page page = pages.get(i);
                bytes += page.getSizeInBytes();
                // break (and don't add) if this page would exceed the limit
                if (!pages.isEmpty() && bytes > maxBytes) {
                    break;
                }
                pages.add(page);
            }
            return new BufferResult(taskInstanceId, sequenceId, sequenceId + pages.size(), false, pages);
        }

        /**
         * Drops pages up to the specified sequence id
         */
        private void acknowledgePages(long sequenceId)
        {
            checkState(!Thread.holdsLock(this), "Can not acknowledge pages while holding a lock on this");

            long bytesRemoved = 0;
            synchronized (this) {
                int pagesToRemove = Ints.checkedCast(sequenceId - currentSequenceId.get());
                checkState(
                        pages.size() >= pagesToRemove,
                        "MasterBuffer does not have any pages to remove: pagesToRemove %s currentSequenceId: %s newSequenceId: %s",
                        pagesToRemove,
                        currentSequenceId.get(),
                        sequenceId);

                for (int i = 0; i < pagesToRemove; i++) {
                    Page page = pages.removeFirst();
                    bytesRemoved += page.getSizeInBytes();
                }

                bufferedBytes.addAndGet(-bytesRemoved);
                verify(bufferedBytes.get() >= 0);

                // if there are no pages, and there will be no more pages, then we are finished
                if (pages.isEmpty() && !state.get().canAddPages()) {
                    finished.set(true);
                }
            }

            // update memory manager out side of lock to not trigger callbacks while holding the lock
            memoryManager.updateMemoryUsage(-bytesRemoved);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bufferId", bufferId)
                    .add("sequenceId", currentSequenceId.get())
                    .add("finished", finished.get())
                    .toString();
        }

        @Immutable
        private static class PendingRead
        {
            private final String taskInstanceId;
            private final long sequenceId;
            private final DataSize maxSize;
            private final CompletableFuture<BufferResult> resultFuture = new CompletableFuture<>();

            public PendingRead(String taskInstanceId, long sequenceId, DataSize maxSize)
            {
                this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
                this.sequenceId = sequenceId;
                this.maxSize = maxSize;
            }

            public long getSequenceId()
            {
                return sequenceId;
            }

            public DataSize getMaxSize()
            {
                return maxSize;
            }

            public CompletableFuture<BufferResult> getResultFuture()
            {
                return resultFuture;
            }

            public void abort()
            {
                resultFuture.complete(emptyResults(taskInstanceId, sequenceId, false));
            }
        }
    }
}
