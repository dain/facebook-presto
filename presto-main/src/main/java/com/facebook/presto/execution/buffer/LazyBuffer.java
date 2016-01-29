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
import com.facebook.presto.execution.SystemMemoryUsageListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferState.FAILED;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LazyBuffer
        implements OutputBuffer
{
    private final StateMachine<BufferState> state;
    private final String taskInstanceId;
    private final SharedBufferMemoryManager memoryManager;

    @GuardedBy("this")
    private OutputBuffer delegate;

    @GuardedBy("this")
    private final Set<TaskId> abortedBuffers = new HashSet<>();

    @GuardedBy("this")
    private final List<PendingRead> pendingReads = new ArrayList<>();

    public LazyBuffer(TaskId taskId, String taskInstanceId, Executor executor, DataSize maxBufferSize, SystemMemoryUsageListener systemMemoryUsageListener)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(executor, "executor is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        state = new StateMachine<>(taskId + "-buffer", executor, OPEN, TERMINAL_BUFFER_STATES);
        requireNonNull(maxBufferSize, "maxBufferSize is null");
        checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1");
        requireNonNull(systemMemoryUsageListener, "systemMemoryUsageListener is null");
        this.memoryManager = new SharedBufferMemoryManager(maxBufferSize.toBytes(), systemMemoryUsageListener);
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
        return 0.0;
    }

    @Override
    public SharedBufferInfo getInfo()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        if (outputBuffer == null) {
            //
            // NOTE: this code must be lock free to not hanging state machine updates
            //
            BufferState state = this.state.get();

            return new SharedBufferInfo(
                    "UNINITIALIZED",
                    state,
                    state.canAddBuffers(),
                    state.canAddPages(),
                    0,
                    0,
                    0,
                    0,
                    ImmutableList.of());
        }
        return outputBuffer.getInfo();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        Set<TaskId> abortedBuffers = ImmutableSet.of();
        List<PendingRead> pendingReads = ImmutableList.of();
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                switch (newOutputBuffers.getType()) {
                    case SHARED:
                        delegate = new SharedBuffer(taskInstanceId, state, memoryManager);
                        break;
                    case ARBITRARY:
                        delegate = new ArbitraryBuffer(taskInstanceId, state, memoryManager);
                        break;
                }

                // process pending aborts and reads outside of synchronized lock
                abortedBuffers = ImmutableSet.copyOf(this.abortedBuffers);
                this.abortedBuffers.clear();
                pendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            outputBuffer = delegate;
        }

        outputBuffer.setOutputBuffers(newOutputBuffers);

        // process pending aborts and reads outside of synchronized lock
        abortedBuffers.forEach(outputBuffer::abort);
        for (PendingRead pendingRead : pendingReads) {
            pendingRead.process(outputBuffer);
        }
    }

    @Override
    public CompletableFuture<BufferResult> get(TaskId outputId, long startingSequenceId, DataSize maxSize)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                PendingRead pendingRead = new PendingRead(outputId, startingSequenceId, maxSize);
                pendingReads.add(pendingRead);
                return pendingRead.getFutureResult();
            }
            outputBuffer = delegate;
        }
        return outputBuffer.get(outputId, startingSequenceId, maxSize);
    }

    @Override
    public void abort(TaskId outputId)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                abortedBuffers.add(outputId);
                // Normally, we should free any pending readers for this buffer,
                // but we assume that the real buffer will be created quickly.
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.abort(outputId);
    }

    @Override
    public ListenableFuture<?> enqueue(Page page)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        return outputBuffer.enqueue(page);
    }

    @Override
    public ListenableFuture<?> enqueue(int partition, Page page)
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        return outputBuffer.enqueue(partition, page);
    }

    @Override
    public void setNoMorePages()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        outputBuffer.setNoMorePages();
    }

    @Override
    public void destroy()
    {
        OutputBuffer outputBuffer;
        List<PendingRead> pendingReads = ImmutableList.of();
        synchronized (this) {
            if (delegate == null) {
                state.set(FINISHED);
                pendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            outputBuffer = delegate;
        }

        // if there is no output buffer, free the pending reads
        if (outputBuffer == null) {
            for (PendingRead pendingRead : pendingReads) {
                pendingRead.getFutureResult().complete(emptyResults(taskInstanceId, 0, true));
            }
            return;
        }

        outputBuffer.destroy();
    }

    @Override
    public void fail()
    {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore fail if the buffer already in a terminal state.
                if (state.get().isTerminal()) {
                    return;
                }
                state.set(FAILED);
                // Do not free readers on fail
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.fail();
    }

    private static class PendingRead
    {
        private final TaskId outputId;
        private final long startingSequenceId;
        private final DataSize maxSize;

        private final CompletableFuture<BufferResult> futureResult = new CompletableFuture<>();

        public PendingRead(TaskId outputId, long startingSequenceId, DataSize maxSize)
        {
            this.outputId = requireNonNull(outputId, "outputId is null");
            this.startingSequenceId = startingSequenceId;
            this.maxSize = requireNonNull(maxSize, "maxSize is null");
        }

        public CompletableFuture<BufferResult> getFutureResult()
        {
            return futureResult;
        }

        public void process(OutputBuffer delegate)
        {
            if (futureResult.isDone()) {
                return;
            }

            try {
                CompletableFuture<BufferResult> result = delegate.get(outputId, startingSequenceId, maxSize);
                result.whenComplete((value, exception) -> {
                    if (exception != null) {
                        futureResult.completeExceptionally(exception);
                    }
                    else {
                        futureResult.complete(value);
                    }
                });
            }
            catch (Exception e) {
                futureResult.completeExceptionally(e);
            }
        }
    }
}
