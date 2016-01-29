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
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.concurrent.CompletableFuture;

public interface OutputBuffer
{
    SharedBufferInfo getInfo();

    boolean isFinished();

    void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener);

    double getUtilization();

    void setOutputBuffers(OutputBuffers newOutputBuffers);

    ListenableFuture<?> enqueue(Page page);

    ListenableFuture<?> enqueue(int partition, Page page);

    CompletableFuture<BufferResult> get(TaskId outputId, long startingSequenceId, DataSize maxSize);

    void abort(TaskId outputId);

    void setNoMorePages();

    /**
     * Destroys the buffer, discarding all pages.
     */
    void destroy();

    /**
     * Fail the buffer, discarding all pages, but blocking readers.
     */
    void fail();
}
