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
package com.facebook.presto.operator;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.function.TableFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class TableFunctionOperator
        implements Operator
{
    public static class TableFunctionOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> types;
        private final TableFunctionImplementation tableFunction;
        private boolean closed;

        public TableFunctionOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                TableFunctionImplementation tableFunction)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.tableFunction = requireNonNull(tableFunction, "tableFunction is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNOperator.class.getSimpleName());
            return new TableFunctionOperator(
                    operatorContext,
                    types,
                    tableFunction);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableFunctionOperatorFactory(operatorId, planNodeId, types, tableFunction);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final TableFunctionImplementation tableFunction;
    private final InputPageSource inputPageSource = new InputPageSource();
    @Nullable
    private ConnectorPageSource outputPageSource;

    public TableFunctionOperator(
            OperatorContext operatorContext,
            List<Type> types,
            TableFunctionImplementation tableFunction)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.tableFunction = requireNonNull(tableFunction, "tableFunction is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!inputPageSource.hasPendingPage()) {
            return NOT_BLOCKED;
        }
        if (outputPageSource == null) {
            return NOT_BLOCKED;
        }
        return toListenableFuture(outputPageSource.isBlocked());
    }

    @Override
    public boolean needsInput()
    {
        return !inputPageSource.hasPendingPage() && !inputPageSource.isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        inputPageSource.setPendingPage(page);
        if (outputPageSource == null) {
            outputPageSource = tableFunction.create(inputPageSource);
        }
    }

    @Override
    public Page getOutput()
    {
        if (outputPageSource == null) {
            return null;
        }
        Page page = outputPageSource.getNextPage();
        return page;
    }

    @Override
    public void finish()
    {
        inputPageSource.finish();
    }

    @Override
    public boolean isFinished()
    {
        if (outputPageSource == null) {
            return inputPageSource.isFinished();
        }
        return outputPageSource.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        try {
            inputPageSource.close();
        }
        finally {
            if (outputPageSource != null) {
                outputPageSource.close();
            }
        }
    }

    private static class InputPageSource
            implements ConnectorPageSource
    {
        private enum State { RUNNING, FINISHING, CLOSED }
        private Page pendingPage;
        private State state;

        public void setPendingPage(Page pendingPage)
        {
            requireNonNull(pendingPage, "pendingPage is null");
            verify(this.pendingPage == null);
            this.pendingPage = pendingPage;
        }

        public boolean hasPendingPage()
        {
            return pendingPage != null;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        public void finish()
        {
            if (pendingPage == null) {
                state = State.CLOSED;
            }
            else {
                state = State.FINISHING;
            }
        }

        @Override
        public boolean isFinished()
        {
            return state == State.CLOSED;
        }

        @Override
        public Page getNextPage()
        {
            Page page = pendingPage;
            pendingPage = null;
            if (state == State.FINISHING) {
                state = State.CLOSED;
            }
            return page;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return pendingPage == null ? 0 : pendingPage.getRetainedSizeInBytes();
        }

        @Override
        public void close()
        {
            pendingPage = null;
            state = State.CLOSED;
        }
    }
}
