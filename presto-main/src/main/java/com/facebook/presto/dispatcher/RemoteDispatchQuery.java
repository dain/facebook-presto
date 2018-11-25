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
package com.facebook.presto.dispatcher;

import com.facebook.presto.Session;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.QueryState.DISPATCHING;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RemoteDispatchQuery<C>
        implements DispatchQuery
{
    private final CoordinatorSelector coordinatorSelector;
    private final QueryDispatcher<C> queryDispatcher;

    private final QueuedQueryStateMachine stateMachine;

    private final ListenableFuture<C> initializeDispatchFuture;

    public RemoteDispatchQuery(
            Session session,
            String slug,
            String query,
            PreparedQuery preparedQuery,
            URI self,
            ResourceGroupId resourceGroup,
            CoordinatorSelector coordinatorSelector,
            QueryDispatcher<C> queryDispatcher,
            QueryMonitor queryMonitor,
            TransactionManager transactionManager,
            Executor executor)
    {
        this.stateMachine = QueuedQueryStateMachine.begin(session.getQueryId(), query, session, self, Optional.of(resourceGroup), queryMonitor, transactionManager, executor);
        this.coordinatorSelector = requireNonNull(coordinatorSelector, "coordinatorSelector is null");
        this.queryDispatcher = requireNonNull(queryDispatcher, "queryDispatcher is null");

        // dispatch initialization may be expensive so start it early
        initializeDispatchFuture = queryDispatcher.initializeDispatch(
                session,
                slug,
                query,
                preparedQuery,
                stateMachine.getCreateTime(),
                stateMachine.getElapsedTime(),
                stateMachine.getResourceGroup().orElseThrow(() -> new IllegalArgumentException("resource is null")));
        addExceptionCallback(initializeDispatchFuture, stateMachine::transitionToFailed);
    }

    @Override
    public void startWaitingForResources()
    {
        if (stateMachine.transitionToWaitingForResources()) {
            ListenableFuture<CoordinatorLocation> locationFuture = coordinatorSelector.selectCoordinator(stateMachine);
            addSuccessCallback(locationFuture, this::start);
            addExceptionCallback(locationFuture, stateMachine::transitionToFailed);
        }
    }

    private void start(CoordinatorLocation coordinatorLocation)
    {
        requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        addSuccessCallback(initializeDispatchFuture, context -> {
            try {
                stateMachine.transitionToDispatched(coordinatorLocation)
                        .ifPresent(query -> queryDispatcher.dispatchQuery(context, query));
            }
            catch (Throwable e) {
                stateMachine.transitionToFailed(e);
            }
        });
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public ListenableFuture<?> getDispatchedFuture()
    {
        return queryDispatchFuture(stateMachine.getQueryState());
    }

    private ListenableFuture<?> queryDispatchFuture(QueryState currentState)
    {
        if (currentState.ordinal() >= DISPATCHING.ordinal()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(stateMachine.getStateChange(currentState), this::queryDispatchFuture, directExecutor());
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        return new DispatchInfo(stateMachine.getCoordinatorLocation(), stateMachine.getFailureCause(), stateMachine.getElapsedTime(), stateMachine.getQueuedTime());
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return stateMachine.getQueryState().isDone();
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return Optional.empty();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, MILLISECONDS);
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return new DataSize(0, BYTE);
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getBasicQueryInfo();
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void fail(Throwable throwable)
    {
        stateMachine.transitionToFailed(throwable);
    }

    @Override
    public void cancel()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void pruneInfo() {}

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return stateMachine.getErrorCode();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }
}
