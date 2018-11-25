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
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.execution.QueryState.DISPATCHING;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_RESOURCES;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.Failures.toFailure;
import static io.airlift.units.Duration.succinctNanos;
import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class QueuedQueryStateMachine
{
    private static final Logger QUERY_STATE_LOG = Logger.get(QueuedQueryStateMachine.class);

    private final DateTime createTime = DateTime.now();
    private final long createNanos;
    private final AtomicReference<Long> endNanos = new AtomicReference<>();

    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final SessionRepresentation sessionRepresentation;
    private final URI self;
    private final Optional<ResourceGroupId> resourceGroup;
    private final QueryMonitor queryMonitor;
    private final Ticker ticker;

    private final AtomicReference<Long> lastHeartbeatNanos;

    private final AtomicReference<Duration> queuedTime = new AtomicReference<>();

    private final AtomicReference<Long> resourceWaitingStartNanos = new AtomicReference<>();
    private final AtomicReference<Duration> resourceWaitingTime = new AtomicReference<>();

    private final StateMachine<QueryState> queryState;

    private final AtomicReference<CoordinatorLocation> coordinatorLocation = new AtomicReference<>();
    private final AtomicReference<Supplier<BasicQueryInfo>> runningQueryInfo = new AtomicReference<>();

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private QueuedQueryStateMachine(
            QueryId queryId,
            String query,
            Session session,
            URI self,
            Optional<ResourceGroupId> resourceGroup,
            QueryMonitor queryMonitor,
            Executor executor,
            Ticker ticker)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.query = requireNonNull(query, "query is null");
        this.session = requireNonNull(session, "session is null");
        this.sessionRepresentation = session.toSessionRepresentation();
        this.self = requireNonNull(self, "self is null");
        this.resourceGroup = requireNonNull(resourceGroup, "resourceGroup is null");
        this.queryMonitor = queryMonitor;

        this.ticker = ticker;
        this.createNanos = tickerNanos();
        this.lastHeartbeatNanos = new AtomicReference<>(tickerNanos());

        this.queryState = new StateMachine<>("query " + queryId, executor, QUEUED, TERMINAL_QUERY_STATES);

        queryMonitor.queryCreatedEvent(getBasicQueryInfo());
    }

    /**
     * Created QueryStateMachines must be transitioned to terminal states to clean up resources.
     */
    public static QueuedQueryStateMachine begin(
            QueryId queryId,
            String query,
            Session session,
            URI self,
            Optional<ResourceGroupId> resourceGroup,
            QueryMonitor queryMonitor,
            TransactionManager transactionManager,
            Executor executor)
    {
        QueuedQueryStateMachine queryStateMachine = new QueuedQueryStateMachine(queryId, query, session, self, resourceGroup, queryMonitor, executor, Ticker.systemTicker());
        queryStateMachine.addStateChangeListener(newState -> {
            QUERY_STATE_LOG.debug("Query %s is %s", queryId, newState);
            if (newState == FAILED) {
                session.getTransactionId().ifPresent(transactionId -> {
                    if (transactionManager.isAutoCommit(transactionId)) {
                        transactionManager.asyncAbort(transactionId);
                    }
                    else {
                        transactionManager.fail(transactionId);
                    }
                });
            }
            if (newState.isDone()) {
                session.getTransactionId().ifPresent(transactionManager::trySetInactive);
            }
        });

        return queryStateMachine;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public String getQuery()
    {
        return query;
    }

    public Session getSession()
    {
        return session;
    }

    public URI getSelf()
    {
        return self;
    }

    public Optional<CoordinatorLocation> getCoordinatorLocation()
    {
        return Optional.ofNullable(coordinatorLocation.get());
    }

    public Optional<ResourceGroupId> getResourceGroup()
    {
        return resourceGroup;
    }

    public QueryState getQueryState()
    {
        return queryState.get();
    }

    public boolean isDone()
    {
        return queryState.get().isDone();
    }

    public boolean transitionToWaitingForResources()
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos).convertToMostSuccinctTimeUnit());
        resourceWaitingStartNanos.compareAndSet(null, tickerNanos());
        return queryState.compareAndSet(QUEUED, WAITING_FOR_RESOURCES);
    }

    public Optional<DispatchedQuery> transitionToDispatched(CoordinatorLocation coordinatorLocation)
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos).convertToMostSuccinctTimeUnit());
        resourceWaitingStartNanos.compareAndSet(null, tickerNanos());
        resourceWaitingTime.compareAndSet(null, nanosSince(resourceWaitingStartNanos.get()).convertToMostSuccinctTimeUnit());

        this.coordinatorLocation.compareAndSet(null, coordinatorLocation);
        if (queryState.setIf(DISPATCHING, currentState -> currentState == QUEUED || currentState == WAITING_FOR_RESOURCES)) {
            return Optional.of(new DispatchedQuery());
        }
        return Optional.empty();
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        recordDoneStats();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        requireNonNull(throwable, "throwable is null");
        failureCause.compareAndSet(null, toFailure(throwable));

        boolean failed = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            QUERY_STATE_LOG.debug(throwable, "Query %s failed", queryId);
            queryMonitor.queryCompleted(createQueryCompletedEvent());
        }
        else {
            QUERY_STATE_LOG.debug(throwable, "Failure after query %s finished", queryId);
        }

        return failed;
    }

    public boolean transitionToCanceled()
    {
        recordDoneStats();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        failureCause.compareAndSet(null, toFailure(new PrestoException(USER_CANCELED, "Query was canceled")));

        boolean failed = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            queryMonitor.queryCompleted(createQueryCompletedEvent());
        }
        return failed;
    }

    private void recordDoneStats()
    {
        Duration durationSinceCreation = nanosSince(createNanos).convertToMostSuccinctTimeUnit();
        queuedTime.compareAndSet(null, durationSinceCreation);
        resourceWaitingTime.compareAndSet(null, succinctNanos(0));
        endNanos.compareAndSet(null, tickerNanos());
    }

    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return queryState.getStateChange(currentState);
    }

    public void recordHeartbeat()
    {
        this.lastHeartbeatNanos.set(tickerNanos());
    }

    public DateTime getCreateTime()
    {
        return createTime;
    }

    public DateTime getLastHeartbeat()
    {
        // if query is dispatched, return now
        if (runningQueryInfo.get() != null) {
            return DateTime.now();
        }
        // not dispatched, so use local heartbeat
        return toDateTime(lastHeartbeatNanos.get());
    }

    public Duration getElapsedTime()
    {
        return tryGetRunningQueryInfo()
                .map(info -> info.getQueryStats().getElapsedTime())
                .orElseGet(() -> {
                    Long failedNanos = this.endNanos.get();
                    if (failedNanos != null) {
                        return new Duration(failedNanos - createNanos, NANOSECONDS);
                    }
                    else {
                        return nanosSince(createNanos);
                    }
                });
    }

    public Duration getQueuedTime()
    {
        Duration queuedTime = this.queuedTime.get();
        if (queuedTime == null) {
            return getElapsedTime();
        }
        return queuedTime;
    }

    public Optional<DateTime> getEndTime()
    {
        Optional<DateTime> endTime = tryGetRunningQueryInfo()
                .flatMap(info -> Optional.ofNullable(info.getQueryStats().getEndTime()));
        if (endTime.isPresent()) {
            return endTime;
        }
        return toDateTime(this.endNanos);
    }

    public Optional<ExecutionFailureInfo> getFailureCause()
    {
        return Optional.ofNullable(failureCause.get());
    }

    public Optional<ErrorCode> getErrorCode()
    {
        return Optional.ofNullable(tryGetRunningQueryInfo()
                .map(BasicQueryInfo::getErrorCode)
                .orElseGet(() -> {
                    ExecutionFailureInfo failureInfo = failureCause.get();
                    if (failureInfo == null) {
                        return null;
                    }
                    return failureInfo.getErrorCode();
                }));
    }

    public BasicQueryInfo getBasicQueryInfo()
    {
        Optional<BasicQueryInfo> basicQueryInfo = tryGetRunningQueryInfo();
        if (basicQueryInfo.isPresent()) {
            return basicQueryInfo.get();
        }

        QueryState queryState = this.queryState.get();
        Optional<ErrorCode> errorCode = getFailureCause().map(ExecutionFailureInfo::getErrorCode);
        return new BasicQueryInfo(
                queryId,
                sessionRepresentation,
                queryState,
                GENERAL_POOL,
                false,
                self,
                query,
                new BasicQueryStats(
                        createTime,
                        getEndTime().orElse(null),
                        getQueuedTime(),
                        getElapsedTime(),
                        new Duration(0, MILLISECONDS),
                        0,
                        0,
                        0,
                        0,
                        new DataSize(0, Unit.BYTE),
                        0,
                        0,
                        new DataSize(0, Unit.BYTE),
                        new DataSize(0, Unit.BYTE),
                        new DataSize(0, Unit.BYTE),
                        new Duration(0, MILLISECONDS),
                        new Duration(0, MILLISECONDS),
                        false,
                        ImmutableSet.of(),
                        OptionalDouble.empty()),
                errorCode.map(ErrorCode::getType).orElse(null),
                errorCode.orElse(null));
    }

    private QueryCompletedEvent createQueryCompletedEvent()
    {
        DateTime endTime = getEndTime().orElseGet(this::getCreateTime);
        return new QueryCompletedEvent(
                createQueryMetadata(),
                createQueryStatistics(),
                queryMonitor.createQueryContext(session, resourceGroup),
                new QueryIOMetadata(ImmutableList.of(), Optional.empty()),
                queryMonitor.createQueryFailureInfo(failureCause.get()),
                ofEpochMilli(createTime.getMillis()),
                ofEpochMilli(endTime.getMillis()),
                ofEpochMilli(endTime.getMillis()));
    }

    private QueryMetadata createQueryMetadata()
    {
        return new QueryMetadata(
                queryId.toString(),
                session.getTransactionId().map(TransactionId::toString),
                query,
                queryState.get().toString(),
                self,
                Optional.empty(),
                Optional.empty());
    }

    private QueryStatistics createQueryStatistics()
    {
        return new QueryStatistics(
                ofMillis(0),
                ofMillis(0),
                ofMillis(getQueuedTime().toMillis()),
                Optional.empty(),
                Optional.empty(),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                ImmutableList.of(),
                0,
                true,
                ImmutableList.of(),
                ImmutableList.of());
    }

    private Optional<BasicQueryInfo> tryGetRunningQueryInfo()
    {
        Supplier<BasicQueryInfo> supplier = runningQueryInfo.get();
        if (supplier == null) {
            return Optional.empty();
        }
        return Optional.of(supplier.get());
    }

    private long tickerNanos()
    {
        return ticker.read();
    }

    private Duration nanosSince(long start)
    {
        return succinctNanos(tickerNanos() - start);
    }

    private Optional<DateTime> toDateTime(AtomicReference<Long> endNanos2)
    {
        Long endNanos = endNanos2.get();
        if (endNanos == null) {
            return Optional.empty();
        }
        return Optional.of(toDateTime(endNanos));
    }

    private DateTime toDateTime(long instantNanos)
    {
        long millisSinceCreate = NANOSECONDS.toMillis(instantNanos - createNanos);
        return new DateTime(createTime.getMillis() + millisSinceCreate);
    }

    public class DispatchedQuery
    {
        public CoordinatorLocation getCoordinatorLocation()
        {
            return coordinatorLocation.get();
        }

        public void setQueryState(QueryState newState)
        {
            // query has been dispatched, so no need to track stats anymore
            queryState.setIf(newState, currentState -> !currentState.isDone() && currentState.ordinal() < newState.ordinal());
        }

        public void setBasicQueryInfoSupplier(Supplier<BasicQueryInfo> basicQueryInfoSupplier)
        {
            runningQueryInfo.compareAndSet(null, basicQueryInfoSupplier);
        }
    }
}
