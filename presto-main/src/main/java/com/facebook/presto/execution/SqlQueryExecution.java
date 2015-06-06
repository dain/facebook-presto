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
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.SqlQueryScheduler;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedExecutionPlanner;
import com.facebook.presto.sql.planner.InputExtractor;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.isBigQueryEnabled;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class SqlQueryExecution
        implements QueryExecution
{
    private static final OutputBuffers ROOT_OUTPUT_BUFFERS = INITIAL_EMPTY_OUTPUT_BUFFERS
            .withBuffer(new TaskId("output", "buffer", "id"), new UnpartitionedPagePartitionFunction())
            .withNoMoreBufferIds();

    private final QueryStateMachine stateMachine;

    private final Statement statement;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final SplitManager splitManager;
    private final NodeScheduler nodeScheduler;
    private final List<PlanOptimizer> planOptimizers;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final int scheduleSplitBatchSize;
    private final int initialHashPartitions;
    private final boolean experimentalSyntaxEnabled;
    private final ExecutorService queryExecutor;

    private final QueryExplainer queryExplainer;
    private final AtomicReference<SqlQueryScheduler> queryScheduler = new AtomicReference<>();
    private final AtomicReference<QueryInfo> finalQueryInfo = new AtomicReference<>();
    private final NodeTaskMap nodeTaskMap;
    private final Session session;
    private final ExecutionPolicy executionPolicy;

    public SqlQueryExecution(QueryId queryId,
            String query,
            Session session,
            URI self,
            Statement statement,
            Metadata metadata,
            SqlParser sqlParser,
            SplitManager splitManager,
            NodeScheduler nodeScheduler,
            List<PlanOptimizer> planOptimizers,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            int scheduleSplitBatchSize,
            int initialHashPartitions,
            boolean experimentalSyntaxEnabled,
            ExecutorService queryExecutor,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            this.statement = checkNotNull(statement, "statement is null");
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
            this.splitManager = checkNotNull(splitManager, "splitManager is null");
            this.nodeScheduler = checkNotNull(nodeScheduler, "nodeScheduler is null");
            this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
            this.remoteTaskFactory = checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.queryExecutor = checkNotNull(queryExecutor, "queryExecutor is null");
            this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
            this.nodeTaskMap = checkNotNull(nodeTaskMap, "nodeTaskMap is null");
            this.session = checkNotNull(session, "session is null");
            this.executionPolicy = checkNotNull(executionPolicy, "executionPolicy is null");

            checkArgument(scheduleSplitBatchSize > 0, "scheduleSplitBatchSize must be greater than 0");
            this.scheduleSplitBatchSize = scheduleSplitBatchSize;

            checkArgument(initialHashPartitions > 0, "initialHashPartitions must be greater than 0");
            this.initialHashPartitions = initialHashPartitions;

            checkNotNull(queryId, "queryId is null");
            checkNotNull(query, "query is null");
            checkNotNull(session, "session is null");
            checkNotNull(self, "self is null");
            this.stateMachine = new QueryStateMachine(queryId, query, session, self, queryExecutor);

            // when the query finishes cache the final query info, and clear the reference to the output stage
            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }

                // query is now done, so abort any work that is still running
                SqlQueryScheduler scheduler = queryScheduler.get();
                if (scheduler != null) {
                    scheduler.abort();
                }

                // capture the final query state and drop reference to the scheduler
                finalQueryInfo.compareAndSet(null, buildQueryInfo(scheduler));
                queryScheduler.set(null);
            });

            this.queryExplainer = new QueryExplainer(session, planOptimizers, metadata, sqlParser, experimentalSyntaxEnabled);
        }
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public long getTotalMemoryReservation()
    {
        // acquire reference to outputStage before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears outputStage when
        // the query finishes.
        SqlQueryScheduler scheduler = queryScheduler.get();
        QueryInfo queryInfo = finalQueryInfo.get();
        if (queryInfo != null) {
            return queryInfo.getQueryStats().getTotalMemoryReservation().toBytes();
        }
        return scheduler.getTotalMemoryReservation();
    }

    @Override
    public Session getSession()
    {
        return session;
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            try {
                // transition to planning
                if (!stateMachine.transitionToPlanning()) {
                    // query already started or finished
                    return;
                }

                // analyze query
                SubPlan subplan = analyzeQuery();

                // plan distribution of query
                planDistribution(subplan);

                // transition to starting
                if (!stateMachine.transitionToStarting()) {
                    // query already started or finished
                    return;
                }

                // if query is not finished, start the scheduler, otherwise cancel it
                SqlQueryScheduler scheduler = queryScheduler.get();

                if (!stateMachine.isDone()) {
                    scheduler.start();
                }
            }
            catch (Throwable e) {
                fail(e);
                Throwables.propagateIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            stateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    private SubPlan analyzeQuery()
    {
        try {
            return doAnalyzeQuery();
        }
        catch (StackOverflowError e) {
            throw new PrestoException(NOT_SUPPORTED, "statement is too large (stack overflow during analysis)", e);
        }
    }

    private SubPlan doAnalyzeQuery()
    {
        // time analysis phase
        long analysisStart = System.nanoTime();

        // analyze query
        Analyzer analyzer = new Analyzer(stateMachine.getSession(), metadata, sqlParser, Optional.of(queryExplainer), experimentalSyntaxEnabled);
        Analysis analysis = analyzer.analyze(statement);

        stateMachine.setUpdateType(analysis.getUpdateType());

        // plan query
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator, metadata);
        Plan plan = logicalPlanner.plan(analysis);

        // extract inputs
        List<Input> inputs = new InputExtractor(metadata).extract(plan.getRoot());
        stateMachine.setInputs(inputs);

        // fragment the plan
        SubPlan subplan = new PlanFragmenter().createSubPlans(plan);

        // record analysis time
        stateMachine.recordAnalysisTime(analysisStart);

        return subplan;
    }

    private void planDistribution(SubPlan subplan)
    {
        // time distribution planning
        long distributedPlanningStart = System.nanoTime();

        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager);
        StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(subplan);
        stateMachine.recordDistributedPlanningTime(distributedPlanningStart);

        if (stateMachine.isDone()) {
            return;
        }

        // record field names
        stateMachine.setOutputFieldNames(outputStageExecutionPlan.getFieldNames());

        // build the stage execution objects (this doesn't schedule execution)
        SqlQueryScheduler scheduler = new SqlQueryScheduler(
                stateMachine,
                locationFactory,
                outputStageExecutionPlan,
                nodeScheduler,
                remoteTaskFactory,
                stateMachine.getSession(),
                scheduleSplitBatchSize,
                initialHashPartitions,
                queryExecutor,
                ROOT_OUTPUT_BUFFERS,
                nodeTaskMap,
                executionPolicy);

        queryScheduler.set(scheduler);

        // if query was canceled during scheduler creation, abort the scheduler
        // directly since the callback may have already fired
        if (stateMachine.isDone()) {
            scheduler.abort();
            queryScheduler.set(null);
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        checkNotNull(stageId, "stageId is null");

        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            SqlQueryScheduler scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        stateMachine.transitionToFailed(cause);
    }

    @Override
    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            return stateMachine.waitForStateChange(currentState, maxWait);
        }
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        QueryInfo queryInfo = finalQueryInfo.get();
        if (queryInfo == null || queryInfo.getOutputStage() == null) {
            return;
        }

        StageInfo prunedOutputStage = new StageInfo(
                queryInfo.getOutputStage().getStageId(),
                queryInfo.getOutputStage().getState(),
                queryInfo.getOutputStage().getSelf(),
                null, // Remove the plan
                queryInfo.getOutputStage().getTypes(),
                queryInfo.getOutputStage().getStageStats(),
                ImmutableList.of(), // Remove the tasks
                ImmutableList.of(), // Remove the substages
                queryInfo.getOutputStage().getFailureCause()
        );

        QueryInfo prunedQueryInfo = new QueryInfo(
                queryInfo.getQueryId(),
                queryInfo.getSession(),
                queryInfo.getState(),
                getMemoryPool().getId(),
                queryInfo.isScheduled(),
                queryInfo.getSelf(),
                queryInfo.getFieldNames(),
                queryInfo.getQuery(),
                queryInfo.getQueryStats(),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                queryInfo.getUpdateType(),
                prunedOutputStage,
                queryInfo.getFailureInfo(),
                queryInfo.getErrorCode(),
                queryInfo.getInputs()
        );
        finalQueryInfo.compareAndSet(queryInfo, prunedQueryInfo);
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            // acquire reference to scheduler before checking finalQueryInfo, because
            // state change listener sets finalQueryInfo and then clears scheduler when
            // the query finishes.
            SqlQueryScheduler scheduler = queryScheduler.get();

            QueryInfo finalQueryInfo = this.finalQueryInfo.get();
            if (finalQueryInfo != null) {
                return finalQueryInfo;
            }

            return buildQueryInfo(scheduler);
        }
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    private QueryInfo buildQueryInfo(SqlQueryScheduler scheduler)
    {
        StageInfo stageInfo = null;
        if (scheduler != null) {
            stageInfo = scheduler.getStageInfo();
        }
        return stateMachine.getQueryInfo(stageInfo);
    }

    public static class SqlQueryExecutionFactory
            implements QueryExecutionFactory<SqlQueryExecution>
    {
        private final int scheduleSplitBatchSize;
        private final int initialHashPartitions;
        private final Integer bigQueryInitialHashPartitions;
        private final boolean experimentalSyntaxEnabled;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final SplitManager splitManager;
        private final NodeScheduler nodeScheduler;
        private final List<PlanOptimizer> planOptimizers;
        private final RemoteTaskFactory remoteTaskFactory;
        private final LocationFactory locationFactory;
        private final ExecutorService executor;
        private final NodeTaskMap nodeTaskMap;
        private final NodeManager nodeManager;
        private final Map<String, ExecutionPolicy> executionPolicies;
        private final String defaultExecutionPolicy;

        @Inject
        SqlQueryExecutionFactory(QueryManagerConfig config,
                FeaturesConfig featuresConfig,
                Metadata metadata,
                SqlParser sqlParser,
                LocationFactory locationFactory,
                SplitManager splitManager,
                NodeScheduler nodeScheduler,
                NodeManager nodeManager,
                List<PlanOptimizer> planOptimizers,
                RemoteTaskFactory remoteTaskFactory,
                @ForQueryExecution ExecutorService executor,
                NodeTaskMap nodeTaskMap,
                Map<String, ExecutionPolicy> executionPolicies)
        {
            checkNotNull(config, "config is null");
            this.scheduleSplitBatchSize = config.getScheduleSplitBatchSize();
            this.initialHashPartitions = config.getInitialHashPartitions();
            this.bigQueryInitialHashPartitions = config.getBigQueryInitialHashPartitions();
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.splitManager = checkNotNull(splitManager, "splitManager is null");
            this.nodeScheduler = checkNotNull(nodeScheduler, "nodeScheduler is null");
            this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
            this.remoteTaskFactory = checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
            checkNotNull(featuresConfig, "featuresConfig is null");
            this.experimentalSyntaxEnabled = featuresConfig.isExperimentalSyntaxEnabled();
            this.executor = checkNotNull(executor, "executor is null");
            this.nodeTaskMap = checkNotNull(nodeTaskMap, "nodeTaskMap is null");
            this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");

            this.executionPolicies = checkNotNull(executionPolicies, "schedulerPolicies is null");
            this.defaultExecutionPolicy = config.getQueryExecutionPolicy();
            checkArgument(executionPolicies.containsKey(defaultExecutionPolicy), "No execution policy %s", defaultExecutionPolicy);
        }

        @Override
        public SqlQueryExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement)
        {
            int initialHashPartitions = this.initialHashPartitions;
            if (isBigQueryEnabled(session, false)) {
                initialHashPartitions = (bigQueryInitialHashPartitions == null) ? nodeManager.getActiveNodes().size() : bigQueryInitialHashPartitions;
            }
            initialHashPartitions = getHashPartitionCount(session, initialHashPartitions);

            String executionPolicyName = SystemSessionProperties.getExecutionPolicy(session, defaultExecutionPolicy);
            ExecutionPolicy executionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(executionPolicy != null, "No execution policy %s", defaultExecutionPolicy);

            SqlQueryExecution queryExecution = new SqlQueryExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    statement,
                    metadata,
                    sqlParser,
                    splitManager,
                    nodeScheduler,
                    planOptimizers,
                    remoteTaskFactory,
                    locationFactory,
                    scheduleSplitBatchSize,
                    initialHashPartitions,
                    experimentalSyntaxEnabled,
                    executor,
                    nodeTaskMap,
                    executionPolicy);

            return queryExecution;
        }
    }
}
