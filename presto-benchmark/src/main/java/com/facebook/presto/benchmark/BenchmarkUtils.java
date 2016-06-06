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
package com.facebook.presto.benchmark;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.MemoryPoolId;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.GenericPageProcessor;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.operator.PageSourceOperator;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchRecordSet;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Collections.singleton;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class BenchmarkUtils
{
    private BenchmarkUtils() {}

    public static TaskContext createTaskContext(Session session, ExecutorService executor)
    {
        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
        MemoryPool systemMemoryPool = new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(1, GIGABYTE));

        return new QueryContext(new QueryId("test"), new DataSize(256, MEGABYTE), memoryPool, systemMemoryPool, executor)
                .addTaskContext(
                        new TaskStateMachine(new TaskId("query", "stage", "task"), executor),
                        session,
                        new DataSize(1, MEGABYTE),
                        false,
                        false);
    }

    public static OperatorFactory createHashOperator(int operatorId, List<Type> sourceTypes, List<Integer> hashChannels)
    {
        TypeManager typeManager = new TypeRegistry();
        MetadataManager metadata = new MetadataManager(
                new FeaturesConfig().setExperimentalSyntaxEnabled(true),
                typeManager,
                new BlockEncodingManager(typeManager),
                new SessionPropertyManager(),
                new TablePropertyManager(),
                createTestTransactionManager());
        ExpressionCompiler compiler = new ExpressionCompiler(metadata);

        List<RowExpression> projections = new ArrayList<>();
        for (int channel = 0; channel < sourceTypes.size(); channel++) {
            Type type = sourceTypes.get(channel);
            projections.add(new InputReferenceExpression(channel, type));
        }

        RowExpression hashExpression = new ConstantExpression(0L, BIGINT);
        for (int hashChannel : hashChannels) {
            Type hashType = sourceTypes.get(hashChannel);
            hashExpression = new CallExpression(
                    new Signature("combine_hash", FunctionKind.SCALAR, BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature()),
                    BIGINT,
                    ImmutableList.of(
                            hashExpression,
                            new CallExpression(
                                    new Signature(mangleOperatorName(HASH_CODE), FunctionKind.SCALAR, BIGINT.getTypeSignature(), hashType.getTypeSignature()),
                                    BIGINT,
                                    ImmutableList.of(new InputReferenceExpression(hashChannel, hashType))
                            )));
        }
        projections.add(hashExpression);

        Supplier<PageProcessor> processor = compiler.compilePageProcessor(new ConstantExpression(true, BOOLEAN), projections);
        return new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                operatorId,
                new PlanNodeId("test"),
                processor,
                projections.stream()
                        .map(RowExpression::getType)
                        .collect(toImmutableList()));
    }

    @SafeVarargs
    public static <E extends TpchEntity> LookupSourceSupplier createLookupSourceSupplier(double scaleFactor, TpchTable<E> table, TpchColumn<E>... columns)
    {
        List<OperatorFactory> buildOperators = new ArrayList<>();

        // table scan
        buildOperators.add(getFixedSource(scaleFactor, table, columns));

        // filter
        FilterAndProjectOperator.FilterAndProjectOperatorFactory filterAndProjectOperator = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                2,
                new PlanNodeId("test"),
                () -> new GenericPageProcessor(new OrderKeyFilter(), ImmutableList.of(singleColumn(BIGINT, 0), singleColumn(VARCHAR, 1))),
                ImmutableList.of(BIGINT, VARCHAR));
        buildOperators.add(filterAndProjectOperator);

        // hash build
        HashBuilderOperatorFactory hashBuilder = new HashBuilderOperatorFactory(
                3,
                new PlanNodeId("test"),
                buildOperators.get(buildOperators.size() - 1).getTypes(),
                ImmutableMap.of(),
                Ints.asList(0),
                Optional.empty(),
                false,
                Optional.empty(),
                1_500_000);
        buildOperators.add(hashBuilder);
        DriverFactory hashBuildDriverFactory = new DriverFactory(true, false, buildOperators, OptionalInt.empty());
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-%s"));
        try {
            Driver hashBuildDriver = hashBuildDriverFactory.createDriver(createTaskContext(testSessionBuilder().build(),  executor).addPipelineContext(true, false).addDriverContext());
            while (!hashBuildDriver.isFinished()) {
                hashBuildDriver.process();
            }
            return hashBuilder.getLookupSourceSupplier();
        }
        finally {
            executor.shutdownNow();
        }
    }

    @SafeVarargs
    public static <E extends TpchEntity> List<Page> getPages(double scaleFactor, TpchTable<E> table, TpchColumn<E>... columns)
    {
        TpchRecordSet<E> recordSet = TpchRecordSet.createTpchRecordSet(
                table,
                ImmutableList.copyOf(columns),
                scaleFactor,
                1,
                1);
        RecordPageSource recordPageSource = new RecordPageSource(recordSet);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        while (!recordPageSource.isFinished()) {
            Page page = recordPageSource.getNextPage();
            if (page != null) {
                pages.add(page);
            }
        }
        return pages.build();
    }

    @SafeVarargs
    static <E extends TpchEntity> OperatorFactory getFixedSource(double scaleFactor, TpchTable<E> table, TpchColumn<E>... columns)
    {
        List<Page> pages = getPages(scaleFactor, table, columns);

        List<Type> types = ImmutableList.copyOf(columns).stream()
                .map(TpchColumn::getType)
                .map(TpchMetadata::getPrestoType)
                .collect(toImmutableList());

        return new OperatorFactory()
        {
            @Override
            public List<Type> getTypes()
            {
                return types;
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), TableScanOperator.class.getSimpleName());
                return new PageSourceOperator(new FixedPageSource(pages), getTypes(), operatorContext);
            }

            @Override
            public void close()
            {
            }

            @Override
            public OperatorFactory duplicate()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static class OrderKeyFilter
            implements FilterFunction
    {
        @Override
        public boolean filter(int position, Block... blocks)
        {
            return BIGINT.getLong(blocks[0], position) % 100 == 0;
        }

        @Override
        public boolean filter(RecordCursor cursor)
        {
            return cursor.getLong(0) % 100 == 0;
        }

        @Override
        public Set<Integer> getInputChannels()
        {
            return singleton(0);
        }
    }
}
