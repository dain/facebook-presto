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
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.NullOutputOperator.NullOutputOperatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.OrderColumn;
import io.airlift.tpch.TpchTable;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.benchmark.BenchmarkUtils.createHashOperator;
import static com.facebook.presto.benchmark.BenchmarkUtils.createLookupSourceSupplier;
import static com.facebook.presto.benchmark.BenchmarkUtils.createTaskContext;
import static com.facebook.presto.benchmark.BenchmarkUtils.getFixedSource;
import static com.facebook.presto.operator.aggregation.AverageAggregations.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkAggregateAndJoin
{
    private static final int INPUT_POSITIONS = 6_001_215;

    @Benchmark
    @OperationsPerInvocation(INPUT_POSITIONS)
    public Driver benchmark(BenchmarkData benchmarkData)
    {
        Driver joinDriver = benchmarkData.createDriver();
        while (!joinDriver.isFinished()) {
            joinDriver.process();
        }
        return joinDriver;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param("1.0")
        private double scaleFactor = 1.0;

        @Param({"false", "true"})
        private boolean hashEnabled = true;

        private TaskContext taskContext;

        private ExecutorService executor;
        private DriverFactory driverFactory;

        @Setup
        public void setup()
        {
            executor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-%s"));

            Session session = testSessionBuilder()
                    .addSystemProperties("columnar_processing", "true")
                    .build();
            this.taskContext = createTaskContext(session, executor);

            List<OperatorFactory> joinOperators = new ArrayList<>();

            // table scan
            joinOperators.add(getFixedSource(scaleFactor, TpchTable.LINE_ITEM, LineItemColumn.ORDER_KEY, LineItemColumn.QUANTITY));

            // generate hash
            Optional<Integer> hashChannel = Optional.empty();
            if (hashEnabled) {
                OperatorFactory hashOperator = createHashOperator(1, joinOperators.get(joinOperators.size() - 1).getTypes(), ImmutableList.of(0));

                joinOperators.add(hashOperator);
                hashChannel = Optional.of(hashOperator.getTypes().size() - 1);
            }

            // aggregation
            HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(
                    2,
                    new PlanNodeId("test"),
                    ImmutableList.of(BIGINT),
                    ImmutableList.of(0),
                    Step.SINGLE,
                    ImmutableList.of(
                            COUNT.bind(ImmutableList.of(), Optional.empty(), Optional.empty(), 1.0),
                            LONG_AVERAGE.bind(ImmutableList.of(1), Optional.empty(), Optional.empty(), 1.0)),
                    Optional.empty(),
                    hashChannel,
                    10_0000,
                    new DataSize(1, GIGABYTE));
            joinOperators.add(aggregationOperator);

            // join
            LookupSourceSupplier lookupSourceSupplier = createLookupSourceSupplier(1, TpchTable.ORDERS, OrderColumn.ORDER_KEY, OrderColumn.ORDER_STATUS);
            if (hashEnabled) {
                // hash channel is placed after group by channels
                hashChannel = Optional.of(1);
            }
            OperatorFactory joinOperator = LookupJoinOperators.innerJoin(
                    3,
                    new PlanNodeId("test"),
                    lookupSourceSupplier,
                    joinOperators.get(joinOperators.size() - 1).getTypes(),
                    Ints.asList(0),
                    hashChannel,
                    false);
            joinOperators.add(joinOperator);

            joinOperators.add(new NullOutputOperatorFactory(4, new PlanNodeId("test"), joinOperators.get(joinOperators.size() - 1).getTypes()));
            driverFactory = new DriverFactory(true, true, joinOperators, OptionalInt.empty());
        }

        public Driver createDriver()
        {
            return driverFactory.createDriver(taskContext.addPipelineContext(true, true).addDriverContext());
        }

        @TearDown
        public void tearDown()
        {
            executor.shutdownNow();

            taskContext = null;
            executor = null;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        Driver driver = new BenchmarkAggregateAndJoin().benchmark(data);
        for (OperatorContext operatorContext : driver.getDriverContext().getOperatorContexts()) {
            System.out.println(operatorContext.getOperatorType() + " " + operatorContext.getInputPositions().getTotalCount() + " -> " + operatorContext.getOutputPositions().getTotalCount());
        }

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkAggregateAndJoin.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
