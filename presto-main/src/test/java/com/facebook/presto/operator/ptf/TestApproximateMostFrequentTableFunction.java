package com.facebook.presto.operator.ptf;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.TableFunctionOperator.TableFunctionOperatorFactory;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.function.PolymorphicTableFunction;
import com.facebook.presto.spi.function.TableFunctionImplementation;
import com.facebook.presto.spi.function.TableFunctionDescriptor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestApproximateMostFrequentTableFunction
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testFunction()
    {
        TypeRegistry typeManager = new TypeRegistry();
        new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());

        PolymorphicTableFunction factory = new ApproximateMostFrequentTableFunctionFactory(typeManager);

        TableFunctionDescriptor descriptor = factory.specialize(ImmutableMap.<String, Object>builder()
                .put("number", 3)
                .put("error", 0.01)
                .put("input", ImmutableList.of(new ColumnMetadata("value", VARCHAR)))
                .build());

        TableFunctionImplementation function = factory.getInstance(descriptor.getHandle());
        List<Type> types = descriptor.getOutputColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        OperatorFactory operatorFactory = new TableFunctionOperatorFactory(0, new PlanNodeId("test"), types, function);

        List<Page> input = rowPagesBuilder(VARCHAR)
                .row("").row("")
                .row("a")
                .row("b")
                .pageBreak()
                .row("b")
                .row("c")
                .row("c")
                .pageBreak()
                .row("c")
                .row("e")
                .row("e")
                .row("e")
                .pageBreak()
                .row("e")
                .row("e")
                .row("d")
                .row("d")
                .row("d")
                .row("d")
                .row("g")
                .pageBreak()
                .row("g")
                .row("g")
                .row("g")
                .pageBreak()
                .row("g")
                .row("g")
                .row("g")
                .pageBreak()
                .row("f")
                .row("f")
                .pageBreak()
                .row("f")
                .row("f")
                .row("f")
                .pageBreak()
                .row("f")
                .build();

        List<Page> expected = rowPagesBuilder(VARCHAR, BIGINT, BIGINT)
                .row("g", 7L, 0L)
                .row("f", 6L, 0L)
                .row("e", 5L, 0L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }
}
