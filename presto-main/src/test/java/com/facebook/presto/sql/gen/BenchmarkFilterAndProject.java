package com.facebook.presto.sql.gen;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.TruffleFilterAndProject;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Charsets.UTF_8;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(5)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkFilterAndProject
{
    private static final int EXTENDED_PRICE = 0;
    private static final int DISCOUNT = 1;
    private static final int SHIP_DATE = 2;
    private static final int QUANTITY = 3;

    private static final Slice MIN_SHIP_DATE = Slices.copiedBuffer("1994-01-01", UTF_8);
    private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1995-01-01", UTF_8);

    private Page inputPage;
    private FilterAndProject handFilterAndProject;
    private FilterAndProject byteCodeFilterAndProject;
    private TruffleFilterAndProject truffleFilterAndProject;

    @Setup
    public void setup()
    {
        MetadataManager metadata = new MetadataManager(new FeaturesConfig(), new TypeRegistry());

        truffleFilterAndProject = TruffleFilterAndProjectCompiler.compile(metadata, FILTER, ImmutableList.of(PROJECT));

        inputPage = createInputPage();

        handFilterAndProject = new Tpch1FilterAndProject();

        byteCodeFilterAndProject = new ExpressionCompiler(metadata).compile(FILTER, ImmutableList.of(PROJECT), "unique");
    }

    @Benchmark
    public Page truffle()
    {
        return execute(inputPage, truffleFilterAndProject);
    }

    @Benchmark
    public Page byteCode()
    {
        return execute(inputPage, byteCodeFilterAndProject);
    }

    @Benchmark
    public Page byHand()
    {
        return execute(inputPage, handFilterAndProject);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        BenchmarkFilterAndProject benchmarkFilterAndProject = new BenchmarkFilterAndProject();
        benchmarkFilterAndProject.setup();
        // verify we are getting the same values from the different implementations
        verify(benchmarkFilterAndProject.byHand());
        verify(benchmarkFilterAndProject.byteCode());
        verify(benchmarkFilterAndProject.truffle());

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkFilterAndProject.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    public static void verify(Page page)
    {
        System.out.printf("%d %s %s %s%n",
                page.getChannelCount(),
                page.getBlock(0).getDouble(0, 0),
                page.getBlock(0).getDouble(page.getPositionCount() / 2, 0),
                page.getBlock(0).getDouble(page.getPositionCount() - 1, 0));
    }

    public static Page execute(Page inputPage, FilterAndProject filterAndProject)
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(DOUBLE));
        filterAndProject.process(null, inputPage, 0, inputPage.getPositionCount(), pageBuilder);
        return pageBuilder.build();
    }

    public static Page createInputPage()
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(DOUBLE, DOUBLE, VARCHAR, BIGINT));
        LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
        Iterator<LineItem> iterator = lineItemGenerator.iterator();
        for (int i = 0; i < 10_000; i++) {
            LineItem lineItem = iterator.next();
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(EXTENDED_PRICE), lineItem.getExtendedPrice());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(DISCOUNT), lineItem.getDiscount());
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(SHIP_DATE), Slices.utf8Slice(lineItem.getShipDate()));
            BIGINT.writeLong(pageBuilder.getBlockBuilder(QUANTITY), lineItem.getQuantity());
        }
        return pageBuilder.build();
    }

    private static final class Tpch1FilterAndProject
            implements FilterAndProject
    {

        @Override
        public int process(ConnectorSession session, Object input, int start, int end, PageBuilder pageBuilder)
        {
            Page page = (Page) input;
            Block discountBlock = page.getBlock(DISCOUNT);
            int position = start;
            for (; position < end; position++) {
                // where shipdate >= '1994-01-01'
                //    and shipdate < '1995-01-01'
                //    and discount >= 0.05
                //    and discount <= 0.07
                //    and quantity < 24;
                if (filter(position, discountBlock, page.getBlock(SHIP_DATE), page.getBlock(QUANTITY))) {
                    project(position, pageBuilder, page.getBlock(EXTENDED_PRICE), discountBlock);
                }
            }
            return position;
        }

        private static void project(int position, PageBuilder pageBuilder, Block extendedPriceBlock, Block discountBlock)
        {
            if (discountBlock.isNull(position) || extendedPriceBlock.isNull(position)) {
                pageBuilder.getBlockBuilder(0).appendNull();
            }
            else {
                DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), DOUBLE.getDouble(extendedPriceBlock, position) * DOUBLE.getDouble(discountBlock, position));
            }
        }

        private static boolean filter(int position, Block discountBlock, Block shipDateBlock, Block quantityBlock)
        {
            return !shipDateBlock.isNull(position) && VARCHAR.getSlice(shipDateBlock, position).compareTo(MIN_SHIP_DATE) >= 0 &&
                    !shipDateBlock.isNull(position) && VARCHAR.getSlice(shipDateBlock, position).compareTo(MAX_SHIP_DATE) < 0 &&
                    !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) >= 0.05 &&
                    !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) <= 0.07 &&
                    !quantityBlock.isNull(position) && BIGINT.getLong(quantityBlock, position) < 24;
        }
    }

    // where shipdate >= '1994-01-01'
    //    and shipdate < '1995-01-01'
    //    and discount >= 0.05
    //    and discount <= 0.07
    //    and quantity < 24;
    public static final RowExpression FILTER = new CallExpression(
            new Signature("AND", BOOLEAN),
            ImmutableList.<RowExpression>of(
                    new CallExpression(
                            new Signature(OperatorType.GREATER_THAN_OR_EQUAL.name(), BOOLEAN, VARCHAR, VARCHAR),
                            ImmutableList.of(
                                    new InputReferenceExpression(SHIP_DATE, VARCHAR),
                                    new ConstantExpression(MIN_SHIP_DATE, VARCHAR)
                            )),
                    new CallExpression(
                            new Signature("AND", BOOLEAN),
                            ImmutableList.<RowExpression>of(
                                    new CallExpression(
                                            new Signature(OperatorType.LESS_THAN.name(), BOOLEAN, VARCHAR, VARCHAR),
                                            ImmutableList.of(
                                                    new InputReferenceExpression(SHIP_DATE, VARCHAR),
                                                    new ConstantExpression(MAX_SHIP_DATE, VARCHAR)
                                            )),
                                    new CallExpression(
                                            new Signature("AND", BOOLEAN),
                                            ImmutableList.<RowExpression>of(
                                                    new CallExpression(
                                                            new Signature(OperatorType.GREATER_THAN_OR_EQUAL.name(), BOOLEAN, DOUBLE, DOUBLE),
                                                            ImmutableList.of(
                                                                    new InputReferenceExpression(DISCOUNT, DOUBLE),
                                                                    new ConstantExpression(0.05, DOUBLE)
                                                            )),
                                                    new CallExpression(
                                                            new Signature("AND", BOOLEAN),
                                                            ImmutableList.<RowExpression>of(
                                                                    new CallExpression(
                                                                            new Signature(OperatorType.LESS_THAN_OR_EQUAL.name(), BOOLEAN, DOUBLE, DOUBLE),
                                                                            ImmutableList.of(
                                                                                    new InputReferenceExpression(DISCOUNT, DOUBLE),
                                                                                    new ConstantExpression(0.07, DOUBLE)
                                                                            )),
                                                                    new CallExpression(
                                                                            new Signature(OperatorType.LESS_THAN.name(), BOOLEAN, BIGINT, BIGINT),
                                                                            ImmutableList.of(
                                                                                    new InputReferenceExpression(QUANTITY, BIGINT),
                                                                                    new ConstantExpression((long) 24, BIGINT)
                                                                            ))
                                                            ))
                                            ))
                            ))
            ));

    public static final RowExpression PROJECT = new CallExpression(
            new Signature(OperatorType.MULTIPLY.name(), DOUBLE, DOUBLE, DOUBLE),
            ImmutableList.<RowExpression>of(
                    new InputReferenceExpression(EXTENDED_PRICE, DOUBLE),
                    new InputReferenceExpression(DISCOUNT, DOUBLE)
            ));

}
