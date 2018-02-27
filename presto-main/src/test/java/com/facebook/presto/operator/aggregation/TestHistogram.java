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

package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestInput;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestInputBuilder;
import com.facebook.presto.operator.aggregation.groupByAggregations.AggregationTestOutput;
import com.facebook.presto.operator.aggregation.groupByAggregations.GroupByAggregationTestUtils;
import com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;
import org.testng.internal.collections.Ints;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringArraysBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.OperatorAssertion.toRow;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.histogram.Histogram.NAME;
import static com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation.LEGACY;
import static com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation.NEW;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertTrue;

public class TestHistogram
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("UTC");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    @Test
    public void testSimpleHistograms()
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        InternalAggregationFunction aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of("a", 1L, "b", 1L, "c", 1L),
                createStringsBlock("a", "b", "c"));

        mapType = mapType(BIGINT, BIGINT);
        aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(100L, 1L, 200L, 1L, 300L, 1L),
                createLongsBlock(100L, 200L, 300L));

        mapType = mapType(DOUBLE, BIGINT);
        aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(0.1, 1L, 0.3, 1L, 0.2, 1L),
                createDoublesBlock(0.1, 0.3, 0.2));

        mapType = mapType(BOOLEAN, BIGINT);
        aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(true, 1L, false, 1L),
                createBooleansBlock(true, false));
    }

    @Test
    public void testSharedGroupBy()
            throws Exception
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        InternalAggregationFunction aggregationFunction = getMetadata(NEW).getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of("a", 1L, "b", 1L, "c", 1L),
                createStringsBlock("a", "b", "c"));

        mapType = mapType(BIGINT, BIGINT);
        aggregationFunction = getMetadata(NEW).getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(100L, 1L, 200L, 1L, 300L, 1L),
                createLongsBlock(100L, 200L, 300L));

        mapType = mapType(DOUBLE, BIGINT);
        aggregationFunction = getMetadata(NEW).getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(0.1, 1L, 0.3, 1L, 0.2, 1L),
                createDoublesBlock(0.1, 0.3, 0.2));

        mapType = mapType(BOOLEAN, BIGINT);
        aggregationFunction = getMetadata(NEW).getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(true, 1L, false, 1L),
                createBooleansBlock(true, false));
    }

    @Test
    public void testDuplicateKeysValues()
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        InternalAggregationFunction aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of("a", 2L, "b", 1L),
                createStringsBlock("a", "b", "a"));

        mapType = mapType(TIMESTAMP_WITH_TIME_ZONE, BIGINT);
        aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)));
        long timestampWithTimeZone1 = packDateTimeWithZone(new DateTime(1970, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY);
        long timestampWithTimeZone2 = packDateTimeWithZone(new DateTime(2015, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY);
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(new SqlTimestampWithTimeZone(timestampWithTimeZone1), 2L, new SqlTimestampWithTimeZone(timestampWithTimeZone2), 1L),
                createLongsBlock(timestampWithTimeZone1, timestampWithTimeZone1, timestampWithTimeZone2));
    }

    @Test
    public void testWithNulls()
    {
        MapType mapType = mapType(BIGINT, BIGINT);
        InternalAggregationFunction aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(1L, 1L, 2L, 1L),
                createLongsBlock(2L, null, 1L));

        mapType = mapType(BIGINT, BIGINT);
        aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                aggregationFunction,
                null,
                createLongsBlock((Long) null));
    }

    @Test
    public void testArrayHistograms()
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        MapType mapType = mapType(arrayType, BIGINT);
        InternalAggregationFunction aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        arrayType.getTypeSignature()));

        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(ImmutableList.of("a", "b", "c"), 1L, ImmutableList.of("d", "e", "f"), 1L, ImmutableList.of("c", "b", "a"), 1L),
                createStringArraysBlock(ImmutableList.of(ImmutableList.of("a", "b", "c"), ImmutableList.of("d", "e", "f"), ImmutableList.of("c", "b", "a"))));
    }

    @Test
    public void testMapHistograms()
    {
        MapType innerMapType = mapType(VARCHAR, VARCHAR);
        MapType mapType = mapType(innerMapType, BIGINT);
        InternalAggregationFunction aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        innerMapType.getTypeSignature()));

        BlockBuilder builder = innerMapType.createBlockBuilder(new BlockBuilderStatus(), 3);
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("a", "b")));
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("c", "d")));
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("e", "f")));

        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(ImmutableMap.of("a", "b"), 1L, ImmutableMap.of("c", "d"), 1L, ImmutableMap.of("e", "f"), 1L),
                builder.build());
    }

    @Test
    public void testRowHistograms()
    {
        RowType innerRowType = RowType.from(ImmutableList.of(
                RowType.field("f1", BIGINT),
                RowType.field("f2", DOUBLE)));
        MapType mapType = mapType(innerRowType, BIGINT);
        InternalAggregationFunction aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        innerRowType.getTypeSignature()));

        BlockBuilder builder = innerRowType.createBlockBuilder(new BlockBuilderStatus(), 3);
        innerRowType.writeObject(builder, toRow(ImmutableList.of(BIGINT, DOUBLE), 1L, 1.0));
        innerRowType.writeObject(builder, toRow(ImmutableList.of(BIGINT, DOUBLE), 2L, 2.0));
        innerRowType.writeObject(builder, toRow(ImmutableList.of(BIGINT, DOUBLE), 3L, 3.0));

        assertAggregation(
                aggregationFunction,
                ImmutableMap.of(ImmutableList.of(1L, 1.0), 1L, ImmutableList.of(2L, 2.0), 1L, ImmutableList.of(3L, 3.0), 1L),
                builder.build());
    }

    @Test
    public void testLargerHistograms()
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        InternalAggregationFunction aggregationFunction = getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                aggregationFunction,
                ImmutableMap.of("a", 25L, "b", 10L, "c", 12L, "d", 1L, "e", 2L),
                createStringsBlock("a", "b", "c", "d", "e", "e", "c", "a", "a", "a", "b", "a", "a", "a", "a", "b", "a", "a", "a", "a", "b", "a", "a", "a", "a", "b", "a", "a", "a", "a", "b", "a", "c", "c", "b", "a", "c", "c", "b", "a", "c", "c", "b", "a", "c", "c", "b", "a", "c", "c"));
    }

    @Test
    public void testEmptyHistogramOutputsNull()
            throws Exception
    {
        InternalAggregationFunction function = getInternalAggregationFunctionVarChar(NEW);
        GroupedAccumulator groupedAccumulator = function.bind(Ints.asList(new int[] {}), Optional.empty())
                .createGroupedAccumulator();
        BlockBuilder blockBuilder = groupedAccumulator.getFinalType().createBlockBuilder(new BlockBuilderStatus(), 1000);

        groupedAccumulator.evaluateFinal(0, blockBuilder);
        assertTrue(blockBuilder.isNull(0));
    }

    @Test
    public void testSharedGroupByWithOverlappingValuesRunner()
            throws Exception
    {
//        InternalAggregationFunction classicFunction = getInternalAggregationFunctionVarChar(LEGACY);
        InternalAggregationFunction singleInstanceFunction = getInternalAggregationFunctionVarChar(NEW);

//        testSharedGroupByWithOverlappingValuesRunner(classicFunction);
        testSharedGroupByWithOverlappingValuesRunner(singleInstanceFunction);
    }

    @Test
    public void testSharedGroupByWithDistinctValuesPerGroup()
            throws Exception
    {
        // test that two groups don't affect one another
        InternalAggregationFunction classicFunction = getInternalAggregationFunctionVarChar(LEGACY);
        InternalAggregationFunction singleInstanceFunction = getInternalAggregationFunctionVarChar(NEW);
        testSharedGroupByWithDistinctValuesPerGroupRunner(classicFunction);
        testSharedGroupByWithDistinctValuesPerGroupRunner(singleInstanceFunction);
    }

    @Test
    public void testSharedGroupByWithOverlappingValuesPerGroup()
            throws Exception
    {
        // test that two groups don't affect one another
        InternalAggregationFunction classicFunction = getInternalAggregationFunctionVarChar(LEGACY);
        InternalAggregationFunction singleInstanceFunction = getInternalAggregationFunctionVarChar(NEW);
        testSharedGroupByWithOverlappingValuesPerGroupRunner(classicFunction);
        testSharedGroupByWithOverlappingValuesPerGroupRunner(singleInstanceFunction);
    }

    @Test
    public void testSharedGroupByWithManyGroups()
    {
        // uses a large enough data set to induce rehashing and test correctness
        InternalAggregationFunction classicFunction = getInternalAggregationFunctionVarChar(LEGACY);
        InternalAggregationFunction singleInstanceFunction = getInternalAggregationFunctionVarChar(NEW);

        // this is to validate the test as there have been test-bugs that looked like code bugs--if both fail, likely a test bug
        testManyValuesInducingRehash(classicFunction);
        testManyValuesInducingRehash(singleInstanceFunction);
    }

    private void testManyValuesInducingRehash(InternalAggregationFunction aggregationFunction)
    {
        double distinctFraction = 0.1f;
        int numGroups = 50000;
        int itemCount = 30;
        Random random = new Random();
        GroupedAccumulator groupedAccumulator = createGroupedAccumulator(aggregationFunction);

        for (int j = 0; j < numGroups; j++) {
            Map<String, Long> expectedValues = new HashMap<>();
            List<String> valueList = new ArrayList<>();

            for (int i = 0; i < itemCount; i++) {
                String str = String.valueOf(i % 10);
                String item = IntStream.range(0, itemCount).mapToObj(x -> str).collect(Collectors.joining());
                boolean distinctValue = random.nextDouble() < distinctFraction;
                if (distinctValue) {
                    // produce a unique value for the histogram
                    item = j + "-" + item;
                    valueList.add(item);
                }
                else {
                    valueList.add(item);
                }
                expectedValues.compute(item, (k, v) -> v == null ? 1L : ++v);
            }

            Block block = createStringsBlock(valueList);
            AggregationTestInputBuilder testInputBuilder = new AggregationTestInputBuilder(
                    new Block[] {block},
                    aggregationFunction);
            AggregationTestInput test1 = testInputBuilder.build();

            test1.runPagesOnAccumulatorWithAssertion(j, groupedAccumulator, new AggregationTestOutput(expectedValues));
        }
    }

    private GroupedAccumulator createGroupedAccumulator(InternalAggregationFunction function)
    {
        int[] args = GroupByAggregationTestUtils.createArgs(function);

        return function.bind(Ints.asList(args), Optional.empty())
                .createGroupedAccumulator();
    }

    private void testSharedGroupByWithOverlappingValuesPerGroupRunner(InternalAggregationFunction aggregationFunction)
    {
        Block block1 = createStringsBlock("a", "b", "c");
        Block block2 = createStringsBlock("b", "c", "d");
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableMap.of("a", 1L, "b", 1L, "c", 1L));
        AggregationTestInputBuilder testBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1},
                aggregationFunction);
        AggregationTestInput test1 = testBuilder1.build();
        GroupedAccumulator groupedAccumulator = test1.createGroupedAccumulator();

        test1.runPagesOnAccumulatorWithAssertion(0L, groupedAccumulator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableMap.of("b", 1L, "c", 1L, "d", 1L));
        AggregationTestInputBuilder testbuilder2 = new AggregationTestInputBuilder(
                new Block[] {block2},
                aggregationFunction);
        AggregationTestInput test2 = testbuilder2.build();
        test2.runPagesOnAccumulatorWithAssertion(255L, groupedAccumulator, aggregationTestOutput2);
    }

    private void testSharedGroupByWithDistinctValuesPerGroupRunner(InternalAggregationFunction aggregationFunction)
    {
        Block block1 = createStringsBlock("a", "b", "c");
        Block block2 = createStringsBlock("d", "e", "f");
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableMap.of("a", 1L, "b", 1L, "c", 1L));
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1},
                aggregationFunction);
        AggregationTestInput test1 = testInputBuilder1.build();
        GroupedAccumulator groupedAccumulator = test1.createGroupedAccumulator();

        test1.runPagesOnAccumulatorWithAssertion(0L, groupedAccumulator, aggregationTestOutput1);

        AggregationTestOutput aggregationTestOutput2 = new AggregationTestOutput(ImmutableMap.of("d", 1L, "e", 1L, "f", 1L));
        AggregationTestInputBuilder testBuilder2 = new AggregationTestInputBuilder(
                new Block[] {block2},
                aggregationFunction);
        AggregationTestInput test2 = testBuilder2.build();
        test2.runPagesOnAccumulatorWithAssertion(255L, groupedAccumulator, aggregationTestOutput2);
    }

    private void testSharedGroupByWithOverlappingValuesRunner(InternalAggregationFunction aggregationFunction)
    {
        Block block1 = createStringsBlock("a", "b", "c", "d", "a1", "b2", "c3", "d4", "a", "b2", "c", "d4", "a3", "b3", "c3", "b2");
        AggregationTestInputBuilder testInputBuilder1 = new AggregationTestInputBuilder(
                new Block[] {block1},
                aggregationFunction);
        AggregationTestOutput aggregationTestOutput1 = new AggregationTestOutput(ImmutableMap.<String, Long>builder()
                .put("a", 2L)
                .put("b", 1L)
                .put("c", 2L)
                .put("d", 1L)
                .put("a1", 1L)
                .put("b2", 3L)
                .put("c3", 2L)
                .put("d4", 2L)
                .put("a3", 1L)
                .put("b3", 1L)
                .build());
        AggregationTestInput test1 = testInputBuilder1.build();

        test1.runPagesOnAccumulatorWithAssertion(0L, test1.createGroupedAccumulator(), aggregationTestOutput1);
    }

    private InternalAggregationFunction getInternalAggregationFunctionVarChar(HistogramGroupImplementation groupMode)
    {
        MapType mapType = mapType(VARCHAR, BIGINT);
        MetadataManager metadata = getMetadata(groupMode);

        return metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.VARCHAR)));
    }

    public MetadataManager getMetadata()
    {
        return getMetadata(NEW);
    }

    public MetadataManager getMetadata(HistogramGroupImplementation groupMode)
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager(new FeaturesConfig()
                .setHistogramGroupImplementation(groupMode));

        return metadata;
    }
}
