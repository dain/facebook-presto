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
package com.facebook.presto.operator.ptf.spacesaving;

import com.facebook.presto.operator.ptf.spacesaving.StreamSummary.TopElement;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static org.testng.Assert.assertEquals;

/**
 * Unit test for {@link StreamSummary}
 */
public class StreamSummaryTest
{
    @Test
    public void testStreamSummaryAllFitsInMemory()
    {
        //100 counters
        StreamSummary<String> streamSummary = new StreamSummary<>(0.01);
        String sample = "a b b c c c e e e e e d d d d g g g g g g g f f f f f f";
        Splitter.on(CharMatcher.whitespace()).split(sample).forEach(streamSummary::offer);
        List<TopElement<String>> top3 = streamSummary.getTopElements(3);
        List<String> top3Items = top3.stream().map(TopElement::getItem).collect(Collectors.toList());
        assertEquals(top3Items, ImmutableList.of("g", "f", "e"));
    }

    @Test
    public void testStreamSummaryAllFitsInMemoryButItemsSeenLargerThanCapacity()
    {
        //100 counters
        StreamSummary<String> streamSummary = new StreamSummary<>(0.01);
        int iterationRange = 200;
        //lets make sure we will iterate more than the capacity
        assertGreaterThan(iterationRange, streamSummary.getCapacity());
        String sample = "a b b c c c e e e e e d d d d g g g g g g g f f f f f f";
        Splitter.on(CharMatcher.whitespace()).split(sample).forEach(streamSummary::offer);
        IntStream.range(0, 200).mapToObj(i -> "a").forEach(streamSummary::offer);
        List<TopElement<String>> top3 = streamSummary.getTopElements(3);
        List<String> top3Items = top3.stream().map(TopElement::getItem).collect(Collectors.toList());
        assertEquals(top3Items, ImmutableList.of("a", "g", "f"));
    }

    /**
     * This test uses "lorem_ipsum.txt" which has 2500 words.
     * 573 unique words:
     * $tr -c '[:alnum:]' '[\n*]' < lorem_ipsum.txt | sort | uniq -c
     * <p>
     * The full top 10 words are:
     * $tr -c '[:alnum:]' '[\n*]' < lorem_ipsum.txt | sort | uniq -c | sort -nr | head  -5
     * 43 et
     * 36 nec
     * 35 vel
     * 35 ac
     * 35 a
     */
    @Test
    public void testSmallFileExample()
            throws Exception
    {
        URL resourceFile = getClass().getClassLoader().getResource("lorem_ipsum.txt");
        String text = Resources.toString(resourceFile, StandardCharsets.UTF_8);
        //500 counters with error range of 5 words (2500 * 0.002)
        StreamSummary<String> streamSummary = new StreamSummary<>(0.002);
        Splitter.on(CharMatcher.whitespace()).omitEmptyStrings().split(text).forEach(streamSummary::offer);
        List<TopElement<String>> top5 = streamSummary.getTopElements(1);
        List<String> top5Items = top5.stream().map(TopElement::getItem).collect(Collectors.toList());
        assertEquals(top5Items, ImmutableList.of("et"));
    }
}
