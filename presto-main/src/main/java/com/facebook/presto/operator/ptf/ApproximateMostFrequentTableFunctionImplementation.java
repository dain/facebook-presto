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
package com.facebook.presto.operator.ptf;

import com.facebook.presto.operator.ptf.spacesaving.StreamSummary;
import com.facebook.presto.operator.ptf.spacesaving.StreamSummary.TopElement;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.TableFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ApproximateMostFrequentTableFunctionImplementation
        implements TableFunctionImplementation
{
    private final List<Type> types;
    private final int number;
    private final double error;

    public ApproximateMostFrequentTableFunctionImplementation(List<Type> types, int number, double error)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        checkArgument(number >= 0, "number is negative");
        this.number = number;
        this.error = error;
    }

    @Override
    public ConnectorPageSource create(ConnectorPageSource inputPageSource)
    {
        return new ApproximateMostFrequentSource(inputPageSource, types, number, error);
    }

    private static class ApproximateMostFrequentSource
            implements ConnectorPageSource
    {
        private final List<Type> types;
        private final ConnectorPageSource inputPageSource;
        private final StreamSummary<PageHolder> streamSummary;
        private final int number;
        private boolean finished;

        public ApproximateMostFrequentSource(ConnectorPageSource inputPageSource, List<Type> types, int number, double error)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            checkArgument(number >= 0, "number is negative");
            this.number = number;
            this.inputPageSource = requireNonNull(inputPageSource, "inputPageSource is null");
            streamSummary = new StreamSummary<>(error);
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

        @Override
        public long getSystemMemoryUsage()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public Page getNextPage()
        {
            if (!inputPageSource.isFinished()) {
                Page page = inputPageSource.getNextPage();
                if (page != null) {
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        Page row = page.getSingleValuePage(position);
                        PageHolder pageHolder = new PageHolder(types, row);
                        streamSummary.offer(pageHolder);
                    }
                }
            }

            if (!inputPageSource.isFinished()) {
                return null;
            }

            // pack results into a single page
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.<Type>builder()
                    .addAll(types)
                    .add(BIGINT)
                    .add(BIGINT)
                    .build());
            streamSummary.getTopElements(number)
                    .forEach(element -> appendToPageBuilder(element, pageBuilder));

            Page result = pageBuilder.build();
            finished = true;
            return result;
        }

        private void appendToPageBuilder(TopElement<PageHolder> element, PageBuilder pageBuilder)
        {
            element.getItem().appendTo(pageBuilder);
            BIGINT.writeLong(pageBuilder.getBlockBuilder(types.size()), element.getCount());
            BIGINT.writeLong(pageBuilder.getBlockBuilder(types.size() + 1), element.getError());
        }

        @Override
        public void close()
                throws IOException
        {
            inputPageSource.close();
        }
    }

    private static class PageHolder
    {
        private final List<Type> types;
        private final Page page;

        public PageHolder(List<Type> types, Page page)
        {
            this.types = types;
            this.page = page;
            verify(page.getPositionCount() == 1);
        }

        private void appendTo(PageBuilder pageBuilder)
        {
            pageBuilder.declarePosition();

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Type type = pageBuilder.getType(channel);
                type.appendTo(page.getBlock(channel), 0, pageBuilder.getBlockBuilder(channel));
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PageHolder that = (PageHolder) o;

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                // broken distinct implementation
                if (page.getBlock(channel).isNull(0)) {
                    if (!that.page.getBlock(channel).isNull(0)) {
                        return false;
                    }
                }
                else if (!types.get(channel).equalTo(page.getBlock(channel), 0, that.page.getBlock(channel), 0)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            long hashCode = 0;
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                long cellHash = block.isNull(0) ? 0 : types.get(channel).hash(block, 0);
                CombineHashFunction.getHash(hashCode, cellHash);
            }
            return Long.hashCode(hashCode);
        }
    }
}
