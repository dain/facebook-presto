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
package com.facebook.presto.operator;

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class JoinToDistinctOperator
        implements Operator, Closeable
{
    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final ListenableFuture<? extends LookupSource> lookupSourceFuture;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable onClose;

    private final PageBuilder pageBuilder;

    private final boolean probeOnOuterSide;

    private LookupSource lookupSource;
    private Page probePage;

    private boolean closed;
    private boolean finishing;

    public JoinToDistinctOperator(
            OperatorContext operatorContext,
            List<Type> probeTypes,
            List<Type> buildTypes,
            JoinType joinType,
            ListenableFuture<LookupSource> lookupSourceFuture,
            JoinProbeFactory joinProbeFactory,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.<Type>builder()
                .addAll(requireNonNull(probeTypes, "probeTypes is null"))
                .addAll(requireNonNull(buildTypes, "buildTypes is null"))
                .add(BOOLEAN)
                .build();

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;

        this.lookupSourceFuture = requireNonNull(lookupSourceFuture, "lookupSourceFuture is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.onClose = requireNonNull(onClose, "onClose is null");

        this.pageBuilder = new PageBuilder(buildTypes);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = finishing && probePage == null && pageBuilder.isEmpty();

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return lookupSourceFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (lookupSource == null) {
            lookupSource = tryGetFutureValue(lookupSourceFuture).orElse(null);
        }
        return lookupSource != null && probePage == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(lookupSource != null, "Lookup source has not been built yet");
        checkState(probePage == null, "Current page has not been completely processed yet");

        probePage = page;
    }
    @Override
    public Page getOutput()
    {
        if (lookupSource == null || probePage == null) {
            return null;
        }

        // create probe
        JoinProbe joinProbe = joinProbeFactory.createJoinProbe(lookupSource, probePage);

        BlockBuilder maskBlockBuilder = BOOLEAN.createFixedSizeBlockBuilder(probePage.getPositionCount());

        while (joinProbe.advanceNextPosition()) {
            long joinPosition = joinProbe.getCurrentJoinPosition();
            pageBuilder.declarePosition();
            if (joinPosition >= 0) {
                BOOLEAN.writeBoolean(maskBlockBuilder, true);
                lookupSource.appendTo(joinPosition, pageBuilder, 0);
            }
            else {
                BOOLEAN.writeBoolean(maskBlockBuilder, probeOnOuterSide);

                // todo add lookupSource.appendNulls(pageBuilder. 0);
                for (int i = 0; i < lookupSource.getChannelCount(); i++) {
                    pageBuilder.getBlockBuilder(i).appendNull();
                }
            }
        }

        Page buildPage = pageBuilder.build();
        pageBuilder.reset();

        Block[] blocks = new Block[probePage.getChannelCount() + buildPage.getChannelCount() + 1];
        for (int probeChannel = 0; probeChannel < probePage.getChannelCount(); probeChannel++) {
            blocks[probeChannel] = probePage.getBlock(probeChannel);
        }
        for (int buildChannel = 0; buildChannel < buildPage.getChannelCount(); buildChannel++) {
            blocks[probePage.getChannelCount() + buildChannel] = buildPage.getBlock(buildChannel);
        }
        blocks[blocks.length - 1] = maskBlockBuilder.build();
        Page page = new Page(probePage.getPositionCount(), blocks);

        probePage = null;

        return page;
    }

    @Override
    public void close()
    {
        // Closing the lookupSource is always safe to do, but we don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        probePage = null;
        pageBuilder.reset();
        onClose.run();
        // closing lookup source is only here for index join
        if (lookupSource != null) {
            lookupSource.close();
        }
    }
}
