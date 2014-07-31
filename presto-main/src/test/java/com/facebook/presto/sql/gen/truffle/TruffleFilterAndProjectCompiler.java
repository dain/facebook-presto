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
package com.facebook.presto.sql.gen.truffle;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.FilterAndProject;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.CompilerDirectives.SlowPath;
import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.dsl.TypeSystem;
import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeUtil;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

public class TruffleFilterAndProjectCompiler
{
    public static TruffleFilterAndProject compile(MetadataManager metadata, RowExpression filter, ImmutableList<RowExpression> projects)
    {
        TruffleExpressionVisitor visitor = new TruffleExpressionVisitor(metadata.getFunctionRegistry());

        FrameDescriptor frameDescriptor = new FrameDescriptor();
        FrameSlot sessionSlot = frameDescriptor.addFrameSlot("session", FrameSlotKind.Object);
        FrameSlot pageSlot = frameDescriptor.addFrameSlot("page", FrameSlotKind.Object);
        FrameSlot rowSlot = frameDescriptor.addFrameSlot("row", FrameSlotKind.Int);
        FrameSlot pageBuilderSlot = frameDescriptor.addFrameSlot("pageBuilder", FrameSlotKind.Object);

        ExpressionNode filterNode = filter.accept(visitor, frameDescriptor);
        OutputNode[] projectNodes = new OutputNode[projects.size()];
        for (int i = 0; i < projectNodes.length; i++) {
            RowExpression project = projects.get(i);
            Type type = project.getType();
            Class<?> javaType = type.getJavaType();
            if (javaType == boolean.class) {
                projectNodes[i] = new BooleanOutputNode(pageBuilderSlot, type, i, project.accept(visitor, frameDescriptor));
            }
            if (javaType == byte.class || javaType == short.class || javaType == int.class || javaType == long.class) {
                projectNodes[i] = new LongOutputNode(pageBuilderSlot, type, i, project.accept(visitor, frameDescriptor));
            }
            if (javaType == float.class || javaType == double.class) {
                projectNodes[i] = new DoubleOutputNode(pageBuilderSlot, type, i, project.accept(visitor, frameDescriptor));
            }
            if (javaType == Slice.class) {
                projectNodes[i] = new SliceOutputNode(pageBuilderSlot, type, i, project.accept(visitor, frameDescriptor));
            }
        }

        FilterAndProjectRootNode rootNode = new FilterAndProjectRootNode(frameDescriptor, sessionSlot, pageSlot, rowSlot, pageBuilderSlot, filterNode, projectNodes);

        TruffleRuntime runtime = Truffle.getRuntime();
        RootCallTarget callTarget = runtime.createCallTarget(rootNode);

        return new TruffleFilterAndProject(callTarget);
    }

    public static class TruffleFilterAndProject
            implements FilterAndProject
    {
        private final RootCallTarget callTarget;

        public TruffleFilterAndProject(RootCallTarget callTarget)
        {
            this.callTarget = callTarget;
        }

        public void dumpAst()
        {
            NodeUtil.printCompactTree(System.out, callTarget.getRootNode());
        }

        @Override
        public int process(ConnectorSession session, Object input, int start, int end, PageBuilder pageBuilder)
        {
            return (int) callTarget.call(session, input, start, end, pageBuilder);
        }
    }

    @TypeSystem({boolean.class, long.class, double.class, Slice.class})
    public static class PrestoTypes
    {
    }

    @TypeSystemReference(PrestoTypes.class)
    public static final class FilterAndProjectRootNode
            extends RootNode
    {
        private final FrameSlot sessionSlot;
        private final FrameSlot pageSlot;
        private final FrameSlot pageBuilderSlot;
        private final FrameSlot rowSlot;

        @Child
        private final ExpressionNode filterNode;

        @Children
        private final OutputNode[] projectNodes;

        public FilterAndProjectRootNode(
                FrameDescriptor frameDescriptor,
                FrameSlot sessionSlot,
                FrameSlot pageSlot,
                FrameSlot rowSlot, FrameSlot pageBufferSlot,
                ExpressionNode filterNode,
                OutputNode... projectNodes)
        {
            super(null, frameDescriptor);
            this.sessionSlot = sessionSlot;
            this.pageSlot = pageSlot;
            this.pageBuilderSlot = pageBufferSlot;
            this.rowSlot = rowSlot;
            this.filterNode = filterNode;
            this.projectNodes = projectNodes;
        }

        @Override
        public Object execute(VirtualFrame frame)
        {
            frame.setObject(sessionSlot, frame.getArguments()[0]);
            frame.setObject(pageSlot, frame.getArguments()[1]);
            frame.setObject(pageBuilderSlot, frame.getArguments()[4]);

            int start = (int) frame.getArguments()[2];
            int end = (int) frame.getArguments()[3];

            int row;
            for (row = start; row < end; row++) {
                frame.setInt(rowSlot, row);
                try {
                    if (filterNode.executeBoolean(frame)) {
                        project(frame);
                    }
                }
                catch (Throwable e) {
                    throw Throwables.propagate(e);
                }
            }

            return row;
        }

        @ExplodeLoop
        private void project(VirtualFrame frame)
                throws Throwable
        {
            for (ExpressionNode projectNode : projectNodes) {
                projectNode.executeVoid(frame);
            }
        }
    }

    @TypeSystemReference(PrestoTypes.class)
    public abstract static class PrestoNode
            extends Node
    {
        /**
         * Execute this node as as statement, where no return value is necessary.
         */
        public abstract void executeVoid(VirtualFrame frame)
                throws Throwable;
    }

    public abstract static class ExpressionNode
            extends PrestoNode
    {
        @Override
        public void executeVoid(VirtualFrame frame)
                throws Throwable
        {
            executeGeneric(frame);
        }

        public boolean executeBoolean(VirtualFrame frame)
                throws Throwable
        {
            return PrestoTypesGen.PRESTOTYPES.expectBoolean(executeGeneric(frame));
        }

        public long executeLong(VirtualFrame frame)
                throws Throwable
        {
            return PrestoTypesGen.PRESTOTYPES.expectLong(executeGeneric(frame));
        }

        public double executeDouble(VirtualFrame frame)
                throws Throwable
        {
            return PrestoTypesGen.PRESTOTYPES.expectDouble(executeGeneric(frame));
        }

        public Slice executeSlice(VirtualFrame frame)
                throws Throwable
        {
            return PrestoTypesGen.PRESTOTYPES.expectSlice(executeGeneric(frame));
        }

        public abstract Object executeGeneric(VirtualFrame frame)
                throws Throwable;
    }

    public static class BooleanConstantNode
            extends ExpressionNode
    {
        private final boolean constant;

        public BooleanConstantNode(boolean constant)
        {
            this.constant = constant;
        }

        @Override
        public boolean executeBoolean(VirtualFrame frame)
        {
            return constant;
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
        {
            return constant;
        }
    }

    public static class LongConstantNode
            extends ExpressionNode
    {
        private final long constant;

        public LongConstantNode(long constant)
        {
            this.constant = constant;
        }

        @Override
        public long executeLong(VirtualFrame frame)
        {
            return constant;
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
        {
            return constant;
        }
    }

    public static class DoubleConstantNode
            extends ExpressionNode
    {
        private final double constant;

        public DoubleConstantNode(double constant)
        {
            this.constant = constant;
        }

        @Override
        public double executeDouble(VirtualFrame frame)
        {
            return constant;
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
        {
            return constant;
        }
    }

    public static class SliceConstantNode
            extends ExpressionNode
    {
        private final Slice constant;

        public SliceConstantNode(Slice constant)
        {
            this.constant = constant;
        }

        @Override
        public Slice executeSlice(VirtualFrame frame)
        {
            return constant;
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
        {
            return constant;
        }
    }

    public abstract static class InputNode
            extends ExpressionNode
    {
        private final FrameSlot pageSlot;
        private final FrameSlot rowSlot;
        protected final Type type;
        private final int channel;

        public InputNode(FrameSlot pageSlot, FrameSlot rowSlot, Type type, int channel)
        {
            this.pageSlot = pageSlot;
            this.rowSlot = rowSlot;
            this.type = type;
            this.channel = channel;
        }

        @Override
        public boolean executeBoolean(VirtualFrame frame)
                throws UnexpectedResultException
        {
            return type.getBoolean(getBlock(frame), getRow(frame));
        }

        @Override
        public long executeLong(VirtualFrame frame)
                throws UnexpectedResultException
        {
            return type.getLong(getBlock(frame), getRow(frame));
        }

        @Override
        public double executeDouble(VirtualFrame frame)
                throws UnexpectedResultException
        {
            return type.getDouble(getBlock(frame), getRow(frame));
        }

        @Override
        public Slice executeSlice(VirtualFrame frame)
        {
            return type.getSlice(getBlock(frame), getRow(frame));
        }

        protected int getRow(VirtualFrame frame)
        {
            return FrameUtil.getIntSafe(frame, rowSlot);
        }

        protected Block getBlock(VirtualFrame frame)
        {
            Page page = (Page) FrameUtil.getObjectSafe(frame, pageSlot);
            return page.getBlock(channel);
        }
    }

    public static class BooleanInputNode
            extends InputNode
    {
        public BooleanInputNode(FrameSlot pageSlot, FrameSlot rowSlot, Type type, int channel)
        {
            super(pageSlot, rowSlot, type, channel);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
        {
            Block block = getBlock(frame);
            int row = getRow(frame);
            if (block.isNull(row)) {
                return null;
            }
            return type.getBoolean(block, row);
        }
    }

    public static class LongInputNode
            extends InputNode
    {
        public LongInputNode(FrameSlot pageSlot, FrameSlot rowSlot, Type type, int channel)
        {
            super(pageSlot, rowSlot, type, channel);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
        {
            Block block = getBlock(frame);
            int row = getRow(frame);
            if (block.isNull(row)) {
                return null;
            }
            return type.getLong(block, row);
        }
    }

    public static class DoubleInputNode
            extends InputNode
    {
        public DoubleInputNode(FrameSlot pageSlot, FrameSlot rowSlot, Type type, int channel)
        {
            super(pageSlot, rowSlot, type, channel);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
        {
            Block block = getBlock(frame);
            int row = getRow(frame);
            if (block.isNull(row)) {
                return null;
            }
            return type.getDouble(block, row);
        }
    }

    public static class SliceInputNode
            extends InputNode
    {
        public SliceInputNode(FrameSlot pageSlot, FrameSlot rowSlot, Type type, int channel)
        {
            super(pageSlot, rowSlot, type, channel);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
        {
            Block block = getBlock(frame);
            int row = getRow(frame);
            if (block.isNull(row)) {
                return null;
            }
            return type.getSlice(block, row);
        }
    }

    public abstract static class OutputNode
            extends ExpressionNode
    {
        private final FrameSlot pageBuilderSlot;
        protected final Type type;
        private final int channel;

        @Child
        protected final ExpressionNode projectNode;

        protected OutputNode(FrameSlot pageBuilderSlot, Type type, int channel, ExpressionNode projectNode)
        {
            this.pageBuilderSlot = pageBuilderSlot;
            this.type = type;
            this.channel = channel;
            this.projectNode = projectNode;
        }

        @Override
        public boolean executeBoolean(VirtualFrame frame)
                throws Throwable
        {
            boolean value = projectNode.executeBoolean(frame);
            type.writeBoolean(getBlockBuilder(frame), value);
            return value;
        }

        @Override
        public long executeLong(VirtualFrame frame)
                throws Throwable
        {
            long value = projectNode.executeLong(frame);
            type.writeLong(getBlockBuilder(frame), value);
            return value;
        }

        @Override
        public double executeDouble(VirtualFrame frame)
                throws Throwable
        {
            double value = projectNode.executeDouble(frame);
            type.writeDouble(getBlockBuilder(frame), value);
            return value;
        }

        @Override
        public Slice executeSlice(VirtualFrame frame)
                throws Throwable
        {
            Slice value = projectNode.executeSlice(frame);
            type.writeSlice(getBlockBuilder(frame), value);
            return value;
        }

        protected BlockBuilder getBlockBuilder(VirtualFrame frame)
        {
            PageBuilder pageBuilder = (PageBuilder) FrameUtil.getObjectSafe(frame, pageBuilderSlot);
            return pageBuilder.getBlockBuilder(channel);
        }
    }

    public static class FunctionNode
            extends ExpressionNode
    {
        protected final MethodHandle methodHandle;

        @Children
        protected final ExpressionNode[] arguments;

        protected FunctionNode(MethodHandle methodHandle, ExpressionNode[] arguments)
        {
            this.methodHandle = methodHandle;
            this.arguments = arguments;
        }

        @Override
        public boolean executeBoolean(VirtualFrame frame)
                throws Throwable
        {
            return invokeBoolean(getArguments(frame));
        }

        @SlowPath
        public boolean invokeBoolean(Object[] arguments)
                throws Throwable
        {
            return (boolean) methodHandle.invokeWithArguments(arguments);
        }

        @Override
        public long executeLong(VirtualFrame frame)
                throws Throwable
        {
            return invokeLong(frame);
        }

        @SlowPath
        public long invokeLong(VirtualFrame frame)
                throws Throwable
        {
            return (long) methodHandle.invokeWithArguments(getArguments(frame));
        }

        @Override
        public double executeDouble(VirtualFrame frame)
                throws Throwable
        {
            return invokeDouble(getArguments(frame));
        }

        @SlowPath
        public double invokeDouble(Object[] arguments)
                throws Throwable
        {
            return (double) methodHandle.invokeWithArguments(arguments);
        }

        @Override
        public Slice executeSlice(VirtualFrame frame)
                throws Throwable
        {
            return invokeSlice(getArguments(frame));
        }

        @SlowPath
        public Slice invokeSlice(Object[] arguments)
                throws Throwable
        {
            return (Slice) methodHandle.invokeWithArguments(arguments);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
                throws Throwable
        {
            return invokeGeneric(getArguments(frame));
        }

        @SlowPath
        public Object invokeGeneric(Object[] arguments)
                throws Throwable
        {
            return methodHandle.invokeWithArguments(arguments);
        }

        @ExplodeLoop
        private Object[] getArguments(VirtualFrame frame)
                throws Throwable
        {
            Object[] values = new Object[arguments.length];
            for (int i = 0; i < values.length; i++) {
                values[i] = arguments[i].executeGeneric(frame);
            }
            return values;
        }
    }

    public static class BooleanOutputNode
            extends OutputNode
    {
        public BooleanOutputNode(FrameSlot pageBuilderSlot, Type type, int channel, ExpressionNode projectNode)
        {
            super(pageBuilderSlot, type, channel, projectNode);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
                throws Throwable
        {
            Boolean value = (Boolean) projectNode.executeGeneric(frame);
            if (value == null) {
                getBlockBuilder(frame).appendNull();
            }
            else {
                type.writeBoolean(getBlockBuilder(frame), value);
            }
            return value;
        }
    }

    public static class LongOutputNode
            extends OutputNode
    {
        public LongOutputNode(FrameSlot pageBuilderSlot, Type type, int channel, ExpressionNode projectNode)
        {
            super(pageBuilderSlot, type, channel, projectNode);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
                throws Throwable
        {
            Long value = (Long) projectNode.executeGeneric(frame);
            if (value == null) {
                getBlockBuilder(frame).appendNull();
            }
            else {
                type.writeLong(getBlockBuilder(frame), value);
            }
            return value;
        }
    }

    public static class DoubleOutputNode
            extends OutputNode
    {
        public DoubleOutputNode(FrameSlot pageBuilderSlot, Type type, int channel, ExpressionNode projectNode)
        {
            super(pageBuilderSlot, type, channel, projectNode);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
                throws Throwable
        {
            Double value = (Double) projectNode.executeGeneric(frame);
            if (value == null) {
                getBlockBuilder(frame).appendNull();
            }
            else {
                type.writeDouble(getBlockBuilder(frame), value);
            }
            return value;
        }
    }

    public static class SliceOutputNode
            extends OutputNode
    {
        public SliceOutputNode(FrameSlot pageBuilderSlot, Type type, int channel, ExpressionNode projectNode)
        {
            super(pageBuilderSlot, type, channel, projectNode);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
                throws Throwable
        {
            Slice value = (Slice) projectNode.executeGeneric(frame);
            if (value == null) {
                getBlockBuilder(frame).appendNull();
            }
            else {
                type.writeSlice(getBlockBuilder(frame), value);
            }
            return value;
        }
    }

    public static class AndNode
            extends ExpressionNode
    {
        @Child
        protected final ExpressionNode left;
        @Child
        protected final ExpressionNode right;

        public AndNode(ExpressionNode left, ExpressionNode right)
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean executeBoolean(VirtualFrame frame)
                throws Throwable
        {
            return left.executeBoolean(frame) && right.executeBoolean(frame);
        }

        @Override
        public Object executeGeneric(VirtualFrame frame)
                throws Throwable
        {
            // if either left or right is false, result is always false regardless of nulls
            Boolean left = (Boolean) this.left.executeGeneric(frame);
            if (left == Boolean.FALSE) {
                return false;
            }
            Boolean right = (Boolean) this.right.executeGeneric(frame);
            if (right == Boolean.FALSE) {
                return false;
            }
            if (left == null || right == null) {
                return null;
            }
            return left & right;
        }
    }
}
