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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.ForLoop.ForLoopBuilder;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.VOLATILE;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.OpCodes.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.google.common.base.Objects.toStringHelper;

public class ExpressionCompiler
{
    private static final Logger log = Logger.get(ExpressionCompiler.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();

    private static final boolean DUMP_BYTE_CODE_TREE = false;
    private static final boolean DUMP_BYTE_CODE_RAW = false;
    private static final boolean RUN_ASM_VERIFIER = false; // verifier doesn't work right now
    private static final AtomicReference<String> DUMP_CLASS_FILES_TO = new AtomicReference<>();

    private final Metadata metadata;

    private final LoadingCache<CacheKey, FilterAndProject> processors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, FilterAndProject>()
            {
                @Override
                public FilterAndProject load(CacheKey key)
                        throws Exception
                {
                    return compileAndInstantiate(key.getFilter(), key.getProjections());
                }
            });

    private final AtomicLong generatedClasses = new AtomicLong();

    @Inject
    public ExpressionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Managed
    public long getGeneratedClasses()
    {
        return generatedClasses.get();
    }

    @Managed
    public long getCacheSize()
    {
        return processors.size();
    }

    public FilterAndProject compile(RowExpression filter, List<RowExpression> projections, Object uniqueKey)
    {
        return processors.getUnchecked(new CacheKey(filter, projections, uniqueKey));
    }

    @VisibleForTesting
    private FilterAndProject compileAndInstantiate(RowExpression filter, List<RowExpression> projections)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        // create filter and project page iterator class
        Class<? extends FilterAndProject> clazz = compileProcessor(filter, projections, classLoader);
        try {
            return clazz.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private Class<? extends FilterAndProject> compileProcessor(
            RowExpression filter,
            List<RowExpression> projections,
            DynamicClassLoader classLoader)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterAndProject_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(FilterAndProject.class));

        classDefinition.declareField(a(PRIVATE, VOLATILE, STATIC), "callSites", Map.class);

        // constructor
        classDefinition.declareConstructor(new CompilerContext(BOOTSTRAP_METHOD), a(PUBLIC))
                .getBody()
                .pushThis()
                .invokeSpecial(Object.class, "<init>", void.class)
                .ret();

        generateProcessMethod(classDefinition, projections.size());

        //
        // filter method
        //
        generateFilterMethod(callSiteBinder, classDefinition, filter);

        //
        // project methods
        //
        List<Type> types = new ArrayList<>();
        int projectionIndex = 0;
        for (RowExpression projection : projections) {
            generateProjectMethod(callSiteBinder, classDefinition, "project_" + projectionIndex, projection);
            types.add(projection.getType());
            projectionIndex++;
        }

        //
        // toString method
        //
        generateToString(
                classDefinition,
                toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filter)
                        .add("projections", projections)
                        .toString());

        Class<? extends FilterAndProject> clazz = defineClass(classDefinition, FilterAndProject.class, classLoader);
        setCallSitesField(clazz, callSiteBinder.getBindings());
        return clazz;
    }

    private void generateToString(ClassDefinition classDefinition, String string)
    {
        // Constant strings can't be too large or the bytecode becomes invalid
        if (string.length() > 100) {
            string = string.substring(0, 100) + "...";
        }

        classDefinition.declareMethod(new CompilerContext(BOOTSTRAP_METHOD), a(PUBLIC), "toString", type(String.class))
                .getBody()
                .push(string)
                .retObject();
    }

    private void generateProcessMethod(ClassDefinition classDefinition, int projections)
    {
        MethodDefinition method = classDefinition.declareMethod(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC),
                "process",
                type(int.class),
                arg("session", ConnectorSession.class),
                arg("input", Object.class),
                arg("start", int.class),
                arg("end", int.class),
                arg("pageBuilder", PageBuilder.class));

        CompilerContext compilerContext = method.getCompilerContext();

        LocalVariableDefinition position = compilerContext.declareVariable(int.class, "position");
        method.getBody()
                .comment("int position = start;")
                .getVariable("start")
                .putVariable(position);

        //
        // for loop loop body
        //
        LabelNode done = new LabelNode("done");
        ForLoopBuilder loop = ForLoop.forLoopBuilder(compilerContext)
                .initialize(NOP)
                .condition(new Block(compilerContext)
                                .comment("position < end")
                                .getVariable(position)
                                .getVariable("end")
                                .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class)
                )
                .update(new Block(compilerContext)
                        .comment("position++")
                        .incrementVariable(position, (byte) 1));

        Block loopBody = new Block(compilerContext);
        loop.body(loopBody);

        loopBody.comment("if (pageBuilder.isFull()) break;")
                .append(new Block(compilerContext)
                        .getVariable("pageBuilder")
                        .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                        .ifTrueGoto(done));

        loopBody.comment("if (!advance(cursor)) break;")
                .append(new Block(compilerContext)
                        .getVariable("input")
                        .invokeDynamic("advanceNextPosition", MethodType.methodType(boolean.class, Object.class), FieldReferenceBootstrap.BOOTSTRAP_METHOD, 0, -1))
                .ifFalseGoto(done);

        // if (filter(cursor))
        IfStatementBuilder filter = new IfStatementBuilder(compilerContext);
        filter.condition(new Block(compilerContext)
                .pushThis()
                .getVariable("session")
                .getVariable("input")
                .getVariable("position")
                .invokeVirtual(classDefinition.getType(),
                        "filter",
                        type(boolean.class),
                        type(ConnectorSession.class),
                        type(Object.class),
                        type(int.class)));

        Block trueBlock = new Block(compilerContext);
        filter.ifTrue(trueBlock);
        if (projections == 0) {
            // pageBuilder.declarePosition();
            trueBlock.getVariable("pageBuilder").invokeVirtual(PageBuilder.class, "declarePosition", void.class);
        }
        else {
            // project_43(session, input, position, pageBuilder.getBlockBuilder(42)));
            for (int projectionIndex = 0; projectionIndex < projections; projectionIndex++) {
                trueBlock.pushThis();
                trueBlock.getVariable("session");
                trueBlock.getVariable("input");
                trueBlock.getVariable("position");

                trueBlock.comment("pageBuilder.getBlockBuilder(" + projectionIndex + ")")
                        .getVariable("pageBuilder")
                        .push(projectionIndex)
                        .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

                trueBlock
                        .comment("project(input, position, session, blockBuilder)")
                        .invokeVirtual(classDefinition.getType(),
                                "project_" + projectionIndex,
                                type(void.class),
                                type(ConnectorSession.class),
                                type(Object.class),
                                type(int.class),
                                type(BlockBuilder.class));
            }
        }
        loopBody.append(filter.build());

        method.getBody()
                .append(loop.build())
                .visitLabel(done)
                .comment("return position;")
                .getVariable("position")
                .retInt();
    }

    private void generateFilterMethod(
            CallSiteBinder callSiteBinder,
            ClassDefinition classDefinition,
            RowExpression filter)
    {
        MethodDefinition filterMethod = classDefinition.declareMethod(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<NamedParameterDefinition>builder()
                        .add(arg("session", ConnectorSession.class))
                        .add(arg("input", Object.class))
                        .add(arg("position", int.class))
                        .build());

        filterMethod.comment("Filter: %s", filter.toString());

        CompilerContext context = filterMethod.getCompilerContext();

        context.declareVariable(type(boolean.class), "wasNull");

        ByteCodeNode body = compileExpression(
                callSiteBinder,
                filter,
                context,
                context.getVariable("session").getValue());

        LabelNode end = new LabelNode("end");
        filterMethod
                .getBody()
                .comment("boolean wasNull = false;")
                .putVariable("wasNull", false)
                .append(body)
                .getVariable("wasNull")
                .ifFalseGoto(end)
                .pop(boolean.class)
                .push(false)
                .visitLabel(end)
                .retBoolean();
    }

    private ByteCodeNode compileExpression(
            CallSiteBinder callSiteBinder,
            RowExpression expression,
            CompilerContext context,
            ByteCodeNode getSessionByteCode)
    {
        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, getSessionByteCode, metadata.getFunctionRegistry());
        return expression.accept(visitor, context);
    }

    private void generateProjectMethod(
            CallSiteBinder callSiteBinder,
            ClassDefinition classDefinition,
            String methodName,
            RowExpression projection)
    {
        MethodDefinition projectionMethod = classDefinition.declareMethod(new CompilerContext(BOOTSTRAP_METHOD),
                a(PUBLIC),
                methodName,
                type(void.class),
                ImmutableList.of(arg("session", ConnectorSession.class),
                        arg("input", Object.class),
                        arg("position", int.class),
                        arg("output", BlockBuilder.class)));

        projectionMethod.comment("Projection: %s", projection.toString());

        // generate body code
        CompilerContext context = projectionMethod.getCompilerContext();
        context.declareVariable(type(boolean.class), "wasNull");

        ByteCodeNode body = compileExpression(
                callSiteBinder,
                projection,
                context,
                context.getVariable("session").getValue());

        Type projectionType = projection.getType();
        projectionMethod
                .getBody()
                .comment("boolean wasNull = false;")
                .putVariable("wasNull", false)
                .invokeStatic(projectionType.getClass(), "getInstance", projectionType.getClass())
                .getVariable("output")
                .append(body);

        Block notNullBlock = new Block(context);
        if (projectionType.getJavaType() == boolean.class) {
            notNullBlock
                    .comment("%s.writeBoolean(output, <booleanStackValue>);", projectionType.getName())
                    .invokeVirtual(projectionType.getClass(), "writeBoolean", void.class, BlockBuilder.class, boolean.class);
        }
        else if (projectionType.getJavaType() == long.class) {
            notNullBlock
                    .comment("%s.writeLong(output, <booleanStackValue>);", projectionType.getName())
                    .invokeVirtual(projectionType.getClass(), "writeLong", void.class, BlockBuilder.class, long.class);
        }
        else if (projectionType.getJavaType() == double.class) {
            notNullBlock
                    .comment("%s.writeDouble(output, <booleanStackValue>);", projectionType.getName())
                    .invokeVirtual(projectionType.getClass(), "writeDouble", void.class, BlockBuilder.class, double.class);
        }
        else if (projectionType.getJavaType() == Slice.class) {
            notNullBlock
                    .comment("%s.writeSlice(output, <booleanStackValue>);", projectionType.getName())
                    .invokeVirtual(projectionType.getClass(), "writeSlice", void.class, BlockBuilder.class, Slice.class);
        }
        else {
            throw new UnsupportedOperationException("Type " + projectionType + " can not be output yet");
        }

        Block nullBlock = new Block(context)
                .comment("output.appendNull();")
                .pop(projectionType.getJavaType())
                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                .pop()
                .pop();

        projectionMethod.getBody()
                .comment("if the result was null, appendNull; otherwise append the value")
                .append(new IfStatement(context, new Block(context).getVariable("wasNull"), nullBlock, notNullBlock))
                .ret();
    }

    private <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, DynamicClassLoader classLoader)
    {
        Class<?> clazz = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next();
        return clazz.asSubclass(superType);
    }

    private Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions, DynamicClassLoader classLoader)
    {
        ClassInfoLoader classInfoLoader = ClassInfoLoader.createClassInfoLoader(classDefinitions, classLoader);

        if (DUMP_BYTE_CODE_TREE) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DumpByteCodeVisitor dumpByteCode = new DumpByteCodeVisitor(new PrintStream(out));
            for (ClassDefinition classDefinition : classDefinitions) {
                dumpByteCode.visitClass(classDefinition);
            }
            System.out.println(new String(out.toByteArray(), StandardCharsets.UTF_8));
        }

        Map<String, byte[]> byteCodes = new LinkedHashMap<>();
        for (ClassDefinition classDefinition : classDefinitions) {
            ClassWriter cw = new SmartClassWriter(classInfoLoader);
            classDefinition.visit(cw);
            byte[] byteCode = cw.toByteArray();
            if (RUN_ASM_VERIFIER) {
                ClassReader reader = new ClassReader(byteCode);
                CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(System.out));
            }
            byteCodes.put(classDefinition.getType().getJavaClassName(), byteCode);
        }

        String dumpClassPath = DUMP_CLASS_FILES_TO.get();
        if (dumpClassPath != null) {
            for (Entry<String, byte[]> entry : byteCodes.entrySet()) {
                File file = new File(dumpClassPath, ParameterizedType.typeFromJavaClassName(entry.getKey()).getClassName() + ".class");
                try {
                    log.debug("ClassFile: " + file.getAbsolutePath());
                    Files.createParentDirs(file);
                    Files.write(entry.getValue(), file);
                }
                catch (IOException e) {
                    log.error(e, "Failed to write generated class file to: %s" + file.getAbsolutePath());
                }
            }
        }
        if (DUMP_BYTE_CODE_RAW) {
            for (byte[] byteCode : byteCodes.values()) {
                ClassReader classReader = new ClassReader(byteCode);
                classReader.accept(new TraceClassVisitor(new PrintWriter(System.err)), ClassReader.SKIP_FRAMES);
            }
        }
        Map<String, Class<?>> classes = classLoader.defineClasses(byteCodes);
        generatedClasses.addAndGet(classes.size());
        return classes;
    }

    private static void setCallSitesField(Class<?> clazz, Map<Long, MethodHandle> callSites)
    {
        try {
            Field field = clazz.getDeclaredField("callSites");
            field.setAccessible(true);
            field.set(null, callSites);
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw Throwables.propagate(e);
        }
    }

    private static final class CacheKey
    {
        private final RowExpression filter;
        private final List<RowExpression> projections;
        private final Object uniqueKey;

        private CacheKey(RowExpression filter, List<RowExpression> projections, Object uniqueKey)
        {
            this.filter = filter;
            this.uniqueKey = uniqueKey;
            this.projections = ImmutableList.copyOf(projections);
        }

        private RowExpression getFilter()
        {
            return filter;
        }

        private List<RowExpression> getProjections()
        {
            return projections;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(filter, projections, uniqueKey);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            return Objects.equal(this.filter, other.filter) &&
                    Objects.equal(this.projections, other.projections) &&
                    Objects.equal(this.uniqueKey, other.uniqueKey);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filter", filter)
                    .add("projections", projections)
                    .add("uniqueKey", uniqueKey)
                    .toString();
        }
    }
}
