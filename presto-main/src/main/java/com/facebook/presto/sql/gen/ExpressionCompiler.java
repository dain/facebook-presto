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

import com.facebook.presto.Session;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.execution.FunctionDecoder;
import com.facebook.presto.execution.FunctionUtils;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static com.facebook.presto.sql.relational.Signatures.COALESCE;
import static com.facebook.presto.sql.relational.Signatures.IF;
import static com.facebook.presto.sql.relational.Signatures.IN;
import static com.facebook.presto.sql.relational.Signatures.IS_NULL;
import static com.facebook.presto.sql.relational.Signatures.NULL_IF;
import static com.facebook.presto.sql.relational.Signatures.SWITCH;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.concat;

public class ExpressionCompiler
{
    private final Metadata metadata;
    private final FunctionDecoder functionDecoder;

    private final LoadingCache<CacheKey, PageProcessor> pageProcessors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, PageProcessor>()
            {
                @Override
                public PageProcessor load(CacheKey key)
                        throws Exception
                {
                    FunctionRegistry functionRegistry = metadata.getFunctionRegistry(key.getSessionScopedFunctions());
                    return compileAndInstantiate(key.getFilter(), key.getProjections(), new PageProcessorCompiler(functionRegistry), PageProcessor.class);
                }
            });

    private final LoadingCache<CacheKey, CursorProcessor> cursorProcessors = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, CursorProcessor>()
            {
                @Override
                public CursorProcessor load(CacheKey key)
                        throws Exception
                {
                    FunctionRegistry functionRegistry = metadata.getFunctionRegistry(key.getSessionScopedFunctions());
                    return compileAndInstantiate(key.getFilter(), key.getProjections(), new CursorProcessorCompiler(functionRegistry), CursorProcessor.class);
                }
            });

    @Inject
    public ExpressionCompiler(Metadata metadata, FunctionDecoder functionDecoder)
    {
        this.metadata = metadata;
        this.functionDecoder = functionDecoder;
    }

    @Managed
    public long getCacheSize()
    {
        return pageProcessors.size();
    }

    public CursorProcessor compileCursorProcessor(RowExpression filter, List<RowExpression> projections, Session session, Object uniqueKey)
    {
        return cursorProcessors.getUnchecked(new CacheKey(
                filter,
                projections,
                extractSessionScopedFunctions(session, metadata.getFunctionRegistry(ImmutableMap.of()), functionDecoder, concat(ImmutableList.of(filter), projections)),
                uniqueKey));
    }

    public PageProcessor compilePageProcessor(RowExpression filter, List<RowExpression> projections, Session session)
    {
        return pageProcessors.getUnchecked(new CacheKey(
                filter,
                projections,
                extractSessionScopedFunctions(session, metadata.getFunctionRegistry(ImmutableMap.of()), functionDecoder, concat(ImmutableList.of(filter), projections)),
                null));
    }

    private <T> T compileAndInstantiate(RowExpression filter, List<RowExpression> projections, BodyCompiler<T> bodyCompiler, Class<? extends T> superType)
    {
        // create filter and project page iterator class
        Class<? extends T> clazz = compileProcessor(filter, projections, bodyCompiler, superType);
        try {
            return clazz.newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private <T> Class<? extends T> compileProcessor(
            RowExpression filter,
            List<RowExpression> projections,
            BodyCompiler<T> bodyCompiler,
            Class<? extends T> superType)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(superType.getSimpleName()),
                type(Object.class),
                type(superType));

        classDefinition.declareDefaultConstructor(a(PUBLIC));

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        bodyCompiler.generateMethods(classDefinition, callSiteBinder, filter, projections);

        //
        // toString method
        //
        generateToString(
                classDefinition,
                callSiteBinder,
                toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filter)
                        .add("projections", projections)
                        .toString());

        return defineClass(classDefinition, superType, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private static void generateToString(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, String string)
    {
        // bind constant via invokedynamic to avoid constant pool issues due to large strings
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(string, String.class), "toString"))
                .retObject();
    }

    private static Map<Signature, FunctionInfo> extractSessionScopedFunctions(Session session, FunctionRegistry registry, FunctionDecoder functionDecoder, Iterable<RowExpression> expressions)
    {
        ImmutableMap.Builder<Signature, FunctionInfo> sessionScopedFunctions = ImmutableMap.builder();

        for (RowExpression expression : expressions) {
            expression.accept(new RowExpressionVisitor<Void, Void>()
            {
                @Override
                public Void visitCall(CallExpression call, Void context)
                {
                    // visit children
                    for (RowExpression argument : call.getArguments()) {
                        argument.accept(this, context);
                    }

                    Signature signature = call.getSignature();

                    // CAST can not be part of a switch because it is not compile time constant
                    if (signature.getName().equals(CAST)) {
                        return null;
                    }

                    switch (signature.getName()) {
                        case IF:
                        case NULL_IF:
                        case SWITCH:
                        case "WHEN":
                        case IS_NULL:
                        case "IS_DISTINCT_FROM":
                        case COALESCE:
                        case IN:
                        case "AND":
                        case "OR":
                            return null;
                    }

                    FunctionInfo function = registry.getExactFunction(signature);
                    if (function == null) {
                        // TODO: temporary hack to deal with magic timestamp literal functions which don't have an "exact" form and need to be "resolved"
                        function = registry.resolveFunction(QualifiedName.of(signature.getName()), signature.getArgumentTypes(), false);
                    }
                    if (function == null) {
                        // check if the function is in the session
                        String functionText = session.getSystemProperties().get(FunctionUtils.getFunctionSessionPropertyName(signature));
                        if (functionText != null) {
                            FunctionInfo functionInfo = functionDecoder.decode(registry, functionText);
                            sessionScopedFunctions.put(signature, functionInfo);
                        }
                    }
                    return null;
                }

                @Override
                public Void visitInputReference(InputReferenceExpression reference, Void context)
                {
                    return null;
                }

                @Override
                public Void visitConstant(ConstantExpression literal, Void context)
                {
                    return null;
                }
            }, null);
        }

        return sessionScopedFunctions.build();
    }

    private static final class CacheKey
    {
        private final RowExpression filter;
        private final List<RowExpression> projections;
        private final Map<Signature, FunctionInfo> sessionScopedFunctions;
        private final Object uniqueKey;

        private CacheKey(RowExpression filter,
                List<RowExpression> projections,
                Map<Signature, FunctionInfo> sessionScopedFunctions,
                Object uniqueKey)
        {
            this.filter = filter;
            this.uniqueKey = uniqueKey;
            this.sessionScopedFunctions = sessionScopedFunctions;
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

        public Map<Signature, FunctionInfo> getSessionScopedFunctions()
        {
            return sessionScopedFunctions;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filter, projections, sessionScopedFunctions, uniqueKey);
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
            return Objects.equals(this.filter, other.filter) &&
                    Objects.equals(this.projections, other.projections) &&
                    Objects.equals(this.sessionScopedFunctions, other.sessionScopedFunctions) &&
                    Objects.equals(this.uniqueKey, other.uniqueKey);
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
