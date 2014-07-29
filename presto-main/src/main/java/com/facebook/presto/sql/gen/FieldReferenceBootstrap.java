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

import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.headius.invokebinder.Binder;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;
import java.lang.reflect.Method;

public final class FieldReferenceBootstrap
{
    public static final Method BOOTSTRAP_METHOD;
    private static final MethodHandle LINK_METHOD;

    static {
        try {
            BOOTSTRAP_METHOD = FieldReferenceBootstrap.class.getMethod("bootstrap", MethodHandles.Lookup.class, String.class, MethodType.class, int.class, long.class);
            LINK_METHOD = MethodHandles.lookup().findStatic(FieldReferenceBootstrap.class, "link", MethodType.methodType(Object.class, FieldAccessCallSite.class, Object[].class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private FieldReferenceBootstrap()
    {
    }

    public static CallSite bootstrap(MethodHandles.Lookup lookup, String name, MethodType callSiteType, int field, long typeConstantId)
    {
        Type type = null;
        if (typeConstantId != -1) {
            try {
                type = (Type) Bootstrap.getMethodHandleForConstant(lookup, name, callSiteType, typeConstantId).invoke();
            }
            catch (Throwable e) {
                throw Throwables.propagate(e);
            }
        }

        FieldAccessCallSite callSite = new FieldAccessCallSite(lookup, name, callSiteType, type, field);

        MethodHandle target = LINK_METHOD.bindTo(callSite)
                .asCollector(Object[].class, callSiteType.parameterCount())
                .asType(callSiteType);

        callSite.setTarget(target);
        return callSite;
    }

    public static Object link(FieldAccessCallSite callSite, Object[] args)
    {
        Object input = args[0];
        String name = callSite.getName();
        MethodHandles.Lookup lookup = callSite.getLookup();
        MethodType callSiteType = callSite.type();
        int field = callSite.getField();

        try {
            if (input instanceof RecordCursor) {
                if (name.equals("advanceNextPosition")) {
                    MethodHandle method = lookup
                            .findVirtual(RecordCursor.class, name, MethodType.methodType(boolean.class))
                            .asType(callSiteType);
                    callSite.setTarget(method);
                    return method.invokeWithArguments(args);
                }
                else {
                    MethodHandle method = lookup
                            .findVirtual(RecordCursor.class, name, MethodType.methodType(callSiteType.returnType(), int.class));
                    method = MethodHandles.insertArguments(method, 1, field);
                    method = MethodHandles.dropArguments(method, 1, int.class);
                    method = method.asType(callSiteType);
                    callSite.setTarget(method);
                    return method.invokeWithArguments(args);
                }
            }
            else if (input instanceof Page) {
                if (name.equals("advanceNextPosition")) {
                    callSite.setTarget(MethodHandles.dropArguments(MethodHandles.constant(boolean.class, true), 0, Object.class));
                    return true;
                }
                else if (name.equals("isNull")) {
                    MethodHandle getBlockMethod = MethodHandles.insertArguments(lookup.findVirtual(Page.class, "getBlock", MethodType.methodType(Block.class, int.class)), 1, field);
                    MethodHandle target = lookup.findVirtual(Block.class, "isNull", MethodType.methodType(boolean.class, int.class));
                    target = MethodHandles.filterArguments(target, 0, getBlockMethod).asType(callSite.type());
                    callSite.setTarget(target);
                    return target.invokeWithArguments(args);
                }

                Type type = callSite.getType();

                MethodHandle getBlockMethod = MethodHandles.insertArguments(lookup.findVirtual(Page.class, "getBlock", MethodType.methodType(Block.class, int.class)), 1, field);

                MethodHandle getFieldMethod = lookup.findVirtual(type.getClass(), name, MethodType.methodType(type.getJavaType(), Block.class, int.class));

                MethodHandle target = Binder.from(type.getJavaType(), Page.class, int.class)
                        .filter(0, getBlockMethod)
                        .insert(0, type)
                        .invoke(getFieldMethod);

                callSite.setTarget(target.asType(callSiteType));
                return target.invokeWithArguments(args);

                /*
                call(page, position) -> <type>.<getXXX>(page.getBlock(<field>), position)
                 */
            }
            else {
                throw new UnsupportedOperationException("Input not supported:" + input.getClass().getName());
            }
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    public static class FieldAccessCallSite
            extends MutableCallSite
    {
        private final MethodHandles.Lookup lookup;
        private final String name;
        private final int field;
        private final Type type;

        public FieldAccessCallSite(MethodHandles.Lookup lookup, String name, MethodType methodType, Type type, int field)
        {
            super(methodType);
            this.lookup = lookup;
            this.name = name;
            this.type = type;
            this.field = field;
        }

        public String getName()
        {
            return name;
        }

        public MethodHandles.Lookup getLookup()
        {
            return lookup;
        }

        public int getField()
        {
            return field;
        }

        public Type getType()
        {
            return type;
        }
    }
}
