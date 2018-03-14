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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableFunction;
import com.facebook.presto.spi.function.PolymorphicTableFunction;
import com.facebook.presto.spi.function.TableFunctionImplementation;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ApproximateMostFrequentTableFunction
        implements PolymorphicTableFunction
{
    private static final JsonCodec<ApproximateMostFrequentTableFunctionHandle> CODEC = JsonCodec.jsonCodec(ApproximateMostFrequentTableFunctionHandle.class);
    private final TypeManager typeManager;

    public ApproximateMostFrequentTableFunction(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return "approx_most_frequent";
    }

    @Override
    public List<Parameter> getParameters()
    {
        return ImmutableList.of(
                new Parameter("number", IntegerType.INTEGER.getTypeSignature()),
                new Parameter("error", DoubleType.DOUBLE.getTypeSignature()),
                new Parameter("input", ExtendedType.TABLE));
    }

    @Override
    public TableFunction specialize(Map<String, Object> arguments)
    {
        RowType input = getParameter(arguments, "input", RowType.class, "table");

        ApproximateMostFrequentTableFunctionHandle handle = new ApproximateMostFrequentTableFunctionHandle(
                getParameter(arguments, "number", Long.class, "integer").intValue(),
                getParameter(arguments, "error", Double.class, "double"),
                input.getFields().stream()
                        .map(RowType.Field::getType)
                        .map(Type::getTypeSignature)
                        .collect(toImmutableList()));

        RowType outputType = RowType.from(
                ImmutableList.<RowType.Field>builder()
                        .addAll(input.getFields())
                        .add(RowType.field("count", BIGINT))
                        .add(RowType.field("error", BIGINT))
                        .build());

        return new TableFunction(
                CODEC.toJsonBytes(handle),
                true,
                IntStream.range(0, input.getFields().size()).boxed().collect(toImmutableList()),
                outputType);
    }

    @Override
    public TableFunctionImplementation getInstance(byte[] handleJson)
    {
        ApproximateMostFrequentTableFunctionHandle handle = CODEC.fromJson(handleJson);

        List<Type> types = handle.getTypes().stream()
                .map(typeManager::getType)
                .collect(toImmutableList());

        return new ApproximateMostFrequentTableFunctionImplementation(types, handle.getNumber(), handle.getError());
    }

    private static <T> T getParameter(Map<String, Object> arguments, String name, Class<T> expectedType, final String sqlType)
    {
        Object value = arguments.get(name);
        if (value == null) {
            throw new PrestoException(SYNTAX_ERROR, "Parameter '" + name + "' is required");
        }
        if (!expectedType.isInstance(value)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Parameter '" + name + "' must be a " + sqlType);
        }
        return expectedType.cast(value);
    }

    private static List<ColumnDescriptor> getDescriptorParameter(Map<String, Object> arguments, String name)
    {
        List<?> input = getParameter(arguments, name, List.class, "descriptor");
        return input.stream()
                .map(ColumnDescriptor.class::cast)
                .collect(toImmutableList());
    }

    public static class ApproximateMostFrequentTableFunctionHandle
    {
        private final int number;
        private final double error;
        private final List<TypeSignature> types;

        @JsonCreator
        public ApproximateMostFrequentTableFunctionHandle(
                @JsonProperty("number") int number,
                @JsonProperty("error") double error,
                @JsonProperty("types") List<TypeSignature> types)
        {
            this.number = number;
            this.error = error;
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @JsonProperty
        public int getNumber()
        {
            return number;
        }

        @JsonProperty
        public double getError()
        {
            return error;
        }

        @JsonProperty
        public List<TypeSignature> getTypes()
        {
            return types;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("number", number)
                    .add("error", error)
                    .add("types", types)
                    .toString();
        }
    }
}
