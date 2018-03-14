package com.facebook.presto.connector.thrift;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.presto.connector.thrift.api.PrestoThriftFunctionService;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableFunction;
import com.facebook.presto.spi.function.PolymorphicTableFunction;
import com.facebook.presto.spi.function.TableFunctionImplementation;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.swift.service.ThriftClientManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class ThriftTableFunction
        implements PolymorphicTableFunction
{
    private static final JsonCodec<ThriftTableFunctionHandle> CODEC = JsonCodec.jsonCodec(ThriftTableFunctionHandle.class);
    private final TypeManager typeManager;

    public ThriftTableFunction(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return "thrift_call";
    }

    @Override
    public List<Parameter> getParameters()
    {
        return ImmutableList.of(
                new Parameter("name", VarcharType.VARCHAR.getTypeSignature()),
                new Parameter("address", VarcharType.VARCHAR.getTypeSignature()),
                new Parameter("input", ExtendedType.TABLE),
                new Parameter("output", ExtendedType.DESCRIPTOR));
    }

    @Override
    public TableFunction specialize(Map<String, Object> arguments)
    {
        String name = getName(arguments);
        HostAndPort address = getAddress(arguments);
        List<ColumnDescriptor> inputs = getInputs(arguments);
        List<ColumnDescriptor> outputs = getOutputs(arguments);

        ThriftTableFunctionHandle handle = new ThriftTableFunctionHandle(
                name,
                address,
                inputs.stream()
                        .map(type -> type.getType().get())
                        .collect(toImmutableList()),
                outputs.stream()
                        .map(type -> type.getType().get())
                        .collect(toImmutableList()));

        RowType outputType = RowType.from(outputs.stream()
                .map(column -> new RowType.Field(Optional.of(column.getName()), typeManager.getType(column.getType().get())))
                .collect(Collectors.toList()));

        return new TableFunction(
                CODEC.toJsonBytes(handle),
                false,
                IntStream.range(0, inputs.size()).boxed().collect(toImmutableList()),
                outputType);
    }

    @Override
    public TableFunctionImplementation getInstance(byte[] handleJson)
    {
        ThriftTableFunctionHandle handle = CODEC.fromJson(handleJson);

        ThriftClientManager clientManager = new ThriftClientManager();
        FramedClientConnector connector = new FramedClientConnector(handle.getAddress());
        PrestoThriftFunctionService service = getFutureValue(clientManager.createClient(connector, PrestoThriftFunctionService.class));

        return new ThriftTableFunctionImplementation(
                service,
                handle.getName(),
                handle.getInputTypes().stream()
                        .map(typeManager::getType)
                        .collect(toImmutableList()),
                handle.getOutputTypes().stream()
                        .map(typeManager::getType)
                        .collect(toImmutableList()));
    }

    private static String getName(Map<String, Object> arguments)
    {
        Object value = arguments.get("name");
        if (value == null) {
            throw new PrestoException(SYNTAX_ERROR, "Parameter 'name' is required");
        }
        if (!(value instanceof Slice)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Parameter 'name' must be a varchar");
        }
        return ((Slice) value).toStringUtf8();
    }

    private static HostAndPort getAddress(Map<String, Object> arguments)
    {
        Object value = arguments.get("address");
        if (value == null) {
            throw new PrestoException(SYNTAX_ERROR, "Parameter 'address' is required");
        }
        if (!(value instanceof Slice)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Parameter 'address' must be a varchar");
        }
        return HostAndPort.fromString(((Slice) value).toStringUtf8());
    }

    private static List<ColumnDescriptor> getInputs(Map<String, Object> arguments)
    {
        Object value = arguments.get("input");
        if (value == null) {
            throw new PrestoException(SYNTAX_ERROR, "Parameter 'input' is required");
        }
        if (!(value instanceof List)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Parameter 'input' must be a descriptor");
        }
        return ((Collection<?>) value).stream()
                .map(ColumnDescriptor.class::cast)
                .collect(toImmutableList());
    }

    private static List<ColumnDescriptor> getOutputs(Map<String, Object> arguments)
    {
        Object value = arguments.get("output");
        if (value == null) {
            throw new PrestoException(SYNTAX_ERROR, "Parameter 'output' is required");
        }
        if (!(value instanceof List)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Parameter 'output' must be a descriptor");
        }
        return ((Collection<?>) value).stream()
                .map(ColumnDescriptor.class::cast)
                .collect(toImmutableList());
    }

    public static class ThriftTableFunctionHandle
    {
        private final String name;
        private final HostAndPort address;
        private final List<TypeSignature> inputTypes;
        private final List<TypeSignature> outputTypes;

        @JsonCreator
        public ThriftTableFunctionHandle(
                @JsonProperty("name") String name,
                @JsonProperty("address") HostAndPort address,
                @JsonProperty("inputTypes") List<TypeSignature> inputTypes,
                @JsonProperty("outputTypes") List<TypeSignature> outputTypes)
        {
            this.name = requireNonNull(name, "name is null");
            this.address = requireNonNull(address, "address is null");
            this.inputTypes = ImmutableList.copyOf(requireNonNull(inputTypes, "inputTypes is null"));
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public HostAndPort getAddress()
        {
            return address;
        }

        @JsonProperty
        public List<TypeSignature> getInputTypes()
        {
            return inputTypes;
        }

        @JsonProperty
        public List<TypeSignature> getOutputTypes()
        {
            return outputTypes;
        }
    }
}
