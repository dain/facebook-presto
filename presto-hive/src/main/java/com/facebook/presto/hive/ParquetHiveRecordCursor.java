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
package com.facebook.presto.hive;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.joda.time.DateTimeZone;
import parquet.column.Dictionary;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import io.airlift.log.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;

class ParquetHiveRecordCursor
        extends HiveRecordCursor
{
    private static final Logger log = Logger.get(ParquetHiveRecordCursor.class);
    private final ParquetRecordReader<Void> recordReader;
    private final DateTimeZone sessionTimeZone;

    @SuppressWarnings("FieldCanBeLocal") // include names for debugging
    private final String[] names;
    private final Type[] types;

    private final boolean[] isPartitionColumn;

    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final boolean[] nulls;
    private final boolean[] nullsRowDefault;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;

    private List<Object> structBuffer = new ArrayList<Object>();

    private Object currentListElement;
    private List<Object> listBuffer = new ArrayList<Object>();

    private Object currentMapKey;
    private Object currentMapValue;
    private Map<Object, Object> mapBuffer = new HashMap<Object, Object>();

    public ParquetHiveRecordCursor(
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone sessionTimeZone)
    {
        checkNotNull(configuration, "jobConf is null");
        checkNotNull(path, "path is null");
        checkArgument(length >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");
        checkNotNull(sessionTimeZone, "sessionTimeZone is null");

        this.recordReader = createParquetRecordReader(configuration, path, start, length, columns);

        this.totalBytes = length;
        this.sessionTimeZone = sessionTimeZone;

        int size = columns.size();

        this.names = new String[size];
        this.types = new Type[size];

        this.isPartitionColumn = new boolean[size];

        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.nulls = new boolean[size];
        this.nullsRowDefault = new boolean[size];

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = column.getType();

            isPartitionColumn[i] = column.isPartitionKey();
            nullsRowDefault[i] = !column.isPartitionKey();
        }

        // parse requested partition columns
        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey.nameGetter());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                if (types[columnIndex].equals(BOOLEAN)) {
                    if (isTrue(bytes, 0, bytes.length)) {
                        booleans[columnIndex] = true;
                    }
                    else if (isFalse(bytes, 0, bytes.length)) {
                        booleans[columnIndex] = false;
                    }
                    else {
                        String valueString = new String(bytes, Charsets.UTF_8);
                        throw new IllegalArgumentException(String.format("Invalid partition value '%s' for BOOLEAN partition key %s", valueString, names[columnIndex]));
                    }
                }
                else if (types[columnIndex].equals(BIGINT)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for BIGINT partition key %s", names[columnIndex]));
                    }
                    longs[columnIndex] = parseLong(bytes, 0, bytes.length);
                }
                else if (types[columnIndex].equals(DOUBLE)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for DOUBLE partition key %s", names[columnIndex]));
                    }
                    doubles[columnIndex] = parseDouble(bytes, 0, bytes.length);
                }
                else if (types[columnIndex].equals(VARCHAR)) {
                    slices[columnIndex] = Slices.wrappedBuffer(bytes);
                }
                else {
                    throw new UnsupportedOperationException("Unsupported column type: " + types[columnIndex]);
                }
            }
        }
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        if (!closed) {
            updateCompletedBytes();
        }
        return completedBytes;
    }

    private void updateCompletedBytes()
    {
        try {
            long newCompletedBytes = (long) (totalBytes * recordReader.getProgress());
            completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
        }
        catch (IOException ignored) {
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            // reset null flags
            System.arraycopy(nullsRowDefault, 0, nulls, 0, isPartitionColumn.length);

            if (closed || !recordReader.nextKeyValue()) {
                close();
                return false;
            }

            return true;
        }
        catch (IOException | RuntimeException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR.toErrorCode(), e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, boolean.class);
        return booleans[fieldId];
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, long.class);
        return longs[fieldId];
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, double.class);
        return doubles[fieldId];
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Slice.class);
        return slices[fieldId];
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");
        return nulls[fieldId];
    }

    private void validateType(int fieldId, Class<?> javaType)
    {
        if (types[fieldId].getJavaType() != javaType) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, actual %s (field %s)", javaType.getName(), types[fieldId].getJavaType().getName(), fieldId));
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        updateCompletedBytes();

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private ParquetRecordReader<Void> createParquetRecordReader(Configuration configuration, Path path, long start, long length, List<HiveColumnHandle> columns)
    {
        try {
            PrestoReadSupport readSupport = new PrestoReadSupport(columns);

            ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(configuration, path);
            List<BlockMetaData> blocks = parquetMetadata.getBlocks();
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

            ReadContext readContext = readSupport.init(configuration, fileMetaData.getKeyValueMetaData(), fileMetaData.getSchema());

            List<BlockMetaData> splitGroup = new ArrayList<>();
            long splitStart = start;
            long splitLength = length;
            for (BlockMetaData block : blocks) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
                    splitGroup.add(block);
                }
            }

            ParquetInputSplit split;
            if (splitGroup.isEmpty()) {
                // split is empty
                return null;
            }

            split = new ParquetInputSplit(path,
                    splitStart,
                    splitLength,
                    null,
                    splitGroup,
                    readContext.getRequestedSchema().toString(),
                    fileMetaData.getSchema().toString(),
                    fileMetaData.getKeyValueMetaData(),
                    readContext.getReadSupportMetadata());

            TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(configuration, new TaskAttemptID());
            ParquetRecordReader<Void> realReader = new ParquetRecordReader<>(readSupport);
            realReader.initialize(split, taskContext);
            return realReader;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    public class PrestoReadSupport
            extends ReadSupport<Void>
    {
        private final List<HiveColumnHandle> columns;
        private List<Converter> converters;

        public PrestoReadSupport(List<HiveColumnHandle> columns)
        {
            this.columns = columns;
        }

        @Override
        @SuppressWarnings("deprecation")
        public ReadContext init(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema)
        {
            ImmutableList.Builder<Converter> converters = ImmutableList.builder();
            ImmutableList.Builder<parquet.schema.Type> fields = ImmutableList.builder();
            for (int i = 0; i < columns.size(); i++) {
                HiveColumnHandle column = columns.get(i);
                if (!column.isPartitionKey()) {
                    fields.add(fileSchema.getType(column.getName()));
                    HiveType hiveType = column.getHiveType();
                    switch (hiveType) {
                        case BOOLEAN:
                        case BYTE:
                        case SHORT:
                        case STRING:
                        case INT:
                        case LONG:
                        case FLOAT:
                        case DOUBLE:
                            converters.add(new ParquetPrimitiveConverter(i));
                            break;
                        case MAP:
                            converters.add(new ParquetMapConverter(i, fileSchema.getType(column.getName())));
                            break;
                        case LIST:
                            converters.add(new ParquetListConverter(i, fileSchema.getType(column.getName())));
                            break;
                        case STRUCT:
                            converters.add(new ParquetStructConverter(i, fileSchema.getType(column.getName())));
                            break;
                        default:
                            break;
                    }
                }
            }
            this.converters = converters.build();
            MessageType requestedProjection = new MessageType(fileSchema.getName(), fields.build());
            return new ReadContext(requestedProjection);
        }

        @Override
        public RecordMaterializer<Void> prepareForRead(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema,
                ReadContext readContext)
        {
            return new ParquetRecordConverter(converters);
        }
    }

    private class ParquetRecordConverter
            extends RecordMaterializer<Void>
    {
        private final GroupConverter groupConverter;

        public ParquetRecordConverter(List<Converter> converters)
        {
            groupConverter = new ParquetGroupConverter(converters);
        }

        @Override
        public Void getCurrentRecord()
        {
            return null;
        }

        @Override
        public GroupConverter getRootConverter()
        {
            return groupConverter;
        }
    }

    public class ParquetGroupConverter
        extends GroupConverter
    {
        private final List<Converter> converters;

        public ParquetGroupConverter(List<Converter> converters)
        {
            this.converters = converters;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converters.get(fieldIndex);
        }

        @Override
        public void start()
        {
        }

        @Override
        public void end()
        {
        }
    }

    private enum ParquetValueType
    {
        PRIMITIVE,
        LIST_ELEMENT,
        STRUCT_ELEMENT,
        MAP_KEY,
        MAP_VALUE
    }

    private enum JsonFieldType
    {
        FIELD_NAME,
        FIELD_VALUE
    }

    private void serializeObject(JsonGenerator generator, PrimitiveTypeName parquetTypeName,
                                Object element, JsonFieldType jsonFieldType)
        throws IOException
    {
        if (parquetTypeName.equals(PrimitiveTypeName.BOOLEAN)) {
            switch (jsonFieldType) {
                case FIELD_NAME :
                    generator.writeFieldName(String.valueOf((Boolean) element));
                    break;
                case FIELD_VALUE :
                    generator.writeBoolean((Boolean) element);
                    break;
                default:
                    break;
            }
        }
        else if (parquetTypeName.equals(PrimitiveTypeName.INT32) ||
                parquetTypeName.equals(PrimitiveTypeName.INT64)) {
            switch (jsonFieldType) {
                case FIELD_NAME :
                    generator.writeFieldName(String.valueOf((long) element));
                    break;
                case FIELD_VALUE :
                    generator.writeNumber((long) element);
                    break;
                default:
                    break;
            }
        }
        else if (parquetTypeName.equals(PrimitiveTypeName.FLOAT) ||
                parquetTypeName.equals(PrimitiveTypeName.DOUBLE)) {
            switch (jsonFieldType) {
                case FIELD_NAME :
                    generator.writeFieldName(String.valueOf((double) element));
                    break;
                case FIELD_VALUE :
                    generator.writeNumber((double) element);
                    break;
                default:
                    break;
            }
        }
        else if (parquetTypeName.equals(PrimitiveTypeName.BINARY) ||
                parquetTypeName.equals(PrimitiveTypeName.INT96) ||
                parquetTypeName.equals(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
            switch (jsonFieldType) {
                case FIELD_NAME :
                    generator.writeFieldName(((Binary) element).toStringUsingUTF8());
                    break;
                case FIELD_VALUE :
                    generator.writeString(((Binary) element).toStringUsingUTF8());
                    break;
                default:
                    break;
            }
        }
        else {
            throw new IOException("Invalid Parquet Primitive Type");
        }
    }

    public class ParquetStructConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final List<Converter> converterList = new ArrayList<Converter>();
        private final parquet.schema.Type parquetSchema;

        private ParquetStructConverter(int fieldIndex, parquet.schema.Type parquetSchema)
        {
            this.fieldIndex = fieldIndex;
            this.parquetSchema = parquetSchema;
            for (int i = 0; i < parquetSchema.asGroupType().getFieldCount(); i++) {
                if (parquetSchema.asGroupType().getType(i).isPrimitive()) {
                    converterList.add(new ParquetPrimitiveConverter(fieldIndex, ParquetValueType.STRUCT_ELEMENT));
                }
                else {
                    throw new IllegalArgumentException("Not Support Nested Struct in Parquet: " + parquetSchema.asGroupType().getType(i));
                }
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converterList.get(fieldIndex);
        }

        @Override
        public void start()
        {
            structBuffer.clear();
        }

        @Override
        public void end()
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (JsonGenerator generator = new JsonFactory().createGenerator(out)) {
                generator.writeStartObject();
                for (int i = 0; i < structBuffer.size(); i++) {
                    PrimitiveTypeName parquetTypeName =
                        this.parquetSchema.asGroupType().getType(i).asPrimitiveType().getPrimitiveTypeName();
                    String fieldName = this.parquetSchema.asGroupType().getFieldName(i);
                    generator.writeFieldName(fieldName);
                    serializeObject(generator, parquetTypeName, structBuffer.get(i), JsonFieldType.FIELD_VALUE);
                }
                generator.writeEndObject();
                generator.close();
            }
            catch (IOException e) {
                log.error("Error Converting Parquet Struct into Json");
                throw Throwables.propagate(e);
            }
            slices[fieldIndex] = Slices.wrappedBuffer(out.toByteArray());
            structBuffer.clear();
        }
    }

    public class ParquetListConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final ParquetArrayConverter array;
        private final parquet.schema.Type parquetSchema;

        private ParquetListConverter(int fieldIndex, parquet.schema.Type parquetSchema)
        {
            if (parquetSchema.asGroupType().getFieldCount() != 1) {
                throw new IllegalArgumentException("Invalid Parquet Array Schema: " + parquetSchema);
            }
            this.fieldIndex = fieldIndex;
            this.parquetSchema = parquetSchema;
            this.array =
                    new ParquetArrayConverter(fieldIndex,
                                                parquetSchema.asGroupType().getType(0).asGroupType());
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("Parquet Array could not reach index: " + fieldIndex);
            }
            return array;
        }

        @Override
        public void start()
        {
            listBuffer.clear();
        }

        @Override
        public void end()
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (JsonGenerator generator = new JsonFactory().createGenerator(out)) {
                generator.writeStartArray();
                Iterator<Object> itr = listBuffer.iterator();
                while (itr.hasNext()) {
                    PrimitiveTypeName parquetTypeName =
                        this.parquetSchema.asGroupType().getType(0).asGroupType().getType(0).asPrimitiveType().getPrimitiveTypeName();
                    Object element = itr.next();
                    serializeObject(generator, parquetTypeName, element, JsonFieldType.FIELD_VALUE);
                }
                generator.writeEndArray();
                generator.close();
            }
            catch (IOException e) {
                log.error("Error Converting Parquet Array into Json");
                throw Throwables.propagate(e);
            }
            slices[fieldIndex] = Slices.wrappedBuffer(out.toByteArray());
        }
    }

    public class ParquetArrayConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final Converter elementConverter;

        private ParquetArrayConverter(int fieldIndex,
                                        parquet.schema.GroupType parquetSchema)
        {
            if (parquetSchema.getFieldCount() != 1) {
                throw new IllegalArgumentException("Invalid Parquet Array Schema: " + parquetSchema.toString());
            }
            this.fieldIndex = fieldIndex;

            if (parquetSchema.getType(0).isPrimitive()) {
                this.elementConverter = new ParquetPrimitiveConverter(fieldIndex, ParquetValueType.LIST_ELEMENT);
            }
            else {
                throw new IllegalArgumentException("Not Support Nested Array in Parquet");
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return elementConverter;
            }
            throw new IllegalArgumentException("Parquet Array could not reach index: " + fieldIndex);
        }

        @Override
        public void start()
        {
        }

        @Override
        public void end()
        {
            listBuffer.add(currentListElement);
        }
    }

    public class ParquetMapConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final ParquetKeyValueConverter keyValue;
        private final parquet.schema.Type parquetSchema;

        private ParquetMapConverter(int fieldIndex, parquet.schema.Type parquetSchema)
        {
            if (parquetSchema.asGroupType().getFieldCount() != 1) {
                throw new IllegalArgumentException("Invalid Parquet Map Schema: " + parquetSchema);
            }
            this.fieldIndex = fieldIndex;
            this.parquetSchema = parquetSchema;
            this.keyValue =
                    new ParquetKeyValueConverter(fieldIndex,
                                                parquetSchema.asGroupType().getType(0).asGroupType());
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("Parquet Map could not reach index: " + fieldIndex);
            }
            return keyValue;
        }

        @Override
        public void start()
        {
            mapBuffer.clear();
        }

        @Override
        public void end()
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (JsonGenerator generator = new JsonFactory().createGenerator(out)) {
                generator.writeStartObject();
                for (Map.Entry<Object, Object> entry : mapBuffer.entrySet()) {
                    PrimitiveTypeName keyTypeName = parquetSchema.asGroupType().getType(0).asGroupType().getType(0).asPrimitiveType().getPrimitiveTypeName();
                    PrimitiveTypeName valueTypeName = parquetSchema.asGroupType().getType(0).asGroupType().getType(1).asPrimitiveType().getPrimitiveTypeName();
                    serializeObject(generator, keyTypeName, entry.getKey(), JsonFieldType.FIELD_NAME);
                    serializeObject(generator, valueTypeName, entry.getValue(), JsonFieldType.FIELD_VALUE);
                }
                generator.writeEndObject();
                generator.close();
            }
            catch (IOException e) {
                log.error("Error Converting Parquet Map into Json");
                throw Throwables.propagate(e);
            }
            slices[fieldIndex] = Slices.wrappedBuffer(out.toByteArray());
        }
    }

    public class ParquetKeyValueConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final Converter keyConverter;
        private final Converter valueConverter;

        private ParquetKeyValueConverter(int fieldIndex,
                                        parquet.schema.GroupType parquetSchema)
        {
            if (parquetSchema.getFieldCount() != 2
                || !parquetSchema.getType(0).getName().equals("key")
                || !parquetSchema.getType(1).getName().equals("value")) {
                    throw new IllegalArgumentException("Invalid Parquet Map Schema: " + parquetSchema.toString());
            }
            this.fieldIndex = fieldIndex;

            if (parquetSchema.getType(0).isPrimitive()) {
                this.keyConverter = new ParquetPrimitiveConverter(fieldIndex, ParquetValueType.MAP_KEY);
            }
            else {
                throw new IllegalArgumentException("Not Support Nested Map in Parquet");
            }

            if (parquetSchema.getType(1).isPrimitive()) {
                this.valueConverter = new ParquetPrimitiveConverter(fieldIndex, ParquetValueType.MAP_VALUE);
            }
            else {
                throw new IllegalArgumentException("Not Support Nested Map in Parquet");
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return keyConverter;
            }
            else if (fieldIndex == 1) {
                return valueConverter;
            }
            throw new IllegalArgumentException("Parquet Map could not reach index: " + fieldIndex);
        }

        @Override
        public void start()
        {
        }

        @Override
        public void end()
        {
            mapBuffer.put(currentMapKey, currentMapValue);
        }
    }

    private enum PrimitiveType {
        BOOLEAN,
        DOUBLE,
        LONG,
        BINARY,
        FLOAT,
        INT
    }

    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    private class ParquetPrimitiveConverter
            extends PrimitiveConverter
    {
        private final int fieldIndex;
        private final ParquetValueType valueType;

        private ParquetPrimitiveConverter(int fieldIndex)
        {
            this(fieldIndex, ParquetValueType.PRIMITIVE);
        }

        private ParquetPrimitiveConverter(int fieldIndex, ParquetValueType valueType)
        {
            this.fieldIndex = fieldIndex;
            this.valueType = valueType;
        }

        @Override
        public boolean isPrimitive()
        {
            return true;
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter()
        {
            return this;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return false;
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
        }

        private void setValue(PrimitiveType primitiveType, Object value)
        {
            nulls[fieldIndex] = false;
            switch (valueType) {
                case MAP_KEY:
                    currentMapKey = value;
                case MAP_VALUE:
                    currentMapValue = value;
                case LIST_ELEMENT:
                    currentListElement = value;
                case STRUCT_ELEMENT:
                    structBuffer.add(value);
                case PRIMITIVE:
                    switch (primitiveType) {
                        case BOOLEAN:
                            booleans[fieldIndex] = ((Boolean) value).booleanValue();
                            break;
                        case DOUBLE:
                        case FLOAT:
                            doubles[fieldIndex] = ((Double) value).doubleValue();
                            break;
                        case INT:
                        case LONG:
                            longs[fieldIndex] = ((Long) value).longValue();
                            break;
                        case BINARY:
                            slices[fieldIndex] = Slices.wrappedBuffer(((Binary) value).getBytes());
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }

        @Override
        public void addBoolean(boolean value)
        {
            setValue(PrimitiveType.BOOLEAN, Boolean.valueOf(value));
        }

        @Override
        public void addDouble(double value)
        {
            setValue(PrimitiveType.DOUBLE, Double.valueOf(value));
        }

        @Override
        public void addLong(long value)
        {
            setValue(PrimitiveType.LONG, Long.valueOf(value));
        }

        @Override
        public void addBinary(Binary value)
        {
            setValue(PrimitiveType.BINARY, value);
        }

        @Override
        public void addFloat(float value)
        {
            setValue(PrimitiveType.FLOAT, Double.valueOf(value));
        }

        @Override
        public void addInt(int value)
        {
            setValue(PrimitiveType.INT, Long.valueOf(value));
        }
    }
}
