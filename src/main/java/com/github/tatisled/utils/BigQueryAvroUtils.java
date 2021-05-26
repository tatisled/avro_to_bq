package com.github.tatisled.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.time.temporal.ChronoField.*;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

public final class BigQueryAvroUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryAvroUtils.class);

    public static class BigQueryAvroMapper {

        private BigQueryAvroMapper() {
        }

        private static final ImmutableMultimap<String, Type> BIG_QUERY_TO_AVRO_TYPES =
                ImmutableMultimap.<String, Type>builder()
                        .put("STRING", Type.STRING)
                        .put("GEOGRAPHY", Type.STRING)
                        .put("BYTES", Type.BYTES)
                        .put("INTEGER", Type.INT)
                        .put("FLOAT", Type.FLOAT)
                        .put("FLOAT64", Type.DOUBLE)
                        .put("NUMERIC", Type.BYTES)
                        .put("BOOLEAN", Type.BOOLEAN)
                        .put("INT64", Type.LONG)
                        .put("TIMESTAMP", Type.LONG)
                        .put("RECORD", Type.RECORD)
                        .put("DATE", Type.INT)
                        .put("DATETIME", Type.STRING)
                        .put("TIME", Type.LONG)
                        .put("STRUCT", Type.RECORD)
                        .put("ARRAY", Type.ARRAY)
                        .build();

        private static final ImmutableMultimap<Type, String> AVRO_TYPES_TO_BIG_QUERY =
                BIG_QUERY_TO_AVRO_TYPES.inverse();

        public static String getBigQueryTypes(Type type) {
            return AVRO_TYPES_TO_BIG_QUERY.get(type).iterator().next();
        }

    }

    /**
     * Formats BigQuery seconds-since-epoch into String matching JSON export. Thread-safe and
     * immutable.
     */
    private static final DateTimeFormatter DATE_AND_SECONDS_FORMATTER =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

    static String formatTimestamp(DateTime ts) {
        String dayAndTime = ts.toString(DATE_AND_SECONDS_FORMATTER);
        return String.format("%s UTC", dayAndTime);
    }

    /**
     * This method formats a BigQuery DATE value into a String matching the format used by JSON
     * export. Date records are stored in "days since epoch" format, and BigQuery uses the proleptic
     * Gregorian calendar.
     */
    private static String formatDate(LocalDate date) {
        return date.toString(ISODateTimeFormat.date());
    }

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_MICROS =
            new DateTimeFormatterBuilder()
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .appendLiteral('.')
                    .appendFraction(NANO_OF_SECOND, 6, 6, false)
                    .toFormatter();

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_MILLIS =
            new DateTimeFormatterBuilder()
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .appendLiteral('.')
                    .appendFraction(NANO_OF_SECOND, 3, 3, false)
                    .toFormatter();

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_SECONDS =
            new DateTimeFormatterBuilder()
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .toFormatter();

    /**
     * This method formats a BigQuery TIME value into a String matching the format used by JSON
     * export. Time records are stored in "microseconds since midnight" format.
     */
    private static String formatTime(long timeMicros) {
        java.time.format.DateTimeFormatter formatter;
        if (timeMicros % 1000000 == 0) {
            formatter = ISO_LOCAL_TIME_FORMATTER_SECONDS;
        } else if (timeMicros % 1000 == 0) {
            formatter = ISO_LOCAL_TIME_FORMATTER_MILLIS;
        } else {
            formatter = ISO_LOCAL_TIME_FORMATTER_MICROS;
        }
        return LocalTime.ofNanoOfDay(timeMicros * 1000).format(formatter);
    }

    private static TableRow convertGenericRecordToTableRow(
            GenericRecord record, List<TableFieldSchema> fields) {
        TableRow row = new TableRow();
        for (TableFieldSchema subSchema : fields) {
            Field field = record.getSchema().getField(subSchema.getName());
            Object convertedValue =
                    getTypedCellValue(field.schema(), subSchema, record.get(field.name()));
            if (convertedValue != null) {
                row.set(field.name(), convertedValue);
            }
        }

        return row;
    }

    public static TableRow convertRecordToTableRow(GenericRecord record, TableSchema schema) {
        return convertRecordToTableRow(record, schema.getFields());
    }

    private static TableRow convertRecordToTableRow(
            GenericRecord record, List<TableFieldSchema> fields) {
        TableRow row = new TableRow();
        for (TableFieldSchema subSchema : fields) {
            Field field = record.getSchema().getField(subSchema.getName());
            if (field == null || field.name() == null) {
                continue;
            }
            Object convertedValue = getTypedCellValue(field.schema(), subSchema, record.get(field.pos()));
            if (convertedValue != null) {
                row.set(field.name(), convertedValue);
            }
        }
        return row;
    }

    @Nullable
    private static Object getTypedCellValue(Schema schema, TableFieldSchema fieldSchema, Object v) {
        String mode = firstNonNull(fieldSchema.getMode(), "NULLABLE");
        switch (mode) {
            case "REQUIRED":
                return convertRequiredField(schema.getTypes().get(0).getType(), schema.getLogicalType(), fieldSchema, v);
            case "REPEATED":
                return convertRepeatedField(schema, fieldSchema, v);
            case "NULLABLE":
                return convertNullableField(schema, fieldSchema, v);
            default:
                throw new UnsupportedOperationException(
                        "Parsing a field with BigQuery field schema mode " + fieldSchema.getMode());
        }
    }

    private static List<Object> convertRepeatedField(
            Schema schema, TableFieldSchema fieldSchema, Object v) {

        if (v == null) {
            return new ArrayList<>();
        }
        @SuppressWarnings("unchecked")
        List<Object> elements = (List<Object>) v;
        ArrayList<Object> values = new ArrayList<>();
        Type elementType = schema.getElementType().getType();
        LogicalType elementLogicalType = schema.getElementType().getLogicalType();
        for (Object element : elements) {
            values.add(convertRequiredField(elementType, elementLogicalType, fieldSchema, element));
        }
        return values;
    }

    private static Object convertRequiredField(
            Type avroType, LogicalType avroLogicalType, TableFieldSchema fieldSchema, Object v) {
        String bqType = fieldSchema.getType();

        switch (bqType) {
            case "STRING":
            case "DATETIME":
            case "GEOGRAPHY":
                return v.toString();
            case "DATE":
                if (avroType == Type.INT) {
                    return formatDate((LocalDate) v);
                } else {
                    return v.toString();
                }
            case "TIME":
                if (avroType == Type.LONG) {
                    return formatTime((Long) v);
                } else {
                    return v.toString();
                }
            case "INTEGER":
            case "INT64":
            case "LONG":
            case "FLOAT64":
            case "FLOAT":
            case "BOOLEAN":
                return v;
            case "NUMERIC":
                BigDecimal numericValue =
                        new Conversions.DecimalConversion()
                                .fromBytes((ByteBuffer) v, Schema.create(avroType), avroLogicalType);
                return numericValue.toString();
            case "TIMESTAMP":
                return formatTimestamp((DateTime) v);
            case "STRUCT":
                return convertRecordToTableRow((GenericRecord) v, fieldSchema.getFields());
            case "RECORD":
                return convertGenericRecordToTableRow((GenericRecord) v, fieldSchema.getFields());
            case "BYTES":
                ByteBuffer byteBuffer = (ByteBuffer) v;
                byte[] bytes = new byte[byteBuffer.limit()];
                byteBuffer.get(bytes);
                return BaseEncoding.base64().encode(bytes);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unexpected BigQuery field schema type %s for field named %s",
                                fieldSchema.getType(), fieldSchema.getName()));
        }
    }

    @Nullable
    private static Object convertNullableField(
            Schema avroSchema, TableFieldSchema fieldSchema, Object v) {
        List<Schema> unionTypes = avroSchema.getTypes();

        if (v == null) {
            return null;
        }

        Type firstType = unionTypes.get(0).getType();
        if (!firstType.equals(Type.NULL)) {
            return convertRequiredField(firstType, unionTypes.get(0).getLogicalType(), fieldSchema, v);
        }
        return convertRequiredField(
                unionTypes.get(1).getType(), unionTypes.get(1).getLogicalType(), fieldSchema, v);
    }

    public static TableSchema getTableSchema(Schema schema) {
        TableSchema ts = new TableSchema();
        List<TableFieldSchema> fields = getTableFieldSchema(schema);
        ts.setFields(fields);
        return ts;
    }

    public static Schema getAvroSchema() {
        ClassLoader classLoader = BigQueryAvroUtils.class.getClassLoader();
        try {
            return new Schema.Parser().parse(new File(Objects.requireNonNull(classLoader.getResource("schema.avsc")).getFile()));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    private static List<TableFieldSchema> getTableFieldSchema(Schema schema) {
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        if (schema != null) {
            for (Schema.Field field : schema.getFields()) {
                String type = getBqType(field);
                if ("ARRAY".equals(type)) {
                    Schema childSchema = field.schema().getElementType();
                    if (childSchema.getType() == Schema.Type.RECORD) {
                        List<TableFieldSchema> child = getTableFieldSchema(field.schema().getElementType());
                        TableFieldSchema tfs =
                                new TableFieldSchema()
                                        .setName(field.name())
                                        .setType("STRUCT")
                                        .setFields(child)
                                        .setMode("REPEATED");
                        tableFieldSchemas.add(tfs);
                    } else {

                        TableFieldSchema tfs =
                                new TableFieldSchema()
                                        .setName(field.name())
                                        .setType(getBqType(childSchema.getFields().get(0)))
                                        .setMode("REPEATED");
                        tableFieldSchemas.add(tfs);
                    }

                } else if ("RECORD".equals(type)) {
                    TableFieldSchema tfs =
                            new TableFieldSchema()
                                    .setName(field.name())
                                    .setType("STRUCT")
                                    .setFields(getTableFieldSchema(field.schema()));
                    tableFieldSchemas.add(tfs);
                } else if (type != null) {
                    TableFieldSchema tfs =
                            new TableFieldSchema().setName(field.name()).setType(type).setMode("NULLABLE");
                    tableFieldSchemas.add(tfs);
                }
            }
        }
        return tableFieldSchemas;
    }

    static String getBqType(Schema.Field field) {
        Schema t = field.schema();
        Schema.Type type;

        if (t.getType().equals(Type.UNION)) {
            if (t.getTypes().size() > 0 && t.getTypes().get(0) != null) {
                type = t.getTypes().get(0).getType();
            } else {
                throw new IllegalArgumentException("Field type cannot be null for UNION field {" + t.getFullName() + "}.");
            }
        } else {
            type = t.getType();
        }

        String logicalType = null;
        if (field.schema().getLogicalType() != null) {
            logicalType = field.schema().getLogicalType().getName();
        }

        if (type.equals(Schema.Type.LONG)
                && "timestamp-millis".equals(logicalType)) {
            return "TIMESTAMP";
        } else if (type.equals(Schema.Type.INT)
                && "date".equals(logicalType)) {
            return "DATE";
        } else {
            return BigQueryAvroUtils.BigQueryAvroMapper.getBigQueryTypes(type);
        }
    }
}
