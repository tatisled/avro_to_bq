package com.github.tatisled;

import com.github.tatisled.utils.BigQueryAvroUtils;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tatisled.utils.BigQueryAvroUtils.getAvroSchema;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class AvroToBq {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToBq.class);

    public interface AvroToBqOptions extends DataflowPipelineOptions {
        @Description("Input path")
        @Validation.Required
        ValueProvider<String> getInputPath();

        @SuppressWarnings("unused")
        void setInputPath(ValueProvider<String> path);

        @Description("BigQuery Table")
        @Validation.Required
        @Default.String("table")
        ValueProvider<String> getBqTable();

        @SuppressWarnings("unused")
        void setBqTable(ValueProvider<String> bqTable);
    }

    public static void main(String[] args) {
        AvroToBqOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(AvroToBqOptions.class);

        TableSchema ts = BigQueryAvroUtils.getTableSchema(getAvroSchema());
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read Avro from DataStorage", AvroIO.readGenericRecords(getAvroSchema())
                .from(options.getInputPath()))
                .apply("Write to BigQuery", BigQueryIO.<GenericRecord>write()
                        .to(options.getBqTable())
                        .withSchema(ts)
                        .withWriteDisposition(WRITE_APPEND)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withFormatFunction(genericRecord -> BigQueryAvroUtils.convertRecordToTableRow(
                                genericRecord
                                , BigQueryAvroUtils.getTableSchema(genericRecord.getSchema())
                        ))
                );

        pipeline.run();
    }
}
