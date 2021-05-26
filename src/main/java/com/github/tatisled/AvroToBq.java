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

    public interface AvroToBqOptions extends PipelineOptions, DataflowPipelineOptions {
        @Description("Google cloud bucket url")
        @Validation.Required
        @SuppressWarnings("unused")
        ValueProvider<String> getGcBucket();

        @SuppressWarnings("unused")
        void setGcBucket(ValueProvider<String> path);

        @Description("Input path")
        @Validation.Required
        ValueProvider<String> getInputPath();

        @SuppressWarnings("unused")
        void setInputPath(ValueProvider<String> path);

        @Description("BigQuery Dataset")
        @Validation.Required
        ValueProvider<String> getDataset();

        @SuppressWarnings("unused")
        void setDataset(ValueProvider<String> dataset);

        @Description("BigQuery Table")
        @Validation.Required
        @Default.String("table")
        ValueProvider<String> getBqTable();

        @SuppressWarnings("unused")
        void setBqTable(ValueProvider<String> bqTable);
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(AvroToBqOptions.class);

        AvroToBqOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(AvroToBqOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        String bqStr = String.format("%s:%s.%s", options.getProject(), options.getDataset().get(), options.getBqTable().get());
        TableSchema ts = BigQueryAvroUtils.getTableSchema(getAvroSchema());

        pipeline.apply("Read Avro from DataStorage", AvroIO.readGenericRecords(getAvroSchema())
                .from(options.getInputPath().get()))
                .apply("Write to BigQuery", BigQueryIO.<GenericRecord>write()
                        .to(bqStr)
                        .withSchema(ts)
                        .withWriteDisposition(WRITE_APPEND)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withFormatFunction(genericRecord -> BigQueryAvroUtils.convertRecordToTableRow(
                                genericRecord
                                , BigQueryAvroUtils.getTableSchema(genericRecord.getSchema())
                        ))
                );

        try {
            pipeline.run().waitUntilFinish();
        } catch (UnsupportedOperationException e) {
            LOG.warn(String.format("Got an exception while creating job template {%s}. Link to follow exception's history {%s}",
                    e.getMessage()
                    , "https://issues.apache.org/jira/browse/BEAM-9337"));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
