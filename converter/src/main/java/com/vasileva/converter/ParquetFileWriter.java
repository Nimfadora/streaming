package com.vasileva.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;

public class ParquetFileWriter {
    private final static Logger LOG = Logger.getLogger(ParquetFileWriter.class);

    private final Configuration configuration;

    public ParquetFileWriter(Configuration config) {
        configuration = config;
    }

    /**
     * Writes records to HDFS in parquet format.
     *
     * @param data     data to write to HDFS
     * @param filePath output file path in HDFS
     * @param schema   file schema
     * @throws IOException when exceptions while reading occurs
     */
    public void writeToParquet(List<GenericRecord> data, Path filePath, Schema schema) throws IOException {
        try (ParquetWriter<GenericRecord> writer = new AvroParquetWriter<>(filePath, schema,
                CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
                true, configuration)) {
            LOG.info("Starting to write data");
            for (GenericRecord record : data) {
                writer.write(record);
            }
        }
    }
}
