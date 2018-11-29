package com.vasileva.converter;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;

public class TextFileReader {
    private final static Logger LOG = Logger.getLogger(TextFileReader.class);


    private final Configuration configuration;

    public TextFileReader(Configuration config) {
        configuration = config;
    }

    /**
     * Reads file of delimited format (csv, tsv etc.) from HDFS and maps it to GenericRecord using schema passed.
     *
     * @param filePath  path to the file in HDFS
     * @param schema    file schema
     * @param delimiter line values delimiter
     * @return list of mapped records
     * @throws IOException when exceptions while reading occurs
     */
    public List<GenericRecord> readFile(Path filePath, Schema schema, String delimiter) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        Preconditions.checkArgument(fs.exists(filePath), "File does not exist");

        LOG.info("Starting to read data");

        try (Scanner sc = new Scanner(fs.open(filePath), "UTF-8")) {
            List<GenericRecord> records = new ArrayList<>();
            String[] header = null;
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                LOG.info("Parsing line: " + line);

                String[] values = line.split(delimiter, -1);
                if(header == null) {
                    header = values;
                } else {
                    Optional.ofNullable(mapValues(values, header, schema)).ifPresent(records::add);
                }
            }

            if (sc.ioException() != null) {
                throw new IOException("Exception occurred while reading: " + sc.ioException().getMessage(), sc.ioException());
            }

            Preconditions.checkArgument(records.size() > 0, "File is empty or missing header");
            return records;
        }
    }

    private GenericRecord mapValues(String[] values, String[] header, Schema schema) {
        if(values.length != header.length) {
            LOG.warn(String.format("Header and line sizes do not match: header[%s], line[%s]", header.length, values.length));
            return null;
        }
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < values.length; i++) {
            record.put(header[i], values[i]);
        }
        return record;
    }
}
