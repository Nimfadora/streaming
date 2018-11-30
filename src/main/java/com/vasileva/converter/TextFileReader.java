package com.vasileva.converter;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
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

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
            String header = reader.readLine();
            Preconditions.checkArgument(header != null, "File is empty");
            String[] headings = header.split(delimiter, -1);

            return reader.lines()
                    .map(l -> mapValues(l.split(delimiter, -1), headings, schema))
                    .filter(Objects::nonNull).collect(Collectors.toList());
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
