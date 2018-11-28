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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TextFileReader {

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

        try (FSDataInputStream in = fs.open(filePath)) {
            List<String> lines = IOUtils.readLines(in, Charsets.UTF_8);
            Preconditions.checkArgument(lines.size() >= 2, "File is empty or missing header");
            String[] header = lines.get(0).split(delimiter);

            return lines.subList(1, lines.size()).stream()
                    .map(line -> line.split(delimiter))
                    .map(values -> mapValues(values, header, schema))
                    .collect(Collectors.toList());
        }
    }

    private GenericRecord mapValues(String[] values, String[] header, Schema schema) {
        Preconditions.checkArgument(values.length == header.length, "Header and line sizes do not match");

        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < values.length; i++) {
            record.put(header[i], values[i]);
        }
        return record;
    }
}
