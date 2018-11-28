package com.vasileva.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class Text2ParquetFileConverter {

    private final TextFileReader fileReader;
    private final ParquetFileWriter fileWriter;

    public Text2ParquetFileConverter(Configuration config) {
        fileReader = new TextFileReader(config);
        fileWriter = new ParquetFileWriter(config);
    }

    /**
     * Converts text file with header, where values in line are separated with delimiter to
     * parquet file with schema passed.
     * @param inputPath input file path
     * @param outputPath output file path
     * @param schemaType input file schema type
     * @param delimiter line values delimiter
     * @throws IOException when exceptions while reading occurs
     */
    public void convert(String inputPath, String outputPath, String schemaType, String delimiter) throws IOException {
        Schema schema = SchemaType.valueOf(schemaType).getSchema();
        List<GenericRecord> records;
        try {
            records = fileReader.readFile(new Path(inputPath), schema, delimiter);
        } catch (IOException ex) {
            throw new IOException(String.format("Failed to read file: %s, message: %s", inputPath, ex.getMessage()), ex);
        }
        try {
            fileWriter.writeToParquet(records, new Path(outputPath), schema);
        } catch (IOException ex) {
            throw new IOException(String.format("Failed to write data to parquet: %s, message: %s", outputPath, ex.getMessage()), ex);
        }
    }
}
