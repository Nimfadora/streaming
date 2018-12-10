package com.vasileva;

import com.google.common.base.Preconditions;
import com.vasileva.converter.Text2ParquetFileConverter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Preconditions.checkArgument(args.length == 4,
                "Invalid number of arguments, should be: inputFilePath outputFilePath schemaType inputFileDelimiter");

        Configuration configuration = new Configuration();
        Text2ParquetFileConverter converter = new Text2ParquetFileConverter(configuration);
        converter.convert(args[0], args[1], args[2], args[3]);
    }
}
