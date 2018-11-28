package com.vasileva.converter;

import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Defines file schema types
 */
public enum SchemaType {

    DESTINATION("destination.avsc"),
    DATASET("dataset.avsc"),
    RESULT("result.avsc");

    private final Schema schema;

    SchemaType(String schemaFileName) {
        Schema.Parser parser = new Schema.Parser();
        try {
            schema = parser.parse(getClass().getResourceAsStream(schemaFileName));
        } catch (IOException e) {
            throw new IllegalStateException("Could not parse schema: " + schemaFileName);
        }
    }

    public Schema getSchema() {
        return schema;
    }
}
