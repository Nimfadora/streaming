package com.vasileva.converter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class TestUtils {
    private TestUtils() {
    }

    public static Path getAbsoluteFilePath(String relativePath) {
        return new Path(TestUtils.class.getResource(relativePath).getPath());
    }

    public static Path getHdfsFilePath(FileSystem fs, String fileName) {
        return new Path(fs.getHomeDirectory(), fileName);
    }
}
