package com.vasileva.converter;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.vasileva.converter.TestUtils.getAbsoluteFilePath;
import static com.vasileva.converter.TestUtils.getHdfsFilePath;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class Text2ParquetFileConverterTest {
    @ClassRule
    public static final TemporaryFolder baseDir = new TemporaryFolder();

    private static MiniDFSCluster dfsCluster;
    private static Text2ParquetFileConverter fileConverter;
    private static Configuration configuration;
    private static FileSystem fs;

    @BeforeClass
    public static void setUp() throws IOException {
        baseDir.create();
        configuration = new Configuration();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getRoot().getAbsolutePath());
        configuration.set("io.compression.codecs", "org.apache.parquet.hadoop.codec.SnappyCodec");
        dfsCluster = new MiniDFSCluster.Builder(configuration).build();
        dfsCluster.waitActive();
        fs = dfsCluster.getFileSystem();
        createInput();
        fileConverter = new Text2ParquetFileConverter(configuration);
    }

    @Test
    @Parameters({
            "data/test.csv, result/test.parquet, /records/test_dataset_records.txt, TEST_DATASET",
            "data/train.csv, result/train.parquet, /records/train_dataset_records.txt, TRAIN_DATASET",
            "data/destinations.csv, result/destinations.parquet, /records/destinations_records.txt, DESTINATION",
            "data/sample_submission.csv, result/result.parquet, /records/results_records.txt, RESULT"})
    public void testConvertFile(String inputPath, String outputPath,
                                String expectedPath, String schemaType) throws IOException {
        fileConverter.convert(getHdfsFilePath(fs, inputPath).toString(), getHdfsFilePath(fs, outputPath).toString(),
                SchemaType.valueOf(schemaType).name(), ",");

        Path resultPath = getHdfsFilePath(fs, outputPath);
        assertTrue(dfsCluster.getFileSystem().exists(resultPath));

        List<GenericRecord> records = readParquet(resultPath);
        InputStream expectedInputStream = TextFileReaderTest.class.getResourceAsStream(expectedPath);
        List<String> expected = IOUtils.readLines(expectedInputStream, Charset.defaultCharset());
        List<String> actual = records.stream().map(Object::toString).collect(Collectors.toList());

        assertThat(actual, is(expected));
    }

    private List<GenericRecord> readParquet(Path filePath) throws IOException {
        List<GenericRecord> results = new ArrayList<>();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(filePath)
                .withConf(configuration)
                .build()) {
            GenericRecord result;
            while ((result = reader.read()) != null) {
                results.add(result);
            }
            return results;
        }
    }

    private static void createInput() throws IOException {
        fs.copyFromLocalFile(getAbsoluteFilePath("/original/destinations.csv"), getHdfsFilePath(fs, "data/destinations.csv"));
        fs.copyFromLocalFile(getAbsoluteFilePath("/original/sample_submission.csv"), getHdfsFilePath(fs, "data/sample_submission.csv"));
        fs.copyFromLocalFile(getAbsoluteFilePath("/original/test.csv"), getHdfsFilePath(fs, "data/test.csv"));
        fs.copyFromLocalFile(getAbsoluteFilePath("/original/train.csv"), getHdfsFilePath(fs, "data/train.csv"));
    }
}