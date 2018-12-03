package com.vasileva.converter;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import static com.vasileva.converter.TestUtils.getAbsoluteFilePath;
import static com.vasileva.converter.TestUtils.getHdfsFilePath;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

@RunWith(JUnitParamsRunner.class)
public class TextFileReaderTest {

    @ClassRule
    public static final TemporaryFolder baseDir = new TemporaryFolder();

    private static MiniDFSCluster dfsCluster;
    private static TextFileReader fileReader;
    private static FileSystem fs;

    @BeforeClass
    public static void setUp() throws IOException {
        baseDir.create();
        Configuration configuration = new Configuration();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getRoot().getAbsolutePath());
        dfsCluster = new MiniDFSCluster.Builder(configuration).build();
        dfsCluster.waitActive();
        fs = dfsCluster.getFileSystem();
        createInput();
        fileReader = new TextFileReader(configuration);
    }

    @AfterClass
    public static void tearDown() {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
    }

    @Test
    @Parameters({
            "data/test.csv, /records/test_dataset_records.txt, TEST_DATASET",
            "data/train.csv, /records/train_dataset_records.txt, TRAIN_DATASET",
            "data/destinations.csv, /records/destinations_records.txt, DESTINATION",
            "data/sample_submission.csv, /records/results_records.txt, RESULT"})
    public void testReadFile(String inputPath, String expectedPath, String schemaType) throws IOException {
        List<GenericRecord> records = fileReader.mapText2GenericRecord(getHdfsFilePath(fs, inputPath),
                SchemaType.valueOf(schemaType).getSchema(), ",");

        assertFalse(records.isEmpty());

        InputStream expectedInputStream = TextFileReaderTest.class.getResourceAsStream(expectedPath);
        List<String> expected = IOUtils.readLines(expectedInputStream, Charset.defaultCharset());
        List<String> actual = records.stream().map(Object::toString).collect(Collectors.toList());

        assertThat(actual, is(expected));
    }

    @Test(expected = AvroRuntimeException.class)
    public void testInvalidSchemaPassed() throws IOException {
        fileReader.mapText2GenericRecord(getHdfsFilePath(fs, "data/test.csv"), SchemaType.TRAIN_DATASET.getSchema(), ",");
    }

    @Test(expected = AvroRuntimeException.class)
    public void testInvalidDelimiterPassed() throws IOException {
        fileReader.mapText2GenericRecord(getHdfsFilePath(fs, "data/test.csv"), SchemaType.TEST_DATASET.getSchema(), ";");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInputDoesNotExist() throws IOException {
        fileReader.mapText2GenericRecord(getHdfsFilePath(fs, "data/abrakadkabra.csv"), SchemaType.TRAIN_DATASET.getSchema(), ",");
    }

    private static void createInput() throws IOException {
        fs.copyFromLocalFile(getAbsoluteFilePath("/original/destinations.csv"), getHdfsFilePath(fs, "data/destinations.csv"));
        fs.copyFromLocalFile(getAbsoluteFilePath("/original/sample_submission.csv"), getHdfsFilePath(fs, "data/sample_submission.csv"));
        fs.copyFromLocalFile(getAbsoluteFilePath("/original/test.csv"), getHdfsFilePath(fs, "data/test.csv"));
        fs.copyFromLocalFile(getAbsoluteFilePath("/original/train.csv"), getHdfsFilePath(fs, "data/train.csv"));
    }
}
