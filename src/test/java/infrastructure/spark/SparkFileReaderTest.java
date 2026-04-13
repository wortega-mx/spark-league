package test.java.infrastructure.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

import main.java.infrastructure.spark.SparkFileReader;

public class SparkFileReaderTest 
{

    private static SparkSession spark;

    @BeforeAll
    static void setUp() 
    {
        spark = SparkSession.builder()
                .appName("SparkFileReaderTest")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() 
    {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void shouldReadTextFileCorrectly() throws Exception 
    {
        // Create a temmporal file
        File tempFile = File.createTempFile("test-file", ".txt");
        FileWriter writer = new FileWriter(tempFile);
        writer.write("Lions 3, Snakes 3\n");
        writer.write("Tarantulas 1, FC Awesome 0\n");
        writer.write("Lions 1, FC Awesome 1\n");
        writer.close();

        // Execute method
        Dataset<String> result = SparkFileReader.read(spark, tempFile.getAbsolutePath());

        // Retrieve results
        List<String> lines = result.collectAsList();

        // Validate
        assertNotNull(result);
        assertEquals(3, lines.size());
        assertTrue(lines.contains("Lions 3, Snakes 3"));
        assertTrue(lines.contains("Tarantulas 1, FC Awesome 0"));
        assertTrue(lines.contains("Lions 1, FC Awesome 1"));

        // Clean up
        tempFile.delete();
    }

    @Test
    void shouldReturnEmptyDatasetForEmptyFile() throws Exception 
    {
        File tempFile = File.createTempFile("empty-file", ".txt");

        Dataset<String> result = SparkFileReader.read(spark, tempFile.getAbsolutePath());
        List<String> lines = result.collectAsList();

        assertNotNull(result);
        assertTrue(lines.isEmpty());

        tempFile.delete();
    }

    @Test
    void shouldThrowExceptionForInvalidPath() 
    {
        String invalidPath = "non_existent_file.txt";

        Exception exception = assertThrows(Exception.class, () -> {
            SparkFileReader.read(spark, invalidPath).collect();
        });

        assertNotNull(exception);
    }
}