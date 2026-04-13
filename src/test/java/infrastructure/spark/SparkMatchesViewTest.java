package test.java.infrastructure.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

import main.java.infrastructure.spark.SparkMatchesView;

public class SparkMatchesViewTest 
{

    private static SparkSession spark;

    @BeforeAll
    static void setup() 
    {
        spark = SparkSession.builder()
                .appName("SparkMatchesViewTest")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() 
    {
        if (spark != null) 
        {
            spark.stop();
        }
    }

    @Test
    void shouldCreateDataFrameWithCorrectSchema() 
    {
        List<String> data = Arrays.asList("TeamA 1, TeamB 2");

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        Dataset<Row> result = SparkMatchesView.create(ds);

        StructType schema = result.schema();

        assertEquals("teamA", schema.fields()[0].name());
        assertEquals("scoreA", schema.fields()[1].name());
        assertEquals("teamB", schema.fields()[2].name());
        assertEquals("scoreB", schema.fields()[3].name());
    }

    @Test
    void shouldParseMatchCorrectly() 
    {
        List<String> data = Arrays.asList("Barcelona 3, Real Madrid 1");

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        Dataset<Row> result = SparkMatchesView.create(ds);

        Row row = result.first();

        assertEquals("Barcelona", row.getString(0));
        assertEquals(3, row.getInt(1));
        assertEquals("Real Madrid", row.getString(2));
        assertEquals(1, row.getInt(3));
    }

    @Test
    void shouldRegisterTempView() {
        List<String> data = Arrays.asList("A 1, B 2");

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        SparkMatchesView.create(ds);

        Dataset<Row> sqlResult = spark.sql("SELECT * FROM matches");

        assertEquals(1, sqlResult.count());
    }

    @Test
    void shouldHandleMultipleMatches() 
    {
        List<String> data = Arrays.asList(
                "A 1, B 2",
                "C 3, D 4",
                "Lions 1, FC Awesome 1"
        );

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        Dataset<Row> result = SparkMatchesView.create(ds);

        assertEquals(3, result.count());
    }

    @Test
    void shouldFailOnInvalidInput() 
    {
        List<String> data = Arrays.asList("INVALID INPUT");

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        assertThrows(Exception.class, () -> {
            SparkMatchesView.create(ds).collect();
        });
    }
}