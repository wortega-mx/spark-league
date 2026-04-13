package test.java.infrastructure.spark;

import main.java.infrastructure.spark.SparkRankings;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class SparkRankingsTest 
{

    private static SparkSession spark;

    @BeforeAll
    public static void setUp() 
    {
        spark = SparkSession.builder()
                .appName("SparkRankingsTest")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDown() 
    {
        if (spark != null) 
        {
            spark.stop();
        }
    }

    private Dataset<Row> createMatchesData() 
    {
        List<Row> data = Arrays.asList(
                RowFactory.create("TeamA", "TeamB", 2, 1), // A wins
                RowFactory.create("TeamA", "TeamC", 1, 1), // tie
                RowFactory.create("TeamB", "TeamC", 0, 3)  // C wins
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("teamA", DataTypes.StringType, false, Metadata.empty()),
                new StructField("teamB", DataTypes.StringType, false, Metadata.empty()),
                new StructField("scoreA", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("scoreB", DataTypes.IntegerType, false, Metadata.empty())
        });

        return spark.createDataFrame(data, schema);
    }

    @Test
    public void testRankingCalculation() 
    {
        // Arrange
        Dataset<Row> matches = createMatchesData();
        matches.createOrReplaceTempView("matches");

        // Act
        Dataset<Row> result = SparkRankings.execute(spark);

        List<Row> rows = result.collectAsList();

        // Expected:
        // TeamA: 4 pts (3 + 1)
        // TeamC: 4 pts (3 + 1)
        // TeamB: 0 pts

        Map<String, Long> expectedPoints = new HashMap<>();
        expectedPoints.put("TeamA", 4L);
        expectedPoints.put("TeamB", 0L);
        expectedPoints.put("TeamC", 4L);

        Map<String, Integer> expectedRanks = new HashMap<>();
        expectedRanks.put("TeamA", 1);
        expectedRanks.put("TeamC", 1);
        expectedRanks.put("TeamB", 3);

        // Assert
        for (Row row : rows) 
        {
            String team = row.getAs("team");
            long points = row.getAs("points");
            int rank = row.getAs("rank");

            assertEquals(expectedPoints.get(team), points, "Incorrect points to " + team);
            assertEquals(expectedRanks.get(team), rank, "Incorrect rank to " + team);
        }
    }

    @Test
    public void testEmptyMatches() 
    {
        // Arrange
        StructType schema = new StructType(new StructField[]{
                new StructField("teamA", DataTypes.StringType, false, Metadata.empty()),
                new StructField("teamB", DataTypes.StringType, false, Metadata.empty()),
                new StructField("scoreA", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("scoreB", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> emptyDF = spark.createDataFrame(new ArrayList<>(), schema);
        emptyDF.createOrReplaceTempView("matches");

        // Act
        Dataset<Row> result = SparkRankings.execute(spark);

        // Assert
        assertEquals(0, result.count(), "The result should be empty");
    }

    @Test
    public void testTieRanking() 
    {
        // Arrange: todos empatan
        List<Row> data = Arrays.asList(
                RowFactory.create("A", "B", 1, 1),
                RowFactory.create("B", "C", 2, 2),
                RowFactory.create("C", "A", 0, 0)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("teamA", DataTypes.StringType, false, Metadata.empty()),
                new StructField("teamB", DataTypes.StringType, false, Metadata.empty()),
                new StructField("scoreA", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("scoreB", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> matches = spark.createDataFrame(data, schema);
        matches.createOrReplaceTempView("matches");

        // Act
        Dataset<Row> result = SparkRankings.execute(spark);

        List<Row> rows = result.collectAsList();

        // Assert: todos con mismo rank = 1
        for (Row row : rows) {
            int rank = row.getAs("rank");
            assertEquals(1, rank, "Todos deberían tener rank 1 en empate total");
        }
    }
}