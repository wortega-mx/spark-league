package test.java.infrastructure.output;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import main.java.infrastructure.output.FormattedOutput;

class FormattedOutputTest 
{

    // Define row schema
    private static final StructType SCHEMA = new StructType(new StructField[]
    {
        new StructField("rank", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("team", DataTypes.StringType, false, Metadata.empty()),
        new StructField("points", DataTypes.LongType, false, Metadata.empty())
    });

    @ParameterizedTest
    @MethodSource("provideRows")
    void shouldFormatOutputCorrectly(Row row, String expected) 
    {
        String result = FormattedOutput.apply(row);
        assertEquals(expected, result);
    }

    private static Stream<org.junit.jupiter.params.provider.Arguments> provideRows() 
    {
        return Stream.of(
            org.junit.jupiter.params.provider.Arguments.of(
                createRow(1, "Barcelona", 3L),
                "1. Barcelona, 3 pts"
            ),
            org.junit.jupiter.params.provider.Arguments.of(
                createRow(2, "Real Madrid", 1L),
                "2. Real Madrid, 1 pt"
            ),
            org.junit.jupiter.params.provider.Arguments.of(
                createRow(10, "Valencia", 25L),
                "10. Valencia, 25 pts"
            ),
            org.junit.jupiter.params.provider.Arguments.of(
                createRow(5, "Sevilla", 0L),
                "5. Sevilla, 0 pts"
            )
        );
    }

    private static Row createRow(int rank, String team, long points) 
    {
        // Create a Row with schema
        return new GenericRowWithSchema(
            new Object[]{rank, team, points},
            SCHEMA
        );
    }
}