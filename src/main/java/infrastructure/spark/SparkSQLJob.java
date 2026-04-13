package main.java.infrastructure.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkSQLJob 
{
    public static Dataset<Row> run(SparkSession spark, Dataset<String> lines) 
    {
        SparkMatchesView.create(lines);

        Dataset<Row> df = SparkRankings.execute(spark);

        return df;
    }
}