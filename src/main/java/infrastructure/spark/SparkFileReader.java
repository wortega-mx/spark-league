package main.java.infrastructure.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SparkFileReader 
{
    public static Dataset<String> read(SparkSession spark, String path)
    {
        return spark.read().textFile(path);
    }
}
