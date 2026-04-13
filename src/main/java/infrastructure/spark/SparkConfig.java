package main.java.infrastructure.spark;

import org.apache.spark.sql.SparkSession;

public class SparkConfig 
{
    public static SparkSession createLocalSession(String appName) 
    {
        SparkSession spark = SparkSession.builder()
                            .appName(appName)
                            .master("local[*]") //Running into MacOS locally
                            .getOrCreate();

        spark.sparkContext().setLogLevel("OFF"); 

        return spark;
    }
}