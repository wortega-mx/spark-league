package main.java.infrastructure.output;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import main.java.infrastructure.output.MessagesOutput;

public class StandardOutput 
{
    public static void print(Dataset<Row> rankings)
    {
        MessagesOutput.Ending("std");
        
        rankings.foreach((ForeachFunction<Row>) row -> {
            System.out.println(FormattedOutput.apply(row));
        });

        System.out.println(String.format("%s%n", ""));
    }
}