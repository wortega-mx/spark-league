package main.java.infrastructure.output;

import org.apache.spark.sql.Row;

public class FormattedOutput 
{
    public static String apply(Row row)
    {
        long points = row.getAs("points");
        
        return String.format("%d. %s, %d %s", 
                             row.getAs("rank"),
                             row.getAs("team"),
                             points,
                             points == 1 ? "pt" : "pts");
    }
}