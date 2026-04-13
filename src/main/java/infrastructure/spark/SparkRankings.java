package main.java.infrastructure.spark;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.Row;

public class SparkRankings 
{
    public static Dataset<Row> execute(SparkSession spark)
    {
        //1. Create a Common Table Expression (CTE) to set points for each match
        //   AND compute the sum points for each team   
        StringBuilder query = new StringBuilder();

        query.append("WITH points AS (");
        query.append("     SELECT teamA AS team,");
        query.append("            CASE");
        query.append("                 WHEN scoreA > scoreB THEN 3");
        query.append("                 WHEN scoreA < scoreB THEN 0");
        query.append("                 ELSE 1");
        query.append("            END AS points");
        query.append("     FROM matches");
        query.append("     UNION ALL");
        query.append("     SELECT teamB AS team,");
        query.append("            CASE");
        query.append("                 WHEN scoreB > scoreA THEN 3");
        query.append("                 WHEN scoreB < scoreA THEN 0");
        query.append("                 ELSE 1");
        query.append("            END AS points");
        query.append("     FROM matches ");
        query.append(") ");
        query.append("SELECT team, SUM(points) AS points ");
        query.append("FROM points ");
        query.append("GROUP BY team");
        
        Dataset<Row> standings = spark.sql(query.toString());

        //2. Calculate rankings (using rank window function to skip numbers after ties)
        Dataset<Row> ranked = standings
                                .withColumn(
                                    "rank",
                                        rank().over(
                                            Window.orderBy(col("points").desc())
                                        )
                                )
                                .orderBy(col("points").desc(), col("team").asc());

        return ranked;
    }
}
