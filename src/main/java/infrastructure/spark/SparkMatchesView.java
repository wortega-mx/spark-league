package main.java.infrastructure.spark;

import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import org.apache.spark.api.java.function.MapFunction;

import main.java.domain.model.Match;
import main.java.infrastructure.parser.MatchParser;

public class SparkMatchesView 
{
    public static Dataset<Row> create(Dataset<String> lines)
    {
        //1. Define schema for matches view
        Encoder<Row> encoder = RowEncoder.apply(
            new StructType()
                        .add("teamA", "string")
                        .add("scoreA", "int")
                        .add("teamB", "string")
                        .add("scoreB", "int")
        );

        //2. Parsing every match line AND
        //   adding a row into the DataSet with parsed data   
        Dataset<Row> df = lines.map((MapFunction<String, Row>) line -> 
        {
            Match match = MatchParser.parse(line);

            return RowFactory.create(match.getTeamA(), match.getScoreA(), 
                                     match.getTeamB(), match.getScoreB());

        }, encoder);

        //3. Register the DataSet as a View called "matches"
        df.createOrReplaceTempView("matches");

        return df;
    }
    
}
