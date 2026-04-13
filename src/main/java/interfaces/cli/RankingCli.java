import main.java.infrastructure.output.StandardOutput;
import main.java.infrastructure.output.MessagesOutput;
import main.java.infrastructure.spark.SparkConfig;
import main.java.infrastructure.spark.SparkFileReader;
import main.java.infrastructure.spark.SparkSQLJob;

import picocli.CommandLine;
import org.apache.spark.sql.*;

@CommandLine.Command(name = "ranking-job", mixinStandardHelpOptions = true)
public class RankingCli implements Runnable
{
    //It's the input filename with filepath (if any)
    @CommandLine.Option(names = {"-i", "--input"}, required = true)
    private String input;

    //It's the output option value: std (console/screen) and file
    @CommandLine.Option(names = {"-o", "--output"}, defaultValue = "std")
    private String output;

    @Override
    public void run() 
    {
        MessagesOutput.Beginning();
        
        SparkSession spark = SparkConfig.createLocalSession("LeagueRanking");

        try
        {
            Dataset<String> lines = SparkFileReader.read(spark, input);
            Dataset<Row> result = SparkSQLJob.run(spark, lines);
            
            if (output == "std")
                StandardOutput.print(result);
            else
                StandardOutput.print(result);
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
        finally
        {
            spark.stop();
        }
    }
    
    public static void main(String[] args) 
    {
        new CommandLine(new RankingCli()).execute(args);
    }
}