package main.java.infrastructure.output;

public class MessagesOutput 
{
    public static void Beginning()
    {
        StringBuilder str = new StringBuilder();

        str.append("**************************************************************************************");
        str.append(String.format("%s%n",""));
        str.append("**************************************************************************************");
        str.append(String.format("%s%n",""));
        str.append("                        Starting Spark Job to Calculate Ranking                       ");
        str.append(String.format("%s%n",""));
        str.append("**************************************************************************************");
        str.append(String.format("%s%n",""));
        str.append("**************************************************************************************");
        str.append(String.format("%s%n",""));
        str.append(String.format("%s%n",""));

        System.out.println(str);
    }

    public static void Ending(String output)
    {
         StringBuilder str = new StringBuilder();
         String outputName = output == "std" ? "standard" : "file";

        str.append(String.format("%s%n",""));
        str.append(String.format("%s%n",""));
        str.append("************************************************************************************");
        str.append(String.format("%s%n",""));
        str.append(String.format("***************** Resultant Ranking List by %s output ************************", outputName));
        str.append(String.format("%s%n",""));
        str.append("************************************************************************************");
        str.append(String.format("%s%n",""));
        str.append("************************************************************************************");
        str.append(String.format("%s%n",""));

        System.out.println(str);
    }
}
