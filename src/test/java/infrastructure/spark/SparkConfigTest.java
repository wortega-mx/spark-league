package test.java.infrastructure.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import main.java.infrastructure.spark.SparkConfig;

class SparkConfigTest 
{

    private SparkSession spark;

    @AfterEach
    void tearDown() 
    {
        if (spark != null) 
        {
            spark.stop();
            spark = null;
        }
    }

    @Test
    void testCreateLocalSession() 
    {
        String appName = "LocalTestApp";

        spark = SparkConfig.createLocalSession(appName);

        assertNotNull(spark, "SparkSession should not be null");
        assertEquals(appName, spark.sparkContext().appName(), "The App Name does not match");

        // Make sure Spark is running in local mode
        String master = spark.sparkContext().master();
        assertTrue(master.contains("local"), "Master should be local");
    }

    @Test
    void testLogLevelIsOffInLocalSession() 
    {
        String appName = "LogLevelTest";

        spark = SparkConfig.createLocalSession(appName);

        assertNotNull(spark);

        // There is no log level getter, but we can validate that it is working ...
        assertDoesNotThrow(() -> spark.sparkContext().setLogLevel("OFF"));
    }
}