
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

import javax.xml.crypto.Data;
import java.lang.Math;
import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class SparkDemo {
    public static void main(String[] args) {

        // Turn off spark logs in console
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        if (args.length > 0) {
            if (args[0].equals("--numbers")) {
                numbersDemo(spark);
            } else if (args[0].equals("--bags")) {
                bagsDemo(spark);
            } else {
                System.out.println("Command line argument must be one of the following values: --numbers or --bags, or no argument should be passed");
            }
        } else {
            numbersDemo(spark);
            bagsDemo(spark);
        }

        spark.stop();
    }

    static void numbersDemo(SparkSession spark) {

        System.out.println("------ Running numbers demo ------");

        String logFile = "data/numbers/numbers_1000.txt";
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        Dataset<Integer> values = logData.flatMap(e -> Arrays.asList(Integer.valueOf(e)).iterator(), Encoders.INT());

        // Max value
        long max = values.reduce((a, b) -> Math.max(a, b));
        // Average value
        double average = values.reduce((a, b) -> a + b) / values.count();
        // Remove duplicates
        Dataset<Integer> noDuplicates = values.dropDuplicates();
        // Count of distinct numbers
        long distinctCount = noDuplicates.count();

        System.out.println("Max : " + max);
        System.out.println("Average: " + average);
        System.out.println("Count: " + values.count() + " distinct count: " + distinctCount);
    }

    static void bagsDemo(SparkSession spark) {

        System.out.println("------ Running bags demo ------");

        Dataset<String> bagData1 = spark.read().textFile("data/sets/set1/*").cache();
        Dataset<String> bagData2 = spark.read().textFile("data/sets/set2/*").cache();

        // Bag union
        Dataset union = bagData1.union(bagData2);
        // Bag intersection
        Dataset intersection = bagData1.intersect(bagData2);
        // Bag difference
        Dataset difference = bagData1.except(intersection);

        System.out.println("bagData1 count: " + bagData1.count());
        System.out.println("bagData2 count: " + bagData2.count());
        System.out.println("Union count: " + union.count());
        System.out.println("Intersection count: " + intersection.count());
        System.out.println("Difference count: " + difference.count());
    }
}