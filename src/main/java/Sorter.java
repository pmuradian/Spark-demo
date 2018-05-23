
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.codehaus.janino.Java;
import org.spark_project.jetty.util.ArrayUtil;
import scala.Array;
import scala.Int;
import scala.Tuple2;

import java.lang.Math;
import java.util.*;

public class Sorter {

    private static int machineCount = 3;
    private static long elementCount = 0;

    public static void main(String[] args) {

        // Turn off spark logs in console
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");

        // Create a Java version of the Spark Context
        JavaSparkContext context = new JavaSparkContext(conf);
        ranking_1(startTerasort_1(context), context);

        context.stop();
    }

    static JavaRDD<Integer> startTerasort(JavaSparkContext context) {

        System.out.println("------ Running numbers demo ------");

        String logFile = "data/numbers/numbers_1000.txt";
        JavaRDD<String> textFile = context.textFile(logFile);
        JavaRDD<Integer> values = textFile.flatMap(s -> Arrays.asList(Integer.valueOf(s.split("[ ]")[0])).iterator());

        elementCount = values.count();
        return terasort(values, context);
    }

    static ArrayList<JavaRDD<Integer>> startTerasort_1(JavaSparkContext context) {

        System.out.println("------ Running numbers demo ------");

        String logFile = "data/numbers/numbers_1000.txt";
        JavaRDD<String> textFile = context.textFile(logFile);
        JavaRDD<Integer> values = textFile.flatMap(s -> Arrays.asList(Integer.valueOf(s.split("[ ]")[0])).iterator());

        elementCount = values.count();
        return terasort_1(values, context);
    }

    static JavaRDD<Integer> terasort(JavaRDD<Integer> dataset, JavaSparkContext context) {

        // select values with probability p
        double m = ((double) elementCount) / machineCount;
        double p = (1 / m) * Math.log(elementCount * machineCount);
        Random rnd  = new Random();
        rnd.nextInt(100);

        JavaRDD<Integer> sortedSamples = dataset.filter(num -> ((double) rnd.nextInt(100)) / 100 < p).sortBy(x -> x, true, 10);
        List<Integer> l = sortedSamples.collect();

        // elements between sortedSamples[i], sortedSamples[i + 1] must be sent to machine i;
        ArrayList<JavaRDD<Integer>> list = new ArrayList();
        Iterator<Integer> it = l.iterator();
        while (it.hasNext()) {
            Integer a = it.next();
            Integer b = it.next();

            JavaRDD<Integer> valuesToSend = dataset.filter(x -> x > a && x < b);
            // send to machine i
            list.add(valuesToSend);
        }

        // sort on every machine, write to a common distributed file
        Iterator<JavaRDD<Integer>> iterator = list.iterator();
        ArrayList<JavaRDD<Integer>> sortedList = new ArrayList();
        while (iterator.hasNext()) {
            JavaRDD<Integer> sorted = iterator.next().sortBy(x -> x, true, 10);
            sortedList.add(sorted);
        }

        // concatenate sorted values, read from the distributed file
        ArrayList<Integer> finalList = new ArrayList();
        iterator = sortedList.iterator();
        while (iterator.hasNext()) {
            finalList.addAll(iterator.next().collect());
        }

        return context.parallelize(finalList);
    }

    static ArrayList<JavaRDD<Integer>> terasort_1(JavaRDD<Integer> dataset, JavaSparkContext context) {

        // select values with probability p
        double m = ((double) elementCount) / machineCount;
        double p = (1 / m) * Math.log(elementCount * machineCount);
        Random rnd  = new Random();
        rnd.nextInt(100);

        JavaRDD<Integer> sortedSamples = dataset.filter(num -> ((double) rnd.nextInt(100)) / 100 < p).sortBy(x -> x, true, 10);
        List<Integer> l = sortedSamples.collect();

        // elements between sortedSamples[i], sortedSamples[i + 1] must be sent to machine i;
        ArrayList<JavaRDD<Integer>> list = new ArrayList();
        Iterator<Integer> it = l.iterator();
        while (it.hasNext()) {
            Integer a = it.next();
            Integer b = it.next();

            JavaRDD<Integer> valuesToSend = dataset.filter(x -> x > a && x < b);
            // send to machine i
            list.add(valuesToSend);
        }

        // sort on every machine, write to a common distributed file
        Iterator<JavaRDD<Integer>> iterator = list.iterator();
        ArrayList<JavaRDD<Integer>> sortedList = new ArrayList();
        while (iterator.hasNext()) {
            JavaRDD<Integer> sorted = iterator.next().sortBy(x -> x, true, 10);
            sortedList.add(sorted);
        }
        return sortedList;

//        // concatenate sorted values, read from the distributed file
//        ArrayList<Integer> finalList = new ArrayList();
//        iterator = sortedList.iterator();
//        while (iterator.hasNext()) {
//            finalList.addAll(iterator.next().collect());
//        }
//
//        return context.parallelize(finalList);
    }

    static JavaRDD<Integer> ranking(JavaRDD<Integer> dataset, JavaSparkContext context) {

        return null;
    }

    static JavaRDD<Integer> ranking_1(ArrayList<JavaRDD<Integer>> dataset, JavaSparkContext context) {
        Iterator<JavaRDD<Integer>> iterator = dataset.iterator();
        ArrayList<JavaPairRDD<Integer, Integer>> pairs = new ArrayList();
        // add weights
        while (iterator.hasNext()) {
            pairs.add(iterator.next().mapToPair(x -> new Tuple2<>(x, 1)));
            // solve prefix sum locally on every machine
        }

        iterator = dataset.iterator();
        ArrayList<JavaPairRDD<Integer, Integer>> rankedPairs = new ArrayList();

        // add ranking locally on every machine
        int i = 0;
        while (iterator.hasNext()) {
            int rank = i;
            rankedPairs.add(iterator.next().mapToPair(x -> new Tuple2<>(x, rank)));
            i++;
        }

        // calculate weight sum on every machine
        Iterator<JavaPairRDD<Integer, Integer>> it = pairs.iterator();
        ArrayList<Integer> localRanking = new ArrayList();
        while (it.hasNext()) {
            JavaPairRDD<Integer, Integer> asd = it.next();
            Tuple2<Integer, Integer> pair = asd.reduce((tuple1, tuple2) -> new Tuple2<>(tuple1._1, tuple1._2 + tuple2._2));
            localRanking.add(pair._2);
            System.out.println(pair);
        }

        // on every machine calculate its rank offset
        ArrayList<Integer> rankOffsets = new ArrayList<>();
        while (!localRanking.isEmpty()) {
            JavaRDD<Integer> sharedRanking = context.parallelize(localRanking);
            rankOffsets.add(sharedRanking.reduce((a, b) -> a + b));
            localRanking.remove(localRanking.size() - 1);
        }

        Collections.reverse(rankOffsets);

        // calculate all ranks
        Iterator<JavaPairRDD<Integer, Integer>> riterator = rankedPairs.iterator();
        i = 0;
        while (riterator.hasNext()) {
            int rank = i;
            JavaRDD<Integer> temp = riterator.next().map(p -> p._2 + rankOffsets.get(rank));
            System.out.println(temp.collect());
            i++;
        }

        return null;
    }
}