import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import javax.print.DocFlavor;
import java.io.*;
import java.lang.reflect.Executable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class SparkBenchMarking {

    public static void main(String[] args) {
        runspark();
//        runSequentially();
    }

    public static void runspark() {

        long start = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder()
                .master("local[" + Runtime.getRuntime().availableProcessors() + "]")
                .appName("JavaSpark SQL basic example")
                .config("spark.executor.cores", Runtime.getRuntime().availableProcessors())
                .config("verbose", "true")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        for (Tuple2 o : spark.sparkContext().env().conf().getAll()) {
            System.out.println(o._1 + " " + o._2);
        }

        System.out.println("#######################################################");


        File dataFolder = new File("/home/dexian/19386/virtualPurchases-2017-11-12gz");
        String[] fileNames = Arrays.stream(dataFolder.listFiles()).map(f -> f.getAbsolutePath()).toArray(String[]::new);
        for (String f : fileNames) System.out.println(f);

        System.out.println("starting reading...");
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(fileNames);


        System.out.println("printing schema...");
        printCurrentTime(start);
        df.printSchema();
        Column columnOfInterest = df.col(df.columns()[0]);

        System.out.println("column count:...");
        printCurrentTime(start);
        System.out.println(df.count());

        System.out.println("distinct command...");
        printCurrentTime(start);
        Dataset<Row> distinct = df.select(columnOfInterest).distinct();

        System.out.println("show distinct result");
        printCurrentTime(start);
        distinct.coalesce(1).write().csv("/home/dexian/19386/virtualPurchases-2017-11-12gz/output");
        printCurrentTime(start);
    }

    private static void printCurrentTime(double start) {
        System.out.println("elapsed time: " + (System.currentTimeMillis() - start) / 1_000);
    }

    public static void runSequentially() {

        long start = System.currentTimeMillis();
        File file = new File("/home/dexian/sales.csv");
        Set<String> set = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while((line = reader.readLine()) != null) {
               set.add(line.split(",")[0]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("sequential read finished");
        System.out.println(set.toString());
        System.out.println("sequential: " + (System.currentTimeMillis() - start));
    }
}
