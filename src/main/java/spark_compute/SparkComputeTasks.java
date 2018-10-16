package spark_compute;

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
import java.util.HashSet;
import java.util.Set;


public class SparkComputeTasks {

    public static void main(String[] args) {
        runspark();
        runSequentially();
    }

    public static void runspark() {
        SparkSession spark = SparkSession.builder()
                .master("local[" + Runtime.getRuntime().availableProcessors() + "]")
                .appName("JavaSpark SQL basic example")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        for (Tuple2 o : spark.sparkContext().env().conf().getAll()) {
            System.out.println(o._1 + " " + o._2);
        }

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/home/dexian/sales.csv");


        Column regionCol = df.col("Region");

        Dataset<Row> distinct = df.select(regionCol).distinct();

        long start = System.currentTimeMillis();
        distinct.show();
        System.out.println("spark: " + (System.currentTimeMillis() - start));
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
