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


public class SparkComputeTasks {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("JavaSpark SQL basic example")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/TangDexian/Downloads/sales.csv");


        Column regionCol = df.col("Region");

        Dataset<Row> distinct = df.select(regionCol).distinct();
//        distinct.coalesce(1).write().text("/Users/TangDexian/Downloads/sales_distinct_region.csv");

        distinct.show();

    }
}
