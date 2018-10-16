import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.launcher.SparkLauncher;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkTaskSubmitter {

    public static void main(String[] args) throws Exception{
        double startTime = System.currentTimeMillis();
        submit(args);
        double finishedTime = System.currentTimeMillis();
        System.out.println("finished in: " + (finishedTime - startTime) / 1000);
    }

    public static void submit(String[] args) throws Exception {

        final String sparkHome = "/Users/TangDexian/spark-2.3.1-bin-hadoop2.7";
        String[] cmds = new String[]{
                "--master", "k8s://https://35.229.152.59",
                "--deploy-mode", "cluster",
                "--conf", "spark.kubernetes.container.image=geekbeta/spark:v100",
                "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
                "--conf", "spark.app.name=wtfhappend",
                "--conf", "spark.executor.instances=3",
                "--class", "FuckingPI",
                "--driver-cores", "0.1",
                "--executor-cores", "1",
                "--name", "spark-pi",
                "local:///opt/spark/tang_stuff/spark-demo.jar"
        };

        SparkSubmit.main(cmds);
    }
}
