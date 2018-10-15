import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.launcher.SparkLauncher;

import java.io.*;

public class SparkTaskSubmitter {

    public static void main(String[] args) throws Exception{
        double startTime = System.currentTimeMillis();
        submit(args);
        double finishedTime = System.currentTimeMillis();
        System.out.println("finished in: " + (finishedTime - startTime) / 1000);
    }

    public static void submit(String[] args) throws Exception {

        final String sparkHome = "/Users/TangDexian/spark-2.3.1-bin-hadoop2.7";

        SparkSubmit

        SparkLauncher sparkLauncher = new SparkLauncher()

                .setSparkHome(sparkHome)

                .setMaster("k8s://https://35.229.152.59")
                .setDeployMode("cluster")
                .setAppName("PI cal")
                .setMainClass("FuckingPI")
                .addJar("local:///opt/spark/tang_stuff/spark-demo.jar")
                .addSparkArg("spark.driver.cores", "0.1")
                .setConf("spark.executor.instance", "3")
                .setConf("spark.kubernetes.container.image", "geekbeta/spark:v100")
                .setConf("spark.kubernetes.authenticate.driver.sericeAccountName", "spark");


        Process proc = sparkLauncher.launch();

//        InputStream input = proc.getInputStream();
//        OutputStream output = proc.getOutputStream();
//        InputStream error = proc.getErrorStream();
//
//        BufferedReader read = new BufferedReader(new OutputS(new BufferedOutputStream(output)));
//        String content;
//        while((content = read.readLine()) != null) {
//            System.out.println(content);
//        }

        int status = proc.waitFor();

        System.out.println("finished with srtatus code: " + status);
    }
}
