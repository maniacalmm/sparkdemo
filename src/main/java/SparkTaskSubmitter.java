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

        File file = new File("gcloudAuth/helloworld");

        BufferedReader reader = new BufferedReader(new FileReader(file));

        System.out.println(reader.readLine());

        String secretPath = "gcloudAuth/secret.json";
        String[] cmds = new String[]{
                "--master", "k8s://https://35.229.152.59",
                "--deploy-mode", "cluster",
                "--conf", "spark.kubernetes.container.image=asia.gcr.io/kubestart-218005/spark:v1.3k -w-depn",
                "--conf","spark.kubernetes.driver.secrets.spark-sa=" + secretPath,
                "--conf","spark.kubernetes.executor.secrets.spark-sa=" + secretPath,
                "--conf","spark.kubernetes.driverEnv.GOOGLE_APPLICATION_CREDENTIALS=" + secretPath,
                "--conf","spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=" + secretPath,
                "--conf", "spark.app.name=plainSparkJob",
                "--conf", "spark.executor.instances=5",
                "--class", "Main",
                "--driver-cores", "0.4",
                "--executor-cores", "1",
                "--name", "spark-pi",
                "local:///opt/spark/ykzn-job/sparkjob-1.0-SNAPSHOT-jar-with-dependencies.jar",
                "distinct"
        };

        SparkSubmit.main(cmds);
    }
}
