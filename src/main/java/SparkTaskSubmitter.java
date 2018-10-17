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


        String secretPath = "/opt/spark/credentials/secret.json";
        String secretParentPath = "/opt/spark/credentials/";
        String[] cmds = new String[]{
                "--verbose",
                "--master", "k8s://https://35.229.152.59",
                "--conf", "spark.kubernetes.namespace=default",
                "--deploy-mode", "cluster",
                "--conf", "spark.kubernetes.driverEnv.GCS_PROJECT_ID=kubestart-218005",
                "--conf", "spark.kubernetes.executorEnv.GCS_PROJECT_ID=kubestart-218005",
                "--conf", "spark.kubernetes.container.image=asia.gcr.io/kubestart-218005/spark:v1.8.1",
                "--conf", "spark.kubernetes.driver.secrets.spark-sa=" + secretParentPath,
                "--conf", "spark.kubernetes.executor.secrets.spark-sa=" + secretParentPath,
                "--conf", "spark.kubernetes.driverEnv.GOOGLE_APPLICATION_CREDENTIALS=" + secretPath,
                "--conf", "spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=" + secretPath,
                "--conf", "spark.app.name=plainSparkJob",
                "--conf", "spark.executor.instances=1",
                "--conf", "spark.hadoop.fs.gs.project.id=kubestart-218005",
                "--conf", "spark.hadoop.google.cloud.auth.service.account.enable=true",
                "--conf", "spark.hadoop.google.cloud.auth.service.account.json.keyfile="+secretPath,
                "--class", "Main",
                "--driver-cores", "0.4",
                "--executor-cores", "1",
                "--name", "spark-pi",
                "local:///opt/spark/ykzn-job/sparkjob-1.0-SNAPSHOT-jar-with-dependencies.jar",
                "distinct",
                "gs://spark-stuff-tang/sales.csv"
        };

        SparkSubmit.main(cmds);
    }
}
