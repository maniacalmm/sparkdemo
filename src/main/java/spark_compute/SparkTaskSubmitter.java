package spark_compute;

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


        String secretPath = "/opt/spark/credentials/keyFile.json";
        String secretParentPath = "/opt/spark/credentials/";
        String projectID = "espa-149808";
        String dockerImage = "asia.gcr.io/" + projectID + "/spark:v1.0.3";
        String bucketName = "gda-store-japan-1";
        String taskName = "SpArK";
        //String readFrom = "gs://gda-store-japan-1/.gda_data_GameAnalytics/store/19386/uploads/virtualPurchases";
        String readFrom = "gs://sparkybucket/vp/";
        String fileList = "virtualPurchases-2018-09-16.gz";
        String[] cmds = new String[]{
                "--verbose",
                "--master", "k8s://https://35.200.3.213",
                "--conf", "spark.kubernetes.namespace=default",
                "--deploy-mode", "cluster",
                "--conf", "spark.kubernetes.driverEnv.GCS_PROJECT_ID=" + projectID,
                "--conf", "spark.kubernetes.executorEnv.GCS_PROJECT_ID=" + projectID,
                "--conf", "spark.kubernetes.container.image=" + dockerImage,
                "--conf", "spark.kubernetes.driver.secrets.spark-sa=" + secretParentPath,
                "--conf", "spark.kubernetes.executor.secrets.spark-sa=" + secretParentPath,
                "--conf", "spark.kubernetes.driverEnv.GOOGLE_APPLICATION_CREDENTIALS=" + secretPath,
                "--conf", "spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=" + secretPath,
                "--conf", "spark.app.name=compute",
                "--conf", "spark.executor.instances=5",
                "--conf", "spark.hadoop.fs.gs.system.bucket=" + bucketName,
                "--conf", "spark.executorEnv.GCS_PROJECT_ID=" + projectID,
                "--conf", "spark.hadoop.fs.gs.project.id=" + projectID,
                "--conf", "spark.hadoop.google.cloud.auth.service.account.enable=true",
                "--conf", "spark.hadoop.google.cloud.auth.service.account.json.keyfile="+secretPath,
                "--class", "Main",
                "--driver-cores", "0.4",
                "--executor-cores", "4",
                "--name", taskName,
                "local:///opt/spark/ykzn-job/sparkjob-1.0-SNAPSHOT-jar-with-dependencies.jar",
                "distinct",
                readFrom,
                fileList
        };

        SparkSubmit.main(cmds);
    }
}
