package routers;

import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.sql.SparkSession;
import spark.Route;
import spark_compute.SparkTaskSubmitter;

public final class Routers {

    public static Route testRoute = (request, response) -> {
        StringBuilder sb = new StringBuilder();
        sb.append("you have reached testing route :)\n");
        sb.append("the following is request info:\n");
        sb.append("-----------------------------------\n");
        sb.append(request.headers().toString() + "\n");
        sb.append(request.attributes().toString() + "\n");
        sb.append(request.userAgent() + "\n");
        sb.append("-----------------------------------\n");
        sb.append("You have a nice day :)");

        return sb.toString();
    };

    public static Route distinctWord = (request, response) -> {
        SparkTaskSubmitter.submit(new String[]{});
        return "it workded";
    };

}
