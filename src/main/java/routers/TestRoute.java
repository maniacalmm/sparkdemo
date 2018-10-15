package routers;

import spark.Request;
import spark.Response;
import spark.Route;

public class TestRoute implements Route {
    @Override
    public Object handle(Request request, Response response) throws Exception {
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
    }
}
