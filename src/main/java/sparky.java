import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import routers.TestRoute;

import static spark.Spark.*;

public class sparky {

    private static Logger logger = LoggerFactory.getLogger(sparky.class.getName());
    public static void main(String[] args) {
        System.setProperty("org.eclipse.jetty.util.log.class",
                "org.eclipse.jetty.util.log.JavaUtilLog");
        System.setProperty("org.eclipse.jetty.util.log.class.LEVEL", "DEBUG");

        //Configure spark
        port(5555);
        logger.info("port running at 5555");

        get("/test", new TestRoute());
    }
}
