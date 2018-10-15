package logger;

import org.apache.log4j.Logger;
import org.spark_project.jetty.server.AbstractNCSARequestLog;

import java.io.IOException;

public class RequestLoggerFactory {

    private Logger logger;

    public RequestLoggerFactory(Logger logger) {
        this.logger = logger;
    }

    AbstractNCSARequestLog create() {
        return new AbstractNCSARequestLog() {
            @Override
            protected boolean isEnabled() {
                return true;
            }

            @Override
            public void write(String s) throws IOException {
                logger.info(s);
            }
        };
    }
}
