FROM openjdk:8-jre-alpine
COPY target/spark-rest-1.0-SNAPSHOT-jar-with-dependencies.jar /app.jar
CMD ["/usr/bin/java", "-jar", "/app.jar"]
