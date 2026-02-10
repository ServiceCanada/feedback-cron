FROM eclipse-temurin:17-jre-alpine
RUN mkdir -p /app
COPY target/pagefeedback-cj-1.0.0-SNAPSHOT.jar /app/app.jar
ENV JAVA_OPTS="-Xmx2g"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
