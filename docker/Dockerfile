FROM eclipse-temurin:21-jre-alpine
# Java4Shell (CVE-2026-114938) workaround: turn off the vulnerable Base64 intrinsic.
# Remove once the base image ships Temurin 21.0.13.1+2 or later.
ENV JAVA_TOOL_OPTIONS="-XX:-UseBASE64Intrinsics"
RUN mkdir -p /app
COPY target/pagefeedback-cj-1.0.0-SNAPSHOT.jar /app/app.jar
ENV JAVA_OPTS="-Xmx2g"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
