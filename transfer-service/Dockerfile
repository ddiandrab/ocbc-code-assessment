# Tahap build
FROM maven:3.9.16-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -DskipTests package

# Tahap runtime
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 10001 appuser
COPY --from=build --chown=appuser \
  /workspace/transfer-service/target/transfer-service-1.0.0-SNAPSHOT.jar \
  /app/app.jar

USER appuser
EXPOSE 8080
ENTRYPOINT ["java", "-Xms256m", "-Xmx256m", "-jar", "/app/app.jar"]
