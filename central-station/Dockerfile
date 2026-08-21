# ---------- build stage (shared for all modules) ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY weather-station/pom.xml weather-station/pom.xml
COPY rain-processor/pom.xml rain-processor/pom.xml
COPY central-station/pom.xml central-station/pom.xml
# warm the dependency cache first (layer is reused between rebuilds)
RUN mvn -B -q dependency:go-offline || true
COPY common/src common/src
COPY weather-station/src weather-station/src
COPY rain-processor/src rain-processor/src
COPY central-station/src central-station/src
RUN mvn -B -DskipTests package

# ---------- runtime stage ----------
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/central-station/target/central-station.jar /app/app.jar
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
