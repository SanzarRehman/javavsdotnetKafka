# Stage 1: Build
FROM openjdk:21-jdk-slim AS build
WORKDIR /app

# Copy gradle wrapper and build files
COPY gradlew .
COPY gradle gradle/
COPY build.gradle .
COPY settings.gradle .

# Copy source code
COPY src src/

# Make gradlew executable and build
RUN chmod +x ./gradlew
RUN ./gradlew build --no-daemon

# Stage 2: Runtime
FROM openjdk:21-jdk-slim
WORKDIR /app

# Copy the built jar from build stage
COPY --from=build /app/build/libs/*.jar app.jar

# Set JVM options for better performance
ENV JAVA_OPTS="-Xmx512m -Xms256m --enable-preview"

# Expose port (optional, for monitoring)
EXPOSE 8080

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]