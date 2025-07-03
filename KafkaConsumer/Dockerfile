# Stage 1: Build
FROM openjdk:21-jdk-slim AS build
WORKDIR /app

# Copy gradle wrapper and build files first (for better layer caching)
COPY gradlew .
COPY gradle gradle/
COPY build.gradle .
COPY settings.gradle .

# Make gradlew executable
RUN chmod +x ./gradlew

# Download dependencies first (this layer will be cached if build.gradle doesn't change)
RUN ./gradlew dependencies --no-daemon || true

# Copy source code (this comes after dependency download for better caching)
COPY src src/

# Build the application
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