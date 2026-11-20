# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM maven:3.9.9-eclipse-temurin-21 AS builder

WORKDIR /build

COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -e dependency:go-offline

COPY src/ ./src/
RUN --mount=type=cache,target=/root/.m2 mvn -B -e clean package -DskipTests

FROM eclipse-temurin:21-jre-jammy

ENV TZ=Asia/Shanghai \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

WORKDIR /app
COPY --from=builder /build/target/r86s-monitor.jar ./app.jar
COPY --from=builder /build/target/lib/ ./lib/

USER 10001:10001
EXPOSE 8080

ENTRYPOINT ["java","-XX:MaxRAMPercentage=75","-Dfile.encoding=UTF-8","-jar","/app/app.jar"]
