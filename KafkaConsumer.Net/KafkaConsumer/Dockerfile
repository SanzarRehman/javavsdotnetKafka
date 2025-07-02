# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY *.csproj ./
RUN dotnet restore
COPY . ./
RUN dotnet publish -c Release -o /app/publish

# Stage 2: Runtime
FROM mcr.microsoft.com/dotnet/runtime:9.0
WORKDIR /app
COPY --from=build /app/publish .

# Optional perf/env tuning
# ENV DOTNET_GCHeapHardLimitPercent=70 \
    # DOTNET_GCServer=1 \
    # DOTNET_ThreadPool_MinThreads=200

ENTRYPOINT ["dotnet", "KafkaConsumer.dll"]