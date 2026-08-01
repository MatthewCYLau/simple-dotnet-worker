FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["simple-dotnet-worker.csproj", "./"]
RUN dotnet restore "simple-dotnet-worker.csproj"
COPY . .
RUN dotnet publish "simple-dotnet-worker.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:9.0 AS final
WORKDIR /app

COPY --from=build --chown=app:app /app/publish .

USER app

ENTRYPOINT ["dotnet", "simple-dotnet-worker.dll"]