FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY GameTrafficProxy.csproj .
RUN dotnet restore
COPY . .
RUN dotnet publish -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:9.0
WORKDIR /app
COPY --from=build /app .
EXPOSE 12121/udp
EXPOSE 12121/tcp
ENTRYPOINT ["dotnet", "GameTrafficProxy.dll"]
