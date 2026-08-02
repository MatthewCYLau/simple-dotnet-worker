using Amazon.DynamoDBv2;
using simple_dotnet_worker;

var builder = Host.CreateApplicationBuilder(args);

// Register AWS S3 options and client via DI
builder.Services.AddDefaultAWSOptions(builder.Configuration.GetAWSOptions());
builder.Services.AddAWSService<Amazon.S3.IAmazonS3>();

builder.Services.AddAWSService<IAmazonDynamoDB>();
builder.Services.AddSingleton<IDynamoDbRepository, DynamoDbRepository>();

// Register background task
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
await host.RunAsync();