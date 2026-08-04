using Amazon.DynamoDBv2;
using simple_dotnet_worker;
using Amazon;
using Amazon.SQS;

var builder = Host.CreateApplicationBuilder(args);

// Register AWS S3 options and client via DI
builder.Services.AddDefaultAWSOptions(builder.Configuration.GetAWSOptions());
builder.Services.AddAWSService<Amazon.S3.IAmazonS3>();

builder.Services.AddAWSService<IAmazonDynamoDB>();
builder.Services.AddSingleton<IDynamoDbRepository, DynamoDbRepository>();

builder.Services.AddKeyedSingleton("PositionsPnLAggregate", (sp, key) =>
{
    var options = builder.Configuration.GetAWSOptions();
    options.Region = RegionEndpoint.USEast1;
    return options.CreateServiceClient<IAmazonDynamoDB>();
});

builder.Services.AddAWSService<IAmazonSQS>();

// Register background task
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
await host.RunAsync();