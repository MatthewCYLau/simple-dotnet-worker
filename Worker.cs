using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace simple_dotnet_worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IAmazonS3 _s3Client;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly IConfiguration _configuration;
    private readonly IDynamoDbRepository _repository;
    private readonly IAmazonDynamoDB _positionsPnLAggregateDynamo;
    private readonly IAmazonSQS _sqsClient;

    private const string QueueUrl = "https://sqs.us-east-1.amazonaws.com/830663695860/aws-app-task-queue";

    public Worker(
        ILogger<Worker> logger,
        IAmazonS3 s3Client,
        IHostApplicationLifetime lifetime,
        IConfiguration configuration,
        IDynamoDbRepository repository,
        [FromKeyedServices("PositionsPnLAggregate")] IAmazonDynamoDB positionsPnLAggregateDynamo,
        IAmazonSQS sqsClient
        )
    {
        _logger = logger;
        _s3Client = s3Client;
        _lifetime = lifetime;
        _configuration = configuration;
        _repository = repository;
        _positionsPnLAggregateDynamo = positionsPnLAggregateDynamo;
        _sqsClient = sqsClient;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string bucketName = _configuration["AWS:BucketName"] ?? "my-default-bucket";

        try
        {
            _logger.LogInformation("Starting S3 object enumeration for bucket: {Bucket}", bucketName);

            var request = new ListObjectsV2Request
            {
                BucketName = bucketName
            };

            ListObjectsV2Response response = await _s3Client.ListObjectsV2Async(request, stoppingToken);

            _logger.LogInformation("Found {Count} object(s) in bucket '{Bucket}':", response.S3Objects?.Count ?? 0, bucketName);

            if (response.S3Objects != null)
            {
                foreach (var s3Object in response.S3Objects)
                {
                    _logger.LogInformation(" -> Key: {Key} | Size: {Size} bytes", s3Object.Key, s3Object.Size);
                }
            }

            _logger.LogInformation("Task completed successfully.");

            var items = await _repository.GetAllItemsAsync("stock_trading_positions", stoppingToken);
            _logger.LogInformation("Fetched {Count} positions.", items.Count);

            foreach (var item in items)
            {
                string positionId = item["PositionId"].S;
                decimal value = decimal.Parse(item["Value"].N);
                _logger.LogInformation($"Position Id: {positionId} Value: {value}");
            }

            var allItems = new List<Dictionary<string, AttributeValue>>();
            Dictionary<string, AttributeValue>? lastEvaluatedKey = null;

            do
            {
                var scanRequest = new ScanRequest
                {
                    TableName = "positions_pnl_aggregate",
                    ExclusiveStartKey = lastEvaluatedKey,
                };

                ScanResponse positionsPnLResponse = await _positionsPnLAggregateDynamo.ScanAsync(scanRequest, stoppingToken);

                // Add current page of items to master list
                allItems.AddRange(positionsPnLResponse.Items);

                // If LastEvaluatedKey is null or empty, we've read the whole table
                lastEvaluatedKey = positionsPnLResponse.LastEvaluatedKey;

            } while (lastEvaluatedKey != null && lastEvaluatedKey.Count > 0);

            foreach (var item in allItems)
            {
                string positionId = item["PositionId"].S;
                decimal totalPnL = decimal.Parse(item["TotalPnL"].N);
                _logger.LogInformation($"Position Id from PnL Aggregation DynamoDB: {positionId} Total PnL: {totalPnL}");
            }

            var receiveRequest = new ReceiveMessageRequest
            {
                QueueUrl = QueueUrl,
                MaxNumberOfMessages = 5,
                WaitTimeSeconds = 5
            };

            var sqsResponse = await _sqsClient.ReceiveMessageAsync(receiveRequest, stoppingToken);
            foreach (var message in sqsResponse.Messages ?? Enumerable.Empty<Message>())
            {
                _logger.LogInformation($"Messages count: {sqsResponse.Messages.Count}");
                _logger.LogInformation(message.Body);
            }
        }


        catch (AmazonS3Exception ex)
        {
            _logger.LogError(ex, "AWS S3 Error encountered: {Message}", ex.Message);
            Environment.ExitCode = 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An unhandled error occurred.");
            Environment.ExitCode = 1;
        }
        finally
        {
            // Signal the generic host to shut down so the K8s Job pod exits gracefully
            _lifetime.StopApplication();
        }
    }
}