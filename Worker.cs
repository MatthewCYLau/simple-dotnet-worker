using Amazon.S3;
using Amazon.S3.Model;

namespace simple_dotnet_worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IAmazonS3 _s3Client;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly IConfiguration _configuration;

    public Worker(
        ILogger<Worker> logger,
        IAmazonS3 s3Client,
        IHostApplicationLifetime lifetime,
        IConfiguration configuration)
    {
        _logger = logger;
        _s3Client = s3Client;
        _lifetime = lifetime;
        _configuration = configuration;
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