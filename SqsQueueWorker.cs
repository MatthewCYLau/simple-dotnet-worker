using Amazon.SQS;
using Amazon.SQS.Model;

public class SqsQueueWorker : BackgroundService
{
    private readonly IAmazonSQS _sqsClient;
    private readonly ILogger<SqsQueueWorker> _logger;
    private readonly string _queueUrl;

    public SqsQueueWorker(
        IAmazonSQS sqsClient,
        IConfiguration configuration,
        ILogger<SqsQueueWorker> logger)
    {
        _sqsClient = sqsClient;
        _logger = logger;
        _queueUrl = configuration["AWS:QueueUrl"]
            ?? throw new ArgumentNullException("QueueUrl is not configured.");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("SQS Worker Service started.");

        var receiveRequest = new ReceiveMessageRequest
        {
            QueueUrl = _queueUrl,
            MaxNumberOfMessages = 10,
            WaitTimeSeconds = 20,
            MessageSystemAttributeNames = new List<string> { "All" },
            MessageAttributeNames = new List<string> { "All" }
        };

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var response = await _sqsClient.ReceiveMessageAsync(receiveRequest, stoppingToken);

                if (response.Messages != null && response.Messages.Count > 0)
                {
                    foreach (var message in response.Messages)
                    {
                        await ProcessMessageAsync(message, stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("SQS Worker is stopping due to cancellation.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while polling or processing SQS messages.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task ProcessMessageAsync(Message message, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Processing message ID: {MessageId}", message.MessageId);
            _logger.LogInformation(message.Body);
            await Task.Delay(100, cancellationToken);

            await DeleteMessageAsync(message.ReceiptHandle, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process message {MessageId}. It will return to the queue.", message.MessageId);
        }
    }

    private async Task DeleteMessageAsync(string receiptHandle, CancellationToken cancellationToken)
    {
        var deleteRequest = new DeleteMessageRequest
        {
            QueueUrl = _queueUrl,
            ReceiptHandle = receiptHandle
        };

        await _sqsClient.DeleteMessageAsync(deleteRequest, cancellationToken);
    }
}