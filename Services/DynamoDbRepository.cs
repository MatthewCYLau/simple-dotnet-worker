using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
public interface IDynamoDbRepository
{
    Task<List<Dictionary<string, AttributeValue>>> GetAllItemsAsync(string tableName, CancellationToken cancellationToken = default);
}

public class DynamoDbRepository : IDynamoDbRepository
{
    private readonly IAmazonDynamoDB _dynamoDb;

    public DynamoDbRepository(IAmazonDynamoDB dynamoDb)
    {
        _dynamoDb = dynamoDb;
    }

    public async Task<List<Dictionary<string, AttributeValue>>> GetAllItemsAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var allItems = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastEvaluatedKey = null;

        do
        {
            var request = new ScanRequest
            {
                TableName = tableName,
                ExclusiveStartKey = lastEvaluatedKey
            };

            ScanResponse response = await _dynamoDb.ScanAsync(request, cancellationToken);
            allItems.AddRange(response.Items);
            lastEvaluatedKey = response.LastEvaluatedKey;

        } while (lastEvaluatedKey != null && lastEvaluatedKey.Count > 0);

        return allItems;
    }
}