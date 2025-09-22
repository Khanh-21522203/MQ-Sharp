namespace KafkaBroker.Responses;

// MetadataResponse => [Broker][TopicMetadata]
// Broker => NodeId Host Port  (any number of brokers may be returned)
//   NodeId => int32
//   Host => string
//   Port => int32
// TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
//   TopicErrorCode => int16
// PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
//   PartitionErrorCode => int16
//   PartitionId => int32
//   Leader => int32
//   Replicas => [int32]
//   Isr => [int32]
public sealed record TopicMetadataResponse(IReadOnlyList<TopicMetadataResponse.Broker> Brokers, IReadOnlyList<TopicMetadataResponse.TopicMetadata> Topics)
{
    public sealed record Broker(int NodeId, string Host, int Port);
    public sealed record TopicMetadata(short TopicTopicErrorCode, string TopicName, IReadOnlyList<PartitionMetadata> Partitions);
    public sealed record PartitionMetadata(short PartitionErrorCode, int PartitionId, int Leader, IReadOnlyList<int> Replicas, IReadOnlyList<int> Isr);
}
