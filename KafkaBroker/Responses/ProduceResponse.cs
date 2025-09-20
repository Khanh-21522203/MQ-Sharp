namespace KafkaBroker.Responses;

//ProduceResponse => [TopicName [Partition ErrorCode Offset]]
// TopicName => string
// Partition => int32
// ErrorCode => int16
// Offset => int64
public sealed record ProduceResponse(IReadOnlyList<ProduceResponse.TopicResult> Results)
{
    public sealed record TopicResult(string TopicName, IReadOnlyList<PartitionResult> Partitions);
    public sealed record PartitionResult(int Partition, short ErrorCode, long Offset);
}
