namespace KafkaBroker.Responses;

public sealed record ProduceResponse(int CorrelationId, IReadOnlyList<ProduceResponse.TopicResult> Results)
{
    public sealed record TopicResult(string Topic, IReadOnlyList<PartitionResult> Partitions);
    public sealed record PartitionResult(int Partition, short ErrorCode, long BaseOffset);
}
