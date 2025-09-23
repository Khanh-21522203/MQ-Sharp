namespace KafkaBroker.Responses;

public sealed record OffsetFetchResponse(IReadOnlyList<OffsetFetchResponse.TopicResult> Topics)
{
    public sealed record TopicResult(string TopicName, IReadOnlyList<PartitionResult> Partitions);

    public sealed record PartitionResult(
        int Partition,
        long Offset,
        string Metadata,
        short ErrorCode
    );
}