namespace KafkaBroker.Responses;

public sealed record OffsetCommitResponse(IReadOnlyList<OffsetCommitResponse.TopicResult> Topics)
{
    public sealed record TopicResult(string TopicName, IReadOnlyList<PartitionResult> Partitions);

    public sealed record PartitionResult(
        int Partition,
        short ErrorCode
    );
}