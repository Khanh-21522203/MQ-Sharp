namespace KafkaBroker.Responses;

// Response:
// ListOffsetResponse => [TopicName [PartitionOffsets]]
// PartitionOffsets => Partition ErrorCode [Offset]
public sealed record ListOffsetResponse(IReadOnlyList<ListOffsetResponse.TopicResult> Topics)
{
    public sealed record TopicResult(string TopicName, IReadOnlyList<PartitionOffsets> Partitions);

    public sealed record PartitionOffsets(int Partition, short ErrorCode, IReadOnlyList<long> Offsets);
}