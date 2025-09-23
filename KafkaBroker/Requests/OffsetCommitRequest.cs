namespace KafkaBroker.Requests;

public sealed record OffsetCommitRequest(string ConsumerGroupId, IReadOnlyList<OffsetCommitRequest.TopicData> Topics)
{
    public sealed record TopicData(string TopicName, IReadOnlyList<PartitionData> Partitions);

    public sealed record PartitionData(
        int Partition,
        long Offset,
        string Metadata // arbitrary string, may be empty
    );
}