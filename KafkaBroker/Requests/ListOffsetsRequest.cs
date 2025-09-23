namespace KafkaBroker.Requests;

// Request:
// ListOffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
public sealed record ListOffsetRequest(int ReplicaId, IReadOnlyList<ListOffsetRequest.TopicData> Topics)
{
    public sealed record TopicData(string TopicName, IReadOnlyList<PartitionData> Partitions);

    public sealed record PartitionData(int Partition, long Time, int MaxNumberOfOffsets);
}