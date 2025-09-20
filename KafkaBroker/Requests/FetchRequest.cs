namespace KafkaBroker.Requests;

// FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
// ReplicaId => int32
// MaxWaitTime => int32
// MinBytes => int32
// TopicName => string
// Partition => int32
// FetchOffset => int64
// MaxBytes => int32
public sealed record FetchRequest(int ReplicaId, int MaxWaitTime, int MinBytes, IReadOnlyList<FetchRequest.TopicData> Topics)
{
    public sealed record TopicData(string TopicName, IReadOnlyList<PartitionData> Partitions);
    public sealed record PartitionData(int Partition, long FetchOffset, int MaxBytes);
}