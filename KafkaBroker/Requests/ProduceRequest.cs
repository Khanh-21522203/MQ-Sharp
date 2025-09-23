namespace KafkaBroker.Requests;

//ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
// RequiredAcks => int16
// Timeout => int32
// Partition => int32
// MessageSetSize => int32
public sealed record ProduceRequest(short RequiredAcks, int TimeoutMs, IReadOnlyList<ProduceRequest.TopicData> Topics)
{
    public sealed record TopicData(string TopicName, IReadOnlyList<PartitionData> Partitions);

    public sealed record PartitionData(int Partition, ReadOnlyMemory<byte> MessageSet);
}