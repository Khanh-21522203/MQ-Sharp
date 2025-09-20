namespace KafkaBroker.Requests;

public sealed record ProduceRequest(short RequiredAcks, int TimeoutMs, IReadOnlyList<ProduceRequest.TopicData> Topics)
{
    public sealed record TopicData(string Topic, IReadOnlyList<PartitionData> Partitions);
    public sealed record PartitionData(int Partition, ReadOnlyMemory<byte> MessageSet);
}