namespace MQ_Sharp.Response;

public sealed class ProduceResponse(IEnumerable<TopicProduceResponse> topics)
{
    public IReadOnlyList<TopicProduceResponse> Topics { get; } = (topics ?? []).ToList();
}

public sealed class TopicProduceResponse(
    string topicName,
    IEnumerable<PartitionProduceResponse> partitions)
{
    public string TopicName { get; } = topicName ?? throw new ArgumentNullException(nameof(topicName));
    public IReadOnlyList<PartitionProduceResponse> Partitions { get; } = (partitions ?? []).ToList();
}

public sealed class PartitionProduceResponse(int partition, short errorCode, long offset)
{
    public int Partition { get; } = partition;
    public short ErrorCode { get; } = errorCode;
    public long Offset { get; } = offset;
}