namespace MQ_Sharp.Response;

public sealed class OffsetResponse(IEnumerable<TopicOffsetResponse> topics)
{
    public IReadOnlyList<TopicOffsetResponse> Topics { get; } = (topics ?? []).ToList();
}

public sealed class TopicOffsetResponse(string topicName, IEnumerable<PartitionOffsetResponse> partitions)
{
    public string TopicName { get; } = topicName ?? throw new ArgumentNullException(nameof(topicName));
    public IReadOnlyList<PartitionOffsetResponse> Partitions { get; } = (partitions ?? []).ToList();
}

public sealed class PartitionOffsetResponse(int partition, short errorCode, IEnumerable<long> offsets)
{
    public int Partition { get; } = partition;
    public short ErrorCode { get; } = errorCode;
    public IReadOnlyList<long> Offsets { get; } = (offsets ?? []).ToList();
}