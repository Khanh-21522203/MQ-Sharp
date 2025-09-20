namespace MQ_Sharp.Response;

public sealed class FetchResponse(IEnumerable<TopicFetchResponse> topics)
{
    public IReadOnlyList<TopicFetchResponse> Topics { get; } = (topics ?? []).ToList();
}

public sealed class TopicFetchResponse(string topicName, IEnumerable<PartitionFetchResponse> partitions)
{
    public string TopicName { get; } = topicName ?? throw new ArgumentNullException(nameof(topicName));
    public IReadOnlyList<PartitionFetchResponse> Partitions { get; } = (partitions ?? []).ToList();
}

public sealed class PartitionFetchResponse(
    int partition,
    short errorCode,
    long highWatermarkOffset,
    int messageSetSize,
    byte[] messageSet)
{
    public int Partition { get; } = partition;
    public short ErrorCode { get; } = errorCode;
    public long HighWatermarkOffset { get; } = highWatermarkOffset;
    public int MessageSetSize { get; } = messageSetSize;

    /// <summary>Raw MessageSet bytes as returned by broker.</summary>
    public byte[] MessageSet { get; } = messageSet ?? [];
}