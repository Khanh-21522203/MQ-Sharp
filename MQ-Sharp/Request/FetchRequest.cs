namespace MQ_Sharp.Request;

public sealed class FetchRequest(
    int replicaId,
    int maxWaitTimeMs,
    int minBytes,
    IEnumerable<TopicFetchData> topics)
{
    /// <summary>Usually -1 for normal clients.</summary>
    public int ReplicaId { get; } = replicaId;

    public int MaxWaitTimeMs { get; } = maxWaitTimeMs;
    public int MinBytes { get; } = minBytes;
    public IReadOnlyList<TopicFetchData> Topics { get; } = (topics ?? []).ToList();
}

public sealed class TopicFetchData(string topicName, IEnumerable<PartitionFetchData> partitions)
{
    public string TopicName { get; } = topicName ?? throw new ArgumentNullException(nameof(topicName));
    public IReadOnlyList<PartitionFetchData> Partitions { get; } = (partitions ?? []).ToList();
}

public sealed class PartitionFetchData(int partition, long fetchOffset, int maxBytes)
{
    public int Partition { get; } = partition;
    public long FetchOffset { get; } = fetchOffset;
    public int MaxBytes { get; } = maxBytes;
}