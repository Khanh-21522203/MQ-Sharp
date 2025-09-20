namespace MQ_Sharp.Request;

public sealed class ListOffsetRequest(int replicaId, IEnumerable<TopicOffsetRequest> topics)
{
    /// <summary>ReplicaId = -1 cho client bình thường.</summary>
    public int ReplicaId { get; } = replicaId;

    public IReadOnlyList<TopicOffsetRequest> Topics { get; } = (topics ?? []).ToList();
}

public sealed class TopicOffsetRequest(string topicName, IEnumerable<PartitionOffsetRequest> partitions)
{
    public string TopicName { get; } = topicName ?? throw new ArgumentNullException(nameof(topicName));
    public IReadOnlyList<PartitionOffsetRequest> Partitions { get; } = (partitions ?? []).ToList();
}

public sealed class PartitionOffsetRequest(int partition, long time, int maxNumberOfOffsets)
{
    public int Partition { get; } = partition;

    /// <summary>-1 = latest, -2 = earliest, hoặc timestamp cụ thể.</summary>
    public long Time { get; } = time;

    public int MaxNumberOfOffsets { get; } = maxNumberOfOffsets;
}