using MQ_Sharp.Cluster;

namespace MQ_Sharp.Response;

public class MetaDataResponse(
    IEnumerable<Broker> brokers,
    IEnumerable<TopicMetadata> topics)
{
    public IReadOnlyList<Broker> Brokers { get; } = (brokers ?? []).ToList();
    public IReadOnlyList<TopicMetadata> Topics { get; } = (topics  ?? []).ToList();
}

public sealed class TopicMetadata(
    short topicErrorCode,
    string topicName,
    IEnumerable<PartitionMetadata> partitions)
{
    /// <summary>Kafka error code for the topic (0 = NoError)</summary>
    public short TopicErrorCode { get; } = topicErrorCode;

    public string TopicName { get; } = topicName ?? throw new ArgumentNullException(nameof(topicName));
    public IReadOnlyList<PartitionMetadata> Partitions { get; } = (partitions ?? []).ToList();
}

public sealed class PartitionMetadata(
    short partitionErrorCode,
    int partitionId,
    int leader,
    IEnumerable<int> replicas,
    IEnumerable<int> isr)
{
    /// <summary>Kafka error code for the partition (0 = NoError)</summary>
    public short PartitionErrorCode { get; } = partitionErrorCode;

    public int PartitionId { get; } = partitionId;

    /// <summary>Leader broker node id (may be -1 if none)</summary>
    public int Leader { get; } = leader;

    /// <summary>All replicas (node ids)</summary>
    public IReadOnlyList<int> Replicas { get; } = (replicas ?? []).ToList();

    /// <summary>In-Sync Replicas (node ids)</summary>
    public IReadOnlyList<int> Isr { get; } = (isr      ?? []).ToList();
}