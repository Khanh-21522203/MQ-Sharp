using KafkaBroker.Responses;

namespace KafkaBroker;

public enum KafkaTopicErrorCode : short
{
    None = 0,
    UnknownTopicOrPartition = 3,
    LeaderNotAvailable = 5,
    InvalidTopic = 17,
}

public interface ITopicMetadataManager
{
    /// <summary>
    /// Ensures that the specified topic exists.
    /// If it does not exist, creates it with the given number of partitions.
    /// In a single-broker setup, the current broker is always the leader for all partitions.
    /// </summary>
    void EnsureTopic(string topicName, int numPartitions);

    /// <summary>
    /// Retrieves metadata for the specified topics.
    /// If <paramref name="requestedTopics"/> is null or empty, returns metadata for all topics.
    /// If a topic does not exist, returns a <see cref="MetadataResponse.TopicMetadata"/> with
    /// error code UNKNOWN_TOPIC_OR_PARTITION.
    /// </summary>
    MetadataResponse GetMetadata(IReadOnlyList<string>? requestedTopics);
}

public sealed class TopicMetadataManager(int nodeId, string host, int port) : ITopicMetadataManager
{
    private readonly string _host = host ?? throw new ArgumentNullException(nameof(host));

    private readonly Dictionary<string, int> _topics = new(StringComparer.Ordinal);
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    
    public void EnsureTopic(string topicName, int numPartitions)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException("Topic name must not be empty.", nameof(topicName));
        if (numPartitions <= 0)
            throw new ArgumentOutOfRangeException(nameof(numPartitions), "numPartitions must be > 0.");

        _lock.EnterWriteLock();
        try
        {
            // Nếu đã tồn tại thì bỏ qua (idempotent)
            if (_topics.TryGetValue(topicName, out var existing))
            {
                // Cho phép tăng số partition (đơn giản hoá; không giảm)
                if (numPartitions > existing)
                    _topics[topicName] = numPartitions;
                return;
            }

            _topics[topicName] = numPartitions;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    public MetadataResponse GetMetadata(IReadOnlyList<string>? requestedTopics)
    {
        var brokers = new List<MetadataResponse.Broker>
        {
            new(nodeId, _host, port)
        };

        List<MetadataResponse.TopicMetadata> topicsMetadata = new();

        _lock.EnterReadLock();
        try
        {
            if (requestedTopics == null || requestedTopics.Count == 0)
            {
                foreach (var kv in _topics)
                {
                    topicsMetadata.Add(BuildTopicMetadata(kv.Key, kv.Value, nodeId));
                }
            }
            else
            {
                foreach (var name in requestedTopics)
                {
                    if (!_topics.TryGetValue(name, out var partitions))
                    {
                        topicsMetadata.Add(new MetadataResponse.TopicMetadata(
                            TopicTopicErrorCode: (short)KafkaTopicErrorCode.UnknownTopicOrPartition,
                            TopicName: name,
                            Partitions: []
                        ));
                        continue;
                    }

                    topicsMetadata.Add(BuildTopicMetadata(name, partitions, nodeId));
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        return new MetadataResponse(brokers, topicsMetadata);
    }

    // ----------------- Helpers -----------------

    private static MetadataResponse.TopicMetadata BuildTopicMetadata(string topicName, int numPartitions, int nodeId)
    {
        var parts = new List<MetadataResponse.PartitionMetadata>(numPartitions);
        for (int p = 0; p < numPartitions; p++)
        {
            parts.Add(new MetadataResponse.PartitionMetadata(
                PartitionErrorCode: (short)KafkaTopicErrorCode.None,
                PartitionId: p,
                Leader: nodeId,
                Replicas: [nodeId],
                Isr: [nodeId]
            ));
        }

        return new MetadataResponse.TopicMetadata(
            TopicTopicErrorCode: (short)KafkaTopicErrorCode.None,
            TopicName: topicName,
            Partitions: parts
        );
    }
}
