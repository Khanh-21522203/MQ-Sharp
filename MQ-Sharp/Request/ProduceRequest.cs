using MQ_Sharp.Messages;
using MQ_Sharp.Producer.Model;

namespace MQ_Sharp.Request;

public class ProduceRequest(
    short requiredAcks,
    int timeoutMs,
    IEnumerable<TopicProduceData> topics)
{
    public short RequiredAcks { get; } = requiredAcks;
    public int TimeoutMs { get; } = timeoutMs;
    public IReadOnlyList<TopicProduceData> Topics { get; } = (topics ?? []).ToList();
}

public sealed class TopicProduceData(
    string topicName,
    IEnumerable<PartitionProduceData> partitions)
{
    public string TopicName { get; } = topicName ?? throw new ArgumentNullException(nameof(topicName));
    public IReadOnlyList<PartitionProduceData> Partitions { get; } = (partitions ?? []).ToList();
}

public sealed class PartitionProduceData(int partition, int messageSetSize, byte[] messageSet)
{
    public int Partition { get; } = partition;
    public int MessageSetSize { get; } = messageSetSize;
    public byte[] MessageSet { get; } = messageSet ?? throw new ArgumentNullException(nameof(messageSet));
}