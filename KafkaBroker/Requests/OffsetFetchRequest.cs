namespace KafkaBroker.Requests;

public sealed record OffsetFetchRequest(string ConsumerGroup, IReadOnlyList<OffsetFetchRequest.TopicData> Topics)
{
    public sealed record TopicData(string TopicName, IReadOnlyList<int> Partitions);
}