namespace KafkaBroker.Responses;

// FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
// TopicName => string
// Partition => int32
// ErrorCode => int16
// HighwaterMarkOffset => int64
// MessageSetSize => int32
public sealed record FetchResponse(IReadOnlyList<FetchResponse.TopicResult> Topics)
{
    public sealed record TopicResult(string TopicName, IReadOnlyList<PartitionResult> Partitions);

    public sealed record PartitionResult(
        int Partition,
        short ErrorCode,
        long HighWatermarkOffset,
        int MessageSetSize,
        ReadOnlyMemory<byte> MessageSet);
}