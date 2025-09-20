namespace KafkaBroker.LogStorage;

public readonly record struct TopicPartitionKey(string Topic, int Partition);

public interface IPartitionLog
{
    TopicPartitionKey Key { get; }
    ValueTask<long> AppendAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken ct = default);
    ValueTask<ReadOnlyMemory<byte>> ReadAsync(long offset, int maxBytes, CancellationToken ct = default);
    ValueTask FlushAsync(CancellationToken ct = default);
}