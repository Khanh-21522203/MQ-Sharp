using KafkaBroker.LogStorage.Interface;

namespace KafkaBroker.LogStorage;

public sealed class MemoryPartitionLog(TopicPartitionKey key) : IPartitionLog, IOffsetIntrospect
{
    public TopicPartitionKey Key { get; } = key;
    private long _nextOffset = 0;
    private readonly List<(long Off, byte[] Data)> _batches = new();

    public ValueTask<long> AppendAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken ct = default)
    {
        var off = _nextOffset++;
        _batches.Add((off, recordBatch.ToArray()));
        return ValueTask.FromResult(off);
    }

    public ValueTask<ReadOnlyMemory<byte>> ReadAsync(long offset, int maxBytes, CancellationToken ct = default)
    {
        var i = _batches.FindIndex(t => t.Off == offset);
        return i < 0
            ? ValueTask.FromResult(ReadOnlyMemory<byte>.Empty)
            : ValueTask.FromResult((ReadOnlyMemory<byte>)_batches[i].Data);
    }

    public ValueTask FlushAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
    public long GetEarliestOffset() => 0; // TODO: update nếu có retention xoá đầu

    public long GetLatestOffset() => _nextOffset;

    // TODO: implement time index
    public long FindOffsetByTimestamp(long timestampMs)
        => throw new NotSupportedException("MemoryPartitionLog: time index not implemented.");
}