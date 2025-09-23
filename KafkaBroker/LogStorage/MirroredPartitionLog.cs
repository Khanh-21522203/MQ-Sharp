using KafkaBroker.LogStorage.Interface;

namespace KafkaBroker.LogStorage;

// Ghi cả 2 nơi (primary là nguồn chân lý), secondary best-effort
public sealed class MirroredPartitionLog(IPartitionLog primary, IPartitionLog secondary)
    : IPartitionLog, IOffsetIntrospect
{
    public TopicPartitionKey Key => primary.Key;

    public async ValueTask<long> AppendAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken ct = default)
    {
        var off = await primary.AppendAsync(recordBatch, ct);
        _ = secondary.AppendAsync(recordBatch, ct); // fire-and-forget
        return off;
    }

    public async ValueTask<ReadOnlyMemory<byte>> ReadAsync(long offset, int maxBytes, CancellationToken ct = default)
    {
        var data = await primary.ReadAsync(offset, maxBytes, ct);
        if (!data.IsEmpty) return data;
        return await secondary.ReadAsync(offset, maxBytes, ct);
    }

    public async ValueTask FlushAsync(CancellationToken ct = default)
    {
        await primary.FlushAsync(ct);
        _ = secondary.FlushAsync(ct);
    }

    public long GetEarliestOffset() => (primary as IOffsetIntrospect)?.GetEarliestOffset() ?? 0;
    public long GetLatestOffset() => (primary as IOffsetIntrospect)?.GetLatestOffset() ?? 0;

    public long FindOffsetByTimestamp(long timestampMs)
    {
        var oi = primary as IOffsetIntrospect
                 ?? throw new NotSupportedException("Primary does not support timestamp lookup.");
        return oi.FindOffsetByTimestamp(timestampMs);
    }
}