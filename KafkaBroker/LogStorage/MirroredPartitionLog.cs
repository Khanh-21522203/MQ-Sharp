namespace KafkaBroker.LogStorage;

// Ghi cả 2 nơi (primary là nguồn chân lý), secondary best-effort
public sealed class MirroredPartitionLog(IPartitionLog primary, IPartitionLog secondary) : IPartitionLog
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
}