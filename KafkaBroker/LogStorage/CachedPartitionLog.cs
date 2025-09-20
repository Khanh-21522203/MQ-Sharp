namespace KafkaBroker.LogStorage;

// Read-through + write-through cache (file = nguồn chân lý)
public sealed class CachedPartitionLog(IPartitionLog primary, int cacheCapacity = 1024) : IPartitionLog
{
    public TopicPartitionKey Key => primary.Key;

    // thường là FilePartitionLog
    private readonly LruBatchCache _cache = new(cacheCapacity);

    public async ValueTask<long> AppendAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken ct = default)
    {
        var off = await primary.AppendAsync(recordBatch, ct);
        try
        {
            _cache.Put(off, recordBatch);
        }
        catch
        {
            // ignored
        }

        return off;
    }

    public async ValueTask<ReadOnlyMemory<byte>> ReadAsync(long offset, int maxBytes, CancellationToken ct = default)
    {
        if (_cache.TryGet(offset, out var hit)) return hit;
        var data = await primary.ReadAsync(offset, maxBytes, ct);
        if (!data.IsEmpty)
        {
            try
            {
                _cache.Put(offset, data);
            }
            catch
            {
                // ignored
            }
        }

        return data;
    }

    public ValueTask FlushAsync(CancellationToken ct = default) => primary.FlushAsync(ct);
}