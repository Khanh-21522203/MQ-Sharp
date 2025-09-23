using KafkaBroker.LogStorage.Interface;

namespace KafkaBroker.LogStorage;

public sealed class FilePartitionLog : IPartitionLog, IDisposable, IOffsetIntrospect
{
    public TopicPartitionKey Key { get; }
    private readonly FileStream _fs;
    private long _nextOffset;

    public FilePartitionLog(TopicPartitionKey key, string filePath)
    {
        Key = key;
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);
        _fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read,
            bufferSize: 1 << 16, FileOptions.Asynchronous | FileOptions.WriteThrough);
        // TODO: rebuild _nextOffset từ index/EOF
        _nextOffset = 0;
    }

    public async ValueTask<long> AppendAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken ct = default)
    {
        await _fs.WriteAsync(recordBatch, ct);
        _nextOffset++;
        return _nextOffset - 1;
    }

    public async ValueTask<ReadOnlyMemory<byte>> ReadAsync(long offset, int maxBytes, CancellationToken ct = default)
    {
        // Demo: chưa có index map offset -> (pos,len)
        await Task.Yield();
        return ReadOnlyMemory<byte>.Empty;
    }

    public ValueTask FlushAsync(CancellationToken ct = default) => new(_fs.FlushAsync(ct));
    public void Dispose() => _fs.Dispose();
    public long GetEarliestOffset() => 0;

    public long GetLatestOffset() => _nextOffset;

    // TODO: implement time index
    public long FindOffsetByTimestamp(long timestampMs)
        => throw new NotSupportedException("FilePartitionLog: time index not implemented.");
}