using System.Collections.Concurrent;
using KafkaBroker.LogStorage.Interface;

namespace KafkaBroker.LogStorage;

public enum LogMode
{
    MemoryOnly,
    FileOnly,
    Mirrored,
    Tiered
}

public interface ILogManager
{
    IPartitionLog GetOrCreate(TopicPartitionKey key);
    bool TryGet(TopicPartitionKey key, out IPartitionLog log);
}

public class LogManager : ILogManager, IDisposable
{
    private readonly ConcurrentDictionary<TopicPartitionKey, IPartitionLog> _map = new();
    private readonly LogMode _mode;
    private readonly string _dataDir;
    private readonly int _cacheCapacity;

    public LogManager(LogMode mode, string dataDir, int cacheCapacity = 4096)
    {
        _mode = mode;
        _dataDir = dataDir;
        _cacheCapacity = cacheCapacity;
        Directory.CreateDirectory(_dataDir);
    }

    public IPartitionLog GetOrCreate(TopicPartitionKey key)
        => _map.GetOrAdd(key, CreateLog);

    public bool TryGet(TopicPartitionKey key, out IPartitionLog log)
        => _map.TryGetValue(key, out log!);

    private IPartitionLog CreateLog(TopicPartitionKey key)
    {
        var filePath = ToPartitionPath(_dataDir, key);
        var file = new FilePartitionLog(key, filePath);
        var mem = new MemoryPartitionLog(key);

        return _mode switch
        {
            LogMode.MemoryOnly => mem,
            LogMode.FileOnly => file,
            LogMode.Mirrored => new MirroredPartitionLog(file, mem), // file = primary
            LogMode.Tiered => new CachedPartitionLog(file, _cacheCapacity), // file + LRU cache
            _ => file
        };
    }

    public static string ToPartitionPath(string root, TopicPartitionKey key)
        => Path.Combine(root, Sanitize(key.Topic), key.Partition.ToString(), "partition.log");

    private static string Sanitize(string s)
    {
        foreach (var c in Path.GetInvalidFileNameChars())
            s = s.Replace(c, '_');
        return s;
    }

    public void Dispose()
    {
        foreach (var log in _map.Values)
            if (log is IDisposable d)
                d.Dispose();
    }
}