using System.Collections.Concurrent;

namespace KafkaBroker.LogStorage;

// LRU cache đơn giản cho (offset -> batch)
public sealed class LruBatchCache(int capacity)
{
    private readonly int _capacity = Math.Max(1, capacity);
    private readonly ConcurrentDictionary<long, LinkedListNode<(long off, byte[] data)>> _map = new();
    private readonly LinkedList<(long off, byte[] data)> _lru = new();
    private readonly Lock _lock = new();

    public bool TryGet(long offset, out ReadOnlyMemory<byte> data)
    {
        lock (_lock)
        {
            if (_map.TryGetValue(offset, out var node))
            {
                lock (_lock)
                {
                    if (!_map.TryGetValue(offset, out node))
                    {
                        data = default;
                        return false;
                    }

                    _lru.Remove(node);
                    _lru.AddFirst(node);
                }

                data = node.Value.data;
                return true;
            }
        }

        data = default;
        return false;
    }

    public void Put(long offset, ReadOnlyMemory<byte> batch)
    {
        var bytes = batch.ToArray();
        lock (_lock)
        {
            if (_map.TryGetValue(offset, out var existed))
            {
                _lru.Remove(existed);
                existed.Value = (offset, bytes);
                _lru.AddFirst(existed);
                return;
            }

            var node = new LinkedListNode<(long, byte[])>((offset, bytes));
            _lru.AddFirst(node);
            _map[offset] = node;
            if (_map.Count > _capacity)
            {
                var tail = _lru.Last;
                if (tail != null)
                {
                    _lru.RemoveLast();
                    _map.TryRemove(tail.Value.off, out _);
                }
            }
        }
    }
}