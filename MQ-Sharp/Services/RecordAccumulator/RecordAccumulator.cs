using MQ_Sharp.Cluster;
using System.Collections.Concurrent;


namespace MQ_Sharp.Services.RecordAccumulator;

public interface IRecordAccumulator
{
    void Append(string topic, int partition, byte[] recordData);
}

public class RecordAccumulator(
    long batchSize,
    int lingerMs): IRecordAccumulator
{
    private readonly ConcurrentDictionary<Partition, Batch> _batches = new();
    private readonly ConcurrentQueue<Batch> _readyBatches = new();
    private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

    public void Append(string topic, int partition, byte[] recordData)
    {
        var tp = new Partition(topic, partition);

        // Try to get an existing batch
        if (!_batches.TryGetValue(tp, out var batch))
        {
            // No batch found, so we need to create one.
            _lock.Wait();
            try
            {
                // Double-check to prevent race conditions
                if (!_batches.TryGetValue(tp, out batch))
                {
                    batch = new Batch(batchSize);
                    _batches.TryAdd(tp, batch);
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        // Check if the current batch is full or has lingered long enough
        if (batch.IsFull() || (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - batch.CreationTimeMs >= lingerMs))
        {
            // Move the full/lingered batch to the ready queue and create a new one
            if (_batches.TryRemove(tp, out var oldBatch))
            {
                _readyBatches.Enqueue(oldBatch);
            }
            batch = new Batch(batchSize);
            _batches.TryAdd(tp, batch);
        }

        // Add the record to the batch
        batch.AddRecord(recordData);
    }

    public IEnumerable<Batch> DrainReadyBatches()
    {
        var drainedList = new List<Batch>();
        while (_readyBatches.TryDequeue(out var batch))
        {
            drainedList.Add(batch);
        }

        // Check for any batches that have lingered but weren't automatically flushed
        foreach (var pair in _batches)
        {
            var batch = pair.Value;
            if (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - batch.CreationTimeMs >= lingerMs)
            {
                if (_batches.TryRemove(pair.Key, out var lingeredBatch))
                {
                    drainedList.Add(lingeredBatch);
                }
            }
        }

        return drainedList;
    }
}