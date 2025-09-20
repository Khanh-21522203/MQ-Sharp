namespace MQ_Sharp.Services.RecordAccumulator;

using System.Collections.Generic;
using System;

public class Batch(long maxBatchSize)
{
    public List<byte[]> Records { get; } = new List<byte[]>();
    public long CurrentSize { get; private set; }
    public long CreationTimeMs { get; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public void AddRecord(byte[] recordData)
    {
        Records.Add(recordData);
        CurrentSize += recordData.Length;
    }

    public bool IsFull() => CurrentSize >= maxBatchSize;
}