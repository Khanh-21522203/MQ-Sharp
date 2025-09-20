using MQ_Sharp.Configuration;
using MQ_Sharp.Messages;
using MQ_Sharp.Serialization;
using MQ_Sharp.Services.RecordAccumulator;

namespace MQ_Sharp.Producer;

public interface IProducer<TKey, TData>
{
    ProducerConfiguration Config { get; }
    void Send(IEnumerable<ProducerData<TKey, TData>> data);
    void Send(ProducerData<TKey, TData> data);
}

public class Producer<TKey, TData>: IProducer<TKey, TData>
{
    public static log4net.ILog Logger = log4net.LogManager.GetLogger("Producer");

    public ProducerConfiguration Config { get; }
    private readonly IRecordAccumulator _recordAccumulator;
    private readonly IEncoder<ProducerData<TKey, TData>> _encoder;
    
    public void Send(IEnumerable<ProducerData<TKey, TData>> data)
    {
        foreach (var record in data)
        {
            Send(record);
        }
    }

    public void Send(ProducerData<TKey, TData> data)
    {
        try
        {
            var encodedData = _encoder.Encode(data);
            var timestamp = DateTime.UtcNow;
            
            // Step 2: Ensure metadata for topic
            var partitions = _metadataManager.GetBrokerPartitionInfo(data.Topic);
            if (!partitions.Any())
            {
                throw new InvalidOperationException($"No partitions available for topic: {data.Topic}");
            }
            
            // Step 3: Choose partition
            int partition;
            if (data.Partition.HasValue)
            {
                // Use explicit partition if provided
                partition = data.Partition.Value;
                if (partition < 0 || partition >= partitions.Count)
                {
                    throw new ArgumentOutOfRangeException(nameof(data.Partition), 
                        $"Partition {partition} is out of range for topic {data.Topic}");
                }
            }
            else
            {
                // Use partitioner to choose partition
                partition = _partitioner.ChoosePartition(data.Topic, data.Key, partitions.Count);
            }
            
            // Step 5: Append to RecordAccumulator
            _recordAccumulator.Append(data.Topic, partition, record);
        }
        catch (Exception ex)
        {
            // Handle serialization or metadata errors
            throw new ProducerException($"Failed to send record to topic {data.Topic}", ex);
        }
    }
    
    private byte[] CreateRecord(byte[] key, byte[] value, Dictionary<string, string> headers, DateTime timestamp)
    {
        // Create a simple record format: [timestamp][key_length][key][value_length][value][headers]
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        
        // Write timestamp
        writer.Write(timestamp.ToBinary());
        
        // Write key
        if (key != null)
        {
            writer.Write(key.Length);
            writer.Write(key);
        }
        else
        {
            writer.Write(0);
        }
        
        // Write value
        if (value != null)
        {
            writer.Write(value.Length);
            writer.Write(value);
        }
        else
        {
            writer.Write(0);
        }
        
        // Write headers count
        writer.Write(headers?.Count ?? 0);
        if (headers != null)
        {
            foreach (var header in headers)
            {
                var keyBytes = System.Text.Encoding.UTF8.GetBytes(header.Key);
                var valueBytes = System.Text.Encoding.UTF8.GetBytes(header.Value);
                
                writer.Write(keyBytes.Length);
                writer.Write(keyBytes);
                writer.Write(valueBytes.Length);
                writer.Write(valueBytes);
            }
        }
        
        return stream.ToArray();
    }
}