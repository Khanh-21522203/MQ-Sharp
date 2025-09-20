namespace MQ_Sharp.Producer;public interface IEncoder<T>
{
    byte[] Encode(T data);
}

public class ProducerData<TKey, TData>(string topic, TKey key, TData data)
{
    public string Topic { get; private set; } = topic;
    public TKey Key { get; private set; } = key;
    public TData Data { get; private set; } = data;
}