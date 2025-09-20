using MQ_Sharp.Messages;

namespace MQ_Sharp.Serialization;

public interface IEncoder<T>
{
    byte[] Encode(T data);
}