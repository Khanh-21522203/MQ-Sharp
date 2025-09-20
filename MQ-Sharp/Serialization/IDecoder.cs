namespace MQ_Sharp.Serialization;

public interface IDecoder<out T>
{
    T Decode(byte[] bytes);
}