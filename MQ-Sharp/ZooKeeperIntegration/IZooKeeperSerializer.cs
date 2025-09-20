namespace MQ_Sharp.ZooKeeperIntegration;

public interface IZooKeeperSerializer
{
    byte[] Serialize(object obj);
    object Deserialize(byte[] bytes);
}