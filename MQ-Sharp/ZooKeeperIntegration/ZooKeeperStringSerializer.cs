using System.Text;
using MQ_Sharp.Utils;

namespace MQ_Sharp.ZooKeeperIntegration;

public class ZooKeeperStringSerializer: IZooKeeperSerializer
{
    public static readonly ZooKeeperStringSerializer Serializer = new ZooKeeperStringSerializer();
    
    public byte[] Serialize(object obj)
    {
        Guard.NotNull(obj, "obj");
        return Encoding.UTF8.GetBytes(obj.ToString()!);
    }
    
    public object Deserialize(byte[] bytes)
    {
        return bytes == null ? null : Encoding.UTF8.GetString(bytes);
    }
}