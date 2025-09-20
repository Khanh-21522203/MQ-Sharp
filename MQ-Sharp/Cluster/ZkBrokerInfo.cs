using System.Runtime.Serialization;

namespace MQ_Sharp.Cluster;

[DataContract]
public class ZkBrokerInfo
{
    [DataMember(Name = "host")] public required string Host;
    [DataMember(Name = "jmx_port")] public int JmxPort;
    [DataMember(Name = "timestamp")] public long Timestamp;
    [DataMember(Name = "version")] public int Version;
}