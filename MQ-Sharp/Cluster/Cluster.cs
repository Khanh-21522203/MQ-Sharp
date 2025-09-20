using System.Globalization;
using MQ_Sharp.ZooKeeperIntegration;

namespace MQ_Sharp.Cluster;

public class Cluster()
{
    private readonly Dictionary<int, Broker> _brokers = new();
    
    public Dictionary<int, Broker> Brokers => _brokers;

    public Cluster(IZooKeeperClient zkClient): this()
    {
        var nodes = zkClient.GetChildrenParentMayNotExist(ZkPaths.BrokerIds);
        foreach (var node in nodes)
        {
            if (zkClient.TryReadData<string>(ZkPaths.BrokerIds + "/" + node, out var brokerZkString, null, false))
            {
                if (brokerZkString is not null && int.TryParse(node, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id))
                {
                    var broker = Broker.CreateBroker(id, brokerZkString);
                    _brokers[broker.Id] = broker;
                }
            }
        }
    }
    
    public Broker? GetBroker(int id)
    {
        return _brokers!.GetValueOrDefault(id, null);
    }
}