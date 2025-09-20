using MQ_Sharp.Cluster;
using MQ_Sharp.Utils;
using MQ_Sharp.ZooKeeperIntegration.Events;

namespace MQ_Sharp.ZooKeeperIntegration.Listener;

public class BrokerTopicsListener: IZooKeeperChildListener
{
    private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(BrokerTopicsListener));
    
    private readonly IDictionary<int, Broker> _actualBrokerIdMap;
    private readonly Action<int, string, int> _callback;
    private readonly IDictionary<string, SortedSet<Partition>> _actualBrokerTopicsPartitionsMap;
    private readonly IZooKeeperClient _zkclient;
    
    private IDictionary<int, Broker> _oldBrokerIdMap;
    private IDictionary<string, SortedSet<Partition>> _oldBrokerTopicsPartitionsMap;
    
    private readonly Lock _syncLock = new Lock();

    public BrokerTopicsListener(
        IZooKeeperClient zkclient,
        IDictionary<string, SortedSet<Partition>> actualBrokerTopicsPartitionsMap,
        IDictionary<int, Broker> actualBrokerIdMap,
        Action<int, string, int> callback)
    {
        _zkclient = zkclient;
        _actualBrokerTopicsPartitionsMap = actualBrokerTopicsPartitionsMap;
        _actualBrokerIdMap = actualBrokerIdMap;
        _callback = callback;

        _oldBrokerIdMap = new Dictionary<int, Broker>(_actualBrokerIdMap);
        _oldBrokerTopicsPartitionsMap = new Dictionary<string, SortedSet<Partition>>(_actualBrokerTopicsPartitionsMap);
    }
    
    public void HandleChildChange(ZooKeeperChildChangedEventArgs args)
    {
        Guard.NotNull(args, "args");
        Guard.NotNullNorEmpty(args.Path, "args.Path");
        Guard.NotNull(args.Children, "args.Children");

        lock (_syncLock)
        {
            try
            {
                var path = args.Path;
                var children = args.Children;

                switch (path)
                {
                    case ZkPaths.BrokerIds:
                        var oldTopics = _oldBrokerTopicsPartitionsMap.Keys.ToList();
                        var newTopics = children.Except(oldTopics).ToList();
                        foreach (var newTopic in newTopics)
                        {
                            var brokerTopicPath = ZkPaths.BrokerTopics + "/" + newTopic;
                            var brokerList = _zkclient.GetChildrenParentMayNotExist(brokerTopicPath);
                            ProcessNewBrokerInExistingTopic(newTopic, brokerList);
                            _zkclient.Subscribe(ZkPaths.BrokerTopics + "/" + newTopic, this);
                        }
                        break;
                    case ZkPaths.BrokerTopics:
                        ProcessBrokerChange(path, children);
                        break;
                    default:
                        var parts = path.Split('/');
                        var topic = parts.Last();
                        if (parts is [_, _, "topics", _])
                        {
                            ProcessNewBrokerInExistingTopic(topic, children);
                        }
                        break;
                }
            }
            catch (Exception ex)
            {
                Logger.Warn("Error while handling " + args, ex);
            }
        }
    }

    private void ProcessNewBrokerInExistingTopic(string topic, IEnumerable<string> children)
    {
        
    }

    private void ProcessBrokerChange(string path, IEnumerable<string> children)
    {
        if (path != ZkPaths.BrokerIds)
        {
            return;
        }
        
    }

    public void ResetState()
    {
        
    }
}