using System.Collections.Concurrent;
using MQ_Sharp.Consumer;
using MQ_Sharp.Utils;
using MQ_Sharp.ZooKeeperIntegration.Events;

namespace MQ_Sharp.ZooKeeperIntegration.Listener;

public class ZKRebalancerListener: IZooKeeperChildListener
{
    private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ZKRebalancerListener));
    
    public event EventHandler ConsumerRebalance;

    private readonly IDictionary<string, IDictionary<int, PartitionTopicInfo>> _topicRegistry;  
    private readonly IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> _queues;
    private readonly string _consumerIdString;
    private readonly Lock _syncLock = new Lock();
    private readonly Lock _asyncLock = new Lock();
    private readonly ConsumerConfiguration _config;
    private readonly IZooKeeperClient _zkClient;
    private readonly ZKGroupDirs _dirs;
    private readonly Fetcher _fetcher;
    private readonly ZookeeperConsumerConnector _zkConsumerConnector;
    private readonly IDictionary<string, IList<KafkaMessageStream<TData>>> _kafkaMessageStreams;
    private readonly TopicCount _topicCount;

    // async/cancel
    private CancellationTokenSource rebalanceCancellationTokenSource = new CancellationTokenSource();
    private Task? rebalanceTask;
    private volatile bool isRebalanceRunning = false;

    public void HandleChildChange(ZooKeeperChildChangedEventArgs args)
    {
        Guard.NotNull(args, "args");
        Guard.NotNullNorEmpty(args.Path, "args.Path");
        Guard.NotNull(args.Children, "args.Children");
        
        Logger.Info("Performing rebalancing. Consumers have been added or removed from the consumer group: " + args.Path);

        try { _zkClient.Subscribe(args.Path, this); } catch { /* best-effort */ }

        AsyncRebalance();
    }

    public void AsyncRebalance(int waitRebalanceFinishTimeoutInMs = 0)
    {
        lock (_asyncLock)
        {
            // Stop currently running rebalance
            StopRebalance_NoLock();

            // Run new rebalance operation asynchronously
            Logger.Info("Asynchronously running rebalance operation");
            rebalanceCancellationTokenSource = new CancellationTokenSource();
            var token = rebalanceCancellationTokenSource.Token;

            isRebalanceRunning = true;
            rebalanceTask = Task.Run(() => SyncedRebalance(token), token);
        }
    }

    public void ResetState()
    {
        
    }

}