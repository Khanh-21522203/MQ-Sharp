using System.Collections.Concurrent;
using MQ_Sharp.Cluster;
using MQ_Sharp.Configuration;
using MQ_Sharp.ZooKeeperIntegration;

namespace MQ_Sharp.Producer.Pool;

public interface IProducerPool:  IDisposable
{
    void AddProducer(Broker broker);
    IProducerWorker? GetProducer(int brokerId);
}

public class ProducerPool(int workersPerBroker, ZooKeeperClient zkClient): IProducerPool
{
    private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ProducerPool));
    private readonly ConcurrentDictionary<int, BrokerWorkerGroup> _pool = new();
    private readonly ZooKeeperClient _zkClient = zkClient;
    private readonly int _workersPerBroker = workersPerBroker;
    
    public void AddProducer(Broker broker)
    {
        _pool.TryAdd(broker.Id, new BrokerWorkerGroup(_workersPerBroker, new ProducerWorkerConfiguration()));
    }

    public IProducerWorker? GetProducer(int brokerId)
    {
        return _pool.TryGetValue(brokerId, out var brokerWorkerGroup) ? brokerWorkerGroup.Next() : null;
    }
    
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
    private void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        foreach (var (_, brokerWorkerGroup) in _pool)
        {
            brokerWorkerGroup.Dispose();
        }
    }
}

sealed class BrokerWorkerGroup : IDisposable
{
    private readonly IProducerWorker[] _workers;
    private int _cursor;   // round-robin counter

    public BrokerWorkerGroup(int workersPerBroker, ProducerWorkerConfiguration workerConfig)
    {
        _workers = new IProducerWorker[workersPerBroker];
        for (var i = 0; i < workersPerBroker; i++)
            _workers[i] = new ProducerWorker(workerConfig);
    }

    public IProducerWorker Next()
    {
        var idx = Math.Abs(Interlocked.Increment(ref _cursor)) % _workers.Length;
        return _workers[idx];
    }

    public void Dispose()
    {
        foreach (var w in _workers)
        {
            try { w.Dispose(); } catch { /* ignore */ }
        }
    }
}