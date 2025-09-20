using MQ_Sharp.Cluster;
using MQ_Sharp.Request;
using MQ_Sharp.Response;
using MQ_Sharp.Utils;

namespace MQ_Sharp.Services;

public interface IMetaDataManager
{
    // void UpdateInfo(short versionId, int correlationId, string clientId, string topic);
    List<Partition> GetBrokerPartitionInfo(string topic);
    IDictionary<int, Broker> GetBrokerPartitionLeaders(string topic);
}

public class MetaDataManager: IMetaDataManager
{
    public static log4net.ILog Logger = log4net.LogManager.GetLogger("MetaDataManager");

    private readonly Dictionary<string, MetaDataResponse> _topicMeta = new();
    private readonly Dictionary<string, DateTime> _lastUpdateUtc = new();
    private readonly ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);


    public List<Partition> GetBrokerPartitionInfo(string topic)
    {
        _rwLock.EnterReadLock();
        try
        {
            if (!_topicMeta.TryGetValue(topic, out var md))
                throw new KafkaException($"There is no metadata for topic {topic}");

            if (md.Error != ErrorMapping.NoError)
                throw new KafkaException($"The metadata status for topic {topic} is abnormal", md.Error);

            // Tạo List<Partition> từ metadata ngay tại đây (không cần cache phụ)
            return md.PartitionsMetadata
                .Select(pm =>
                {
                    var p = new Partition(topic, pm.PartitionId);
                    if (pm.Leader != null)
                        p.Leader = new Replica(pm.Leader.Id, topic);
                    return p;
                })
                .OrderBy(p => p.PartId)
                .ToList();
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public IDictionary<int, Broker> GetBrokerPartitionLeaders(string topic)
    {
        _rwLock.EnterReadLock();
        try
        {
            if (!_topicMeta.TryGetValue(topic, out var md))
                throw new KafkaException($"There is no metadata for topic {topic}");

            if (md.Error != ErrorMapping.NoError)
                throw new KafkaException($"The metadata status for topic {topic} is abnormal", md.Error);

            var dict = new Dictionary<int, Broker>(capacity: md.PartitionsMetadata.Count);
            foreach (var p in md.PartitionsMetadata)
            {
                if (p.Leader != null && !dict.ContainsKey(p.PartitionId))
                {
                    dict.Add(p.PartitionId, p.Leader);
                    Logger.DebugFormat("Topic {0} partition {1} has leader {2}", topic, p.PartitionId, p.Leader.Id);
                }
                else if (p.Leader == null)
                {
                    Logger.DebugFormat("Topic {0} partition {1} does not have a leader yet", topic, p.PartitionId);
                }
            }
            return dict;
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }
    
    public void UpdateInfo(short versionId, int correlationId, string clientId, string topic)
    {
        Logger.InfoFormat("Will update metadata for topic:{0}", topic);
        Guard.NotNullNorEmpty(topic, "topic");

        var shuffled = _syncProducerPool.GetShuffledProducers();
        var i = 0; var updated = false;

        while (i < shuffled.Count && !updated)
        {
            var producer = shuffled[i++];
            try
            {
                var req = TopicMetadataRequest.Create(new List<string> { topic }, versionId, correlationId, clientId);
                var list = producer.Send(req);
                var md = list.FirstOrDefault();
                if (md == null) continue;

                if (md.Error != ErrorMapping.NoError)
                {
                    Logger.WarnFormat("Get metadata of topic {0} from {1}({2}) got error: {3}",
                        topic, producer.Config.BrokerId, producer.Config.Host, md.Error);
                    continue;
                }

                // ghi dưới write-lock
                _rwLock.EnterWriteLock();
                try
                {
                    _topicMeta[topic] = md;
                    _lastUpdateUtc[topic] = DateTime.UtcNow;
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }

                Logger.InfoFormat("Updated metadata topic {0}", topic);
                updated = true;

                if (_zkClient != null)
                {
                    var zkInfo = ZkUtils.GetTopicMetadataInzookeeper(_zkClient, topic);
                    if (zkInfo != null && zkInfo.Any())
                    {
                        if (md.PartitionsMetadata.Count != zkInfo.Count)
                        {
                            Logger.ErrorFormat("NOT all partition has metadata. ZK:{0} client:{1}",
                                zkInfo.Count, md.PartitionsMetadata.Count);
                            throw new UnavailableProducerException(
                                $"Please ensure every partition has one broker running. ZK:{zkInfo.Count} client:{md.PartitionsMetadata.Count}");
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Logger.ErrorFormat("Get metadata of topic {0} from {1}({2}) error: {3}",
                    topic, producer.Config.BrokerId, producer.Config.Host, e.FormatException());
            }
        }
    }
    
    private void UpdateInfoInternal(short versionId, int correlationId, string clientId, string topic)
    {
        Logger.DebugFormat("Check need update metadata for topic {0}", topic);

        var needUpdate = false;
        DateTime last;
        _rwLock.EnterReadLock();
        try
        {
            if (!_topicMeta.ContainsKey(topic) || _topicMeta[topic].Error != ErrorMapping.NoError)
            {
                needUpdate = true;
            }
            else if (_lastUpdateUtc.TryGetValue(topic, out last))
            {
                if (DateTime.UtcNow - last > _refreshInterval)
                    needUpdate = true;
            }
            else
            {
                needUpdate = true;
            }
        }
        finally
        {
            _rwLock.ExitReadLock();
        }

        if (!needUpdate) return;

        lock (_updateLock)
        {
            // double-check sau khi giành lock
            _rwLock.EnterReadLock();
            try
            {
                if (_topicMeta.TryGetValue(topic, out var md) &&
                    md.Error == ErrorMapping.NoError &&
                    _lastUpdateUtc.TryGetValue(topic, out last) &&
                    DateTime.UtcNow - last <= _refreshInterval)
                {
                    return; // đã có thread khác cập nhật
                }
            }
            finally
            {
                _rwLock.ExitReadLock();
            }

            UpdateInfo(versionId, correlationId, clientId, topic);

            _rwLock.EnterReadLock();
            try
            {
                if (!_topicMeta.ContainsKey(topic))
                    throw new KafkaException($"Failed to fetch topic metadata for topic: {topic}");
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
    }
}