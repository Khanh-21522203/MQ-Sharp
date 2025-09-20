using MQ_Sharp.Utils;
using MQ_Sharp.ZooKeeperIntegration.Events;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace MQ_Sharp.ZooKeeperIntegration;

public partial class ZooKeeperClient
{
    #region Data

    public bool TryReadData<T>(string path, out T? data, Stat? stats, bool watch) where T : class
    {
        Guard.NotNullNorEmpty(path, nameof(path));
        EnsuresNotDisposed();

        try
        {
            var bytes = RetryUntilConnected(() => connection.ReadData(path, stats, watch));
            data = serializer.Deserialize(bytes) as T;
            return true;
        }
        catch
        {
            data = null;
            return false;
        }
    }
    
    public void WriteData(string path, object data, int expectedVersion = -1)
    {
        Guard.NotNullNorEmpty(path, nameof(path));
        EnsuresNotDisposed();

        var bytes = serializer.Serialize(data);
        RetryUntilConnected<object?>(() =>
        {
            connection.WriteData(path, bytes, expectedVersion);
            return null;
        });
    }
    #endregion
    
    private bool HasListeners(string path)
    {
        return (_childChangedHandlers.TryGetValue(path, out var ch) && ch.Count > 0)
               || (_dataChangedHandlers.TryGetValue(path, out var dh) && dh.TotalCount > 0);
    }
}