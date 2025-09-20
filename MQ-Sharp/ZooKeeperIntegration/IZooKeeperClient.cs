using MQ_Sharp.ZooKeeperIntegration.Listener;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace MQ_Sharp.ZooKeeperIntegration;

public interface IZooKeeperClient: IWatcher, IDisposable
{
    int? IdleTime { get; }
    ReaderWriterLockSlim SlimLock { get; }
    
    #region Lifecycle
    void Connect();
    void Disconnect();
    void Reconnect(string servers, int connectionTimeout);
    KeeperState GetClientState();
    #endregion
    
    #region Connection gating
    bool WaitUntilConnected(int connectionTimeout);
    T RetryUntilConnected<T>(Func<T> callback);
    T RetryUntilConnected<T>(Func<T> callback, TimeSpan timeout);
    #endregion
    
    #region Subscriptions
    void Subscribe(IZooKeeperStateListener listener);
    void Unsubscribe(IZooKeeperStateListener listener);
    void Subscribe(string path, IZooKeeperChildListener listener);
    void Unsubscribe(string path, IZooKeeperChildListener listener);
    void Subscribe(string path, IZooKeeperDataListener listener);
    void Unsubscribe(string path, IZooKeeperDataListener listener);
    void UnsubscribeAll();
    #endregion
    
    #region Watchers
    IEnumerable<string> WatchForChilds(string path);
    void WatchForData(string path);
    #endregion

    #region Existence
    bool Exists(string path);
    bool Exists(string path, bool watch);
    IEnumerable<string> GetChildren(string path);
    IEnumerable<string> GetChildren(string path, bool watch);
    int CountChildren(string path);
    void MakeSurePersistentPathExists(string path);
    IEnumerable<string> GetChildrenParentMayNotExist(string path);
    #endregion

    #region Data

    bool TryReadData<T>(string path, out T? data, Stat? stats, bool watch) where T : class;
    void WriteData(string path, object data, int expectedVersion);
    #endregion

    #region Nodes
    void CreatePersistent(string path, bool createParents);
    void CreatePersistent(string path);
    void CreatePersistent(string path, object data);
    string CreatePersistentSequential(string path, object data);
    void CreateEphemeral(string path);
    void CreateEphemeral(string path, object data);
    string CreateEphemeralSequential(string path, object data);
    #endregion

    #region Delete
    bool Delete(string path);
    bool DeleteRecursive(string path);
    #endregion
    


}