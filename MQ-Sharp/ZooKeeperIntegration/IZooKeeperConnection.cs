using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace MQ_Sharp.ZooKeeperIntegration;

public interface IZooKeeperConnection: IDisposable
{
    KeeperState ClientState { get; }
    string Servers { get; }
    int SessionTimeout { get; }

    ZooKeeper GetInternalZKClient();
    
    void Connect(IWatcher watcher);

    string Create(string path, byte[] data, CreateMode mode);
    void Delete(string path);
    
    bool Exists(string path, bool watch);
    IEnumerable<string> GetChildren(string path, bool watch);
    
    byte[] ReadData(string path, Stat? stats, bool watch);
    void WriteData(string path, byte[] data);
    void WriteData(string path, byte[] data, int version);
    
    long GetCreateTime(string path);
}