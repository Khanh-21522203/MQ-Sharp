using MQ_Sharp.Exceptions;
using MQ_Sharp.Utils;
using Org.Apache.Zookeeper.Data;
using ZooKeeperNet;

namespace MQ_Sharp.ZooKeeperIntegration;

public class ZooKeeperConnection(string servers, int sessionTimeout = ZooKeeperConnection.DefaultSessionTimeout)
    : IZooKeeperConnection
{
    private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ZooKeeperConnection));

    public const int DefaultSessionTimeout = 30000;

    private readonly Lock _syncLock = new Lock();
    private readonly Lock _shuttingDownLock = new Lock();
    private volatile bool _disposed;
    private volatile ZooKeeper _zkclient;
    public string Servers { get; private set; } = servers;
    public int SessionTimeout { get; private set; } = sessionTimeout;

    public KeeperState ClientState
    {
        get
        {
            if (_zkclient == null)
            {
                return KeeperState.Unknown;
            }
            else
            {
                var currentState = this._zkclient.State.State;
                if (currentState == ZooKeeper.States.CONNECTED.State)
                {
                    return KeeperState.SyncConnected;
                }
                if (currentState == ZooKeeper.States.CLOSED.State
                    || currentState == ZooKeeper.States.NOT_CONNECTED.State
                    || currentState == ZooKeeper.States.CONNECTING.State)
                {
                    return KeeperState.Disconnected;
                }
                else
                {
                    return KeeperState.Unknown;
                }
            }
        }
    }
    public ZooKeeper GetInternalZKClient()
    {
        return _zkclient;
    }

    public void Connect(IWatcher watcher)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        lock (_syncLock)
        {
            if (_zkclient != null)
            {
                throw new InvalidOperationException("ZooKeeper client has already been started");
            }

            try
            {
                _zkclient = new ZooKeeper(Servers, new TimeSpan(0, 0, 0, 0, SessionTimeout), watcher);
            }
            catch (IOException exc)
            {
                throw new ZooKeeperException("Unable to connect to " + Servers, exc);
            }
        }
    }

    public string Create(string path, byte[] data, CreateMode mode)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposedAndNotNull();
        var result = _zkclient.Create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
        return result;
    }

    public void Delete(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposedAndNotNull();
        _zkclient.Delete(path, -1);
    }

    public bool Exists(string path, bool watch)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposedAndNotNull();
        return _zkclient.Exists(path, true) != null;
    }

    public IEnumerable<string> GetChildren(string path, bool watch)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposedAndNotNull();
        return _zkclient.GetChildren(path, watch);
    }

    public byte[] ReadData(string path, Stat? stats, bool watch)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposedAndNotNull();
        var nodeData = _zkclient.GetData(path, watch, stats);
        return nodeData;
    }

    public void WriteData(string path, byte[] data)
    {
        WriteData(path, data, -1);
    }

    public void WriteData(string path, byte[] data, int version)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposedAndNotNull();
        _zkclient.SetData(path, data, version);
    }

    public long GetCreateTime(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposedAndNotNull();
        var stats = _zkclient.Exists(path, false);
        return stats != null ? ToUnixTimestampMillis(new DateTime(stats.Ctime)) : -1;
    }
    private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static long ToUnixTimestampMillis(DateTime time)
    {
        DateTime t = DateTime.SpecifyKind(time, DateTimeKind.Utc);
        return (long)(t.ToUniversalTime() - UnixEpoch).TotalMilliseconds;
    }
    
    private void EnsuresNotDisposedAndNotNull()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        if (_zkclient == null)
        {
            throw new ApplicationException("internal ZkClient is null.");
        }
    }
    
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        lock (_shuttingDownLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        try
        {
            if (_zkclient != null)
            {
                Logger.Debug("Closing ZooKeeper client connected to " + Servers);
                _zkclient.Dispose();
                _zkclient = null;
                Logger.Debug("ZooKeeper client connection closed");
            }
        }
        catch (Exception exc)
        {
            Logger.WarnFormat("Ignoring unexpected errors on closing {0}", exc.ToString());
        }
    }
}