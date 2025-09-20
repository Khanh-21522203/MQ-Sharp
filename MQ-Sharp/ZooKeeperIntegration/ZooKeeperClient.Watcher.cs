using System.Collections.Concurrent;
using MQ_Sharp.Utils;
using MQ_Sharp.ZooKeeperIntegration.Events;
using MQ_Sharp.ZooKeeperIntegration.Listener;
using ZooKeeperNet;

namespace MQ_Sharp.ZooKeeperIntegration;

public partial class ZooKeeperClient
{
    private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ZooKeeperClient));
    
    public delegate void ZooKeeperEventHandler<T>(T args) where T : ZooKeeperEventArgs;
    
    private readonly ConcurrentQueue<ZooKeeperEventArgs> _eventsQueue = new ConcurrentQueue<ZooKeeperEventArgs>();
    private readonly ConcurrentDictionary<string, ChildChangedEventItem> _childChangedHandlers = new ConcurrentDictionary<string, ChildChangedEventItem>();
    private readonly ConcurrentDictionary<string, DataChangedEventItem> _dataChangedHandlers = new ConcurrentDictionary<string, DataChangedEventItem>();
    private ZooKeeperEventHandler<ZooKeeperStateChangedEventArgs> _stateChangedHandlers;
    private ZooKeeperEventHandler<ZooKeeperSessionCreatedEventArgs> _sessionCreatedHandlers;
    
    private Thread zooKeeperEventWorker;
    private bool shutdownTriggered;
    private readonly object stateChangedLock = new object();
    private readonly object znodeChangedLock = new object();
    private readonly object somethingChanged = new object();
    private readonly object shuttingDownLock = new object();
    private readonly object eventLock = new object();

    
    private volatile bool disposed;

    
    /// <summary>
    /// Occurs when ZooKeeper connection state changes
    /// </summary>
    public event ZooKeeperEventHandler<ZooKeeperStateChangedEventArgs> StateChanged
    {
        add
        {
            EnsuresNotDisposed();
            lock (eventLock)
            {
                _stateChangedHandlers -= value;
                _stateChangedHandlers += value;
            }
        }
        remove
        {
            EnsuresNotDisposed();
            lock (eventLock)
            {
                _stateChangedHandlers -= value;
            }
        }
    }

    /// <summary>
    /// Occurs when ZooKeeper session re-creates
    /// </summary>
    public event ZooKeeperEventHandler<ZooKeeperSessionCreatedEventArgs> SessionCreated
    {
        add
        {
            EnsuresNotDisposed();
            lock (eventLock)
            {
                _sessionCreatedHandlers -= value;
                _sessionCreatedHandlers += value;
            }
        }
        remove
        {
            EnsuresNotDisposed();
            lock (eventLock)
            {
                _sessionCreatedHandlers -= value;
            }
        }
    }
    
    public void Process(WatchedEvent e)
    {
        Logger.DebugFormat("Process called by handler. Received event, e.EventType:{0}  e.State: {1} e.Path :{2} ", e.Type, e.State, e.Path);
        zooKeeperEventWorker = Thread.CurrentThread;
        if (shutdownTriggered)
        {
            Logger.DebugFormat("Shutdown triggered. Ignoring event. Type: {0}, Path: {1}, State: {2} ", e.Type, (e.Path ?? "null"), e.State);
            return;
        }
        
        try
        {
            EnsuresNotDisposed();
            var stateChanged = string.IsNullOrEmpty(e.Path);
            var znodeChanged = !string.IsNullOrEmpty(e.Path);
            var dataChanged =
                e.Type is EventType.NodeDataChanged or EventType.NodeDeleted 
                    or EventType.NodeCreated or EventType.NodeChildrenChanged;

            Logger.DebugFormat("Process called by handler. stateChanged:{0} znodeChanged:{1}  dataChanged:{2} ", stateChanged, znodeChanged, dataChanged);

            lock (somethingChanged)
            {
                try
                {
                    if (stateChanged)
                    {
                        ProcessStateChange(e);
                    }

                    if (dataChanged)
                    {
                        ProcessDataOrChildChange(e);
                    }
                }
                finally
                {
                    if (stateChanged)
                    {
                        lock (stateChangedLock)
                        {
                            Monitor.PulseAll(stateChangedLock);
                        }

                        if (e.State == KeeperState.Expired)
                        {
                            lock (znodeChangedLock)
                            {
                                Monitor.PulseAll(znodeChangedLock);
                            }

                            foreach (var path in _childChangedHandlers.Keys)
                            {
                                Enqueue(new ZooKeeperChildChangedEventArgs(path));
                            }

                            foreach (var path in _dataChangedHandlers.Keys)
                            {
                                Enqueue(new ZooKeeperDataChangedEventArgs(path));
                            }
                        }
                    }

                    if (znodeChanged)
                    {
                        lock (znodeChangedLock)
                        {
                            Monitor.PulseAll(znodeChangedLock);
                        }
                    }
                }

                Monitor.PulseAll(somethingChanged);
            }
        }
        catch (Exception ex)
        {
            Logger.Error("Error occurred while processing event: " + ex.ToString());
        }
    }

    
    #region Subscriptions

    public void Subscribe(IZooKeeperStateListener listener)
    {
        Guard.NotNull(listener, "listener");

        this.EnsuresNotDisposed();
        this.StateChanged += listener.HandleStateChanged;
        this.SessionCreated += listener.HandleSessionCreated;
        Logger.Debug("Subscribed state changes handler " + listener.GetType().Name);
    }

    public void Unsubscribe(IZooKeeperStateListener listener)   
    {
        Guard.NotNull(listener, "listener");

        this.EnsuresNotDisposed();
        this.StateChanged -= listener.HandleStateChanged;
        this.SessionCreated -= listener.HandleSessionCreated;
        Logger.Debug("Unsubscribed state changes handler " + listener.GetType().Name);
    }

    public void Subscribe(string path, IZooKeeperChildListener listener)
    {
        Guard.NotNullNorEmpty(path, "path");
        Guard.NotNull(listener, "listener");

        EnsuresNotDisposed();
        _childChangedHandlers.AddOrUpdate(
            path,
            new ChildChangedEventItem(listener.HandleChildChange),
            (key, oldValue) => { oldValue.ChildChanged += listener.HandleChildChange; return oldValue; });
        WatchForChilds(path);
        Logger.Debug("Subscribed child changes handler " + listener.GetType().Name + " for path: " + path);
    }

    public void Unsubscribe(string path, IZooKeeperChildListener listener)
    {
        Guard.NotNullNorEmpty(path, "path");
        Guard.NotNull(listener, "listener");

        EnsuresNotDisposed();
        _childChangedHandlers.AddOrUpdate(
            path,
            new ChildChangedEventItem(),
            (key, oldValue) => { oldValue.ChildChanged -= listener.HandleChildChange; return oldValue; });
        Logger.Debug("Unsubscribed child changes handler " + listener.GetType().Name + " for path: " + path);
    }

    public void Subscribe(string path, IZooKeeperDataListener listener)
    {
        Guard.NotNullNorEmpty(path, "path");
        Guard.NotNull(listener, "listener");

        EnsuresNotDisposed();
        _dataChangedHandlers.AddOrUpdate(
            path,
            new DataChangedEventItem(listener.HandleDataChange, listener.HandleDataDelete),
            (key, oldValue) =>
            {
                oldValue.DataChanged += listener.HandleDataChange;
                oldValue.DataDeleted += listener.HandleDataDelete;
                return oldValue;
            });
        WatchForData(path);
        Logger.Debug("Subscribed data changes handler " + listener.GetType().Name + " for path: " + path);
    }

    public void Unsubscribe(string path, IZooKeeperDataListener listener)
    {
        Guard.NotNullNorEmpty(path, "path");
        Guard.NotNull(listener, "listener");

        EnsuresNotDisposed();
        _dataChangedHandlers.AddOrUpdate(
            path,
            new DataChangedEventItem(),
            (key, oldValue) =>
            {
                oldValue.DataChanged -= listener.HandleDataChange;
                oldValue.DataDeleted -= listener.HandleDataDelete;
                return oldValue;
            });
        Logger.Debug("Unsubscribed data changes handler " + listener.GetType().Name + " for path: " + path);
    }

    public void UnsubscribeAll()
    {
        EnsuresNotDisposed();
        lock (eventLock)
        {
            _stateChangedHandlers = null!;
            _sessionCreatedHandlers = null!;
            _childChangedHandlers.Clear();
            _dataChangedHandlers.Clear();
        }

        Logger.Debug("Unsubscribed all handlers");
    }
    #endregion
    
    #region Watchers

    public IEnumerable<string> WatchForChilds(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
        if (zooKeeperEventWorker != null && Thread.CurrentThread == zooKeeperEventWorker)
        {
            throw new InvalidOperationException("Must not be done in the zookeeper event thread.");
        }

        return RetryUntilConnected(
            () =>
            {
                Exists(path);
                try
                {
                    return GetChildren(path);
                }
                catch (KeeperException e)
                {
                    if (e.ErrorCode == KeeperException.Code.NONODE)
                        return null;
                    else
                        throw;
                }
            })!;
    }

    public void WatchForData(string path)
    {
        Guard.NotNullNorEmpty(path, "path");    

        EnsuresNotDisposed();
        RetryUntilConnected(() => Exists(path, true));
    }
    #endregion
    
    private void ProcessDataOrChildChange(WatchedEvent watchedEvent)
    {
        throw new NotImplementedException();
    }

    private void ProcessStateChange(WatchedEvent watchedEvent)
    {
        throw new NotImplementedException();
    }

    private void Enqueue(ZooKeeperEventArgs e)
    {
        Logger.Debug("New event queued: " + e);
        _eventsQueue.Enqueue(e);
    }
    
    private void EnsuresNotDisposed()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
    }
}