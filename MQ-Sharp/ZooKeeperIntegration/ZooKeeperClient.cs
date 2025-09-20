using System.Collections.Concurrent;
using System.Diagnostics;
using MQ_Sharp.Exceptions;
using MQ_Sharp.Utils;
using MQ_Sharp.ZooKeeperIntegration.Events;
using ZooKeeperNet;

namespace MQ_Sharp.ZooKeeperIntegration;

public partial class ZooKeeperClient: IZooKeeperClient
{
    private readonly ReaderWriterLockSlim slimLock = new ReaderWriterLockSlim();
    private DateTime? idleTime;
    public int? IdleTime => idleTime.HasValue ? Convert.ToInt32((DateTime.Now - idleTime.Value).TotalMilliseconds) : (int?)null;
    public ReaderWriterLockSlim SlimLock => slimLock;

    private Thread eventWorker;
    private KeeperState currentState;
    private volatile IZooKeeperConnection connection;
    private readonly ConcurrentQueue<ZooKeeperEventArgs> eventsQueue = new ConcurrentQueue<ZooKeeperEventArgs>();
    private readonly int connectionTimeout;


    #region Lifecycle
    public void Connect()
    {
        EnsuresNotDisposed();
        var started = false;
        try
        {
            Logger.Info("Enter connect ...");
            shutdownTriggered = false;
            eventWorker = new Thread(RunEventWorker) { IsBackground = true };
            eventWorker.Name = "ZooKeeperkWatcher-EventThread-" + eventWorker.ManagedThreadId + "-" + connection.Servers;
            eventWorker.Start();
            Logger.Info("Will connect ...");
            connection.Connect(this);
            Logger.Info("Finish connect ...");
            Logger.Debug("Awaiting connection to Zookeeper server");
            if (!WaitUntilConnected(connectionTimeout))
            {
                throw new ZooKeeperException(
                    "Unable to connect to zookeeper server within timeout: " + connectionTimeout);
            }
            started = true;
            Logger.Debug("Connection to Zookeeper server established");
        }
        catch (ThreadInterruptedException)
        {
            throw new InvalidOperationException(
                "Not connected with zookeeper server yet. Current state is " + connection.ClientState);
        }
        finally
        {
            if (!started)
            {
                Disconnect();
            }
        }
    }

    public void Disconnect()
    {
        Logger.Debug("Closing ZooKeeperClient");
        shutdownTriggered = true;
        eventWorker.Interrupt();
        try
        {
            if (eventWorker.ThreadState != System.Threading.ThreadState.Unstarted)
            {
                eventWorker.Join(5000);
            }
        }
        catch (ThreadStateException)
        {
            // exception means worker thread was interrupted even before we started waiting.
        }

        if (connection != null)
        {
            connection.Dispose();
        }

        connection = null;
    }

    public void Reconnect(string servers, int connectionTimeout)
    {
        EnsuresNotDisposed();
        Logger.Debug("Reconnecting");
        connection.Dispose();
        connection = new ZooKeeperConnection(servers, connectionTimeout);
        connection.Connect(this);
        Logger.Debug("Reconnected");
    }

    public KeeperState GetClientState()
    {
        return connection == null ? KeeperState.Disconnected : connection.ClientState;
    }
    #endregion
    
    #region Connection gating
    public bool WaitUntilConnected(int connectionTimeout)
    {
        Guard.Greater(connectionTimeout, 0, "connectionTimeout");

        EnsuresNotDisposed();
        if (eventWorker != null && eventWorker == Thread.CurrentThread)
        {
            throw new InvalidOperationException("Must not be done in the ZooKeeper event thread.");
        }

        Logger.Debug("Waiting for keeper state: " + KeeperState.SyncConnected + " time out: " + connectionTimeout);//.SyncConnected);

        bool stillWaiting = true;
        lock (stateChangedLock)
        {
            currentState = connection.ClientState;
            if (currentState == KeeperState.SyncConnected)
                return true;
            Logger.DebugFormat("Current state:{0} in the lib:{1}", currentState, connection.GetInternalZKClient().State);

            Stopwatch stopWatch = Stopwatch.StartNew();
            while (currentState != KeeperState.SyncConnected)
            {
                if (!stillWaiting)
                {
                    return false;

                }

                stillWaiting = Monitor.Wait(stateChangedLock, connectionTimeout);
                Logger.DebugFormat("Current state:{0} in the lib:{1}", currentState, connection.GetInternalZKClient().State);
                currentState = connection.ClientState;
                if (currentState == KeeperState.SyncConnected)
                    return true;
                if (stopWatch.Elapsed.TotalMilliseconds > connectionTimeout)
                    break;
                Thread.Sleep(1000);
            }

            Logger.DebugFormat("Current state:{0} in the lib:{1}", currentState, connection.GetInternalZKClient().State);
            currentState = connection.ClientState;
            if (this.currentState == KeeperState.SyncConnected)
                return true;
            else
                return false;
        }
    }

    public T RetryUntilConnected<T>(Func<T> callback)
    {
        Guard.NotNull(callback, "callback");

        EnsuresNotDisposed();
        if (zooKeeperEventWorker != null && zooKeeperEventWorker == Thread.CurrentThread)
        {
            throw new InvalidOperationException("Must not be done in the zookeeper event thread");
        }

        var connectionWatch = Stopwatch.StartNew();
        long maxWait = connectionTimeout * 4L;
        while (connectionWatch.ElapsedMilliseconds < maxWait)
        {
            try
            {
                return callback();
            }
            catch (KeeperException e)
            {
                if (e.ErrorCode == KeeperException.Code.CONNECTIONLOSS)// KeeperException.ConnectionLossException)
                {
                    Thread.Yield();//TODO: is it better than Sleep(1) ?
                    WaitUntilConnected(connection.SessionTimeout);
                }
                else if (e.ErrorCode == KeeperException.Code.SESSIONEXPIRED)//  catch (KeeperException.SessionExpiredException)
                {
                    Thread.Yield();
                    WaitUntilConnected(connection.SessionTimeout);
                }
                else
                    throw;
            }
        }
        connectionWatch.Stop();

        // exceeded time out, any good callers will handle the exception properly for their situation.
        throw KeeperException.Create(KeeperException.Code.OPERATIONTIMEOUT);
    }

    public T RetryUntilConnected<T>(Func<T> callback, TimeSpan timeout)
    {
        Guard.NotNull(callback, "callback");

        EnsuresNotDisposed();

        var currentTime = DateTime.UtcNow;
        while (true)
        {
            try
            {
                return callback();
            }
            catch (KeeperException e)
            {
                if (e.ErrorCode == KeeperException.Code.CONNECTIONLOSS)
                {
                    if (OperationTimedOut(currentTime, timeout))
                    {
                        throw;
                    }

                    Thread.Yield();
                }
                else if (e.ErrorCode == KeeperException.Code.SESSIONEXPIRED)
                {
                    if (OperationTimedOut(currentTime, timeout))
                    {
                        throw;
                    }

                    Thread.Yield();
                }
                else
                    throw;
            }
        }
    }
    #endregion
    
    private bool OperationTimedOut(DateTime currentTime, TimeSpan timeout)
    {
        return DateTime.UtcNow.Subtract(currentTime)
            .CompareTo(timeout) < 0;
    }
    
    #region Existence

    public bool Exists(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
        var hasListeners = HasListeners(path);
        return Exists(path, hasListeners);
    }

    public bool Exists(string path, bool watch)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
        return RetryUntilConnected(() => connection.Exists(path, watch));
    }

    public IEnumerable<string> GetChildren(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        this.EnsuresNotDisposed();
        var hasListeners = this.HasListeners(path);
        return this.GetChildren(path, hasListeners);
    }

    public IEnumerable<string> GetChildren(string path, bool watch)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
        return RetryUntilConnected(() => connection.GetChildren(path, watch));
    }

    public int CountChildren(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
        try
        {
            return GetChildren(path).Count();
        }
        catch (KeeperException e)// KeeperException.NoNodeException)
        {
            if (e.ErrorCode == KeeperException.Code.NONODE)
                return 0;
            else
                throw;
        }
    }
    public void MakeSurePersistentPathExists(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
        if (!Exists(path))
        {
            CreatePersistent(path, true);
        }
    }

    public IEnumerable<string> GetChildrenParentMayNotExist(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
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
    }
    #endregion

    private void RunEventWorker()
    {
        Logger.Debug("Starting ZooKeeper watcher event thread");
        try
        {
            PoolEventsQueue();
        }
        catch (ThreadInterruptedException)
        {
            Logger.Debug("Terminate ZooKeeper watcher event thread");
        }
    }
    private void PoolEventsQueue()
    {
        while (true)
        {
            while (!eventsQueue.IsEmpty)
            {
                Dequeue();
            }

            lock (somethingChanged)
            {
                Logger.Debug("Awaiting events ...");
                idleTime = DateTime.Now;
                Monitor.Wait(somethingChanged);
                idleTime = null;
            }
        }
    }
    
    private void Dequeue()
    {
        try
        {
            ZooKeeperEventArgs e;
            var success = this.eventsQueue.TryDequeue(out e);
            if (success)
            {
                if (e != null)
                {
                    Logger.Debug("Event dequeued: " + e);
                    switch (e.Type)
                    {
                        case ZooKeeperEventTypes.StateChanged:
                            this.OnStateChanged((ZooKeeperStateChangedEventArgs)e);
                            break;
                        case ZooKeeperEventTypes.SessionCreated:
                            this.OnSessionCreated((ZooKeeperSessionCreatedEventArgs)e);
                            break;
                        case ZooKeeperEventTypes.ChildChanged:
                            this.OnChildChanged((ZooKeeperChildChangedEventArgs)e);
                            break;
                        case ZooKeeperEventTypes.DataChanged:
                            this.OnDataChanged((ZooKeeperDataChangedEventArgs)e);
                            break;
                        default:
                            throw new InvalidOperationException("Not supported event type");
                    }
                }
            }
        }
        catch (Exception exc)
        {
            Logger.WarnFormat("Error handling event  {0}", exc.ToString());
        }
    }
    
    private void OnStateChanged(ZooKeeperStateChangedEventArgs e)
    {
        try
        {
            var handlers = _stateChangedHandlers;
            if (handlers == null)
            {
                return;
            }

            foreach (var handler in handlers.GetInvocationList())
            {
                Logger.Debug(e + " sent to " + handler.Target);
            }

            handlers(e);
        }
        catch (Exception exc)
        {
            Logger.ErrorFormat("Failed to handle state changed event.  {0}", exc.ToString());
        }
    }
    
    private void OnSessionCreated(ZooKeeperSessionCreatedEventArgs e)
    {
        var handlers = _sessionCreatedHandlers;
        if (handlers == null)
        {
            return;
        }

        foreach (var handler in handlers.GetInvocationList())
        {
            Logger.Debug(e + " sent to " + handler.Target);
        }

        handlers(e);
    }
    
    private void OnChildChanged(ZooKeeperChildChangedEventArgs e)
    {
        ChildChangedEventItem handlers;
        _childChangedHandlers.TryGetValue(e.Path, out handlers);
        if (handlers == null || handlers.Count == 0)
        {
            return;
        }

        Exists(e.Path);
        try
        {
            IEnumerable<string> children = GetChildren(e.Path);
            e.Children = children;
        }
        catch (KeeperException ex)// KeeperException.NoNodeException)
        {
            if (ex.ErrorCode == KeeperException.Code.NONODE)
            {

            }
            else
                throw;
        }

        handlers.OnChildChanged(e);
    }
    
    private void OnDataChanged(ZooKeeperDataChangedEventArgs e)
    {
        DataChangedEventItem handlers;
        _dataChangedHandlers.TryGetValue(e.Path, out handlers);
        if (handlers == null || handlers.TotalCount == 0)
        {
            return;
        }

        try
        {
            Exists(e.Path, true);
            var data = ReadData<string>(e.Path, null, true);
            e.Data = data;
            handlers.OnDataChanged(e);
        }
        catch (KeeperException ex)
        {
            if (ex.ErrorCode == KeeperException.Code.NONODE)
                handlers.OnDataDeleted(e);
            else
                throw;
        }
    }
    
    public void Dispose()
    {
        slimLock.EnterWriteLock();
        try
        {
            if (disposed)
            {
                return;
            }

            lock (shuttingDownLock)
            {
                if (disposed)
                {
                    return;
                }

                disposed = true;
            }

            try
            {
                Disconnect();
            }
            catch (ThreadInterruptedException)
            {
            }
            catch (Exception exc)
            {
                Logger.Debug("Ignoring unexpected errors on closing ZooKeeperClient", exc);
            }

            Logger.Debug("Closing ZooKeeperClient... done");
        }
        finally
        {
            slimLock.ExitWriteLock();
        }
    }
}