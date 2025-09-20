namespace MQ_Sharp.ZooKeeperIntegration.Events;

public class DataChangedEventItem
{
    private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(DataChangedEventItem));
    
    private ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs>? _dataChanged;
    private ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs>? _dataDeleted;
    
    public event ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> DataChanged
    {
        add
        {
            _dataChanged -= value;
            _dataChanged += value;
        }

        remove => _dataChanged -= value;
    }

    public event ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> DataDeleted
    {
        add
        {
            _dataDeleted -= value;
            _dataDeleted += value;
        }

        remove => _dataDeleted -= value;
    }
    public DataChangedEventItem(){}
    
    public DataChangedEventItem(
        ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> changedHandler,
        ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperDataChangedEventArgs> deletedHandler)
    {

        DataChanged += changedHandler;
        DataDeleted += deletedHandler;
    }
    
    public void OnDataChanged(ZooKeeperDataChangedEventArgs e)
    {
        var handlers = _dataChanged;
        handlers?.Invoke(e);
    }
    
    public void OnDataDeleted(ZooKeeperDataChangedEventArgs e)
    {
        var handlers = _dataDeleted;
        handlers?.Invoke(e);
    }
    
    public int TotalCount =>
        (_dataChanged != null ? _dataChanged.GetInvocationList().Length : 0) +
        (_dataDeleted != null ? _dataDeleted.GetInvocationList().Length : 0);
}