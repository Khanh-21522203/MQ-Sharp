namespace MQ_Sharp.ZooKeeperIntegration.Events;

public class ChildChangedEventItem
{
    private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ChildChangedEventItem));

    private ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperChildChangedEventArgs>? _childChanged;

    public event ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperChildChangedEventArgs> ChildChanged
    {
        add
        {
            _childChanged -= value;
            _childChanged += value;
        }
        remove => _childChanged -= value;
    }
    
    public ChildChangedEventItem(){}
    
    public ChildChangedEventItem(ZooKeeperClient.ZooKeeperEventHandler<ZooKeeperChildChangedEventArgs> handler)
    {
        ChildChanged += handler;
    }
    
    public void OnChildChanged(ZooKeeperChildChangedEventArgs e)
    {
        var handlers = _childChanged;
        handlers?.Invoke(e);
    }
    
    public int Count => _childChanged != null ? _childChanged.GetInvocationList().Length : 0;
}