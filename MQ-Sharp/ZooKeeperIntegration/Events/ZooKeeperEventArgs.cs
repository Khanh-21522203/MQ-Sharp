namespace MQ_Sharp.ZooKeeperIntegration.Events;

public enum ZooKeeperEventTypes
{
    Unknow = 0,
    StateChanged = 1,
    SessionCreated = 2,
    ChildChanged = 3,
    DataChanged = 4,
}

public abstract class ZooKeeperEventArgs(string description) : EventArgs
{
    public override string ToString()
    {
        return "ZooKeeperEvent[" + description + "]";
    }

    public abstract ZooKeeperEventTypes Type { get; }
}