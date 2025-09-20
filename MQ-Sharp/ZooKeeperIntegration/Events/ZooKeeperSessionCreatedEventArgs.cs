namespace MQ_Sharp.ZooKeeperIntegration.Events;

public class ZooKeeperSessionCreatedEventArgs: ZooKeeperEventArgs
{
    public new static readonly ZooKeeperSessionCreatedEventArgs Empty = new ZooKeeperSessionCreatedEventArgs();
    
    protected ZooKeeperSessionCreatedEventArgs()
        : base("New session created")
    {
    }
    
    public override ZooKeeperEventTypes Type => ZooKeeperEventTypes.SessionCreated;
}