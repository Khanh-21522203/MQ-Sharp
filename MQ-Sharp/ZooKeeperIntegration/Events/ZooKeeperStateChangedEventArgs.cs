using ZooKeeperNet;

namespace MQ_Sharp.ZooKeeperIntegration.Events;

public class ZooKeeperStateChangedEventArgs(KeeperState state) : ZooKeeperEventArgs("State changed to " + state)
{
    public KeeperState State { get; private set; } = state;

    public override ZooKeeperEventTypes Type => ZooKeeperEventTypes.StateChanged;
}