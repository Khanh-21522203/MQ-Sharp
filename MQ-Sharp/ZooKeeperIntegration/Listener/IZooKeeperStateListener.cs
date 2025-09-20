using MQ_Sharp.ZooKeeperIntegration.Events;

namespace MQ_Sharp.ZooKeeperIntegration.Listener;

public interface IZooKeeperStateListener
{
    void HandleStateChanged(ZooKeeperStateChangedEventArgs args);
    void HandleSessionCreated(ZooKeeperSessionCreatedEventArgs args);
}