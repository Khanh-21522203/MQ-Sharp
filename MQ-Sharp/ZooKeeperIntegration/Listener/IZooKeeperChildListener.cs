using MQ_Sharp.ZooKeeperIntegration.Events;

namespace MQ_Sharp.ZooKeeperIntegration.Listener;

public interface IZooKeeperChildListener
{
    void HandleChildChange(ZooKeeperChildChangedEventArgs args);
    void ResetState();
}