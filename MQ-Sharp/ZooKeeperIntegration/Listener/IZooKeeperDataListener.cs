using MQ_Sharp.ZooKeeperIntegration.Events;

namespace MQ_Sharp.ZooKeeperIntegration.Listener;

public interface IZooKeeperDataListener
{
    void HandleDataChange(ZooKeeperDataChangedEventArgs args);
    void HandleDataDelete(ZooKeeperDataChangedEventArgs args);
}