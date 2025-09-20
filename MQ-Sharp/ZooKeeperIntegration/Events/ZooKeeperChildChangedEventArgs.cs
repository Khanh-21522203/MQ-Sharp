namespace MQ_Sharp.ZooKeeperIntegration.Events;

public class ZooKeeperChildChangedEventArgs(string path) : ZooKeeperEventArgs("Children of " + path + " changed")
{
    public string Path { get; private set; } = path;

    public IEnumerable<string> Children { get; set; }
    
    public override ZooKeeperEventTypes Type => ZooKeeperEventTypes.ChildChanged;
}