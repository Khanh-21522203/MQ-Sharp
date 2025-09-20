namespace MQ_Sharp.ZooKeeperIntegration.Events;

public class ZooKeeperDataChangedEventArgs(string path) : ZooKeeperEventArgs("Data of " + path + " changed")
{
    public string Path { get; private set; } = path;

    public string Data { get; set; }
    
    public override ZooKeeperEventTypes Type => ZooKeeperEventTypes.DataChanged;

    public bool DataDeleted => string.IsNullOrEmpty(this.Data);

    public override string ToString()
    {
        return this.DataDeleted ? base.ToString().Replace("changed", "deleted") : base.ToString();
    }
}