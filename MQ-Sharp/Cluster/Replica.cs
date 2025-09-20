namespace MQ_Sharp.Cluster;

public class Replica(int brokerId, string topic)
{
    public int BrokerId { get; private set; } = brokerId;
    public string Topic { get; private set; } = topic;
}