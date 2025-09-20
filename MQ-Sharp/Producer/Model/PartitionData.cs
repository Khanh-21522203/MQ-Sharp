using MQ_Sharp.Messages;

namespace MQ_Sharp.Producer.Model;

public class PartitionData
{
    public BufferedMessageSet MessageSet { get; private set; }

}