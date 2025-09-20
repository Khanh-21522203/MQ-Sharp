namespace MQ_Sharp.Messages;

public abstract class MessageSet
{
    public abstract IEnumerable<Message> Messages { get; protected set; }
}