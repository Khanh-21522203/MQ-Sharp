namespace MQ_Sharp.Messages;

public class BufferedMessageSet: MessageSet
{
    public override sealed IEnumerable<Message> Messages { get; protected set; }

}