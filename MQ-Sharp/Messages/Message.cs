namespace MQ_Sharp.Messages;

public class Message
{
    public byte[] Payload { get; private set; }
    public int PayloadSize => this.Payload.Length;
}