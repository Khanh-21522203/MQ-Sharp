using System.Runtime.Serialization;

namespace MQ_Sharp.Exceptions;

public class MessageSizeTooLargeException: Exception
{
    public MessageSizeTooLargeException() : base() {}
    public MessageSizeTooLargeException(SerializationInfo info, StreamingContext context) : base(info, context) { }
}