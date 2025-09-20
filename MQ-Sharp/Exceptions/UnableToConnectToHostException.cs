using System.Runtime.Serialization;

namespace MQ_Sharp.Exceptions;

public class UnableToConnectToHostException: Exception
{
    public UnableToConnectToHostException(string server, int port)
        : base($"Unable to connect to {server}:{port}")
    {
    }

    public UnableToConnectToHostException(string server, int port, Exception innterException)
        : base($"Unable to connect to {server}:{port}", innterException)
    {
    }

    public UnableToConnectToHostException() : base()
    {
    }
    public UnableToConnectToHostException(string message, Exception innterException)
        : base(message, innterException)
    {
    }
    public UnableToConnectToHostException(SerializationInfo info, StreamingContext context) : base(info, context) { }
}