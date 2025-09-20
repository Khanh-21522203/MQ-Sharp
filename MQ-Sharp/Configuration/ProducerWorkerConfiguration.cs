using MQ_Sharp.Utils;

namespace MQ_Sharp.Configuration;

public class ProducerWorkerConfiguration
{
    private const int DefaultBufferSize = 100 * 1024;

    private const int DefaultConnectTimeout = 5 * 1000;

    private const int DefaultReceiveTimeout = 5 * 1000;

    private const int DefaultSendTimeout = 5 * 1000;

    private const int DefaultReconnectInterval = 30 * 1000;

    private const int DefaultMaxMessageSize = 1024 * 1024;

    private const int DefaultCorrelationId = -1;

    private const string DefaultClientId = "";

    private const short DefaultRequiredAcks = 0;

    private const int DefaultAckTimeout = 300;

    public ProducerWorkerConfiguration()
    {
        this.BufferSize = DefaultBufferSize;
        this.ConnectTimeout = DefaultConnectTimeout;
        this.MaxMessageSize = DefaultMaxMessageSize;
        this.CorrelationId = DefaultCorrelationId;
        this.ClientId = DefaultClientId;
        this.RequiredAcks = DefaultRequiredAcks;
        this.AckTimeout = DefaultAckTimeout;
        this.ReconnectInterval = DefaultReconnectInterval;
        this.ReceiveTimeout = DefaultReceiveTimeout;
        this.SendTimeout = DefaultSendTimeout;
    }

    // public ProducerWorkerConfiguration(ProducerConfiguration config, int id, string host, int port) 
    // {
    //     Guard.NotNull(config, "config");
    //
    //     this.Host = host;
    //     this.Port = port;
    //     this.BrokerId = id;
    //     this.BufferSize = config.BufferSize;
    //     this.ConnectTimeout = config.ConnectTimeout;
    //     this.MaxMessageSize = config.MaxMessageSize;
    //     this.ReconnectInterval = config.ReconnectInterval;
    //     this.ReceiveTimeout = config.ReceiveTimeout;
    //     this.SendTimeout = config.SendTimeout;
    //     this.ClientId = config.ClientId;
    //     this.CorrelationId = DefaultCorrelationId;
    //     this.RequiredAcks = config.RequiredAcks;
    //     this.AckTimeout = config.AckTimeout;
    // }

    public int BufferSize { get; set; }

    public int ConnectTimeout { get; set; }

    public int KeepAliveTime { get; set; }

    public int KeepAliveInterval { get; set; }

    public int ReceiveTimeout { get; set; }
    
    public int SendTimeout { get; set; }

    public int MaxMessageSize { get; set; }

    public string Host { get; set; }

    public int Port { get; set; }

    public int BrokerId { get; set; }

    public int CorrelationId{ get; set; }

    public string ClientId{ get; set; }

    public short RequiredAcks{ get; set; }

    public int AckTimeout { get; set; }

    public int ReconnectInterval { get; set; }      
}