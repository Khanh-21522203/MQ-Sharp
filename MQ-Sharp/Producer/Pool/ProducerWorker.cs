using MQ_Sharp.Configuration;
using MQ_Sharp.Exceptions;
using MQ_Sharp.Messages;
using MQ_Sharp.Request;
using MQ_Sharp.Response;

namespace MQ_Sharp.Producer.Pool;

public interface IProducerWorker: IDisposable
{
    ProduceResponse Send(ProduceRequest request);
    IEnumerable<MetaDataResponse> Send(TopicMetadataRequest request);
    ProducerWorkerConfiguration Config { get; }
}

public sealed class ProducerWorker(ProducerWorkerConfiguration configuration, IKafkaConnection connection)
    : IProducerWorker
{
    private bool _disposed;
    public ProducerWorkerConfiguration Config { get; private set; } = configuration;

    public ProducerWorker(ProducerWorkerConfiguration configuration) : this(configuration, new KafkaConnection(
        configuration.Host,
        configuration.Port,
        configuration.BufferSize,
        configuration.SendTimeout,
        configuration.ReceiveTimeout,
        configuration.ReconnectInterval
    ))
    {
    }

    public ProduceResponse Send(ProduceRequest request)
    {
        EnsuresNotDisposed();

        foreach (var topicData in request.Data)
        {
            foreach (var partitionData in topicData.PartitionData)
            {
                VerifyMessageSize(partitionData.MessageSet.Messages);
            }
        }

        return connection.Send(request);
    }

    public IEnumerable<MetaDataResponse> Send(TopicMetadataRequest request)
    {
        return connection.Send(request);
    }
    
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        connection.Dispose();
    }
    
    private void EnsuresNotDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private void VerifyMessageSize(IEnumerable<Message> messages)
    {
        if (messages.Any(message => message.PayloadSize > Config.MaxMessageSize))
        {
            throw new MessageSizeTooLargeException();
        }
    }
}