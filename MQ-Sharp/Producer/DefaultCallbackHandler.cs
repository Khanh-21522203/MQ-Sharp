using MQ_Sharp.Messages;
using MQ_Sharp.Producer.Pool;
using MQ_Sharp.Serialization;

namespace MQ_Sharp.Producer;

public interface ICallbackHandler<K,V> : IDisposable
{
    void Handle(IEnumerable<ProducerData<K,V>> events);
}

public class DefaultCallbackHandler<TK, TV> : ICallbackHandler<TK, TV>
{
    public static log4net.ILog Logger = log4net.LogManager.GetLogger("DefaultCallbackHandler");
    private readonly IEncoder<TV> _encoder;
    
    private readonly IProducerPool _producerPool;

    public void Handle(IEnumerable<ProducerData<TK,TV>> events)
    {
        var serializedData = events.Select(e => new ProducerData<TK, Message>());

        var dispatchResult = new ProducerDispatchResult<TK>(
            new List<Exception>(),
            serializedData,
            null,
            true
        );
        
        
    }
    //
    // private ProducerDispatchResult<TK> DispatchSerializedData(IEnumerable<ProducerData<TK, Message>> messages,
    //     bool lastRetry)
    // {
    //     
    // }
    //
    // private IEnumerable<KeyValuePair<int, Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>>>>
    //     PartitionAndCollate(IEnumerable<ProducerData<TK, Message>> events)
    // {
    //     
    // }
    //
    // private Dictionary<TopicAndPartition, BufferedMessageSet> GroupMessagesToSet(
    //     Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>> eventsPerTopicAndPartition)
    // {
    //     
    // }
    //
    // private ProducerSendResult<IEnumerable<Tuple<TopicAndPartition, ProducerResponseStatus>>> Send(int brokerId,
    //     IDictionary<TopicAndPartition, BufferedMessageSet> messagesPerTopic)
    // {
    //     
    // }
    public void Dispose()
    {
        throw new NotImplementedException();
    }
}