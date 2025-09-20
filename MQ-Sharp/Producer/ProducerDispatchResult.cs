using MQ_Sharp.Messages;

namespace MQ_Sharp.Producer;

public class ProducerDispatchResult<K>(
    IEnumerable<Exception> exceptions,
    IEnumerable<ProducerData<K, Message>> failedProducerDatas,
    List<Tuple<int, TopicAndPartition, ProducerResponseStatus>> failedDetail,
    bool hasDataNeedDispatch)
{
    public IEnumerable<ProducerData<K, Message>> FailedProducerDatas { get; private set; } = failedProducerDatas;
    public IEnumerable<Exception> Exceptions { get; private set; } = exceptions;
    public List<Tuple<int, TopicAndPartition, ProducerResponseStatus>> FailedDetail { get; private set; } = failedDetail;
    public bool HasDataNeedDispatch { get; private set; } = hasDataNeedDispatch;
}