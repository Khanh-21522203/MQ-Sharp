using MQ_Sharp.Request;
using MQ_Sharp.Response;

namespace MQ_Sharp;

public interface IKafkaConnection: IDisposable
{
    FetchResponse Send(FetchRequest request);
    ProduceResponse Send(ProduceRequest request);
    OffsetResponse Send(ListOffsetRequest request);
    IEnumerable<MetaDataResponse> Send(TopicMetadataRequest request);
}