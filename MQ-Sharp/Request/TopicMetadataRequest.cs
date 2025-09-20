using MQ_Sharp.Serialization;
using MQ_Sharp.Utils;

namespace MQ_Sharp.Request;

public class TopicMetadataRequest(IEnumerable<string> topics)
{
    public IEnumerable<string> Topics { get; private set; } = new List<string>(topics);
}