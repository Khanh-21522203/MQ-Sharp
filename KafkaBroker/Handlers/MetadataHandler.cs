namespace KafkaBroker.Handlers;

public class MetadataHandler : IRequestHandler
{
    private readonly LogManager _logs;
    private readonly BrokerInfo _broker;

    public MetadataHandler(LogManager logs, BrokerInfo broker)
    {
        _logs = logs;
        _broker = broker;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        int topicCount = r.ReadInt32Be();
        var topics = new List<string>(topicCount);
        for (int i = 0; i < topicCount; i++) topics.Add(r.ReadKafkaString());
        if (topicCount == 0) topics = _logs.AllTopics().ToList();

        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w =>
        {
            // Brokers array
            w.WriteInt32BE(1); // broker count
            w.WriteInt32BE(_broker.NodeId);
            w.WriteKafkaString(_broker.Host);
            w.WriteInt32BE(_broker.Port);

            // Topics
            w.WriteInt32BE(topics.Count);
            foreach (var t in topics)
            {
                // Auto-create nếu chưa có
                _logs.EnsureTopic(t, partitions: 1);

                w.WriteInt16BE(ErrorCodes.None); // TopicErrorCode
                w.WriteKafkaString(t);

                var parts = _logs.GetPartitions(t);
                w.WriteInt32BE(parts.Length); // Partition array count
                foreach (var pid in parts)
                {
                    w.WriteInt16BE(ErrorCodes.None); // PartitionErrorCode
                    w.WriteInt32BE(pid); // PartitionId
                    w.WriteInt32BE(_broker.NodeId); // Leader
                    // Replicas
                    w.WriteInt32BE(1);
                    w.WriteInt32BE(_broker.NodeId);
                    // ISR
                    w.WriteInt32BE(1);
                    w.WriteInt32BE(_broker.NodeId);
                }
            }
        });
    }
}