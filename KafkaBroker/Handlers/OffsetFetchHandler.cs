using KafkaBroker.Requests;
using KafkaBroker.Responses;
using Serilog;

namespace KafkaBroker.Handlers;

sealed class OffsetFetchHandler(ILogger logger, IOffsetStore offsetStore): IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<OffsetFetchHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        try
        {
            var req = ParseOffsetFetchRequest(reader);

            _logger.Debug(
                "OffsetFetch: corrId={CorrelationId}, group={Group}, topics={TopicCount}",
                header.CorrelationId, req.ConsumerGroup, req.Topics.Count
            );

            var resp = offsetStore.Fetch(req);

            WriteOffsetFetchResponseFrame(output, header.CorrelationId, resp);

            _logger.Debug(
                "OffsetFetch DONE: corrId={CorrelationId}, topics={TopicCount}",
                header.CorrelationId, resp.Topics.Count
            );
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "OffsetFetch failed: corrId={CorrelationId}", header.CorrelationId);

            var fail = new OffsetFetchResponse([]);
            WriteOffsetFetchResponseFrame(output, header.CorrelationId, fail);
            throw;
        }
    }
    
    private static OffsetFetchRequest ParseOffsetFetchRequest(KafkaBinaryReader reader)
    {
        var groupId = reader.ReadKafkaString();
        var topicCount = reader.ReadInt32Be();

        var topics = new List<OffsetFetchRequest.TopicData>(topicCount);
        for (int t = 0; t < topicCount; t++)
        {
            var topicName = reader.ReadKafkaString();
            var partCount = reader.ReadInt32Be();
            var parts = new List<int>(partCount);
            for (int i = 0; i < partCount; i++)
            {
                parts.Add(reader.ReadInt32Be());
            }
            topics.Add(new OffsetFetchRequest.TopicData(topicName, parts));
        }

        return new OffsetFetchRequest(groupId, topics);
    }

    private static void WriteOffsetFetchResponseFrame(Stream output, int correlationId, OffsetFetchResponse response)
    {
        using var bodyStream = new MemoryStream();
        var w = new KafkaBinaryWriter(bodyStream);

        w.WriteInt32Be(response.Topics.Count);
        foreach (var topic in response.Topics)
        {
            w.WriteKafkaString(topic.TopicName);
            w.WriteInt32Be(topic.Partitions.Count);

            foreach (var p in topic.Partitions)
            {
                w.WriteInt32Be(p.Partition);
                w.WriteInt64Be(p.Offset);
                w.WriteKafkaString(p.Metadata);
                w.WriteInt16Be(p.ErrorCode);
            }
        }

        var body = bodyStream.ToArray();

        using var frameStream = new MemoryStream(capacity: 4 + 4 + body.Length);
        var fw = new KafkaBinaryWriter(frameStream);
        fw.WriteInt32Be(4 + body.Length);
        fw.WriteInt32Be(correlationId);
        fw.WriteBytes(body);

        var frame = frameStream.ToArray();
        output.Write(frame, 0, frame.Length);
    }

}