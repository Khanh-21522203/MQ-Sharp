using KafkaBroker.Requests;
using KafkaBroker.Responses;
using Serilog;

namespace KafkaBroker.Handlers;

public sealed class OffsetCommitHandler(ILogger logger, IOffsetStore offsetStore) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<OffsetCommitHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        try
        {
            var req = ParseOffsetCommitRequest(reader);
            var resp = offsetStore.Commit(req);
            WriteOffsetCommitResponseFrame(output, header.CorrelationId, resp);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "OffsetCommit failed: corrId={CorrelationId}", header.CorrelationId);
            var fail = new OffsetCommitResponse([]);
            WriteOffsetCommitResponseFrame(output, header.CorrelationId, fail);
            throw;
        }
    }

    private static OffsetCommitRequest ParseOffsetCommitRequest(KafkaBinaryReader reader)
    {
        var consumerGroupId = reader.ReadKafkaString();
        var topicCount = reader.ReadInt32Be();
        var topics = new List<OffsetCommitRequest.TopicData>(topicCount);

        for (int t = 0; t < topicCount; t++)
        {
            var topicName = reader.ReadKafkaString();
            var partCount = reader.ReadInt32Be();
            var partitions = new List<OffsetCommitRequest.PartitionData>(partCount);

            for (int p = 0; p < partCount; p++)
            {
                var partition = reader.ReadInt32Be();
                var offset = reader.ReadInt64Be();
                var metadata = reader.ReadKafkaString();

                partitions.Add(new OffsetCommitRequest.PartitionData(partition, offset, metadata));
            }

            topics.Add(new OffsetCommitRequest.TopicData(topicName, partitions));
        }

        return new OffsetCommitRequest(consumerGroupId, topics);
    }

    private static void WriteOffsetCommitResponseFrame(Stream output, int correlationId, OffsetCommitResponse response)
    {
        using var bodyStream = new MemoryStream();
        var w = new KafkaBinaryWriter(bodyStream);

        w.WriteInt32Be(response.Topics.Count);
        foreach (var topic in response.Topics)
        {
            w.WriteKafkaString(topic.TopicName);
            w.WriteInt32Be(topic.Partitions.Count);

            foreach (var part in topic.Partitions)
            {
                w.WriteInt32Be(part.Partition);
                w.WriteInt16Be(part.ErrorCode);
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