using KafkaBroker.LogStorage;
using KafkaBroker.Requests;
using KafkaBroker.Responses;
using Serilog;

namespace KafkaBroker.Handlers;

public class TopicMetadataHandler(ILogger logger, ITopicMetadataManager topicMetadataManager) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<TopicMetadataHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        try
        {
            var request = ParseTopicMetadataRequest(reader);

            _logger.Debug(
                "Handling Metadata request. CorrelationId={CorrelationId}, Topics=[{Topics}]",
                header.CorrelationId,
                request.Topics is { Count: > 0 } ? string.Join(",", request.Topics) : "<ALL>"
            );

            var response = topicMetadataManager.GetMetadata(request.Topics);

            WriteMetadataResponseFrame(output, header.CorrelationId, response);

            _logger.Debug(
                "Metadata response sent. CorrelationId={CorrelationId}, Brokers={BrokerCount}, Topics={TopicCount}",
                header.CorrelationId, response.Brokers.Count, response.Topics.Count
            );
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Failed to handle Metadata request. CorrelationId={CorrelationId}", header.CorrelationId);

            try
            {
                var empty = new TopicMetadataResponse(
                    Brokers: new List<TopicMetadataResponse.Broker>(0),
                    Topics: new List<TopicMetadataResponse.TopicMetadata>(0)
                );
                WriteMetadataResponseFrame(output, header.CorrelationId, empty);
            }
            catch (Exception writeEx)
            {
                _logger.Error(writeEx, "Failed to write fallback Metadata response. CorrelationId={CorrelationId}",
                    header.CorrelationId);
                throw;
            }
        }
    }

    private static TopicMetadataRequest ParseTopicMetadataRequest(KafkaBinaryReader reader)
    {
        var topicCount = reader.ReadInt32Be();
        var topics = new List<string>(topicCount);

        for (var i = 0; i < topicCount; i++)
            topics.Add(reader.ReadKafkaString());

        return new TopicMetadataRequest(topics);
    }

    private static void WriteMetadataResponseFrame(Stream output, int correlationId, TopicMetadataResponse response)
    {
        // Body serialize: [Broker][TopicMetadata]
        using var bodyStream = new MemoryStream();
        var bodyWriter = new KafkaBinaryWriter(bodyStream);

        // [Broker]
        bodyWriter.WriteInt32Be(response.Brokers.Count);
        foreach (var broker in response.Brokers)
        {
            bodyWriter.WriteInt32Be(broker.NodeId);
            bodyWriter.WriteKafkaString(broker.Host);
            bodyWriter.WriteInt32Be(broker.Port);
        }

        // [TopicMetadata]
        bodyWriter.WriteInt32Be(response.Topics.Count);
        foreach (var topicMeta in response.Topics)
        {
            bodyWriter.WriteInt16Be(topicMeta.TopicTopicErrorCode);
            bodyWriter.WriteKafkaString(topicMeta.TopicName);

            bodyWriter.WriteInt32Be(topicMeta.Partitions.Count);
            foreach (var pm in topicMeta.Partitions)
            {
                bodyWriter.WriteInt16Be(pm.PartitionErrorCode);
                bodyWriter.WriteInt32Be(pm.PartitionId);
                bodyWriter.WriteInt32Be(pm.Leader);

                // Replicas => [int32]
                bodyWriter.WriteInt32Be(pm.Replicas.Count);
                foreach (var r in pm.Replicas)
                    bodyWriter.WriteInt32Be(r);

                // Isr => [int32]
                bodyWriter.WriteInt32Be(pm.Isr.Count);
                foreach (var isr in pm.Isr)
                    bodyWriter.WriteInt32Be(isr);
            }
        }

        var bodyBytes = bodyStream.ToArray();

        // Frame = [length:int32][correlationId:int32][body...]
        using var frameStream = new MemoryStream(capacity: 4 + 4 + bodyBytes.Length);
        var frameWriter = new KafkaBinaryWriter(frameStream);

        frameWriter.WriteInt32Be(4 + bodyBytes.Length); // length = sizeof(correlationId) + body
        frameWriter.WriteInt32Be(correlationId);
        frameWriter.WriteBytes(bodyBytes);

        var frameBytes = frameStream.ToArray();
        output.Write(frameBytes, 0, frameBytes.Length);
    }
}