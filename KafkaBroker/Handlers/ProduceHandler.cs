using KafkaBroker.Requests;
using KafkaBroker.Utils;
using Serilog;
using KafkaBroker.LogStorage;
using KafkaBroker.Responses;

namespace KafkaBroker.Handlers;


public sealed class ProduceHandler(ILogManager logManager, ILogger logger) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<ProduceHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        var request = ParseRequest(reader);
        var results = new List<ProduceResponse.TopicResult>(request.Topics.Count);

        foreach (var t in request.Topics)
        {
            var partResults = new List<ProduceResponse.PartitionResult>(t.Partitions.Count);

            foreach (var p in t.Partitions)
            {
                var key = new TopicPartitionKey(t.TopicName, p.Partition);

                try
                {
                    var logStore = logManager.GetOrCreate(key);
                    var baseOffset = logStore.AppendAsync(p.MessageSet).GetAwaiter().GetResult();
                    partResults.Add(new ProduceResponse.PartitionResult(p.Partition, (short)ErrorCodes.None, baseOffset));
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "Produce append failed for {Topic}-{Partition}", key.Topic, key.Partition);
                    partResults.Add(new ProduceResponse.PartitionResult(p.Partition, (short)ErrorCodes.RequestTimedOut, -1));
                }
            }

            results.Add(new ProduceResponse.TopicResult(t.TopicName, partResults));
        }

        // acks=0: not send response
        if (request.RequiredAcks == 0)
            return;

        var resp = new ProduceResponse(results);
        WriteResponseFrame(output, header.CorrelationId, resp);
    }
    
    private static ProduceRequest ParseRequest(KafkaBinaryReader reader)
    {
        var requiredAcks = reader.ReadInt16Be();
        var timeoutMs = reader.ReadInt32Be();

        var topicCount = reader.ReadInt32Be();
        var topics = new List<ProduceRequest.TopicData>(topicCount);

        for (var t = 0; t < topicCount; t++)
        {
            var topic = reader.ReadKafkaString();
            var partCount = reader.ReadInt32Be();
            var parts = new List<ProduceRequest.PartitionData>(partCount);

            for (var p = 0; p < partCount; p++)
            {
                var partition = reader.ReadInt32Be();
                var msgSetSize = reader.ReadInt32Be();
                var msgSet = reader.ReadBytesExact(msgSetSize);
                parts.Add(new ProduceRequest.PartitionData(partition, msgSet));
            }

            topics.Add(new ProduceRequest.TopicData(topic, parts));
        }

        return new ProduceRequest(requiredAcks, timeoutMs, topics);
    }

    private static void WriteResponseFrame(Stream output, int correlationId, ProduceResponse response)
    {
        // Serialize body of ProduceResponse
        using var bodyStream = new MemoryStream();
        var bodyWriter = new KafkaBinaryWriter(bodyStream);

        bodyWriter.WriteInt32Be(response.Results.Count);
        foreach (var topicResult in response.Results)
        {
            bodyWriter.WriteKafkaString(topicResult.TopicName);
            bodyWriter.WriteInt32Be(topicResult.Partitions.Count);
            foreach (var partitionResult in topicResult.Partitions)
            {
                bodyWriter.WriteInt32Be(partitionResult.Partition);
                bodyWriter.WriteInt16Be(partitionResult.ErrorCode);
                bodyWriter.WriteInt64Be(partitionResult.Offset);
            }
        }

        var bodyBytes = bodyStream.ToArray();

        // frame = [length][correlationId][body]
        using var frameStream = new MemoryStream(capacity: 4 + 4 + bodyBytes.Length);
        var frameWriter = new KafkaBinaryWriter(frameStream);

        frameWriter.WriteInt32Be(4 + bodyBytes.Length); // length = corrId + body
        frameWriter.WriteInt32Be(correlationId);        // header: correlationId
        frameWriter.WriteBytes(bodyBytes);              // body

        var frameBytes = frameStream.ToArray();
        output.Write(frameBytes, 0, frameBytes.Length);
    }
}
