using KafkaBroker.Requests;
using KafkaBroker.Utils;
using Serilog;
using KafkaBroker.LogStorage;
using KafkaBroker.Responses;

namespace KafkaBroker.Handlers;


public sealed class ProduceHandler(Broker broker, ILogManager logManager, ILogger logger) : IRequestHandler
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
                var key = new TopicPartitionKey(t.Topic, p.Partition);

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

            results.Add(new ProduceResponse.TopicResult(t.Topic, partResults));
        }

        // acks=0: not send response
        if (request.RequiredAcks == 0)
            return;

        var resp = new ProduceResponse(header.CorrelationId, results);
        WriteResponseFrame(output, header.CorrelationId, resp);
    }
    
    private static ProduceRequest ParseRequest(KafkaBinaryReader r)
    {
        var requiredAcks = r.ReadInt16Be();
        var timeoutMs = r.ReadInt32Be();

        var topicCount = r.ReadInt32Be();
        var topics = new List<ProduceRequest.TopicData>(topicCount);

        for (var t = 0; t < topicCount; t++)
        {
            var topic = r.ReadKafkaString();
            var partCount = r.ReadInt32Be();
            var parts = new List<ProduceRequest.PartitionData>(partCount);

            for (var p = 0; p < partCount; p++)
            {
                var partition = r.ReadInt32Be();
                var msgSetSize = r.ReadInt32Be();
                var msgSet = r.ReadBytesExact(msgSetSize);
                parts.Add(new ProduceRequest.PartitionData(partition, msgSet));
            }

            topics.Add(new ProduceRequest.TopicData(topic, parts));
        }

        return new ProduceRequest(requiredAcks, timeoutMs, topics);
    }

    private static void WriteResponseFrame(Stream output, int correlationId, ProduceResponse response)
    {
        using var bodyMs = new MemoryStream();
        var w = new KafkaBinaryWriter(bodyMs);

        // body:
        //   int32 topicCount
        //   [ topic(string), partitionCount(int32),
        //       [ partition(int32), errorCode(int16), baseOffset(int64) ]* ]*
        w.WriteInt32Be(response.Results.Count);
        foreach (var tr in response.Results)
        {
            w.WriteKafkaString(tr.Topic);
            w.WriteInt32Be(tr.Partitions.Count);
            foreach (var pr in tr.Partitions)
            {
                w.WriteInt32Be(pr.Partition);
                w.WriteInt16Be(pr.ErrorCode);
                w.WriteInt64Be(pr.BaseOffset);
            }
        }

        var body = bodyMs.ToArray();

        using var frameMs = new MemoryStream(capacity: 4 + 4 + body.Length);
        var fw = new KafkaBinaryWriter(frameMs);
        fw.WriteInt32Be(4 + body.Length);  // length = sizeof(corrId) + body
        fw.WriteInt32Be(correlationId);    // response header
        fw.WriteBytes(body);                // body

        var frame = frameMs.ToArray();
        output.Write(frame, 0, frame.Length);
    }
}
