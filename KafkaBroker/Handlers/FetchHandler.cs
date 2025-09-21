using KafkaBroker.LogStorage;
using KafkaBroker.LogStorage.Interface;
using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;
using Serilog;

namespace KafkaBroker.Handlers;

public class FetchHandler(ILogManager logManager, ILogger logger): IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<FetchHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        // 1) Parse FetchRequest
        var fetchRequest = ParseFetchRequest(reader);
        var topicResultList = new List<FetchResponse.TopicResult>(fetchRequest.Topics.Count);
    
        // 2) Xử lý từng topic/partition
        foreach (var topicRequest in fetchRequest.Topics)
        {
            var partitionResultList = new List<FetchResponse.PartitionResult>(topicRequest.Partitions.Count);
    
            foreach (var partitionRequest in topicRequest.Partitions)
            {
                try
                {
                    var tpKey = new TopicPartitionKey(topicRequest.TopicName, partitionRequest.Partition);
    
                    if (!logManager.TryGet(tpKey, out var partitionLog))
                    {
                        _logger.Warning(
                            "Fetch failed: Unknown topic or partition {Topic}-{Partition}",
                            topicRequest.TopicName, partitionRequest.Partition);
    
                        partitionResultList.Add(new FetchResponse.PartitionResult(
                            partitionRequest.Partition,
                            (short)ErrorCodes.UnknownTopicOrPartition,
                            -1,
                            0,
                            ReadOnlyMemory<byte>.Empty));
    
                        continue;
                    }
    
                    var messageSet = partitionLog
                        .ReadAsync(partitionRequest.FetchOffset, partitionRequest.MaxBytes)
                        .GetAwaiter().GetResult();
    
                    // TODO: lấy high watermark thực sự từ log (nếu có exposed)
                    long highWatermarkOffset = 0;
                    int messageSetSize = messageSet.Length;
    
                    partitionResultList.Add(new FetchResponse.PartitionResult(
                        partitionRequest.Partition,
                        (short)ErrorCodes.None,
                        highWatermarkOffset,
                        messageSetSize,
                        messageSet));
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "Fetch failed for {Topic}-{Partition}",
                        topicRequest.TopicName, partitionRequest.Partition);
    
                    partitionResultList.Add(new FetchResponse.PartitionResult(
                        partitionRequest.Partition,
                        (short)ErrorCodes.RequestTimedOut,
                        -1,
                        0,
                        ReadOnlyMemory<byte>.Empty));
                }
            }
    
            topicResultList.Add(new FetchResponse.TopicResult(topicRequest.TopicName, partitionResultList));
        }
    
        var fetchResponse = new FetchResponse(topicResultList);
        // 4) Serialize response frame
        WriteFetchResponseFrame(output, header.CorrelationId, fetchResponse);
    }

    
    private static FetchRequest ParseFetchRequest(KafkaBinaryReader reader)
    {
        var replicaId   = reader.ReadInt32Be();
        var maxWaitTime = reader.ReadInt32Be();
        var minBytes    = reader.ReadInt32Be();
    
        var topicCount = reader.ReadInt32Be();
        var topics = new List<FetchRequest.TopicData>(topicCount);
    
        for (var t = 0; t < topicCount; t++)
        {
            var topicName = reader.ReadKafkaString();
            var partCount = reader.ReadInt32Be();
            var partitions = new List<FetchRequest.PartitionData>(partCount);
    
            for (var p = 0; p < partCount; p++)
            {
                var partition   = reader.ReadInt32Be();
                var fetchOffset = reader.ReadInt64Be();
                var maxBytesForPart = reader.ReadInt32Be();
    
                partitions.Add(new FetchRequest.PartitionData(
                    Partition: partition,
                    FetchOffset: fetchOffset,
                    MaxBytes: maxBytesForPart
                ));
            }
    
            topics.Add(new FetchRequest.TopicData(
                TopicName: topicName,
                Partitions: partitions
            ));
        }
    
        return new FetchRequest(replicaId, maxWaitTime, minBytes, topics);
    }
    
    private static void WriteFetchResponseFrame(Stream output, int correlationId, FetchResponse response)
    {
        // Serialize body of FetchResponse
        using var bodyStream = new MemoryStream();
        var bodyWriter = new KafkaBinaryWriter(bodyStream);
    
        bodyWriter.WriteInt32Be(response.Topics.Count);
        foreach (var topic in response.Topics)
        {
            bodyWriter.WriteKafkaString(topic.TopicName);
            bodyWriter.WriteInt32Be(topic.Partitions.Count);
    
            foreach (var part in topic.Partitions)
            {
                bodyWriter.WriteInt32Be(part.Partition);
                bodyWriter.WriteInt16Be(part.ErrorCode);
                bodyWriter.WriteInt64Be(part.HighWatermarkOffset);
    
                // MessageSetSize + MessageSet bytes
                bodyWriter.WriteInt32Be(part.MessageSetSize);
                if (part.MessageSetSize > 0)
                {
                    bodyWriter.WriteBytes(part.MessageSet.ToArray());
                }
            }
        }
    
        var bodyBytes = bodyStream.ToArray();
    
        // frame = [length:int32][correlationId:int32][body...]
        using var frameStream = new MemoryStream(capacity: 4 + 4 + bodyBytes.Length);
        var frameWriter = new KafkaBinaryWriter(frameStream);
    
        frameWriter.WriteInt32Be(4 + bodyBytes.Length);   // length = sizeof(correlationId) + body
        frameWriter.WriteInt32Be(correlationId);          // header
        frameWriter.WriteBytes(bodyBytes);                 // body
    
        var frameBytes = frameStream.ToArray();
        output.Write(frameBytes, 0, frameBytes.Length);
    }
}