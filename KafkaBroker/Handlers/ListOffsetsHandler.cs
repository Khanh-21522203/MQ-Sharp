using KafkaBroker.LogStorage;
using KafkaBroker.LogStorage.Interface;
using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;
using Serilog;

namespace KafkaBroker.Handlers;

public class ListOffsetsHandler(ILogManager logManager, ILogger logger) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<ListOffsetsHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        // 1) Parse ListOffsetRequest
        var listReq = ParseListOffsetRequest(reader);

        var topicResultList = new List<ListOffsetResponse.TopicResult>(listReq.Topics.Count);

        // 2) Handle each topic/partition
        foreach (var topicReq in listReq.Topics)
        {
            var partitionOffsetsList = new List<ListOffsetResponse.PartitionOffsets>(topicReq.Partitions.Count);

            foreach (var partReq in topicReq.Partitions)
            {
                try
                {
                    var key = new TopicPartitionKey(topicReq.TopicName, partReq.Partition);

                    if (!logManager.TryGet(key, out var partitionLog))
                    {
                        _logger.Warning("ListOffset: unknown topic/partition {Topic}-{Partition}", topicReq.TopicName,
                            partReq.Partition);
                        partitionOffsetsList.Add(new ListOffsetResponse.PartitionOffsets(
                            partReq.Partition,
                            (short)ErrorCodes.UnknownTopicOrPartition,
                            []
                        ));
                        continue;
                    }

                    // ---- Resolve offsets by Time ----
                    // Legacy API: Time = -1 (latest), -2 (earliest), other = timestamp (ms)
                    long resolvedOffset;

                    if (partReq.Time == -1)
                    {
                        resolvedOffset = partitionLog is IOffsetIntrospect o ? o.GetLatestOffset() : 0L;
                    }
                    else if (partReq.Time == -2)
                    {
                        resolvedOffset = partitionLog is IOffsetIntrospect o2 ? o2.GetEarliestOffset() : 0L;
                    }
                    else
                    {
                        resolvedOffset = partitionLog is IOffsetIntrospect o3
                            ? o3.FindOffsetByTimestamp(partReq.Time)
                            : 0L;
                    }
                    
                    var offsetsToReturn = new List<long>(capacity: 1) { resolvedOffset };

                    partitionOffsetsList.Add(new ListOffsetResponse.PartitionOffsets(
                        partReq.Partition,
                        (short)ErrorCodes.None,
                        offsetsToReturn
                    ));
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "ListOffset failed for {Topic}-{Partition}", topicReq.TopicName,
                        partReq.Partition);
                    partitionOffsetsList.Add(new ListOffsetResponse.PartitionOffsets(
                        partReq.Partition,
                        (short)ErrorCodes.RequestTimedOut,
                        []
                    ));
                }
            }

            topicResultList.Add(new ListOffsetResponse.TopicResult(topicReq.TopicName, partitionOffsetsList));
        }

        var listResp = new ListOffsetResponse(topicResultList);

        // 3) Serialize frame = [length][correlationId][body]
        WriteListOffsetResponseFrame(output, header.CorrelationId, listResp);
    }


    private static ListOffsetRequest ParseListOffsetRequest(KafkaBinaryReader reader)
    {
        var replicaId = reader.ReadInt32Be();

        var topicCount = reader.ReadInt32Be();
        var topics = new List<ListOffsetRequest.TopicData>(topicCount);

        for (var t = 0; t < topicCount; t++)
        {
            var topicName = reader.ReadKafkaString();
            var partCount = reader.ReadInt32Be();
            var parts = new List<ListOffsetRequest.PartitionData>(partCount);

            for (var p = 0; p < partCount; p++)
            {
                var partition = reader.ReadInt32Be();
                var time = reader.ReadInt64Be();
                var maxNumberOfOffsets = reader.ReadInt32Be();

                parts.Add(new ListOffsetRequest.PartitionData(partition, time, maxNumberOfOffsets));
            }

            topics.Add(new ListOffsetRequest.TopicData(topicName, parts));
        }

        return new ListOffsetRequest(replicaId, topics);
    }

    private static void WriteListOffsetResponseFrame(Stream output, int correlationId, ListOffsetResponse response)
    {
        // Body serialize:
        using var bodyStream = new MemoryStream();
        var bodyWriter = new KafkaBinaryWriter(bodyStream);

        bodyWriter.WriteInt32Be(response.Topics.Count);
        foreach (var topic in response.Topics)
        {
            bodyWriter.WriteKafkaString(topic.TopicName);
            bodyWriter.WriteInt32Be(topic.Partitions.Count);

            foreach (var po in topic.Partitions)
            {
                bodyWriter.WriteInt32Be(po.Partition);
                bodyWriter.WriteInt16Be(po.ErrorCode);

                bodyWriter.WriteInt32Be(po.Offsets.Count);
                foreach (var off in po.Offsets)
                    bodyWriter.WriteInt64Be(off);
            }
        }

        var bodyBytes = bodyStream.ToArray();

        // Frame = [length:int32][correlationId:int32][body...]
        using var frameStream = new MemoryStream(capacity: 4 + 4 + bodyBytes.Length);
        var frameWriter = new KafkaBinaryWriter(frameStream);

        frameWriter.WriteInt32Be(4 + bodyBytes.Length); // length = sizeof(correlationId) + body
        frameWriter.WriteInt32Be(correlationId); // header
        frameWriter.WriteBytes(bodyBytes); // body

        var frameBytes = frameStream.ToArray();
        output.Write(frameBytes, 0, frameBytes.Length);
    }
}