using System.Text;
using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;
using Serilog;

namespace KafkaBroker.Handlers;

public sealed class SyncGroupHandler(ILogger logger, IGroupManager groupManager) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<SyncGroupHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        try
        {
            var req = ParseSyncGroupRequest(reader);

            _logger.Debug(
                "SyncGroup: corrId={CorrelationId}, group={Group}, gen={Gen}, member={Member}, assignItems={Count}",
                header.CorrelationId, req.GroupId, req.GenerationId, req.MemberId, req.GroupAssignment.Count
            );

            var resp = groupManager.SyncGroup(req);

            WriteSyncGroupResponseFrame(output, header.CorrelationId, resp);

            _logger.Debug(
                "SyncGroup DONE: corrId={CorrelationId}, error={Error}, assignmentBytes={Bytes}",
                header.CorrelationId, resp.ErrorCode, resp.MemberAssignment.Length
            );
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "SyncGroup failed: corrId={CorrelationId}", header.CorrelationId);

            var fail = new SyncGroupResponse((short)KafkaErrorCode.Unknown, ReadOnlyMemory<byte>.Empty);
            WriteSyncGroupResponseFrame(output, header.CorrelationId, fail);
            throw;
        }
    }

    private static SyncGroupRequest ParseSyncGroupRequest(KafkaBinaryReader reader)
    {
        var groupId = reader.ReadKafkaString();
        var generationId = reader.ReadInt32Be();
        var memberId = reader.ReadKafkaString();

        var count = reader.ReadInt32Be();
        var items = new List<SyncGroupRequest.Assignment>(count);
        for (int i = 0; i < count; i++)
        {
            var mid = reader.ReadKafkaString();
            var data = reader.ReadKafkaBytes() ?? []; // length-prefixed int32; -1 => null
            items.Add(new SyncGroupRequest.Assignment(mid, data));
        }

        return new SyncGroupRequest(groupId, generationId, memberId, items);
    }

    private static void WriteSyncGroupResponseFrame(Stream output, int correlationId, SyncGroupResponse response)
    {
        using var bodyStream = new MemoryStream();
        var w = new KafkaBinaryWriter(bodyStream);

        w.WriteInt16Be(response.ErrorCode);
        w.WriteKafkaBytes(response.MemberAssignment.ToArray()); // bytes: int32 length + payload (or -1)

        var body = bodyStream.ToArray();

        using var frameStream = new MemoryStream(capacity: 4 + 4 + body.Length);
        var fw = new KafkaBinaryWriter(frameStream);
        fw.WriteInt32Be(4 + body.Length); // length = sizeof(correlationId) + body
        fw.WriteInt32Be(correlationId);
        fw.WriteBytes(body);

        var frame = frameStream.ToArray();
        output.Write(frame, 0, frame.Length);
    }
}