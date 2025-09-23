using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;
using Serilog;

namespace KafkaBroker.Handlers;

sealed class LeaveGroupHandler(ILogger logger, IGroupManager groupManager) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<LeaveGroupHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        try
        {
            var req = ParseLeaveGroupRequest(reader);

            _logger.Debug(
                "LeaveGroup: corrId={CorrelationId}, group={GroupId}, member={MemberId}",
                header.CorrelationId, req.GroupId, req.MemberId
            );

            var resp = groupManager.LeaveGroup(req);

            WriteLeaveGroupResponseFrame(output, header.CorrelationId, resp);

            _logger.Debug(
                "LeaveGroup DONE: corrId={CorrelationId}, error={Error}",
                header.CorrelationId, resp.ErrorCode
            );
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "LeaveGroup failed: corrId={CorrelationId}", header.CorrelationId);
            var fail = new LeaveGroupResponse((short)KafkaErrorCode.Unknown);
            WriteLeaveGroupResponseFrame(output, header.CorrelationId, fail);
            throw;
        }
    }

    private static LeaveGroupRequest ParseLeaveGroupRequest(KafkaBinaryReader reader)
    {
        var groupId = reader.ReadKafkaString();
        var memberId = reader.ReadKafkaString();
        return new LeaveGroupRequest(groupId, memberId);
    }

    private static void WriteLeaveGroupResponseFrame(Stream output, int correlationId, LeaveGroupResponse response)
    {
        using var bodyStream = new MemoryStream();
        var w = new KafkaBinaryWriter(bodyStream);
        w.WriteInt16Be(response.ErrorCode);

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