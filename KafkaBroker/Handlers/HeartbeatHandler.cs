using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;
using Serilog;

namespace KafkaBroker.Handlers;

public sealed class HeartbeatHandler(ILogger logger) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<HeartbeatHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        try
        {
            var request = ParseHeartbeatRequest(reader);

            _logger.Debug(
                "Heartbeat: corrId={CorrelationId}, group={Group}, member={Member}, gen={Gen}",
                header.CorrelationId, request.GroupId, request.MemberId, request.GenerationId
            );

            // Prototype: just validate existence; later can add session timeout mgmt
            var error = ValidateHeartbeat(request);

            var response = new HeartbeatResponse(error);
            WriteHeartbeatResponseFrame(output, header.CorrelationId, response);

            _logger.Debug(
                "Heartbeat response sent. corrId={CorrelationId}, error={ErrorCode}",
                header.CorrelationId, error
            );
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Heartbeat failed: corrId={CorrelationId}", header.CorrelationId);

            var fail = new HeartbeatResponse((short)KafkaErrorCode.Unknown);
            WriteHeartbeatResponseFrame(output, header.CorrelationId, fail);
            throw;
        }
    }

    private short ValidateHeartbeat(HeartbeatRequest req)
    {
        // Nếu chưa implement quản lý group chi tiết:
        // Chỉ kiểm tra groupId tồn tại trong GroupManager (nếu có hàm)
        // hoặc trả về NoError luôn.
        try
        {
            // TODO: gọi GroupManager.ValidateMember(req.GroupId, req.MemberId, req.GenerationId)
            return (short)KafkaErrorCode.NoError;
        }
        catch
        {
            return (short)KafkaErrorCode.UnknownMemberId;
        }
    }

    private static HeartbeatRequest ParseHeartbeatRequest(KafkaBinaryReader reader)
    {
        var groupId = reader.ReadKafkaString();
        var generationId = reader.ReadInt32Be();
        var memberId = reader.ReadKafkaString();

        return new HeartbeatRequest(groupId, generationId, memberId);
    }

    private static void WriteHeartbeatResponseFrame(Stream output, int correlationId, HeartbeatResponse response)
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