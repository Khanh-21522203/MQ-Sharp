using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;
using Serilog;

namespace KafkaBroker.Handlers;

public sealed class JoinGroupHandler(ILogger logger, IGroupManager groupManager) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<JoinGroupHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        try
        {
            var request = ParseJoinGroupRequest(reader);

            _logger.Debug(
                "JoinGroup: corrId={CorrelationId}, group={Group}, member={Member}, protoType={Type}, protos={Count}",
                header.CorrelationId, request.GroupId,
                string.IsNullOrEmpty(request.MemberId) ? "<EMPTY>" : request.MemberId,
                request.ProtocolType, request.GroupProtocols.Count);

            var response = groupManager.JoinGroup(request);

            WriteJoinGroupResponseFrame(output, header.CorrelationId, response);

            _logger.Debug("JoinGroup DONE: corrId={CorrelationId}, gen={Gen}, leader={Leader}, members={Count}",
                header.CorrelationId, response.GenerationId, response.LeaderId, response.Members.Count);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "JoinGroup failed: corrId={CorrelationId}", header.CorrelationId);

            var fail = new JoinGroupResponse(
                ErrorCode: (short)KafkaErrorCode.Unknown,
                GenerationId: 0,
                GroupProtocol: string.Empty,
                LeaderId: string.Empty,
                MemberId: string.Empty,
                Members: []
            );
            WriteJoinGroupResponseFrame(output, header.CorrelationId, fail);
            throw;
        }
    }

    private static JoinGroupRequest ParseJoinGroupRequest(KafkaBinaryReader reader)
    {
        var groupId = reader.ReadKafkaString();
        var sessionTimeout = reader.ReadInt32Be();

        var memberId = reader.ReadKafkaString();
        var protocolType = reader.ReadKafkaString();

        var protoCount = reader.ReadInt32Be();
        var protocols = new List<JoinGroupRequest.JoinGroupProtocol>(protoCount);
        for (var i = 0; i < protoCount; i++)
        {
            var name = reader.ReadKafkaString();
            var meta = reader.ReadKafkaBytes(); // length-prefixed bytes
            protocols.Add(new JoinGroupRequest.JoinGroupProtocol(name, meta ?? []));
        }

        return new JoinGroupRequest(
            GroupId: groupId,
            SessionTimeout: sessionTimeout,
            MemberId: memberId,
            ProtocolType: protocolType,
            GroupProtocols: protocols
        );
    }

    private static void WriteJoinGroupResponseFrame(Stream output, int correlationId, JoinGroupResponse resp)
    {
        using var bodyStream = new MemoryStream();
        var w = new KafkaBinaryWriter(bodyStream);

        w.WriteInt16Be(resp.ErrorCode);
        w.WriteInt32Be(resp.GenerationId);
        w.WriteKafkaString(resp.GroupProtocol);
        w.WriteKafkaString(resp.LeaderId);
        w.WriteKafkaString(resp.MemberId);

        w.WriteInt32Be(resp.Members?.Count ?? 0);
        if (resp.Members is not null)
        {
            foreach (var m in resp.Members)
            {
                w.WriteKafkaString(m.MemberId);
                w.WriteKafkaBytes(m.MemberMetadata);
            }
        }

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