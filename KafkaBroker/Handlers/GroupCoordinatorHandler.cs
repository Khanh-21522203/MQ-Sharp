using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;
using Serilog;

namespace KafkaBroker.Handlers;

public class GroupCoordinatorHandler(ILogger logger, IGroupManager groupManager) : IRequestHandler
{
    private readonly ILogger _logger = logger.ForContext<GroupCoordinatorHandler>();

    public void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output)
    {
        try
        {
            var request = ParseGroupCoordinatorRequest(reader);

            _logger.Debug(
                "Handling GroupCoordinator request. CorrelationId={CorrelationId}, GroupId={GroupId}",
                header.CorrelationId,
                string.IsNullOrEmpty(request.GroupId) ? "<EMPTY>" : request.GroupId
            );

            var response = groupManager.FindCoordinator(request.GroupId);

            WriteGroupCoordinatorResponseFrame(output, header.CorrelationId, response);

            _logger.Debug(
                "GroupCoordinator response sent. CorrelationId={CorrelationId}, CoordinatorId={CoordinatorId}",
                header.CorrelationId, response.CoordinatorId
            );
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Failed to handle GroupCoordinator request. CorrelationId={CorrelationId}",
                header.CorrelationId);

            // Best-effort fallback; upstream may close the connection anyway.
            var fail = new GroupCoordinatorResponse(
                ErrorCode: (short)KafkaErrorCode.Unknown,
                CoordinatorId: -1,
                CoordinatorHost: string.Empty,
                CoordinatorPort: -1
            );
            WriteGroupCoordinatorResponseFrame(output, header.CorrelationId, fail);
            throw;
        }
    }

    private static GroupCoordinatorRequest ParseGroupCoordinatorRequest(KafkaBinaryReader reader)
    {
        // Only one field in v0: GroupId
        var groupId = reader.ReadKafkaString();
        return new GroupCoordinatorRequest(groupId);
    }

    private static void WriteGroupCoordinatorResponseFrame(Stream output, int correlationId,
        GroupCoordinatorResponse response)
    {
        // ---- Serialize body ----
        using var bodyStream = new MemoryStream();
        var bodyWriter = new KafkaBinaryWriter(bodyStream);

        bodyWriter.WriteInt16Be(response.ErrorCode);
        bodyWriter.WriteInt32Be(response.CoordinatorId);
        bodyWriter.WriteKafkaString(response.CoordinatorHost);
        bodyWriter.WriteInt32Be(response.CoordinatorPort);

        var bodyBytes = bodyStream.ToArray();

        // ---- Frame: [length:int32][correlationId:int32][body...] ----
        using var frameStream = new MemoryStream(capacity: 4 + 4 + bodyBytes.Length);
        var frameWriter = new KafkaBinaryWriter(frameStream);

        frameWriter.WriteInt32Be(4 + bodyBytes.Length); // length = sizeof(correlationId) + body
        frameWriter.WriteInt32Be(correlationId); // header
        frameWriter.WriteBytes(bodyBytes); // body

        var frameBytes = frameStream.ToArray();
        output.Write(frameBytes, 0, frameBytes.Length);
    }
}