namespace KafkaBroker.Handlers;

sealed class HeartbeatHandler : IRequestHandler
{
    private readonly GroupCoordinator _coord;

    public HeartbeatHandler(GroupCoordinator coord)
    {
        _coord = coord;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        string groupId = r.ReadKafkaString();
        int generationId = r.ReadInt32Be();
        string memberId = r.ReadKafkaString();

        var g = _coord.GetOrCreate(groupId);
        if (!g.Members.ContainsKey(memberId) || generationId != g.GenerationId)
        {
            // Rebalance required (giản lược: trả lỗi bất kỳ để client join lại)
            ResponseWriter.WriteResponse(output, hdr.CorrelationId, w =>
            {
                w.WriteInt16BE(ErrorCodes.RequestTimedOut); // placeholder cho "rebalance"
            });
            return;
        }

        _coord.MarkHeartbeat(groupId, memberId);
        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w => w.WriteInt16BE(ErrorCodes.None));
    }
}