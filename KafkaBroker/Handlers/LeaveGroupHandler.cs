namespace KafkaBroker.Handlers;

sealed class LeaveGroupHandler : IRequestHandler
{
    private readonly GroupCoordinator _coord;

    public LeaveGroupHandler(GroupCoordinator coord)
    {
        _coord = coord;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        string groupId = r.ReadKafkaString();
        string memberId = r.ReadKafkaString();
        var g = _coord.GetOrCreate(groupId);

        if (g.Members.Remove(memberId))
            _coord.StartRebalance(g); // thay đổi membership => bump generation

        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w => w.WriteInt16BE(ErrorCodes.None));
    }
}