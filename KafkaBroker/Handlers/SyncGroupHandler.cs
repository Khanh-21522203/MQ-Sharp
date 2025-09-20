using System.Text;

namespace KafkaBroker.Handlers;

sealed class SyncGroupHandler : IRequestHandler
{
    private readonly GroupCoordinator _coord;

    public SyncGroupHandler(GroupCoordinator coord)
    {
        _coord = coord;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        string groupId = r.ReadKafkaString();
        int generationId = r.ReadInt32Be();
        string memberId = r.ReadKafkaString();

        var g = _coord.GetOrCreate(groupId);

        // Nếu đang rebalance, tính assignment
        if (g.RebalanceInProgress) _coord.ComputeAssignment(g);

        // Lấy assignment cho member
        var assigned = g.Assignments.Where(kv => kv.Value == memberId)
            .Select(kv => kv.Key).ToList();

        // Response: Error:int16, Assignment(bytes)
        // Ta encode assignment thành chuỗi "topic:partition,..."
        var payload = Encoding.UTF8.GetBytes(string.Join(",",
            assigned.Select(tp => $"{tp.topic}:{tp.partition}")));

        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w =>
        {
            w.WriteInt16BE(ErrorCodes.None);
            w.WriteInt32BE(payload.Length);
            w.WriteBytes(payload);
        });
    }
}