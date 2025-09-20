using System.Text;
using KafkaBroker.GroupCoordinator;

namespace KafkaBroker.Handlers;

sealed class JoinGroupHandler : IRequestHandler
{
    private readonly GroupCoordinator _coord;

    public JoinGroupHandler(GroupCoordinator coord)
    {
        _coord = coord;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        string groupId = r.ReadKafkaString();
        int sessionTimeoutMs = r.ReadInt32Be(); // bỏ qua
        string memberId = r.ReadKafkaString(); // "" hoặc member-xxx
        string protocolType = r.ReadKafkaString(); // "consumer"
        int subCount = r.ReadInt32Be();
        var subs = new List<string>(subCount);
        for (int i = 0; i < subCount; i++) subs.Add(r.ReadKafkaString());

        var g = _coord.GetOrCreate(groupId);

        if (string.IsNullOrEmpty(memberId) || !g.Members.ContainsKey(memberId))
            memberId = _coord.NewMemberId();

        if (!g.Members.TryGetValue(memberId, out var m))
        {
            m = new GroupMember { MemberId = memberId, ClientId = hdr.ClientId };
            g.Members[memberId] = m;
            _coord.StartRebalance(g); // thành viên thay đổi => bump generation
            if (string.IsNullOrEmpty(g.LeaderMemberId)) g.LeaderMemberId = memberId;
        }

        m.Subscriptions = subs;
        m.LastHeartbeatUtc = DateTime.UtcNow;

        // Trả JoinGroupResponse tối giản:
        // Error:int16, GenerationId:int32, GroupProtocol:string, LeaderId:string, MemberId:string, MembersCount:int32, [MemberId, MemberMetadata(bytes?)]
        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w =>
        {
            w.WriteInt16BE(g.RebalanceInProgress ? ErrorCodes.None : ErrorCodes.None);
            w.WriteInt32BE(g.GenerationId);
            w.WriteKafkaString(g.ProtocolName);
            w.WriteKafkaString(g.LeaderMemberId);
            w.WriteKafkaString(memberId);

            // Trả metadata các member (đơn giản: chỉ tên + subscriptions dưới dạng csv bytes)
            w.WriteInt32BE(g.Members.Count);
            foreach (var kv in g.Members)
            {
                w.WriteKafkaString(kv.Key); // memberId
                var md = Encoding.UTF8.GetBytes(string.Join(",", kv.Value.Subscriptions));
                w.WriteInt32BE(md.Length);
                w.WriteBytes(md);
            }
        });
    }
}