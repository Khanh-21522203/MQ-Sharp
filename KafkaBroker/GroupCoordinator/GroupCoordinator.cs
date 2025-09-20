using System.Collections.Concurrent;

namespace KafkaBroker.GroupCoordinator;

sealed class GroupMember
{
    public string MemberId = ""; // do server phát sinh nếu client gửi rỗng
    public string ClientId = "";
    public DateTime LastHeartbeatUtc;
    public List<string> Subscriptions = new(); // topics
}

sealed class GroupState
{
    public string GroupId = "";
    public int GenerationId = 0;
    public string ProtocolName = "range"; // cố định
    public string LeaderMemberId = ""; // member leader
    public Dictionary<string, GroupMember> Members = new(); // memberId -> member
    public Dictionary<(string topic, int partition), string> Assignments = new(); // tp -> memberId
    public bool RebalanceInProgress = false;
}

sealed class OffsetsStore
{
    // lưu offsets commit theo group
    public readonly ConcurrentDictionary<(string group, string topic, int partition), long> Committed =
        new();
}

sealed class GroupCoordinator
{
    private readonly LogManager _logs; // để biết partitions của topic
    private readonly OffsetsStore _offsets;
    private readonly ConcurrentDictionary<string, GroupState> _groups = new();

    public GroupCoordinator(LogManager logs, OffsetsStore offsets)
    {
        _logs = logs;
        _offsets = offsets;
    }

    public GroupState GetOrCreate(string groupId)
        => _groups.GetOrAdd(groupId, gid => new GroupState { GroupId = gid });

    public string NewMemberId() => "member-" + Guid.NewGuid().ToString("N");

    public void MarkHeartbeat(string groupId, string memberId)
    {
        if (_groups.TryGetValue(groupId, out var g) && g.Members.TryGetValue(memberId, out var m))
            m.LastHeartbeatUtc = DateTime.UtcNow;
    }

    public void StartRebalance(GroupState g)
    {
        g.RebalanceInProgress = true;
        g.GenerationId++; // bump generation khi rebalance
        g.LeaderMemberId = ""; // sẽ set lại trong Join
        g.Assignments.Clear();
    }

    public void ComputeAssignment(GroupState g)
    {
        // Gộp subscriptions tất cả members → unique topics
        var topics = g.Members.Values.SelectMany(m => m.Subscriptions).Distinct().ToList();
        var tps = new List<(string topic, int partition)>();
        foreach (var t in topics)
        foreach (var p in _logs.GetPartitions(t))
            tps.Add((t, p));

        var members = g.Members.Keys.OrderBy(id => id).ToList();
        if (members.Count == 0) return;

        // Round-robin
        int i = 0;
        foreach (var tp in tps)
        {
            var owner = members[i % members.Count];
            g.Assignments[tp] = owner;
            i++;
        }

        g.RebalanceInProgress = false;
    }

    public void CommitOffsets(string groupId, IEnumerable<(string topic, int partition, long offset)> offsets)
    {
        foreach (var (t, p, o) in offsets)
            _offsets.Committed[(groupId, t, p)] = o;
    }

    public Dictionary<(string topic, int partition), long> FetchOffsets(string groupId,
        IEnumerable<(string topic, int partition)> tps)
    {
        var res = new Dictionary<(string, int), long>();
        foreach (var tp in tps)
            if (_offsets.Committed.TryGetValue((groupId, tp.topic, tp.partition), out var off))
                res[tp] = off;
        return res;
    }
}