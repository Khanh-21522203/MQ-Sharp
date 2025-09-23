using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;

namespace KafkaBroker;

public interface IGroupManager
{
    /// <summary>Returns the coordinator broker for the given consumer group.</summary>
    GroupCoordinatorResponse FindCoordinator(string groupId);

    /// <summary>
    /// Handles JoinGroup for a consumer group. 
    /// Supports only ProtocolType="consumer" and ProtocolName="range".
    /// </summary>
    JoinGroupResponse JoinGroup(JoinGroupRequest req);
}


public sealed class GroupManager(int nodeId, string host, int port) : IGroupManager
{
    private sealed class GroupState
    {
        public int GenerationId;
        public string? ProtocolType;
        public string? ChosenProtocol;
        public string? LeaderId;
        public readonly Dictionary<string, byte[]> Members = new(); // memberId -> last provided metadata (of chosen protocol)
    }

    private readonly Dictionary<string, GroupState> _groups = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();
    public GroupCoordinatorResponse FindCoordinator(string groupId)
    {
        return new GroupCoordinatorResponse(
            ErrorCode: (short)KafkaErrorCode.NoError,
            CoordinatorId: nodeId,
            CoordinatorHost: host,
            CoordinatorPort: port
        );
    }

    public JoinGroupResponse JoinGroup(JoinGroupRequest req)
    {
        if (string.IsNullOrEmpty(req.GroupId))
            return BuildErrorResponse(req.MemberId, KafkaErrorCode.InvalidGroupId);

        // Only support ProtocolType = "consumer"
        if (!string.Equals(req.ProtocolType, GroupProtocolType.Consumer.ToKafkaString(), StringComparison.Ordinal))
            return BuildErrorResponse(req.MemberId, KafkaErrorCode.InconsistentGroupProtocol);

        if (req.GroupProtocols.Count == 0)
            return BuildErrorResponse(req.MemberId, KafkaErrorCode.InconsistentGroupProtocol);

        // Only accept AssignmentStrategy = "range"
        var rangeName = AssignmentStrategy.Range.ToKafkaString();
        var rangeProtocol = req.GroupProtocols.FirstOrDefault(p =>
            string.Equals(p.ProtocolName, rangeName, StringComparison.Ordinal));
        if (rangeProtocol is null)
            return BuildErrorResponse(req.MemberId, KafkaErrorCode.InconsistentGroupProtocol);

        // Resolve memberId (new vs rejoin)
        var memberId = string.IsNullOrEmpty(req.MemberId)
            ? Guid.NewGuid().ToString("N")
            : req.MemberId;

        lock (_lock)
        {
            if (!_groups.TryGetValue(req.GroupId, out var state))
            {
                state = new GroupState
                {
                    GenerationId = 0,
                    LeaderId = memberId
                };
                _groups[req.GroupId] = state;
            }

            // Upsert caller's metadata for the chosen protocol ("range")
            state.Members[memberId] = rangeProtocol.ProtocolMetadata;

            // Elect leader if none
            if (string.IsNullOrEmpty(state.LeaderId))
                state.LeaderId = memberId;

            // Bump generation (prototype: bump per join)
            state.GenerationId++;

            // Build response
            if (memberId != state.LeaderId)
                return new JoinGroupResponse(
                    ErrorCode: (short)KafkaErrorCode.NoError,
                    GenerationId: state.GenerationId,
                    GroupProtocol: rangeName,
                    LeaderId: state.LeaderId!,
                    MemberId: memberId,
                    Members: []
                );
            var members = state.Members
                .Select(kv => new JoinGroupResponse.JoinGroupMember(kv.Key, kv.Value))
                .ToList();

            return new JoinGroupResponse(
                ErrorCode: (short)KafkaErrorCode.NoError,
                GenerationId: state.GenerationId,
                GroupProtocol: rangeName,
                LeaderId: state.LeaderId!,
                MemberId: memberId,
                Members: members
            );

        }
    }


    private static JoinGroupResponse BuildErrorResponse(string memberId, KafkaErrorCode code)
        => new(
            ErrorCode: (short)code,
            GenerationId: 0,
            GroupProtocol: string.Empty,
            LeaderId: string.Empty,
            MemberId: memberId ?? string.Empty,
            Members: []
        );
}
