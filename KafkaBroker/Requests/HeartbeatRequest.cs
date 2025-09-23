namespace KafkaBroker.Requests;

/// <summary>
/// Heartbeat request: sent periodically by members to keep session alive.
/// </summary>
/// <param name="GroupId">Group this member belongs to.</param>
/// <param name="GenerationId">Generation ID obtained from JoinGroupResponse.</param>
/// <param name="MemberId">Member ID obtained from JoinGroupResponse.</param>
public sealed record HeartbeatRequest(
    string GroupId,
    int GenerationId,
    string MemberId
);