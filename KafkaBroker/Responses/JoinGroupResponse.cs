namespace KafkaBroker.Responses;

public sealed record JoinGroupResponse(
    short ErrorCode,
    int GenerationId,
    string GroupProtocol,
    string LeaderId,
    string MemberId,
    IReadOnlyList<JoinGroupResponse.JoinGroupMember> Members)
{
    public sealed record JoinGroupMember(string MemberId, byte[] MemberMetadata);
}