namespace KafkaBroker.Requests;

public sealed record SyncGroupRequest(
    string GroupId,
    int GenerationId,
    string MemberId,
    IReadOnlyList<SyncGroupRequest.Assignment> GroupAssignment
)
{
    public sealed record Assignment(string MemberId, ReadOnlyMemory<byte> MemberAssignment);
}