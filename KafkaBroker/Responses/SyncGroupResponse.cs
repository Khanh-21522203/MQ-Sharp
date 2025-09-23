namespace KafkaBroker.Responses;

public sealed record SyncGroupResponse(
    short ErrorCode,
    ReadOnlyMemory<byte> MemberAssignment
);