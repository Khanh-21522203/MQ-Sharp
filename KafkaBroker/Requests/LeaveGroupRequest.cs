namespace KafkaBroker.Requests;

public sealed record LeaveGroupRequest(string GroupId, string MemberId);