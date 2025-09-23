namespace KafkaBroker.Requests;

public sealed record JoinGroupRequest(
    string GroupId,
    int SessionTimeout,
    string MemberId,
    string ProtocolType,
    IReadOnlyList<JoinGroupRequest.JoinGroupProtocol> GroupProtocols)
{
    public sealed record JoinGroupProtocol(string ProtocolName, byte[] ProtocolMetadata);
}