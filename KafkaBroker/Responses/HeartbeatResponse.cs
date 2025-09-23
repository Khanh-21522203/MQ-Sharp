namespace KafkaBroker.Responses;

/// <summary>
/// Heartbeat response: contains only an error code.
/// </summary>
/// <param name="ErrorCode">0=NoError; non-zero if coordinator signals rejoin.</param>
public sealed record HeartbeatResponse(short ErrorCode);