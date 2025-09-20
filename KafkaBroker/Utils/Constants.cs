namespace KafkaBroker.Utils;

public enum ApiKeys : short
{
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,

    FindCoordinator = 4,
    JoinGroup = 5,
    Heartbeat = 6,
    LeaveGroup = 7,
    SyncGroup = 8,

    OffsetCommit = 9,
    OffsetFetch = 10
}

public enum ErrorCodes : short
{
    None = 0,
    OffsetOutOfRange = 1,
    UnknownTopicOrPartition = 3,
    RequestTimedOut = 7
}