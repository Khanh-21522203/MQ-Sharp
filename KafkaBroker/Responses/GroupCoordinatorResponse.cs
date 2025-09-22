namespace KafkaBroker.Responses;

public sealed record GroupCoordinatorResponse(
    short ErrorCode,
    int CoordinatorId,
    string CoordinatorHost,
    int CoordinatorPort
);