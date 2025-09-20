namespace KafkaBroker.Requests;

public readonly record struct RequestHeader(
    short ApiKey,
    short ApiVersion,
    int CorrelationId,
    string ClientId
);