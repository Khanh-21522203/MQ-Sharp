namespace KafkaBroker.Requests;

// TopicMetadataRequest => [TopicName]
public sealed record TopicMetadataRequest(IReadOnlyList<string> Topics);