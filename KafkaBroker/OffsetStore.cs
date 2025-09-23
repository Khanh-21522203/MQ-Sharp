using KafkaBroker.Requests;
using KafkaBroker.Responses;
using KafkaBroker.Utils;

namespace KafkaBroker;

public interface IOffsetStore
{
    // Commit offsets for a group across topics/partitions.
    OffsetCommitResponse Commit(OffsetCommitRequest request);

    // (Optional) Fetch offsets to verify commits or implement OffsetFetch API
    OffsetFetchResponse Fetch(OffsetFetchRequest request);
}

public sealed class InMemoryOffsetStore : IOffsetStore
{
    // group -> topic -> partition -> (offset, metadata)
    private readonly Dictionary<string, Dictionary<string, Dictionary<int, (long offset, string meta)>>> _store
        = new(StringComparer.Ordinal);

    private readonly Lock _lock = new();

    public OffsetCommitResponse Commit(OffsetCommitRequest request)
    {
        var topicResults = new List<OffsetCommitResponse.TopicResult>(request.Topics.Count);

        lock (_lock)
        {
            if (!_store.TryGetValue(request.ConsumerGroupId, out var byTopic))
            {
                byTopic = new Dictionary<string, Dictionary<int, (long, string)>>(StringComparer.Ordinal);
                _store[request.ConsumerGroupId] = byTopic;
            }

            foreach (var t in request.Topics)
            {
                if (!byTopic.TryGetValue(t.TopicName, out var byPartition))
                {
                    byPartition = new Dictionary<int, (long, string)>();
                    byTopic[t.TopicName] = byPartition;
                }

                var parts = new List<OffsetCommitResponse.PartitionResult>(t.Partitions.Count);
                foreach (var p in t.Partitions)
                {
                    byPartition[p.Partition] = (p.Offset, p.Metadata ?? string.Empty);
                    parts.Add(new OffsetCommitResponse.PartitionResult(p.Partition, (short)KafkaErrorCode.NoError));
                }

                topicResults.Add(new OffsetCommitResponse.TopicResult(t.TopicName, parts));
            }
        }

        return new OffsetCommitResponse(topicResults);
    }

    public OffsetFetchResponse Fetch(OffsetFetchRequest request)
    {
        var topicResults = new List<OffsetFetchResponse.TopicResult>(request.Topics.Count);

        lock (_lock)
        {
            _store.TryGetValue(request.ConsumerGroup, out var byTopic);

            foreach (var topic in request.Topics)
            {
                var parts = new List<OffsetFetchResponse.PartitionResult>(topic.Partitions.Count);

                if (byTopic is not null && byTopic.TryGetValue(topic.TopicName, out var byPartition))
                {
                    // byPartition chắc chắn khác null tại đây
                    foreach (var part in topic.Partitions)
                    {
                        if (byPartition.TryGetValue(part, out var val))
                        {
                            parts.Add(new OffsetFetchResponse.PartitionResult(
                                Partition: part,
                                Offset: val.offset,
                                Metadata: val.meta,
                                ErrorCode: (short)KafkaErrorCode.NoError
                            ));
                        }
                        else
                        {
                            parts.Add(new OffsetFetchResponse.PartitionResult(
                                Partition: part,
                                Offset: -1,
                                Metadata: string.Empty,
                                ErrorCode: (short)KafkaErrorCode.NoError
                            ));
                        }
                    }
                }
                else
                {
                    // Không có topic trong store → trả tất cả partition = -1
                    foreach (var part in topic.Partitions)
                    {
                        parts.Add(new OffsetFetchResponse.PartitionResult(
                            Partition: part,
                            Offset: -1,
                            Metadata: string.Empty,
                            ErrorCode: (short)KafkaErrorCode.NoError
                        ));
                    }
                }

                topicResults.Add(new OffsetFetchResponse.TopicResult(topic.TopicName, parts));
            }
        }

        return new OffsetFetchResponse(topicResults);
    }
}